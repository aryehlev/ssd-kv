"""Shared helpers for the ssd-kv black-box compatibility harness.

* ``TypedRESPParser`` -- a redis-py parser plug-in that keeps the RESP wire
  type of every reply (``+OK`` vs ``$2 OK``, ``$-1`` vs ``_``, raw integer and
  double text, ...).  Combined with ``execute_command`` and cleared response
  callbacks this gives byte-exact, type-exact replies.
* process helpers to launch a server from a shell command, wait for PING and
  kill the whole process group.
* small utilities: free ports, host:port parsing, redis-cli style quoting.

Reply representation (plain tuples so they compare / hash naturally)::

    ('+', b'OK')                 simple string
    ('-', b'ERR ...')            simple error         ('!', b'...') blob error
    (':', b'42')                 integer (raw text)   ('(', b'123') big number
    ('$', b'...') / ('$', None)  bulk string / RESP2 null bulk
    ('*', (r, ...)) / ('*', None) array / RESP2 null array
    ('_', None)                  RESP3 null
    (',', b'1.5')                RESP3 double (raw text)
    ('#', b't')                  RESP3 boolean
    ('=', b'txt:...')            RESP3 verbatim string
    ('%', ((k, v), ...))         RESP3 map            ('~', (r, ...)) set
    ('|', (((k, v), ...), r))    RESP3 attribute + the reply it decorates
    ('>', (r, ...))              RESP3 push
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import select
import shlex
import signal
import socket
import subprocess
import time
from typing import Optional

import redis
from redis._parsers.base import BaseParser
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import InvalidResponse

CRLF = b"\r\n"
_INT_RE = re.compile(rb"^-?[0-9]+$")
MAX_LINE = 1 << 20  # refuse header/simple lines longer than 1 MiB


# --------------------------------------------------------------------------
# RESP parsing
# --------------------------------------------------------------------------
class TypedRESPParser(BaseParser):
    """Type-preserving RESP2/RESP3 parser usable as redis-py ``parser_class``.

    Never raises for ``-ERR`` replies (they are returned as ``('-', msg)``), so
    ``execute_command`` returns errors as values.  Protocol violations raise
    ``InvalidResponse``.
    """

    def __init__(self, socket_read_size: int = 65536):
        self.socket_read_size = max(int(socket_read_size or 0), 65536)
        self._sock: Optional[socket.socket] = None
        self._buf = bytearray()
        self._pos = 0
        self.encoder = None

    # -- redis-py parser interface ------------------------------------
    def on_connect(self, connection):
        self._sock = connection._sock
        self._buf = bytearray()
        self._pos = 0
        self.encoder = connection.encoder

    def on_disconnect(self):
        self._sock = None
        self._buf = bytearray()
        self._pos = 0

    def can_read(self, timeout):
        if self._pos < len(self._buf):
            return True
        if self._sock is None:
            return False
        r, _, _ = select.select([self._sock], [], [], timeout or 0)
        return bool(r)

    def read_response(self, disable_decoding=False, push_request=False):
        if self._sock is None:
            raise RedisConnectionError("parser not connected")
        reply = self._read()
        # compact the buffer once a full top-level reply has been consumed
        if self._pos:
            del self._buf[: self._pos]
            self._pos = 0
        return reply

    # -- internals ----------------------------------------------------
    def _recv(self, want: int) -> None:
        chunk = self._sock.recv(min(max(want, self.socket_read_size), 4 << 20))
        if not chunk:
            raise RedisConnectionError("Connection closed by server.")
        self._buf += chunk

    def _readline(self) -> bytes:
        start = self._pos
        while True:
            idx = self._buf.find(CRLF, start)
            if idx >= 0:
                line = bytes(self._buf[self._pos : idx])
                self._pos = idx + 2
                return line
            if len(self._buf) - self._pos > MAX_LINE:
                raise InvalidResponse("Protocol error: line longer than 1 MiB without CRLF")
            start = max(self._pos, len(self._buf) - 1)
            self._recv(self.socket_read_size)

    def _readexact(self, n: int) -> bytes:
        need = n + 2
        while len(self._buf) - self._pos < need:
            self._recv(need - (len(self._buf) - self._pos))
        data = bytes(self._buf[self._pos : self._pos + n])
        if self._buf[self._pos + n : self._pos + need] != CRLF:
            raise InvalidResponse("Protocol error: bulk payload not terminated by CRLF")
        self._pos += need
        return data

    @staticmethod
    def _int(text: bytes, what: str) -> int:
        if not _INT_RE.match(text):
            raise InvalidResponse(f"Protocol error: bad {what} {text!r}")
        return int(text)

    def _read(self):
        line = self._readline()
        if not line:
            raise InvalidResponse("Protocol error: empty line")
        t = chr(line[0])
        rest = line[1:]
        if t in "+-":
            return (t, rest)
        if t == ":":
            self._int(rest, "integer")
            return (t, rest)
        if t == "(":
            self._int(rest, "big number")
            return (t, rest)
        if t in ",#":
            return (t, rest)
        if t == "_":
            if rest:
                raise InvalidResponse(f"Protocol error: bad null {line!r}")
            return ("_", None)
        if t in "$!=":
            n = self._int(rest, "length")
            if n == -1 and t == "$":
                return ("$", None)
            if n < 0:
                raise InvalidResponse(f"Protocol error: bad length {line!r}")
            return (t, self._readexact(n))
        if t in "*~>":
            n = self._int(rest, "count")
            if n == -1 and t == "*":
                return ("*", None)
            if n < 0:
                raise InvalidResponse(f"Protocol error: bad count {line!r}")
            return (t, tuple(self._read() for _ in range(n)))
        if t == "%":
            n = self._int(rest, "count")
            if n < 0:
                raise InvalidResponse(f"Protocol error: bad count {line!r}")
            return (t, tuple((self._read(), self._read()) for _ in range(n)))
        if t == "|":
            n = self._int(rest, "count")
            attrs = tuple((self._read(), self._read()) for _ in range(n))
            return ("|", (attrs, self._read()))
        raise InvalidResponse(f"Protocol error: unknown type byte {line[:40]!r}")


def make_typed_client(host: str, port: int, *, timeout: float = 60.0) -> redis.Redis:
    """A single-connection redis-py client returning typed raw replies.

    The connection is always opened in RESP2 (no implicit HELLO, no CLIENT
    SETINFO); callers switch to RESP3 by sending ``HELLO 3`` themselves so the
    handshake itself is visible / comparable.
    """
    pool = redis.ConnectionPool(
        host=host,
        port=port,
        parser_class=TypedRESPParser,
        socket_timeout=timeout,
        socket_connect_timeout=10,
        decode_responses=False,
        lib_name=None,
        lib_version=None,
        protocol=2,
    )
    client = redis.Redis(connection_pool=pool, single_connection_client=True)
    client.response_callbacks.clear()
    return client


def is_error(reply) -> bool:
    return isinstance(reply, tuple) and reply and reply[0] in ("-", "!")


# --------------------------------------------------------------------------
# Raw socket client (used by scenarios / persistence for protocol checks)
# --------------------------------------------------------------------------
def encode_command(*args) -> bytes:
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        elif isinstance(a, int):
            a = str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class RawConn:
    """Minimal RESP client on a plain socket using ``TypedRESPParser``."""

    def __init__(self, host: str, port: int, timeout: float = 30.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.parser = TypedRESPParser()

        class _C:  # shim for TypedRESPParser.on_connect
            _sock = self.sock
            encoder = None

        self.parser.on_connect(_C)

    def send_raw(self, data: bytes) -> None:
        self.sock.sendall(data)

    def send(self, *args) -> None:
        self.sock.sendall(encode_command(*args))

    def read(self):
        return self.parser.read_response()

    def call(self, *args):
        self.send(*args)
        return self.read()

    def close(self):
        with contextlib.suppress(OSError):
            self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# --------------------------------------------------------------------------
# Process / network helpers
# --------------------------------------------------------------------------
def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def port_is_free(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.2)
        return s.connect_ex((host, port)) != 0


def parse_hostport(text: str, default_host: str = "127.0.0.1") -> tuple[str, int]:
    text = text.strip()
    if text.startswith("["):  # [::1]:7777
        host, _, port = text[1:].partition("]:")
        return host, int(port)
    if ":" in text:
        host, port = text.rsplit(":", 1)
        return host or default_host, int(port)
    return default_host, int(text)


def ping_once(host: str, port: int, timeout: float = 1.0) -> bytes:
    """Send one PING on a fresh connection, return the raw first reply line."""
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        data = b""
        while not data.endswith(CRLF):
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        return data


class ServerProcess:
    """A server launched from a shell command in its own process group."""

    def __init__(self, cmd: str, *, cwd: Optional[str] = None, log_path: Optional[str] = None,
                 name: str = "server"):
        self.cmd = cmd
        self.name = name
        self.log_path = log_path
        self._log = open(log_path, "ab") if log_path else subprocess.DEVNULL
        self.proc = subprocess.Popen(
            cmd, shell=True, cwd=cwd, stdout=self._log, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, start_new_session=True,
        )

    @property
    def pid(self) -> int:
        return self.proc.pid

    def alive(self) -> bool:
        return self.proc.poll() is None

    def log_tail(self, n: int = 30) -> str:
        if not self.log_path or not os.path.exists(self.log_path):
            return "(no log)"
        with open(self.log_path, "rb") as f:
            lines = f.read().decode("utf-8", "replace").splitlines()
        return "\n".join(lines[-n:])

    def wait_ready(self, host: str, port: int, timeout: float = 30.0) -> None:
        """Wait until PING answers +PONG (``-LOADING`` keeps us waiting)."""
        deadline = time.monotonic() + timeout
        last = "no answer"
        while time.monotonic() < deadline:
            if not self.alive():
                raise RuntimeError(
                    f"{self.name} exited with code {self.proc.returncode} before answering PING\n"
                    f"command: {self.cmd}\n--- log tail ---\n{self.log_tail()}")
            try:
                line = ping_once(host, port)
                if line.startswith(b"+PONG"):
                    return
                last = repr(line)
            except OSError as e:
                last = str(e)
            time.sleep(0.05)
        raise RuntimeError(f"{self.name} did not answer PING within {timeout}s (last: {last})\n"
                           f"command: {self.cmd}\n--- log tail ---\n{self.log_tail()}")

    def kill(self, sig: int = signal.SIGKILL, timeout: float = 15.0) -> None:
        if self.proc.poll() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(self.proc.pid, sig)
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(self.proc.pid, signal.SIGKILL)
                self.proc.wait(timeout=timeout)
        else:
            # leader is gone; still sweep any children left in the group
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(self.proc.pid, signal.SIGKILL)
        if self._log is not subprocess.DEVNULL:
            with contextlib.suppress(Exception):
                self._log.close()


def start_reference_redis(redis_server: str, workdir: str, port: Optional[int] = None,
                          extra: str = "") -> tuple[ServerProcess, int]:
    port = port or free_port()
    cmd = (f"exec {shlex.quote(redis_server)} --port {port} --bind 127.0.0.1 "
           f"--save '' --appendonly no --daemonize no --dir {shlex.quote(workdir)} {extra}")
    proc = ServerProcess(cmd, cwd=workdir, log_path=os.path.join(workdir, "redis.log"),
                         name="reference redis-server")
    proc.wait_ready("127.0.0.1", port, timeout=20)
    return proc, port


def fill_template(template: str, **values) -> str:
    """Replace ``{name}`` placeholders without tripping over other braces."""
    out = template
    for k, v in values.items():
        out = out.replace("{" + k + "}", str(v))
    return out


# --------------------------------------------------------------------------
# Formatting
# --------------------------------------------------------------------------
def quote_arg(b: bytes, limit: int = 120) -> str:
    """redis-cli compatible double-quoted form (pasteable into redis-cli)."""
    if isinstance(b, str):
        b = b.encode()
    suffix = ""
    if len(b) > limit:
        suffix = f"...<{len(b)} bytes, sha1={hashlib.sha1(b).hexdigest()[:12]}>"
        b = b[:limit]
    out = []
    for c in b:
        ch = chr(c)
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif 32 <= c < 127:
            out.append(ch)
        else:
            out.append(f"\\x{c:02x}")
    return '"' + "".join(out) + '"' + suffix


_TYPE_NAMES = {
    "+": "simple", "-": "error", "!": "blob-error", ":": "integer", "(": "bignum",
    "$": "bulk", "*": "array", "_": "null", ",": "double", "#": "bool",
    "=": "verbatim", "%": "map", "~": "set", "|": "attribute", ">": "push",
}


def fmt_reply(r, indent: str = "", limit: int = 120, max_items: int = 50) -> str:
    if not isinstance(r, tuple) or len(r) != 2 or not isinstance(r[0], str):
        return indent + repr(r)
    t, v = r
    name = _TYPE_NAMES.get(t, t)
    if v is None:
        return indent + {"_": "(RESP3 null _)", "$": "(nil bulk $-1)", "*": "(nil array *-1)"}.get(t, f"({name} nil)")
    if t in "*~>":
        if not v:
            return f"{indent}({name}) (empty)"
        lines = [f"{indent}({name}) {len(v)} items:"]
        for i, item in enumerate(v[:max_items]):
            lines.append(fmt_reply(item, indent + f"  {i + 1}) ", limit, max_items))
        if len(v) > max_items:
            lines.append(f"{indent}  ... {len(v) - max_items} more")
        return "\n".join(lines)
    if t == "%":
        lines = [f"{indent}(map) {len(v)} pairs:"]
        for k, val in v[:max_items]:
            lines.append(fmt_reply(k, indent + "  key: ", limit) + "\n" + fmt_reply(val, indent + "    => ", limit))
        return "\n".join(lines)
    if t == "|":
        attrs, inner = v
        return (fmt_reply(("%", attrs), indent + "(attributes) ", limit) + "\n" +
                fmt_reply(inner, indent, limit))
    if t in ":(,#" and isinstance(v, (bytes, bytearray)):
        return f"{indent}({name}) {bytes(v).decode('ascii', 'replace')}"
    if isinstance(v, (bytes, bytearray)):
        return f"{indent}({name}) {quote_arg(bytes(v), limit)}"
    return f"{indent}({name}) {v!r}"
