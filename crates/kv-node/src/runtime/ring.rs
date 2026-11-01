//! io_uring wrapper (DESIGN §6.1, §6.4, §7.1, §13.6, §13.8).
//!
//! One ring per worker with `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
//! IORING_SETUP_COOP_TASKRUN`, SQ size 4096, CQ size 16384 (Linux >= 6.1). Operations: multishot
//! accept, single-shot recv into the connection's buffer, sendmsg with iovecs for shared value
//! bodies, `O_DIRECT` reads and writes (with `RWF_DSYNC` / `RWF_ATOMIC` where configured),
//! datasync flushes, the eventfd doorbell read, and a 1 ms timeout armed only while background
//! work is pending (timers drive background pacing only, never page writes). Optional fixed
//! buffers for page-cache frames.
//!
//! This is the only file of kv-node allowed to contain `unsafe` (§24); every block carries a
//! `SAFETY:` comment. Buffers handed to the kernel stay owned by the ring until their completion.
//!
//! Ownership model: every operation moves the memory the kernel reads or writes (receive
//! buffer, send segments with their iovec array and `msghdr`, disk buffer, doorbell word,
//! timespec) into the ring's in-flight table, keyed by the SQE's `user_data`; the completion
//! hands it back. Nothing the kernel can touch is ever borrowed from the caller, so no caller
//! can free or move it early. The small set of plain system calls kv-node needs (sockets,
//! eventfd, `prctl`) lives in [`sys`], each behind a safe, narrowly typed wrapper.
#![allow(unsafe_code)]

use std::io;
use std::os::fd::RawFd;

use io_uring::{IoUring, cqueue, opcode, squeue, types};
use kv_common::shared_buf::SharedBuf;
use kv_storage::io::{IoDone, IoOp, IoReq};

/// SQ entries.
pub const SQ_ENTRIES: u32 = 4096;
/// CQ entries.
pub const CQ_ENTRIES: u32 = 16384;

/// Most iovecs per sendmsg (Linux `UIO_MAXIOV`).
const MAX_IOV: usize = 1024;

/// One piece of a connection's output: plain bytes (from `start`), or a shared value body sent
/// without copying (§7.1).
pub enum OutSeg {
    Bytes(Vec<u8>, usize),
    Shared(SharedBuf, usize),
}

impl OutSeg {
    /// Bytes not yet sent.
    #[inline]
    pub fn remaining(&self) -> &[u8] {
        match self {
            OutSeg::Bytes(v, s) => &v[*s..],
            OutSeg::Shared(b, s) => &b.as_bytes()[*s..],
        }
    }

    #[inline]
    fn advance(&mut self, n: usize) {
        match self {
            OutSeg::Bytes(_, s) | OutSeg::Shared(_, s) => *s += n,
        }
    }
}

/// Consumes `n` sent bytes from the front of `segs`; returns fully sent byte buffers (for
/// reuse) through `recycle`.
pub fn advance_segments(segs: &mut Vec<OutSeg>, mut n: usize, recycle: &mut impl FnMut(Vec<u8>)) {
    let mut done = 0;
    for seg in segs.iter_mut() {
        let r = seg.remaining().len();
        if n >= r {
            n -= r;
            done += 1;
        } else {
            seg.advance(n);
            break;
        }
    }
    for seg in segs.drain(..done) {
        if let OutSeg::Bytes(v, _) = seg {
            recycle(v);
        }
    }
}

/// What a disk completion was.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DiskKind {
    Read,
    Write,
    Flush,
    Discard,
}

/// A finished operation.
pub enum Completion {
    /// One accepted socket (`res` = fd or `-errno`); `more` = the multishot accept stays armed.
    Accept { listener: u32, res: i32, more: bool },
    /// A receive into the buffer given to [`Ring::recv`] (returned as is); `res` = bytes
    /// written after the offset given there, or `-errno` (0 = end of stream).
    Recv { token: u64, res: i32, buf: Vec<u8> },
    /// A sendmsg of `segs` (not yet advanced); `res` = bytes sent or `-errno`.
    Send { token: u64, res: i32, segs: Vec<OutSeg> },
    /// A device request.
    Disk { kind: DiskKind, dev: u8, done: IoDone },
    /// The doorbell eventfd was signalled (the read is not re-armed automatically).
    Doorbell,
    /// The armed timeout expired.
    Timer,
}

/// Memory owned by an in-flight operation.
enum Op {
    Accept {
        listener: u32,
    },
    Recv {
        token: u64,
        buf: Vec<u8>,
    },
    Send {
        token: u64,
        segs: Vec<OutSeg>,
        // the kernel reads these while the op is in flight
        _iov: Box<[libc::iovec]>,
        _msg: Box<libc::msghdr>,
    },
    Disk {
        kind: DiskKind,
        req: IoReq,
    },
    Doorbell {
        _word: Box<[u8; 8]>,
    },
    Timer {
        _ts: Box<types::Timespec>,
    },
}

pub struct Ring {
    ring: IoUring,
    ops: Vec<Option<Op>>,
    free: Vec<u32>,
    inflight: usize,
    timer_armed: bool,
    doorbell_armed: bool,
}

impl Ring {
    pub fn new() -> io::Result<Self> {
        let full = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .setup_taskrun_flag()
            .setup_cqsize(CQ_ENTRIES)
            .build(SQ_ENTRIES);
        let ring = match full {
            Ok(r) => r,
            // older kernels: fewer setup flags, same semantics (completions are reaped by the
            // same thread in the same places)
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => IoUring::builder()
                .setup_coop_taskrun()
                .setup_taskrun_flag()
                .setup_cqsize(CQ_ENTRIES)
                .build(SQ_ENTRIES)
                .or_else(|_| IoUring::builder().setup_cqsize(CQ_ENTRIES).build(SQ_ENTRIES))?,
            Err(e) => return Err(e),
        };
        Ok(Ring { ring, ops: Vec::new(), free: Vec::new(), inflight: 0, timer_armed: false, doorbell_armed: false })
    }

    /// Operations in flight.
    #[cfg(test)]
    pub fn inflight(&self) -> usize {
        self.inflight
    }

    fn slot(&mut self, op: Op) -> u64 {
        self.inflight += 1;
        match self.free.pop() {
            Some(i) => {
                self.ops[i as usize] = Some(op);
                u64::from(i)
            }
            None => {
                self.ops.push(Some(op));
                (self.ops.len() - 1) as u64
            }
        }
    }

    fn push(&mut self, e: squeue::Entry) -> io::Result<()> {
        loop {
            // SAFETY: every entry references memory owned by `self.ops` (moved in by `slot`
            // before the entry is built) or plain values copied into the SQE; that memory stays
            // in the table until the entry's final completion is reaped in `reap`.
            let pushed = unsafe { self.ring.submission().push(&e) };
            if pushed.is_ok() {
                return Ok(());
            }
            // SQ full: hand the queued entries to the kernel and retry
            match self.ring.submit() {
                Ok(_) => {}
                Err(err) if err.raw_os_error() == Some(libc::EBUSY) || err.raw_os_error() == Some(libc::EINTR) => {}
                Err(err) => return Err(err),
            }
        }
    }

    /// Arms a multishot accept on `fd`; completions carry `listener`.
    pub fn accept(&mut self, fd: RawFd, listener: u32) -> io::Result<()> {
        let ud = self.slot(Op::Accept { listener });
        let e = opcode::AcceptMulti::new(types::Fd(fd)).flags(libc::SOCK_CLOEXEC).build().user_data(ud);
        self.push(e)
    }

    /// Receives into `buf[at..]` (`at < buf.len()`; the buffer is fully initialized, so no
    /// length bookkeeping is needed when the kernel fills part of it).
    pub fn recv(&mut self, fd: RawFd, token: u64, mut buf: Vec<u8>, at: usize) -> io::Result<()> {
        assert!(at < buf.len(), "recv needs room after the received bytes");
        let len = u32::try_from(buf.len() - at).unwrap_or(u32::MAX);
        // SAFETY: `at < buf.len()`, so the pointer and the `len` bytes after it are inside the
        // live, initialized buffer; the Vec moves into the in-flight table below, so its heap
        // buffer stays put until the completion.
        let ptr = unsafe { buf.as_mut_ptr().add(at) };
        let ud = self.slot(Op::Recv { token, buf });
        self.push(opcode::Recv::new(types::Fd(fd), ptr, len).build().user_data(ud))
    }

    /// Sends `segs` with one sendmsg (at most 1024 iovecs; the rest stays for the next send).
    pub fn send(&mut self, fd: RawFd, token: u64, segs: Vec<OutSeg>) -> io::Result<()> {
        let iov: Box<[libc::iovec]> = segs
            .iter()
            .map(OutSeg::remaining)
            .filter(|r| !r.is_empty())
            .take(MAX_IOV)
            .map(|r| libc::iovec { iov_base: r.as_ptr() as *mut libc::c_void, iov_len: r.len() })
            .collect();
        // SAFETY: an all-zero msghdr is a valid "no address, no control data" header.
        let mut msg: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
        msg.msg_iov = iov.as_ptr() as *mut libc::iovec;
        msg.msg_iovlen = iov.len();
        let msg_ptr: *const libc::msghdr = &*msg;
        // the iovecs point into `segs`' heap buffers (Vec and Arc contents do not move when the
        // handles move), and both boxes move into the table with them
        let ud = self.slot(Op::Send { token, segs, _iov: iov, _msg: msg });
        self.push(opcode::SendMsg::new(types::Fd(fd), msg_ptr).flags(libc::MSG_NOSIGNAL as u32).build().user_data(ud))
    }

    /// Submits a storage request on device `dev` (open as `fd`).
    pub fn disk(&mut self, fd: RawFd, dev: u8, mut req: IoReq) -> io::Result<()> {
        let (kind, valid) = match &req.op {
            IoOp::Read { buf } => (DiskKind::Read, req.len as usize <= buf.len()),
            IoOp::Write { buf, buf_off, .. } => (DiskKind::Write, (*buf_off as usize).saturating_add(req.len as usize) <= buf.len()),
            IoOp::Flush => (DiskKind::Flush, true),
            IoOp::Discard => (DiskKind::Discard, true),
        };
        if !valid {
            return self.fail_disk(kind, req, libc::EINVAL);
        }
        if kind == DiskKind::Discard {
            // No discard opcode is used: the request completes at once as unsupported, which
            // the engine treats like a device without discard (§20.2 `discard` is advisory).
            return self.fail_disk(kind, req, libc::EOPNOTSUPP);
        }
        let fd = types::Fd(fd);
        let entry = match &mut req.op {
            IoOp::Read { buf } => opcode::Read::new(fd, buf.as_mut_ptr(), req.len).offset(req.offset).build(),
            IoOp::Write { buf, buf_off, dsync, atomic } => {
                let mut fl = 0;
                if *dsync {
                    fl |= libc::RWF_DSYNC;
                }
                if *atomic {
                    fl |= libc::RWF_ATOMIC;
                }
                // SAFETY: `buf_off + len <= buf.len()` was checked above, so the span is inside
                // the buffer, which moves into the in-flight table with `req` (moving the
                // request does not move the buffer's heap allocation).
                let ptr = unsafe { buf.as_ptr().add(*buf_off as usize) };
                opcode::Write::new(fd, ptr, req.len).offset(req.offset).rw_flags(fl).build()
            }
            // (a discard completed above)
            IoOp::Flush | IoOp::Discard => opcode::Fsync::new(fd).flags(types::FsyncFlags::DATASYNC).build(),
        };
        let _ = dev;
        let ud = self.slot(Op::Disk { kind, req });
        self.push(entry.user_data(ud))
    }

    /// Completes a request at once with `-errno` (through a NOP, so it is reaped in order).
    fn fail_disk(&mut self, kind: DiskKind, req: IoReq, errno: i32) -> io::Result<()> {
        let ud = self.slot(Op::Disk { kind, req });
        // the NOP completes with 0; `reap` turns a NOP disk completion into `-errno`
        self.push(opcode::Nop::new().build().user_data(ud | (u64::from(errno as u32) << 32)))
    }

    /// Arms the doorbell read on `eventfd` (no-op when armed).
    pub fn arm_doorbell(&mut self, eventfd: RawFd) -> io::Result<()> {
        if self.doorbell_armed {
            return Ok(());
        }
        let mut word = Box::new([0u8; 8]);
        let ptr = word.as_mut_ptr();
        let ud = self.slot(Op::Doorbell { _word: word });
        self.doorbell_armed = true;
        self.push(opcode::Read::new(types::Fd(eventfd), ptr, 8).build().user_data(ud))
    }

    /// Arms a one-shot timeout of `ns` nanoseconds (no-op when one is armed).
    pub fn arm_timer(&mut self, ns: u32) -> io::Result<()> {
        if self.timer_armed {
            return Ok(());
        }
        let ts = Box::new(types::Timespec::new().nsec(ns));
        let ptr: *const types::Timespec = &*ts;
        let ud = self.slot(Op::Timer { _ts: ts });
        self.timer_armed = true;
        self.push(opcode::Timeout::new(ptr).build().user_data(ud))
    }

    #[cfg(test)]
    pub fn timer_armed(&self) -> bool {
        self.timer_armed
    }

    /// Submits queued entries and waits for at least `wait` completions.
    pub fn submit_and_wait(&mut self, wait: usize) -> std::io::Result<usize> {
        match self.ring.submitter().submit_and_wait(wait) {
            Ok(n) => Ok(n),
            Err(e) if matches!(e.raw_os_error(), Some(libc::EINTR) | Some(libc::EBUSY) | Some(libc::ETIME)) => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Moves every available completion into `out`.
    pub fn reap(&mut self, out: &mut Vec<Completion>) {
        let mut cq = self.ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let ud = cqe.user_data();
            let idx = (ud & 0xffff_ffff) as usize;
            let forced_errno = (ud >> 32) as i32;
            let res = cqe.result();
            let more = cqueue::more(cqe.flags());
            let Some(slot) = self.ops.get_mut(idx) else { continue };
            // multishot accepts keep their slot while armed
            if let (Some(Op::Accept { listener }), true) = (slot.as_ref(), more) {
                out.push(Completion::Accept { listener: *listener, res, more: true });
                continue;
            }
            let Some(op) = slot.take() else { continue };
            self.free.push(idx as u32);
            self.inflight -= 1;
            match op {
                Op::Accept { listener } => out.push(Completion::Accept { listener, res, more: false }),
                Op::Recv { token, buf } => out.push(Completion::Recv { token, res, buf }),
                Op::Send { token, segs, .. } => out.push(Completion::Send { token, res, segs }),
                Op::Disk { kind, req } => {
                    let (tag, dev, len) = (req.tag, req.dev, req.len);
                    let res = if forced_errno != 0 {
                        Err(-forced_errno)
                    } else if res < 0 {
                        Err(res)
                    } else if matches!(kind, DiskKind::Flush) || res as u32 == len {
                        Ok(res as u32)
                    } else {
                        // a short O_DIRECT transfer is an I/O error for the engine
                        Err(-libc::EIO)
                    };
                    out.push(Completion::Disk { kind, dev, done: IoDone { tag, res, buf: req.into_buf() } });
                }
                Op::Doorbell { .. } => {
                    self.doorbell_armed = false;
                    out.push(Completion::Doorbell);
                }
                Op::Timer { .. } => {
                    self.timer_armed = false;
                    out.push(Completion::Timer);
                }
            }
        }
    }
}

/// Plain system calls behind safe wrappers (sockets, eventfd, process flags).
pub mod sys {
    use std::io;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
    use std::os::fd::RawFd;

    fn cvt(r: libc::c_int) -> io::Result<libc::c_int> {
        if r < 0 { Err(io::Error::last_os_error()) } else { Ok(r) }
    }

    /// A non-blocking, close-on-exec eventfd (the worker doorbell).
    pub fn eventfd() -> io::Result<RawFd> {
        // SAFETY: plain syscall with constant flags; returns a new fd or -1.
        cvt(unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) })
    }

    /// Adds 1 to an eventfd counter (rings a doorbell). Errors are ignored: a full counter
    /// already means "wake up".
    pub fn eventfd_signal(fd: RawFd) {
        let one: u64 = 1;
        // SAFETY: writes 8 bytes from a live local to `fd`; an invalid fd only yields EBADF.
        let _ = unsafe { libc::write(fd, (&one as *const u64).cast(), 8) };
    }

    /// Closes `fd` (negative values are ignored).
    pub fn close(fd: RawFd) {
        if fd >= 0 {
            // SAFETY: the caller owns `fd` and does not use it afterwards.
            unsafe { libc::close(fd) };
        }
    }

    /// `shutdown(fd, SHUT_RDWR)`: in-flight receives and sends on the socket complete.
    pub fn shutdown(fd: RawFd) {
        // SAFETY: plain syscall on an fd the caller owns.
        unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
    }

    /// `prctl(PR_SET_DUMPABLE, enabled)` (core dumps, LC-32).
    pub fn set_dumpable(enabled: bool) -> io::Result<()> {
        // SAFETY: PR_SET_DUMPABLE takes one integer argument.
        cvt(unsafe { libc::prctl(libc::PR_SET_DUMPABLE, libc::c_ulong::from(enabled), 0, 0, 0) }).map(|_| ())
    }

    fn setsockopt_int(fd: RawFd, level: libc::c_int, name: libc::c_int, v: libc::c_int) -> io::Result<()> {
        // SAFETY: passes a pointer to a live c_int and its exact size.
        cvt(unsafe {
            libc::setsockopt(fd, level, name, (&v as *const libc::c_int).cast(), std::mem::size_of::<libc::c_int>() as libc::socklen_t)
        })
        .map(|_| ())
    }

    fn to_sockaddr(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
        // SAFETY: an all-zero sockaddr_storage is a valid (AF_UNSPEC) value.
        let mut st: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let len = match addr {
            SocketAddr::V4(a) => {
                let sin = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: a.port().to_be(),
                    sin_addr: libc::in_addr { s_addr: u32::from_ne_bytes(a.ip().octets()) },
                    sin_zero: [0; 8],
                };
                // SAFETY: sockaddr_storage is large and aligned enough for any sockaddr type.
                unsafe { std::ptr::write((&mut st as *mut libc::sockaddr_storage).cast(), sin) };
                std::mem::size_of::<libc::sockaddr_in>()
            }
            SocketAddr::V6(a) => {
                let sin6 = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: a.port().to_be(),
                    sin6_flowinfo: a.flowinfo(),
                    sin6_addr: libc::in6_addr { s6_addr: a.ip().octets() },
                    sin6_scope_id: a.scope_id(),
                };
                // SAFETY: as above.
                unsafe { std::ptr::write((&mut st as *mut libc::sockaddr_storage).cast(), sin6) };
                std::mem::size_of::<libc::sockaddr_in6>()
            }
        };
        (st, len as libc::socklen_t)
    }

    fn from_sockaddr(st: &libc::sockaddr_storage) -> Option<SocketAddr> {
        match libc::c_int::from(st.ss_family) {
            libc::AF_INET => {
                // SAFETY: the family says the storage holds a sockaddr_in.
                let a: libc::sockaddr_in = unsafe { std::ptr::read((st as *const libc::sockaddr_storage).cast()) };
                Some(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(a.sin_addr.s_addr.to_ne_bytes()), u16::from_be(a.sin_port))))
            }
            libc::AF_INET6 => {
                // SAFETY: the family says the storage holds a sockaddr_in6.
                let a: libc::sockaddr_in6 = unsafe { std::ptr::read((st as *const libc::sockaddr_storage).cast()) };
                Some(SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(a.sin6_addr.s6_addr),
                    u16::from_be(a.sin6_port),
                    a.sin6_flowinfo,
                    a.sin6_scope_id,
                )))
            }
            _ => None,
        }
    }

    /// A listening TCP socket bound with `SO_REUSEADDR` and `SO_REUSEPORT` (one per worker,
    /// §7.1); IPv6 sockets are v6-only so `bind 0.0.0.0 ::` works.
    pub fn tcp_listener(addr: SocketAddr, backlog: u32) -> io::Result<RawFd> {
        let family = if addr.is_ipv4() { libc::AF_INET } else { libc::AF_INET6 };
        // SAFETY: plain syscall; returns a new fd or -1.
        let fd = cvt(unsafe { libc::socket(family, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) })?;
        let r = (|| {
            setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 1)?;
            setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, 1)?;
            if addr.is_ipv6() {
                setsockopt_int(fd, libc::IPPROTO_IPV6, libc::IPV6_V6ONLY, 1)?;
            }
            let (st, len) = to_sockaddr(&addr);
            // SAFETY: `st` holds a valid sockaddr of `len` bytes.
            cvt(unsafe { libc::bind(fd, (&st as *const libc::sockaddr_storage).cast(), len) })?;
            let backlog = libc::c_int::try_from(backlog).unwrap_or(libc::c_int::MAX);
            // SAFETY: plain syscall on the socket created above.
            cvt(unsafe { libc::listen(fd, backlog) })?;
            Ok(())
        })();
        match r {
            Ok(()) => Ok(fd),
            Err(e) => {
                close(fd);
                Err(e)
            }
        }
    }

    /// `TCP_NODELAY` and `SO_KEEPALIVE` with `TCP_KEEPIDLE` = `keepalive_s` (0 = no keepalive).
    pub fn tune_accepted(fd: RawFd, keepalive_s: u32) {
        let _ = setsockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1);
        if keepalive_s > 0 {
            let ka = libc::c_int::try_from(keepalive_s).unwrap_or(libc::c_int::MAX);
            let _ = setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1);
            let _ = setsockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, ka);
            let _ = setsockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, (ka / 3).max(1));
            let _ = setsockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_KEEPCNT, 3);
        }
    }

    fn name_of(fd: RawFd, peer: bool) -> Option<SocketAddr> {
        // SAFETY: zeroed storage is valid; the syscall writes at most `len` bytes into it.
        let mut st: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let p = (&mut st as *mut libc::sockaddr_storage).cast();
        // SAFETY: `p` points to `len` writable bytes.
        let r = unsafe { if peer { libc::getpeername(fd, p, &mut len) } else { libc::getsockname(fd, p, &mut len) } };
        if r < 0 { None } else { from_sockaddr(&st) }
    }

    /// The remote address of a connected socket.
    pub fn peer_addr(fd: RawFd) -> Option<SocketAddr> {
        name_of(fd, true)
    }

    /// The local address of a socket.
    pub fn local_addr(fd: RawFd) -> Option<SocketAddr> {
        name_of(fd, false)
    }

    /// Best-effort non-blocking send of a short message (the `maxclients` refusal, §6.7).
    pub fn send_now(fd: RawFd, bytes: &[u8]) {
        // SAFETY: reads `bytes.len()` bytes from a live slice.
        let _ = unsafe { libc::send(fd, bytes.as_ptr().cast(), bytes.len(), libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_storage::io::{AlignedBuf, IoTag};
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;

    fn wait_for(ring: &mut Ring, out: &mut Vec<Completion>, pred: impl Fn(&Completion) -> bool) -> Completion {
        for _ in 0..1000 {
            ring.submit_and_wait(1).unwrap();
            ring.reap(out);
            if let Some(i) = out.iter().position(&pred) {
                return out.remove(i);
            }
        }
        panic!("completion did not arrive");
    }

    #[test]
    fn disk_write_read_flush_round_trip() {
        let mut ring = Ring::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let f = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.path().join("d")).unwrap();
        let fd = f.as_raw_fd();
        let tag = IoTag { partition: 3, id: 9 };
        let mut buf = AlignedBuf::zeroed(8192);
        buf.as_mut_slice()[4096..4100].copy_from_slice(b"page");
        ring.disk(fd, 0, IoReq::write_span(tag, 0, 4096, buf, 4096, 4096, false, false)).unwrap();
        let mut out = Vec::new();
        let c = wait_for(&mut ring, &mut out, |c| matches!(c, Completion::Disk { .. }));
        let Completion::Disk { kind, dev, done } = c else { unreachable!() };
        assert_eq!((kind, dev, done.tag, done.res), (DiskKind::Write, 0, tag, Ok(4096)));
        assert!(done.buf.is_some());
        ring.disk(fd, 0, IoReq::flush(tag, 0)).unwrap();
        let Completion::Disk { kind, done, .. } = wait_for(&mut ring, &mut out, |c| matches!(c, Completion::Disk { .. })) else {
            unreachable!()
        };
        assert_eq!((kind, done.res), (DiskKind::Flush, Ok(0)));
        ring.disk(fd, 0, IoReq::read(tag, 0, 4096, AlignedBuf::zeroed(4096))).unwrap();
        let Completion::Disk { kind, done, .. } = wait_for(&mut ring, &mut out, |c| matches!(c, Completion::Disk { .. })) else {
            unreachable!()
        };
        assert_eq!((kind, done.res), (DiskKind::Read, Ok(4096)));
        assert_eq!(&done.buf.unwrap().as_slice()[..4], b"page");
        ring.disk(fd, 0, IoReq::discard(tag, 0, 0, 4096)).unwrap();
        let Completion::Disk { kind, done, .. } = wait_for(&mut ring, &mut out, |c| matches!(c, Completion::Disk { .. })) else {
            unreachable!()
        };
        assert_eq!((kind, done.res), (DiskKind::Discard, Err(-libc::EOPNOTSUPP)));
        assert_eq!(ring.inflight(), 0);
    }

    #[test]
    fn doorbell_and_timer() {
        let mut ring = Ring::new().unwrap();
        let efd = sys::eventfd().unwrap();
        ring.arm_doorbell(efd).unwrap();
        ring.arm_doorbell(efd).unwrap(); // idempotent
        sys::eventfd_signal(efd);
        let mut out = Vec::new();
        wait_for(&mut ring, &mut out, |c| matches!(c, Completion::Doorbell));
        ring.arm_timer(1_000_000).unwrap();
        assert!(ring.timer_armed());
        wait_for(&mut ring, &mut out, |c| matches!(c, Completion::Timer));
        assert!(!ring.timer_armed());
        sys::close(efd);
    }

    #[test]
    fn accept_recv_send_over_loopback() {
        let mut ring = Ring::new().unwrap();
        let lfd = sys::tcp_listener("127.0.0.1:0".parse().unwrap(), 16).unwrap();
        let addr = sys::local_addr(lfd).unwrap();
        ring.accept(lfd, 7).unwrap();
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        let mut out = Vec::new();
        let Completion::Accept { listener, res, more } = wait_for(&mut ring, &mut out, |c| matches!(c, Completion::Accept { .. })) else {
            unreachable!()
        };
        assert_eq!(listener, 7);
        assert!(more, "multishot accept stays armed");
        let cfd = res;
        assert!(cfd >= 0);
        sys::tune_accepted(cfd, 300);
        assert_eq!(sys::peer_addr(cfd).unwrap(), client.local_addr().unwrap());
        client.write_all(b"PING\r\n").unwrap();
        let mut b = vec![0u8; 64];
        b[..2].copy_from_slice(b"ab");
        ring.recv(cfd, 42, b, 2).unwrap();
        let Completion::Recv { token, res, buf } = wait_for(&mut ring, &mut out, |c| matches!(c, Completion::Recv { .. })) else {
            unreachable!()
        };
        assert_eq!((token, res), (42, 6));
        assert_eq!(&buf[..8], b"abPING\r\n");
        let body = SharedBuf::new(vec![b'x'; 100_000]);
        let segs = vec![OutSeg::Bytes(b"$100000\r\n".to_vec(), 0), OutSeg::Shared(body, 0), OutSeg::Bytes(b"\r\n".to_vec(), 0)];
        let mut segs = segs;
        let mut total = 0;
        while !segs.is_empty() {
            ring.send(cfd, 5, segs).unwrap();
            let Completion::Send { token, res, segs: back } = wait_for(&mut ring, &mut out, |c| matches!(c, Completion::Send { .. }))
            else {
                unreachable!()
            };
            assert_eq!(token, 5);
            assert!(res > 0);
            total += res as usize;
            segs = back;
            advance_segments(&mut segs, res as usize, &mut |_| {});
            // drain the client side so large sends can make progress
            let mut sink = vec![0u8; 200_000];
            client.set_read_timeout(Some(std::time::Duration::from_millis(50))).unwrap();
            let _ = client.read(&mut sink);
        }
        assert_eq!(total, 9 + 100_000 + 2);
        sys::shutdown(cfd);
        sys::close(cfd);
        sys::close(lfd);
    }

    #[test]
    fn segment_advance_recycles_finished_buffers() {
        let mut segs = vec![OutSeg::Bytes(b"abc".to_vec(), 0), OutSeg::Shared(SharedBuf::new(b"defg".to_vec()), 0), OutSeg::Bytes(b"hi".to_vec(), 0)];
        let mut recycled = 0;
        advance_segments(&mut segs, 5, &mut |_| recycled += 1);
        assert_eq!(recycled, 1);
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].remaining(), b"fg");
        advance_segments(&mut segs, 4, &mut |_| recycled += 1);
        assert!(segs.is_empty());
        assert_eq!(recycled, 2);
    }
}
