#!/usr/bin/env bash
# Real-process crash loop against kvd (DESIGN §25.4): wraps the black-box harness in
# tests/compat/persistence.py with the kvd launch template. Extra arguments are passed through,
# e.g. `--kill-mode inflight` or `--kill-mode quiesced`.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
kvd="${KVD:-$root/target/release/kvd}"
exec python3 "$root/tests/compat/persistence.py" --sync \
  --target-cmd "$kvd --port {port} --data-dir {dir}" "$@"
