#!/usr/bin/env bash
# Power-cut test loop (DESIGN §25.4): kvd runs a workload on a RAM-only faultdev NBD device; at a
# random point the device drops unflushed writes and tears in-flight ones; a fresh kvd recovers
# the resulting image and is verified against the model. Failure artifacts are the seed and the
# final image only. Requires root (NBD).
set -euo pipefail
echo "powercut.sh: not yet implemented" >&2
exit 2
