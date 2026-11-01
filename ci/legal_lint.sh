#!/usr/bin/env bash
# Alias kept for existing tooling: the lint is ci/invariant_lint.sh (docs/DESIGN.md §24.1, §25.7).
exec bash "$(dirname "$0")/invariant_lint.sh" "$@"
