#!/usr/bin/env bash
# Contained entry point for the versioned benchmark harness.
# Historical constants and completion-only PASS labels are retired.
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
case "${1:-}" in
    prepare|convert|validate|run|report) ;;
    *)
        cat >&2 <<'EOF'
Legacy benchmark arguments are retired: they did not validate answers or enforce
a fresh matched DuckDB ceiling. Use prepare/convert/validate/run/report instead.
See scripts/benchmark/README.md for complete commands and current mode support.
Set BENCHMARK_PYTHON to the Python environment containing DuckDB and PyArrow.
EOF
        exit 2
        ;;
esac
cd -- "$REPO_ROOT"
mkdir -p .scratch
export TMPDIR="$REPO_ROOT/.scratch"
export PYTHONPATH="$REPO_ROOT/scripts${PYTHONPATH:+:$PYTHONPATH}"
exec "$REPO_ROOT/scripts/claude-safe-build.sh" "${BENCHMARK_PYTHON:-python3}" -m benchmark "$@"
