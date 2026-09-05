#!/usr/bin/env bash
# PreToolUse hook (matcher: Bash) — blocks memory-heavy commands that would
# run OUTSIDE this repo's cgroup-capped safe wrappers.
#
# Why this exists: 2026-08-22 a bare `cargo test --release --features lance`
# peaked at 105G inside the terminal's own cgroup and systemd-oomd killed the
# whole terminal scope (session + remote-control bridge). scripts/
# claude-safe-build.sh was built to fix that, and CLAUDE.md mandates it for
# every build/test/bench command — but on 2026-08-28 12:35 it happened AGAIN
# (vte-spawn scope killed, 107.2G peak, 11 processes) because .claude/
# settings.local.json's own permission allowlist pre-approves bare
# `cargo run --release`, `cargo build`, `cargo test`, `cargo bench`, and
# direct `./target/{release,debug}/query_engine` execution — so the rule was
# documented but never actually enforced; nothing stopped a command from
# silently bypassing the wrapper. This hook is the enforcement layer: it
# fires before every Bash call regardless of what the allowlist permits, and
# denies anything memory-heavy that isn't already wrapped.
#
# Scope: deliberately broad. It also flags `scripts/*.sh` / `scripts/*.py`
# invocations, since several of this repo's own benchmark/diagnostic drivers
# (native_bench_compare.py, sf100_full_benchmark.sh, iceberg_bench_compare.py,
# ...) spawn the engine binary as a subprocess with no cap of their own. A
# false positive here just means wrapping an already-safe command in
# `systemd-run --user --scope -p MemoryMax=...` — cheap. An unwrapped OOM
# that kills the whole session is not.

set -euo pipefail

input="$(cat)"
command=$(printf '%s' "$input" | jq -r '.tool_input.command // empty' 2>/dev/null || true)

# Nothing to check (not a Bash call, or malformed input) — allow silently.
if [[ -z "$command" ]]; then
  exit 0
fi

allow() {
  exit 0
}

deny() {
  local reason="$1"
  jq -n --arg reason "$reason" '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: $reason
    }
  }'
  exit 0
}

# Already wrapped in one of the project's own safe-execution mechanisms —
# always allow, regardless of what else the command contains.
if grep -qE 'claude-safe-build\.sh|oomsafe\.sh|safe_benchmark\.sh' <<<"$command"; then
  allow
fi
if grep -qE 'systemd-run[^|;&]*-p[[:space:]]+Memory(Max|High)=' <<<"$command"; then
  allow
fi

# Risky pattern 1: cargo build/test/bench/run (any flags), or an explicit
# `cargo run --example`. Deliberately does NOT match `cargo fmt`/`check`/
# `clippy`/`--version`/`tree`/`metadata` — those never run the heavy
# compiled artifact.
if grep -qE '(^|[^a-zA-Z_.-])cargo[[:space:]]+(build|test|bench|run)([[:space:]]|$)' <<<"$command"; then
  deny "Bare 'cargo build/test/bench/run' bypasses this repo's mandatory memory-capped sandbox (see CLAUDE.md's Sandboxed Build Rule) — a bare release build/test/bench OOM-killed the whole terminal (including this session) on 2026-08-22 and again on 2026-08-28. Re-run through the wrapper instead: 'scripts/claude-safe-build.sh $command' (tune with SAFE_BUILD_MEM=/SAFE_BUILD_JOBS=)."
fi

# Risky pattern 2: direct execution of the compiled engine binary or an
# example binary, bypassing cargo entirely.
if grep -qE '(^|[^a-zA-Z0-9_./-])\.?/?target/(release|debug)/(query_engine|examples/[a-zA-Z0-9_-]+)([[:space:]]|$)' <<<"$command"; then
  deny "Direct execution of the compiled engine binary/example bypasses this repo's mandatory memory-capped sandbox (CLAUDE.md's Sandboxed Build Rule) — this exact bypass OOM-killed the whole terminal on 2026-08-28. Re-run through the wrapper: 'scripts/claude-safe-build.sh $command', or for a bounded ad-hoc run: 'systemd-run --user --scope -p MemoryMax=<N>G -- $command'."
fi

# Risky pattern 3: one of the project's own scripts/*.sh or scripts/*.py
# driver scripts, invoked directly. These frequently spawn the engine
# binary as a subprocess with no memory cap of their own (e.g.
# native_bench_compare.py, sf100_full_benchmark.sh, iceberg_bench_compare.py).
# Matches only when the script looks like it's being EXECUTED (as the
# command itself, or via an interpreter/./ prefix) — not merely read/
# grepped/listed.
if grep -qE '(^|[|;&(]|[[:space:]])(\./)?scripts/[A-Za-z0-9_./-]+\.(sh|py)([[:space:]]|$)' <<<"$command" \
   && ! grep -qE '(^|[|;&(]|[[:space:]])(cat|less|more|head|tail|grep|rg|wc|ls|find|diff|vim|nvim|nano|code|realpath|dirname|basename|stat|file|shellcheck)[[:space:]]+[^|;&]*scripts/' <<<"$command"; then
  deny "This runs a project script directly, outside any memory cap — several scripts/ drivers (native_bench_compare.py, sf100_full_benchmark.sh, iceberg_bench_compare.py, etc.) spawn the engine binary as a subprocess with no cap of their own, which is how the 2026-08-28 OOM kill happened. Wrap it: 'systemd-run --user --scope -p MemoryMax=<N>G -- $command', or if it's just a build/test/bench/binary invocation under the hood, use 'scripts/claude-safe-build.sh' instead."
fi

allow
