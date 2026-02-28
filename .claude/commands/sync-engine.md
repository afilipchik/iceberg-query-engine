---
description: Sync iceberg-query-engine with upstream, run regression tests, and suggest next improvements
allowed-tools: Bash, Read, Grep, Glob, Write, Edit, WebFetch
---

# Sync Engine

Automate the recurring workflow of syncing the iceberg-query-engine with upstream changes,
verifying nothing is broken, and identifying next improvement opportunities.

## Step 1: Fetch upstream and show new commits

Run `git fetch origin` and then `git log HEAD..origin/main --oneline` to show any new
commits on the upstream main branch that are not yet in our branch.

If there are no new commits, say so and skip to Step 4.

## Step 2: Analyze upstream changes

For each new upstream commit, run `git show --stat <commit>` to summarize what changed.
Identify:
- New files that should be integrated
- Modified files that might conflict with our changes
- Files we've already superseded (like join_reordering.rs vs our join_reorder.rs)

Present a table:
| Commit | Files | Action | Reason |
|--------|-------|--------|--------|

## Step 3: Integrate changes (with user approval)

If there are changes worth integrating:
1. Ask the user which changes to integrate
2. For each approved change, cherry-pick or manually port the relevant code
3. Run `cargo check` to verify compilation
4. Run `cargo fmt --all` to ensure formatting

If there are conflicts, explain them and propose resolution strategies.

## Step 4: Run regression tests

Run the TPC-H benchmark regression check:
```bash
cargo test --lib --tests 2>&1 | grep "test result:"
```

Report:
- Total tests passed/failed
- Any new test failures since last sync
- TPC-H query status (all 22 should pass)

## Step 5: Quick performance check

If the user has Parquet data available (check for `data/tpch-*` directories):
```bash
cargo run --release -- benchmark-parquet --path <data-dir> --iterations 1
```

Compare against the baseline from CLAUDE.md's performance table.

## Step 6: Cross-reference open issues

Check GitHub issues for any that might be addressed by recent changes:
- Issue #9: Performance vs DuckDB
- Issue #10: Trino function parity
- Issue #11: LIMIT bug
- Issue #5: Parallel agent work

Read IMPROVEMENT_SUGGESTIONS.md and suggest the top 3 most impactful next improvements
based on the current state.

## Step 7: Summary

Provide a concise summary:
- Upstream status (ahead/behind)
- Integration actions taken
- Test results
- Recommended next steps
