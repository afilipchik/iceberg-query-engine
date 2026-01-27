# Memory Safety [DONE]
## Status: COMPLETE

Cross joins and large datasets no longer crash. Graceful failure before OOM.

---

## Two-Tier Protection

### Tier 1: Memory Limits (Default)

hash_join.rs:173-195:
- MAX_BUILD_ROWS: 50M rows
- MAX_BUILD_BYTES: 4GB
- Checks during build collection (fail early)

### Tier 2: Spillable Operators (Opt-in)

Enable: ExecutionConfig::new().with_spilling(true)

Has bugs - disabled by default.

## Key Files

- hash_join.rs - Memory limits
- planner.rs - Config integration
- memory.rs - ExecutionConfig
- spillable.rs - Spill operators
