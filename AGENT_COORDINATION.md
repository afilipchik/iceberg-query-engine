# Agent Coordination Consensus Policy

## Overview

This document defines how multiple AI agents can work together on the `iceberg-query-engine` project in parallel without conflicts. The goal is to maximize throughput while maintaining code quality and consistency.

---

## Core Principles

1. **Single Source of Truth**: The `master` branch is authoritative
2. **Non-Blocking Work**: Agents should work on independent modules/features
3. **Explicit Coordination**: All shared state changes must be declared
4. **Conflict Resolution**: First to commit wins; others rebase
5. **Test-Driven**: Changes must pass tests before integration

---

## Work Distribution Strategy

### Module Ownership

| Module | Owner | Status | Notes |
|--------|-------|--------|-------|
| `src/parser/` | Agent A | Stable | Rarely changes |
| `src/planner/` | Agent B | Stable | Schema system may evolve |
| `src/optimizer/` | Any | Active | New rules being added |
| `src/physical/operators/` | Multiple | Active | Each operator can be worked independently |
| `src/metastore/` | Agent C | New | Recently integrated |
| `src/execution/` | Agent D | Stable | Core orchestration |
| `src/tpch/` | Agent E | Stable | Query definitions |
| `tests/` | All | Always | Mirror module changes |

### Independent Work Items

Agents can **safely work in parallel** on:

1. **Different physical operators** - Filter, Project, Sort, Limit, etc.
2. **Optimizer rules** - Each rule is independent
3. **New scalar functions** - As long as they're different functions
4. **Test files** - Different test targets
5. **Documentation** - Different files
6. **Benchmarks** - Different query numbers

Agents **MUST coordinate** on:

1. Shared type definitions (Schema, Expr, LogicalPlan nodes)
2. Operator trait definitions
3. Physical planner integration
4. Execution context changes
5. TPC-H schema modifications

---

## Coordination Protocol

### Step 1: Claim Work

Before starting, agents must:
1. Check `AGENT_COORDINATION.md` for active work
2. Add their work item to the "Active Work" section below
3. Tag their work item with their agent ID

Format:
```markdown
### Agent X: Feature Y
- **Module**: `src/module/`
- **Files**: `file1.rs`, `file2.rs`
- **Started**: YYYY-MM-DD HH:MM
- **Status**: In Progress | Completed | Blocked
```

### Step 2: Before Committing

1. **Pull latest master**:
   ```bash
   git checkout master
   git pull
   ```

2. **Check for conflicts**:
   ```bash
   # See what files changed since your branch
   git diff master...HEAD --name-only
   ```

3. **If conflicts detected**:
   - Check "Active Work" section for overlapping claims
   - Coordinate with conflicting agent via:
     - Comments in this file
     - GitHub issues/discussions
     - Direct communication if available

### Step 3: Making Changes

1. **Create feature branch**:
   ```bash
   git checkout -b feature/agent-x-descriptive-name
   ```

2. **Make changes** with:
   - Tests for new functionality
   - Documentation for public APIs
   - `cargo test` passing
   - `cargo clippy` warnings addressed

3. **Commit frequently** with descriptive messages:
   ```bash
   git commit -m "Add SIMD support for SUM aggregation

   - Use Arrow kernels for scalar aggregates
   - Fallback to hash-based for GROUP BY
   - Tests: Q1, Q6 passing

   Co-Authored-By: Agent X <noreply@anthropic.com>"
   ```

### Step 4: Integration

1. **Rebase on master**:
   ```bash
   git fetch origin master
   git rebase origin/master
   ```

2. **Resolve conflicts** (if any):
   - Coordinate with agent who made conflicting changes
   - Discuss in person/direct message to resolve
   - Test the merged code together

3. **Run full test suite**:
   ```bash
   cargo test --all
   cargo test --test subquery_test  # specifically test subqueries
   ```

4. **Create PR**:
   - Reference any related issues
   - Tag coordinating agents as reviewers
   - Include test results

---

## Active Work

*Last updated: 2025-01-23*

### Agent Current (Claude): Nested Correlated Subquery Support
- **Module**: `src/physical/operators/subquery.rs`
- **Files**: `src/physical/operators/subquery.rs`, `ROADMAP.md`
- **Started**: 2025-01-23 ~10:00
- **Status**: Partial - Single-level correlation working, multi-level (Q20) requires architectural changes

### Agent A: None
*No active work*

### Agent B: None
*No active work*

### Agent C: None
*No active work*

### Agent D: None
*No active work*

### Agent E: None
*No active work*

---

## Conflict Resolution Guidelines

### Type System Changes

**Rule**: Modifying shared types requires announcement 24h in advance

When you need to change:
- `Schema`, `PlanSchema`, `SchemaField`
- `Expr` variants
- `LogicalPlan` nodes
- `PhysicalOperator` trait

1. Add announcement to "Pending Changes" section
2. Wait for other agents to respond
3. Coordinate changes together if needed

### Physical Operators

**Rule**: Each operator can be modified independently

Creating a NEW operator is safe:
- No coordination needed
- Add to `src/physical/operators/mod.rs` exports
- Integrate in `src/physical/planner.rs`

Modifying an EXISTING operator:
- Check if other agents are working on it
- If yes, coordinate via this file
- If no ownership claimed, proceed

### Test Failures

**Rule**: Breaking changes require immediate coordination

If your changes break existing tests:
1. Revert or fix the breakage
2. Announce in "Pending Changes"
3. Work with affected agent to resolve

---

## File Touch Policy

### Safe-To-Modify (No Coordination Needed)

- New test files
- New benchmark files
- Documentation-only changes
- Comment-only changes
- `.md` files (except this one)

### Requires Announcement

- Any `src/lib.rs` changes
- Export list changes in `mod.rs`
- Trait method additions
- Public API changes

### Requires Coordination

- Shared type definitions
- Operator trait modifications
- Planning algorithm changes
- Execution context changes

---

## Integration Checklist

Before submitting PR, verify:

- [ ] All tests pass (`cargo test --all`)
- [ ] No new clippy warnings
- [ ] New code has tests
- [ ] Public APIs have documentation
- [ ] Performance impact assessed (for critical paths)
- [ ] Conflicting agents notified/reviewed

---

## Emergency Procedures

### Merge Conflict

1. Don't force push
2. Contact conflicting agent
3. Discuss resolution strategy
4. Test together if possible

### Test Failure in Master

1. Immediate notification to all agents
2. Do NOT merge new changes until fixed
3. Coordinate on who fixes (closest to the code)
4. Fix verified by all agents before proceeding

### Breaking Change Detected

1. Revert the breaking change
2. Discuss proper approach in PR comments
3. Create coordinated fix if needed
4. All agents review and approve

---

## Communication Channels

1. **This file**: Primary coordination mechanism
2. **GitHub Issues**: For discussion and decisions
3. **PR Comments**: For code review and coordination
4. **Direct Messages**: For time-sensitive coordination

---

## Agent Capabilities Map

| Agent | Strength | Preferred Work |
|-------|----------|----------------|
| A | Parser, SQL language | New SQL functions, syntax extensions |
| B | Planner, Types | Schema evolution, type coercion |
| C | Metastore, External | Iceberg integration, connectors |
| D | Execution, Performance | Parallel execution, memory management |
| E | TPC-H, Testing | Benchmarks, test coverage |

---

## Decision Making

### Technical Disagreements

1. State positions in this file under "Pending Changes"
2. Provide reasoning/evidence
3. Allow 24h for discussion
4. If no consensus, project lead decides

### API Design Changes

1. Propose in GitHub issue with detailed rationale
2. Allow 48h for feedback
3. Document decision in `CLAUDE.md` or `ARCHITECTURE.md`
4. Implement and coordinate integration

---

## Success Metrics

### Coordination Effectiveness
- <5% PR merge conflicts
- <24h resolution time for conflicts
- Zero test failures in master

### Development Velocity
- Features completed per week
- Lines of code written per day
- Test coverage percentage

---

## Revision History

| Date | Change | Agent |
|------|--------|-------|
| 2025-01-23 | Initial policy created | Claude (Current) |

