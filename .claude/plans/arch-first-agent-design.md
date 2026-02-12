# Architecture-First Agent: Design Document

## Purpose
Force architectural thinking before implementation, preventing the "patch spiral" that leads to O(n²) solutions when O(n) solutions exist.

## Core Workflow

```
┌─────────────────────────────────────────────────────────────┐
│  USER REQUEST: "Implement X" or "Fix Y"                    │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
              ┌─────────────────┐
              │ INTERCEPT PHASE │
              └────────┬────────┘
                       ▼
            Has architecture doc been created?
            ┌────────┴────────┐
            │                 │
           NO                YES
            │                 │
            ▼                 ▼
     ┌───────────┐     ┌─────────────┐
     │ REQUIRE   │     │ PROCEED     │
     │ RESEARCH  │     │ WITH        │
     │ + DESIGN  │     │ CAUTIONS    │
     └─────┬─────┘     └──────┬──────┘
           │                  │
           ▼                  ▼
    ┌────────────┐    ┌──────────────┐
    │ CREATE DOC │    │ CHECK MODE:  │
    │           │    │ - Proper?    │
    │           │    │ - Patch?     │
    │           │    │ - Spike?     │
    └─────┬──────┘    └──────┬───────┘
          │                  │
          └────────┬─────────┘
                   ▼
          ┌────────────────┐
          │ IMPLEMENT      │
          │ WITH TRACKING │
          └────────────────┘
```

## Components

### 1. Hook: PreToolUse (Interception)
- Triggers on ALL tool calls (Read, Edit, Write, Bash, Task, etc.)
- Checks if the user is asking to implement/fix something
- If so, pauses and requires architecture doc

### 2. Skill: architecture-research (Interactive)
- Guides user through researching DuckDB/Velox/DataFusion
- Creates template for documenting findings
- Takes 30-60 minutes

### 3. Skill: architecture-design (Interactive)
- Helps create architecture document
- Includes diagrams (Mermaid or ASCII)
- Defines success metrics
- Takes 30-60 minutes

### 4. Skill: architecture-review (Validation)
- Reviews existing architecture docs
- Identifies gaps or inconsistencies
- Suggests improvements

## Decision Tree

```
Is this a implementation task?
├─ No → Allow (this is research/exploration)
└─ Yes → Has architecture doc?
    ├─ No → Block: Require architecture-research + architecture-design
    └─ Yes → Is doc recent (< 7 days old)?
        ├─ No → Block: Require update (tech debt accumulates)
        └─ Yes → Allow with tracking (mark implementation mode)
```

## Implementation Modes

1. **Proper Implementation**: Following documented architecture
   - Add comment: `// ARCHITECTURE: Implements [doc-name]`
   - Track metrics from success criteria

2. **Tactical Patch**: Quick fix with documented debt
   - Add comment: `// PATCH: [issue] - TODO: Proper fix in [ticket]`
   - Create tech debt ticket
   - Schedule review

3. **Exploration/Spiking**: Understanding the problem
   - Add comment: `// SPIKE: Investigating [question]`
   - Document findings
   - Propose next steps

## Anti-Pattern Detection

Monitor for these patterns and escalate:

1. **Patch Spiral**: 3+ patches to same module in 24 hours
   - Suggest: Architecture reset / refactor

2. **Performance Degradation**: 2x slowdown in core metrics
   - Suggest: Performance review / architecture audit

3. **Complexity Explosion**: File grows > 500 lines during work
   - Suggest: Refactor for simplicity

4. **Import Proliferation**: Adding dependencies without justification
   - Suggest: Review against requirements

## File Structure

```
.claude/plugins/architecture-first/
├── plugin.json                      # Plugin manifest
├── skills/
│   ├── architecture-research.md    # Research guidance skill
│   ├── architecture-design.md      # Design guidance skill
│   └── architecture-review.md       # Review validation skill
└── hooks/
    └── pre-tool-use.md              # Interception hook
```

## Success Criteria

1. ✅ Agent intercepts 100% of implementation attempts
2. ✅ Users complete research phase before coding
3. ✅ Architecture docs are created for all features
4. ✅ Patches are explicitly tracked and documented
5. ✅ Anti-patterns are detected and escalated
6. ✅ Zero regression to "patch spiral" behavior

## Testing Strategy

1. Test hook triggers correctly on implementation tasks
2. Test skills guide users through creating docs
3. Test that proper mode is tracked in code comments
4. Test that patches are tagged with tech debt
5. Test anti-pattern detection and escalation
