# 1. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-01-12

## Context

A data platform accretes decisions faster than it accretes code: which table
format, which delivery semantics, how late data is handled, where the contract
between producers and consumers lives. Six months later the code shows *what*
was done and never *why*. New engineers then either cargo-cult the decision or
silently reverse it, and both are expensive.

## Decision

Every structural decision is recorded as a numbered, immutable ADR in
`docs/adr/`. ADRs are never edited once accepted; they are superseded by a new
ADR that links back. A pull request that introduces a structural change without
an ADR is not merged.

A decision is "structural" if reversing it would require changing more than one
component, would require a data migration, or would change the guarantees the
platform offers its consumers.

## Consequences

- Positive: the reasoning survives staff turnover, and rejected alternatives
  stay visible so they are not silently re-litigated.
- Positive: ADRs give code review a stable reference point ("this violates
  ADR-0006") instead of relying on reviewer memory.
- Negative: a small tax on every structural change. Accepted deliberately.
