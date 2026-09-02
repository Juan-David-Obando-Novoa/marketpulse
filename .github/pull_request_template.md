# What and why

<!-- One paragraph. What changes, and what problem it solves. -->

## Data impact

<!-- Delete the lines that do not apply. -->

- [ ] Changes a **contract** (`contracts/schemas/*.avsc`) — an ADR and a
      compatibility check are required, see ADR-0006.
- [ ] Changes a **table schema** in bronze, silver or gold.
- [ ] Changes a **partition spec** — note that Iceberg keeps the old layout on
      existing data forever.
- [ ] Requires a **backfill**, and the range is stated below.
- [ ] Changes **delivery semantics** or the dedup window (ADR-0007).
- [ ] None of the above; transformation or tooling only.

## Backfill plan

<!-- If a backfill is needed: which assets, which partitions, and roughly how
     long. If not: "none". -->

## Verification

- [ ] `make check` passes locally.
- [ ] New behaviour is covered by a test that fails without the change.
- [ ] For a transformation change: `dbt build` run against a local stack, and
      the affected model's row count compared before and after.
- [ ] For a contract change: `marketpulse schemas check` passes against the
      registry.

## Rollback

<!-- How to undo this if it goes wrong in production. "Revert the commit" is a
     valid answer only when no data was written in the new shape. -->
