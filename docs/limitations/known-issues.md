# Known issues

This page lists behaviours that surprise operators and are still open. Items
that have been resolved live in the [release notes](../changelog.md) and are
not duplicated here.

## Intra-operand duplicate over-delete in `UNION ALL` CTE wrappers

An intermediate `UNION ALL` CTE wrapper (e.g. `WITH x AS (SELECT … FROM a UNION ALL SELECT … FROM b) SELECT … FROM x …`) matches per-operand DELETEs by `__reflex_src_idx = <operand_idx> AND (cols) IS NOT DISTINCT FROM (old cols)`. If a single operand projects multiple rows that are **identical across every projected column**, a DELETE of one of those rows from the source removes all the wrapper rows that came from that operand and matched those values.

The `__reflex_src_idx` discriminator (1.7.0+) fixes the orthogonal cross-operand case (rows with identical values contributed by *different* operands are now isolated).

**Workaround**: include a primary key or unique column in each operand's projection. The wrapper then matches by that column and no over-delete occurs.

## Passthrough duplicate-row collapse

Passthrough IMVs match rows by content for incremental DELETE/UPDATE. If the
IMV produces rows that are **identical across every projected column**, a
single-row source DELETE removes every matching row in the target.

**Workaround**: include a primary key or unique column in the SELECT list.
From 1.2.1, pg_reflex auto-infers the PK for single-source passthroughs; the
6-arg `create_reflex_ivm` overload also accepts an explicit
`unique_columns` list.

## DEFERRED single-session flush

`reflex_flush_deferred(source)` processes the source's pending queue inside
the session that fired `COMMIT`. For very wide cascades (1 000 + IMVs
depending on one source), commit latency rises proportionally with cascade
width.

**Workaround**: keep cascades narrow. `reflex_ivm_status` reports
`graph_depth` and `graph_child` so cascade shape can be audited before it
becomes a problem.

## Composite type changes mid-flight

If a source column's type changes (`ALTER TABLE … ALTER COLUMN … TYPE`), the
intermediate column's type does not auto-migrate. Run
`reflex_rebuild_imv('<name>')` after such an `ALTER`, or set
`pg_reflex.alter_source_policy = 'error'` (1.2.1+) to make the `ALTER` fail
fast instead of leaving a typing skew.

## Top-K UPDATE recompute on dense groups

Top-K MIN/MAX IMVs whose **group cardinality is at or below `K`** pay a
scoped source-scan recompute on every UPDATE — the heap holds the whole
group, so any UPDATE shrinks it and trips the N1 gate. Workloads where
`K ≪ group_cardinality` (the common case for K=16 on large groups) skip the
recompute via the heap-shrinkage gate (~30 × on small batches in
`benchmarks/bench_n1_topk_update.sql`).

**Workaround**: opt out via `topk = 0` on the 6-arg
`create_reflex_ivm` overload; the IMV falls back to the 1.2.0
scoped-recompute behaviour on retraction.

## Concurrent DROP+CREATE on the same name

A `drop_reflex_ivm('v')` race against `create_reflex_ivm('v', …)` is
serialised by the registry's `PRIMARY KEY(name)` — one wins, the other
returns a clean error. Tested with up to 4 concurrent sessions; not
stress-tested beyond.

## `reflex_enable_topk(name, k)` retrofit SPI

There is no SPI to flip top-K on or off for an in-flight IMV. `drop_reflex_ivm`
followed by `create_reflex_ivm` is the supported retrofit path. A retrofit
SPI becomes warranted when an external user reports needing it without a
recreate; until then it stays out of scope.
