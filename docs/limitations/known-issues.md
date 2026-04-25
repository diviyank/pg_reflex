# Known issues

## Passthrough duplicate-row collapse

Passthrough IMVs use row-matching for incremental DELETE/UPDATE. If the IMV produces rows that are **identical across all columns** (exact duplicates), a single-row source DELETE removes every matching row.

**Workaround**: always include a PK or unique column in the SELECT list. From 1.2.1, pg_reflex auto-infers the PK from a single-source passthrough; from 1.0.x for explicit `unique_columns`.

## DEFERRED single-session flush

`reflex_flush_deferred(source)` processes the source's pending queue in a single session (the one that fired `COMMIT`). For very wide cascades (1000+ IMVs depending on one source), commit latency spikes proportional to cascade width.

**Workaround**: keep cascades narrow. Use `reflex_ivm_status` + `graph_depth` to audit cascade width.

## Composite type changes mid-flight

If a source column's type changes (`ALTER TABLE ... ALTER COLUMN ... TYPE`), the intermediate column's type doesn't auto-migrate. Run `reflex_rebuild_imv` after such an ALTER, or use `pg_reflex.alter_source_policy = 'error'` (1.2.1+) to gate.

## Concurrent DROP+CREATE on the same name

If session A `drop_reflex_ivm('v')` and session B `create_reflex_ivm('v', ...)` race, the registry `PRIMARY KEY(name)` constraint serialises them — one wins, the other errors cleanly. Tested with up to 4 concurrent sessions; not stress-tested beyond.

## Top-K (1.3.0) — open follow-ups

Tracked for 1.3.1:

1. **`reflex_enable_topk(name, k)` retrofit SPI** — to opt an existing IMV into top-K without `drop + create`.
2. **Element types beyond NUMERIC** — DATE / TIMESTAMP / TEXT inherit through `resolve_column_type` and should work, but lack dedicated test coverage.

## What changed the verdict from "controlled production use"

The audit's path to "drop-in REFRESH replacement" required:

1. ~~Top-K heap for MIN/MAX (R3)~~ → landed in 1.3.0.
2. ~~Auto-drop intermediates on source drop (R1)~~ → landed in 1.2.0.
3. ~~`pg_stat_statements` hook + per-IMV histogram (R6)~~ → landed in 1.3.0.
4. ~~Background drift scanner (R7)~~ → landed in 1.2.1 as `reflex_scheduled_reconcile` + pg_cron recipe.
5. ~~Runbook section in docs~~ → landed (you're reading it).

R4 (DEFERRED latency) and R8 (multi-tenant) remain architectural choices, not bugs.
