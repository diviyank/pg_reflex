# 2026-07-24 — `reflex_reconcile` RAISES on a decomposed wrapper IMV (set-op / DISTINCT ON / window), aborting the caller's transaction

**Status: untreated.** Found under PS-10 while writing the no-collateral-heal test
for a decomposed wrapper. Probe-confirmed on `fix/ps10-intermediate-audit-bugs`
@ 1.11.1. **Pre-existing**, independent of PS-10's two fixes.

## Reproduction (pg17)

```sql
CREATE TABLE ra (id BIGINT, v NUMERIC);
CREATE TABLE rb (id BIGINT, v NUMERIC);
INSERT INTO ra VALUES (1,10);
INSERT INTO rb VALUES (2,20);
SELECT create_reflex_ivm('rv', 'SELECT id, v FROM ra UNION ALL SELECT id, v FROM rb', 'id');
SELECT reflex_reconcile('rv');
```

Observed:

```
ERROR:  "rv" is not a table
CONTEXT:  tablecmds.c:2325
```

`reflex_rebuild_imv('rv')` fails identically — it is a literal alias
(`src/lib.rs:823`).

## Why

A top-level `UNION ALL` with no downstream consumer registers the wrapper as a
**VIEW** over its `__union_N` sub-IMVs (`src/create_ivm/decompose.rs:196-231`,
"zero-overhead VIEW"), with `end_query = ''`. `reconcile_one` therefore takes the
passthrough branch (`src/reconcile.rs:139`) and issues `TRUNCATE {view}` — which
PostgreSQL rejects on a VIEW, as a raised ERROR rather than a returned
`ERROR: …` string, so it aborts the whole calling transaction. The same holds for
`UNION`/`INTERSECT`/`EXCEPT`, DISTINCT ON and window wrappers, all of which are
VIEWs.

## Why the obvious fix is NOT obviously right

"Skip the target rebuild for wrapper rows" is wrong for the *materialised*
UNION-ALL wrapper (`install_union_all_intermediate_wrapper`, used when a CTE body
is consumed by an aggregate). That wrapper is an UNLOGGED TABLE with a
`__reflex_src_idx` discriminator, maintained by
`__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>` triggers on each operand.
Those mirror triggers cover INSERT/UPDATE/DELETE but **not TRUNCATE**, so the
bottom-up reconcile of an operand sub-IMV (which is `TRUNCATE` + `INSERT`) fires
the INSERT mirror and appends the operand's rows to the wrapper a second time
while never removing the old ones. Any change here has to settle what a wrapper
reconcile means for that shape first — a no-op leaves a doubled wrapper, and the
current raise at least fails loudly.

## Severity

S2. Loud, not silent, and `reflex_audit`/`reflex_doctor` no longer send anyone
here (PS-10 removed the `internal-tables-exist` false positive whose remedy was
`reflex_rebuild_imv('<wrapper>')`). But `reflex_reconcile` is the primitive
operators reach for first, and on a set-op IMV it aborts their transaction. Worth
checking whether `reflex_scheduled_reconcile` walks wrapper rows — if it does, one
set-op IMV poisons the whole batch.

## What is already covered

`src/tests/pg_test_ps10.rs::ps10_reconcile_does_not_heal_passthrough_unpartitioned`
documents this in a comment and asserts the neighbouring shapes instead, because a
raised ERROR aborts a `#[pg_test]` transaction and cannot be asserted around.
