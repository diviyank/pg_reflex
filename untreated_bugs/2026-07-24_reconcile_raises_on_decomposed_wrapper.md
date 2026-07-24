# 2026-07-24 — `reflex_reconcile` RAISES on a decomposed wrapper IMV (set-op / DISTINCT ON / window), aborting the caller's transaction

**Status: untreated, narrowed.** Found under PS-10. Probe-confirmed on
`fix/ps10-intermediate-audit-bugs` @ 1.11.1. **Pre-existing.**

**What PS-10 already fixed:** `reflex_scheduled_reconcile` no longer reaches this.
Its candidate CTE now skips rows with no plan
(`COALESCE(aggregations::text, '{}') <> '{}'`, `src/reconcile.rs`), so one set-op
IMV no longer kills the whole sweep — it used to raise out of the function and
return nothing, leaving every other IMV in the batch unreconciled, permanently,
because a wrapper's `last_update_date` is stamped at create and never advances
(the only writer is the tail of `reconcile_one`, which a wrapper never reaches).
Regression test:
`src/tests/pg_test_ps10.rs::ps10_scheduled_reconcile_survives_a_set_op_imv`.

**What remains:** the direct call still raises, and two other callers still route
into it.

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
passthrough branch and issues `TRUNCATE {view}` — which PostgreSQL rejects on a
VIEW, as a raised ERROR rather than a returned `ERROR: …` string, so it aborts the
whole calling transaction. The same holds for `UNION`/`INTERSECT`/`EXCEPT`,
DISTINCT ON and window wrappers, all of which are VIEWs. The `REBUILDABLE_NODE`
comment in the same file already documents that these nodes are not rebuildable;
nothing stops a *direct* call from trying.

## Still-reachable callers (not audited further)

- `reflex_reconcile('<wrapper>')` / `reflex_rebuild_imv('<wrapper>')` — the operator
  path.
- `refresh_imv_depending_on(<source>)` (`src/reconcile.rs`) selects
  `WHERE $1 = ANY(depends_on)` with no plan filter. A wrapper's `depends_on` holds
  its sub-IMVs, so refreshing a matview that a sub-IMV reads can route into the
  wrapper.
- `reflex_doctor(fix => true)`'s F4 / F4b repairs call `reflex_reconcile(imv_name)`
  for any `known_stale` row. Whether a wrapper row can be marked `known_stale` was
  not established.

## Why the obvious fix is NOT obviously right

"Skip the target rebuild for wrapper rows" is wrong for the *materialised*
UNION-ALL wrapper (`install_union_all_intermediate_wrapper`, used when a CTE body
is consumed by an aggregate). That wrapper is an UNLOGGED TABLE with a
`__reflex_src_idx` discriminator, maintained by
`__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>` triggers on each operand.
Those mirror triggers cover INSERT/UPDATE/DELETE but **not TRUNCATE**, so the
bottom-up reconcile of an operand sub-IMV (which is `TRUNCATE` + `INSERT`) fires
the INSERT mirror and appends that operand's rows to the wrapper a second time
while never removing the old ones. Any change here has to settle what a wrapper
reconcile *means* for that shape first — a no-op leaves a doubled wrapper, and the
current raise at least fails loudly. That is why PS-10 fixed the sweep, where
skipping is unambiguously right, and left the direct call alone.

## Severity

S3 after the sweep fix (was S2). Loud, not silent; `reflex_audit` / `reflex_doctor`
no longer send anyone here (PS-10 removed the `internal-tables-exist` false positive
whose remedy was `reflex_rebuild_imv('<wrapper>')`), and the scheduled sweep skips
it. But `reflex_reconcile` is the primitive operators reach for first, and on a
set-op IMV it aborts their transaction.

## What is already covered

`src/tests/pg_test_ps10.rs::ps10_reconcile_does_not_heal_passthrough_unpartitioned`
documents this in a comment and asserts the neighbouring shapes instead, because a
raised ERROR aborts a `#[pg_test]` transaction and cannot be asserted around.
