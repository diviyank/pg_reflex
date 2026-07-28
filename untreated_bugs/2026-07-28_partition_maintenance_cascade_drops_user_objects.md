# 2026-07-28 — partition maintenance silently destroys user objects attached to IMV partition children (`DROP TABLE … CASCADE`)

**Status: untreated. PRE-EXISTING — not a regression.** Verified by building and
installing `2f8b786` and running the reproduction below against it: identical result
(view destroyed at COMMIT). All eight `DROP TABLE … CASCADE` statements in
`src/partition.rs` are byte-identical between `2f8b786` and the reconcile-atomicity
branch; the only CASCADE-containing line that branch touches is a doc comment.

Found while building a test fixture for the reconcile-atomicity work: a view pinned to an
IMV partition child, created purely to block a `DROP`, **vanished on its own** before the
code under test ran. That is an observed instance, not a constructed hypothesis.

## The mechanism

pg_reflex removes IMV partition children with `CASCADE`, which in PostgreSQL means
"destroy everything that depends on this relation, recursively, without asking".
Occurrences in `src/partition.rs`:

| line | context |
|---|---|
| `1285`, `1297` | `reflex_sync_partitions_impl` orphan drop (intermediate / target) |
| `1352` | F3 shape-drift heal — drops a child whose relkind no longer matches its source node |
| `1565` | `drop_bound_collision_orphan` — swap-renamed source leaf leaves a bounds-colliding orphan |
| `2510` | `cleanup_orphan_swap_tables` — leftover `__reflex_swap_*` tables (low risk: internal, short-lived names) |
| `3025`, `3029` | `reflex_flush_partitions_impl` `root_stmts`, per `to_drop` node — **the path in the reproduction** |

The statement is, verbatim:

```rust
format!("DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE", schema, tgt)   // src/partition.rs:3025
format!("DROP TABLE IF EXISTS \"{}\".\"{}\" CASCADE", schema, int)   // src/partition.rs:3029
```

## Reproduction (pg16.11, real IMV over a real partitioned source)

```sql
CREATE TABLE csc (id BIGINT, region TEXT NOT NULL, amount NUMERIC) PARTITION BY LIST (region);
CREATE TABLE csc_n PARTITION OF csc FOR VALUES IN ('N');
CREATE TABLE csc_s PARTITION OF csc FOR VALUES IN ('S');
INSERT INTO csc VALUES (1,'N',10),(2,'S',20);
SELECT create_reflex_ivm('cscv','SELECT region, SUM(amount) AS total FROM csc GROUP BY region',
                         NULL,NULL,NULL,NULL, ARRAY['region']);

-- Anything a DBA might reasonably attach to a partition child:
CREATE VIEW csc_user_view AS SELECT * FROM cscv_csc_s;
CREATE MATERIALIZED VIEW csc_user_matview AS SELECT * FROM cscv_csc_s;
CREATE INDEX csc_user_index ON cscv_csc_s (total);

BEGIN;
ALTER TABLE csc DETACH PARTITION csc_s;      -- an ordinary DBA operation
-- NOTICE: pg_reflex: orphan target partition 'cscv_csc_s' preserved (drop_orphans=false)
-- child = 1, view = 1                        <- still there, and pg_reflex SAID it preserved it
COMMIT;
-- NOTICE: drop cascades to view csc_user_view
-- child = 0, view = 0                        <- destroyed at COMMIT, by the partition flush
```

The destruction happens at **COMMIT**, from the automatic partition flush draining the
`__reflex_partition_pending` row the DETACH enqueued — not from the DDL hook that ran
during the statement.

### What makes this worse than a bare CASCADE

The DDL hook emits `orphan target partition 'cscv_csc_s' preserved (drop_orphans=false)`
during the statement, and then the commit-time flush drops that same child anyway. An
operator reading the session output is told their partition was **preserved**, moments
before it and everything depending on it is destroyed. The only trace is PostgreSQL's own
`drop cascades to …` NOTICE, which is easy to miss and absent from any log that filters
NOTICEs.

## Which user object classes are actually exposed — measured, not assumed

| object | destroyed? | note |
|---|---|---|
| `VIEW` on a child | **yes** | `drop cascades to view csc_user_view` |
| `MATERIALIZED VIEW` on a child | **yes** | `drop cascades to materialized view csc_user_matview` |
| user `INDEX` on a child | **yes** | dies with the relation; not recreated (the resynced child only inherits the parent's partitioned indexes) |
| `FOREIGN KEY` referencing a child | **no** — not constructible under the default storage mode | `constraints on permanent tables may reference only permanent tables`: IMV children are UNLOGGED by default. **Untested for `storage_mode = LOGGED`**, where it likely becomes reachable — worth confirming before closing. |

A view or matview over a *whole* IMV (`cscv`) is not at risk — only objects bound to an
individual **child**. That is a narrower blast radius than it first appears, but pointing
at a specific partition is exactly what someone does for a hot slice.

## Blast radius

* **Trigger surface is wide and mostly automatic.** Any source-side partition lifecycle
  event — `DETACH`, `DROP` of a partition, a swap-rename — enqueues a pending row, and the
  commit-time flush then drops the mirrored children. It needs no explicit pg_reflex call.
* **Not confined to the flush.** The orphan drop, the shape-drift heal and the
  bound-collision drop use the same `CASCADE` and are reached from `reflex_sync_partitions`,
  from every `reflex_reconcile_partition` pre-sync, and from `reflex_doctor` repairs.
* **Silent and unlogged by pg_reflex.** No pg_reflex WARNING names the collateral; nothing
  is recorded in the registry; there is no dry-run that would list what a sync is about to
  destroy.
* **Not recoverable.** A dropped view/matview definition is gone with the transaction that
  dropped it.

## What was ruled out

* Not the reconcile failure-atomicity bug (fixed on the atomicity branch) — here the flush
  **succeeds**; the CASCADE is on the happy path and would commit under any atomicity scheme.
* Not the flush's swallowed-`ERROR` accounting gap
  (`2026-07-27_reconcile_partition_error_string_swallowed_by_perform.md`) or the
  destructive-DDL-on-failure residual
  (`2026-07-27_flush_do_block_commits_destructive_ddl_on_failed_reconcile.md`). Those are
  about a *failing* reconcile; this fires when everything works as designed.
* Not `cleanup_orphan_swap_tables` (`:2510`) in practice — those names are internal and
  short-lived, so a user object attached to one is implausible.

## Fix direction

The honest question first: is CASCADE ever *needed*? The children pg_reflex drops are its
own, and their legitimate dependents (the parent's partitioned indexes, the partition
attachment) are handled by PostgreSQL without CASCADE. CASCADE is most likely load-bearing
only for pg_reflex-owned objects hanging off the child.

Suggested direction, cheapest first:

1. **Probe before dropping.** Query `pg_depend` for non-pg_reflex dependents of the child.
   If any exist, emit a WARNING naming them (`pg_reflex: dropping partition child X will
   also destroy view Y`) — so the destruction is at least loud. This alone closes the
   "silently" half and is low risk.
2. **Prefer `RESTRICT`, fall back deliberately.** Try the drop without CASCADE; on
   `dependent objects still exist`, either refuse loudly (per the "refuse loudly, never
   no-op silently" principle) or retry with CASCADE only after the WARNING above. Refusing
   changes behaviour for anyone currently relying on the CASCADE, so it needs the
   `pg_depend` survey first to know whether that reliance is real.
3. Fix the misleading `preserved (drop_orphans=false)` NOTICE, which is followed by a drop
   from a different code path in the same transaction.

Pin whichever is chosen with a test asserting a user view on a child survives (or that the
operation refuses loudly and names it), and mutation-check it — "the user's object is still
there" is exactly the kind of claim that goes false-green.

Severity: medium-high. Silent, unrecoverable destruction of user objects on an automatic
path, triggered by an ordinary `ALTER TABLE … DETACH PARTITION`; mitigated by requiring the
object to be bound to an individual partition child rather than to the IMV.
