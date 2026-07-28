# 2026-07-28 — `reflex_sync_partitions`' own `DISABLE TRIGGER USER` trips the alter-source alarm, so `alter_source_policy='error'` blocks `reflex_reconcile` outright

**Status: untreated.** Split out of
`2026-07-28_partitioned_reconcile_destroys_dependent_imvs.md` §5.1 while fixing that
report on `fix/swap-ddl-destroys-dependents`. The parent report attributed this block to
the partition swap's `ALTER TABLE`s; **that attribution is wrong and was falsified by
measurement** (below). The swap's ALTERs are now suppressed and the block still happens,
so this is a genuine, independent residual.

Severity: **medium — availability, not correctness.** No wrong data. But an operator
running the non-default `alter_source_policy = 'error'` cannot reconcile a partitioned
IMV that has a dependent at all, and the HINT they are given recommends destroying the
IMV.

## Measured

pg17 under pgrx, on `fix/swap-ddl-destroys-dependents` (i.e. **with** the swap's
`ddl_command_end` suppression in place):

```sql
CREATE TABLE rdd7s (k TEXT NOT NULL, bucket INT NOT NULL, amt NUMERIC) PARTITION BY LIST (k);
CREATE TABLE rdd7s_a PARTITION OF rdd7s FOR VALUES IN ('A');   -- + _b, _c
INSERT INTO rdd7s SELECT v.k, (g % 5), (g % 97)::numeric
  FROM generate_series(1,300) g CROSS JOIN (VALUES ('A'),('B'),('C')) v(k);
ANALYZE rdd7s;

SELECT create_reflex_ivm('rdd7p','SELECT k, bucket, SUM(amt) AS total FROM rdd7s GROUP BY k, bucket',
                         NULL, NULL, NULL, NULL, ARRAY['k']);
SELECT create_reflex_ivm('rdd7d','SELECT k, SUM(total) AS t FROM rdd7p GROUP BY k');

SET pg_reflex.alter_source_policy = 'error';
SELECT reflex_reconcile('rdd7p');
```

```
ERROR:  pg_reflex: ALTER blocked by pg_reflex.alter_source_policy='error' on tracked source(s);
        affected: public.rdd7p -> rdd7d
HINT:   Set pg_reflex.alter_source_policy = 'warn' (default) or drop_reflex_ivm() first.
```

## Mechanism (measured, not inferred)

Not the swap. The offending `ALTER TABLE` is issued by
`reflex_sync_partitions_impl`'s **default-relocation trigger suppression**
(`src/partition.rs:1596-1611`):

```rust
client.update(&format!("ALTER TABLE {} DISABLE TRIGGER USER", root), ...)
```

`root` here is the IMV root itself. `reflex_reconcile` runs that sync before the rebuild
(`src/reconcile.rs:400`), so the `ALTER TABLE public.rdd7p …` reaches
`__reflex_on_ddl_command_end`'s alter-source branch (`src/lib.rs`), which finds `rdd7d`
listing `rdd7p` in `depends_on`, appends `public.rdd7p -> rdd7d` to `_affected`, and
RAISES under the `error` policy.

This is **exactly** the class of problem `pg_reflex.internal_reconcile_root` already
exists for: `reflex_reconcile_with_orphans` brackets its own
`DISABLE/ENABLE TRIGGER USER` on generated sub-IMVs with that GUC precisely so this alarm
does not fire on pg_reflex's own trigger suppression (`src/reconcile.rs:850-856`, comment
verbatim: *"under 'error' policy, abort the reconcile outright"*). The sync's suppression
is the same manoeuvre at a different call site, and was never bracketed.

## What was ruled out

* **"It is the partition swap's DETACH/ATTACH."** Refuted. Those are now suppressed via
  `pg_reflex.internal_swap_root` and the block still reproduces, with the alarm naming
  `public.rdd7p` from the pre-swap sync.
* **"It is `rebuild_convergence_advisory`."** No — that covers `ignore_sources` archive
  residue and matview-fed IMVs only, and does not fire here.
* **"It only affects partitioned IMVs."** Not established either way. The relocation
  branch is partition-specific, but any other unbracketed `ALTER TABLE` on a tracked
  source inside a reflex primitive would behave identically.

## Fix direction

Bracket the sync's relocation trigger-suppression `ALTER`s the way the reconcile chain
descent already brackets its own — a transaction-scoped GUC the event trigger honours.
The suppression must cover **both** the `DISABLE` and the `ENABLE`, and must be cleared on
every exit path including the partial-failure loop at `src/partition.rs:1612-1621`.

Consider instead generalising: pg_reflex now has **three** separate "this ALTER is mine,
do not alarm" mechanisms (`internal_reconcile_root`, `internal_swap_root`, and whatever
this becomes). One flag meaning "pg_reflex maintenance DDL in flight" would be simpler
than a third name, but it must stay narrow enough that a genuine user ALTER interleaved
in the same transaction is still reported — which is why the existing two are scoped to a
named relation rather than being booleans.

## Acceptance test

Real IMVs over a real partitioned source, parent partitioned with one dependent. Under
`SET pg_reflex.alter_source_policy = 'error'`, `reflex_reconcile(parent)` must return
`RECONCILED` and both IMVs must satisfy the bidirectional `EXCEPT ALL` oracle. Must be
shown RED before the fix — it currently aborts the transaction, which is an unambiguous
signal.

A second assertion is needed to stop the fix being over-broad: a genuine user
`ALTER TABLE <source> ADD COLUMN` in the same session must still raise under the `error`
policy.
