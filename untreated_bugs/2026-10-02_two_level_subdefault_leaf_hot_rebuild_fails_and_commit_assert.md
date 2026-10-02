# Two-level source with a populated sub-level DEFAULT leaf: hot child rebuild fails; at real COMMIT an assert build traps — S2 (loud), crash on assert builds

Found in the r5 fix-round-3 re-review while probing NULL partition values. Pre-existing: identical on
4125467, 45075e0 and the 1.11.4 tag (pg18, assert-enabled pgrx build).

## Reproduction
- Source `LIST (plan)`: `(1)`, `(2)`, `DEFAULT`; every child is `PARTITION BY RANGE (m)` with
  `(0..6)`, `(6..12)` and a `DEFAULT` leaf; some rows have `m IS NULL` (so the sub-level DEFAULT leaf
  holds rows).
- DEFERRED aggregate IMV `SELECT plan, m, id, SUM(v), COUNT(*) … GROUP BY plan, m, id`.
- `SET reflex.wipe_threshold = 0.2; UPDATE src SET v = v + 1 WHERE plan = 9;` then flush.

## Observed
- `SET CONSTRAINTS ALL IMMEDIATE`: `WARNING: flush failed at cascade: relation "public.t7_v_t7_d" does
  not exist`; the IMV is `known_stale` with 4000 mismatching rows. A later flush fails again with
  `missing target bound for child 's5_v_s5_d_a'` — the mirror has no target for the sub-level DEFAULT
  leaf, so the hot child's swap cannot be built.
- Real COMMIT: `TRAP: failed Assert("s->blockState == TBLOCK_SUBINPROGRESS …") xact.c:4847` from
  RollbackAndReleaseCurrentSubTransaction ← plpgsql DO exception block ← SPI ←
  `reflex_flush_deferred` ← AfterTriggerFireDeferred ← CommitTransaction. Crash recovery then failed
  with `could not create file "base/…": File exists` and the throwaway cluster stayed down.
- Non-assert (production) build behaviour at COMMIT: not yet checked.

## Exposure
base_db's `scripts/partition_tenant.py` two-level layout (`LIST plan × RANGE month`) creates monthly
sub-partitions with no sub-level DEFAULT leaf (the top-level DEFAULT is not sub-partitioned), so
this shape is not expected in prod. The COMMIT-time trap may however be reachable by any flush error
raised under the savepoint DO block at a real COMMIT — worth checking on its own.

## Directions
1. Mirror the sub-level DEFAULT leaf (or refuse the two-level shape at create time with a clear error).
2. Separately: reproduce a forced flush error at a real COMMIT on a non-assert build; if the
   subtransaction state is wrong there too, the per-IMV savepoint handling at TBLOCK_END needs a fix.
