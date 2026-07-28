# 2026-07-28 — reader-priority mode for central IMVs: requirement, options, and recommendation

**Status: design note + untreated requirement.** All measurements taken on PostgreSQL 16.11 under
pgrx, pg_reflex 1.11.2 @ `2f8b786`; lock-semantics probes on bare PostgreSQL 17.7 and 16.11.
Every claim is marked **measured** or **inferred**.

This note consolidates the reader-lock findings that are otherwise spread across
`2026-07-28_full_reconcile_swaps_every_partition_and_cascades.md`. That report is the evidence
base; this one is the decision document. **No fix is proposed for implementation here** — this
records the requirement, what is available today, what was measured, and what was ruled out.

---

## 1. The requirement, and why it is not met

**Reader blocking on an IMV is unacceptable. Some IMVs are central: blocking reads of them blocks
the whole application.** This is a standing requirement, not a one-off complaint.

It is not met today. Partition maintenance in pg_reflex is built on `DETACH`/`ATTACH` DDL, and
**measured on bare PostgreSQL 17.7**, a plain `ALTER TABLE parent DETACH PARTITION child` takes
`AccessExclusiveLock` on **both the child and the parent**, held to **commit** — not to
end-of-statement. Because the full-reconcile loop iterates the source's *immediate* children
(`list_partition_children`, `src/partition.rs:158-191`) and swaps each one out of the IMV **root**
(`src/reconcile.rs:445-449`), the very first child's `DETACH` locks the root for the rest of the
transaction. Partition pruning gives readers no protection: a reader of a partition that will never
be touched blocks just the same.

**Measured, depth 1** (`v1`, 6 partitions, 9 M source rows): `reflex_reconcile` ran 5.31 s; the root
`v1` went `AccessExclusive` 0.92 s in and stayed locked **4.39 s = 83 % of the transaction**. A
reader of the *last* partition at `lock_timeout='2s'` took one hard cancellation plus a 2.02 s wait
that only completed at commit.

**Measured, depth 2** (`v4`, 4 branches × 3 leaves, 2.8 M rows): 7.73 s transaction, root locked
**5.41 s = 70 %**, reader on an untouched branch took **two consecutive `lock_timeout`
cancellations**. Depth buys nothing — the DETACH is from the root at any depth.

**Measured, no `reflex_*` call and no DDL**: `UPDATE s5 SET amt = amt+1 WHERE k='A'` on a
6-partition IMV with default settings produced
`pg_reflex partition dispatch: hot=1 total=6 thr=0.5 floor=1000` →
`reflex_reconcile_partition` → **`v5 AccessExclusiveLock`**. At mirror depth 1 — which is what a
single-column `partition_by` produces, and what an auto-mirrored aggregate IMV gets regardless of
source depth — changing **one** partition freezes the whole IMV.

The comment at `src/reconcile.rs:424-431` claims this window is "microseconds" and that "readers
pruning to a not-yet-swapped partition stay live throughout". **Both are false** (measured, both
depths). That comment is the load-bearing justification for the current design and should be
corrected regardless of which option below is chosen.

### 1.1 Why this is urgent rather than theoretical: the safe path is effectively unreachable

pg_reflex already contains a reader-free maintenance path —
`build_scoped_cascade_reconcile` (`src/partition.rs:1858-1919`, reached at `:1812`), which emits a
literal-pruned `DELETE`+`INSERT` against the intermediate and target with **no DDL**. It is one of
three branches the dependent cascade picks from (`src/partition.rs:1805-1828`).

**Measured: none of three natural dependent shapes took it.** `reflex_reconcile_partition('v1','B')`
— *one* partition of *one* IMV — put `AccessExclusiveLock` on the roots of **all four** IMVs in the
closure (`v1`, `b_same`, `c_other`, `d_part`), every one held to commit.

**Inferred cause, from code:** the two preconditions are mutually exclusive.
`create_reflex_ivm` **auto-partitions** any dependent whose source is partitioned and whose
partition column is a bare projected output column (`src/create_ivm/mod.rs:664-740`) — and
`build_scoped_cascade_reconcile` returns `None` the moment the dependent is partitioned
(`src/partition.rs:1866-1868`). A dependent that qualifies for the reader-free path has already
been auto-partitioned out of it. The residual reachable cases are dependents reading **two or more
partitioned sources** (`partitioned_sources.len() != 1` declines auto-mirroring,
`src/create_ivm/mod.rs:673`) — a minority shape.

So the safe path exists, is trusted, and is almost never taken. That is what makes a deliberate
mode worth building rather than waiting for the cascade heuristics to improve.

---

## 2. Options

| option | reader lock | correctness risk | complexity | cost | available |
|---|---|---|---|---|---|
| **`wipe_threshold` = huge** (config only) | **none** for ordinary DML; **unchanged** for partition DDL and manual repairs | none | zero | **+6 %** measured on the worst case | **today** |
| **Per-IMV force-DML "reader priority" mode** | **none** | none — reuses an already-trusted statement shape | low | **+25 %** measured, plus unquantified bloat | code change |
| **Deferred DDL burst** | ~0.14 s × N partitions (0.83 s at N=6 measured; ~14 s at N=100 inferred) | **high in the naive form** — dependents computed from pre-swap rows | medium, plus a full-IMV-sized transient | fastest | code change |
| **`DETACH PARTITION CONCURRENTLY`** | would be none | — | — | — | **impossible** (§4) |

**Recommendation, in order:**

1. **Document the `wipe_threshold` lever now** (§3) — measured, costs 6 %, needs no release. State
   its two limits loudly.
2. **Build the per-IMV force-DML mode, defaulting off** (§5). It is the only option that satisfies
   "reader blocking is unacceptable" without qualification.
3. **Reject the deferred burst** (§6).
4. **Correct `src/reconcile.rs:424-431` regardless** — it currently tells the next reader this is
   already solved.

A cheap independent partial improvement: make the full-reconcile loop resolve **leaves** the way
`reflex_reconcile_partition_impl` already does (`src/partition.rs:1578-1610`) instead of top-level
children. It does not remove the lock, but at mirror depth ≥ 2 it moves it from the root to the
branches — which is what the §1 comment already claims happens. (It is also the fix for the
sub-partition flattening data loss filed separately today.)

---

## 3. The operator lever available today

```sql
SELECT reflex_set_wipe_threshold('<imv>', 1000000000::numeric);
-- …and on EVERY dependent IMV in the closure.
```

**Why one knob covers everything.** The per-partition hot/cold classifier
(`src/trigger/dispatch.rs:326-328`) and the trip-cap that escalates to a full reconcile
(`src/trigger/dispatch.rs:337-341`, and the passthrough sibling at `:566-570`) read the **same**
`_thr`, resolved at `src/trigger/dispatch.rs:276-279` as
`per-IMV wipe_threshold → GUC reflex.wipe_threshold → compiled 0.5`. Setting `_thr` beyond reach
makes every child cold, so neither the swap-based hot path nor the trip-cap ever fires.

**There is no `CHECK` constraint on the column** (`src/lib.rs:127` — plain `wipe_threshold NUMERIC`),
and `reflex_set_wipe_threshold` writes it verbatim (`src/sql_writer/registry.rs:534-553`), so an
arbitrarily large value is accepted. A value like `1.0` is **not** sufficient: `dirty/reltuples` can
exceed 1 when a statement inserts more rows than the stale `reltuples` estimate.
`reflex_set_wipe_floor_rows` is an equivalent second knob (it inflates the denominator).

### Measured results

* **Single IMV, worst case for the lever** — whole-table `UPDATE` touching all 6 partitions of a
  1.2 M-row fixture:
  * default: `INFO: pg_reflex: reconciled IMV 'v5' (partitioned, 6 children swapped)` (trip-cap
    fired), **9.01 s**, root `AccessExclusive`.
  * `wipe_threshold = 1e9`: `pg_reflex partition dispatch: hot=0 total=6 thr=1000000000 floor=1000`,
    **9.53 s**, **no `AccessExclusiveLock` on `v5` or its intermediate at any sample**.
  * **+6 %** on the case most favourable to the swap (every row of every partition changed).
* **Whole chain** — `wipe_threshold = 1e9` on `v1`, `b_same`, `c_other`, `d_part`;
  `UPDATE s1 SET amt = amt+1 WHERE k='C'` (1.5 M rows): `hot=0 total=6`; **zero
  `AccessExclusiveLock` rows anywhere in the 4-IMV closure** (only `ShareUpdateExclusive` from
  `ANALYZE`, which does not block readers); **zero blocked reader iterations**; 14.13 s.

### The two limits — an operator must not believe this makes them safe

1. **It must be set on every IMV in the closure.** Each IMV's dispatch reads **its own**
   `wipe_threshold` row. Setting it on the central IMV alone leaves a dependent free to swap, and
   the dependent's own reconcile takes `AccessExclusive` on the dependent's root — which is the
   relation the application may actually be reading.
2. **It does not cover partition DDL.** **Measured:** with `wipe_threshold = 1e9` still set on
   `v5`, `ALTER TABLE s5 ATTACH PARTITION s5_g_new FOR VALUES IN ('G')` still produced
   **`v5 AccessExclusiveLock`**. The `ddl_command_end` sync and the `DEFERRABLE INITIALLY DEFERRED`
   COMMIT-time flush (`src/lib.rs:1256`ff) never consult `wipe_threshold`. That gap is the subject
   of `2026-07-27_sync_partition_add_holds_accessexclusive_on_imv_root.md`.

   It also does not cover explicit `reflex_reconcile` / `reflex_rebuild_imv` / `reflex_doctor`
   repairs, which call the swap path directly.

**Plainly: yes for ordinary DML, no for partition DDL, no for manual repairs.**

**The trade being given up.** The threshold exists to find the crossover where a full-partition
rebuild beats an incremental MERGE. Setting it to `1e9` gives that optimisation up permanently.
Measured, the penalty is 6 % at the crossover's worst point; on a *small* delta the incremental path
was already the faster one, so the lever is free there. What it costs is unbounded only in the
pathological case of a delta far larger than the partition, which the trip-cap was written for.

---

## 4. `DETACH PARTITION CONCURRENTLY` is unavailable — record this so it is not re-proposed

`DETACH CONCURRENTLY` takes only `ShareUpdateExclusive` and is the obvious answer. It cannot be
used anywhere in pg_reflex.

**Measured on PostgreSQL 17.7 (bare) and 16.11 (pgrx), identical on both:**

```
BEGIN; ALTER TABLE lm DETACH PARTITION lm1 CONCURRENTLY;
ERROR:  ALTER TABLE ... DETACH CONCURRENTLY cannot run inside a transaction block

DO $$ BEGIN EXECUTE 'ALTER TABLE lm DETACH PARTITION lm1 CONCURRENTLY'; END $$;
ERROR:  ALTER TABLE ... DETACH CONCURRENTLY cannot be executed from a function
```

The **second** restriction is the decisive one, and it is the one usually overlooked. It is not
merely "cannot run inside an explicit transaction block" — it **cannot be executed from a function
at all**. Every pg_reflex maintenance path runs inside a SQL-callable function
(`#[pg_extern] fn reflex_reconcile`, `src/lib.rs:724`; the plpgsql dispatch bodies; the COMMIT-time
flush trigger), so the restriction applies to the **manually invoked** `SELECT reflex_reconcile(…)`
path too, not just the trigger-driven ones.

**There is therefore no partial fix here, not even one restricted to manual reconciles.** Using it
would require moving the DETACH out of pg_reflex entirely into operator-issued top-level SQL, which
is a different product.

---

## 5. Recommended fix: a per-IMV force-DML "reader priority" mode, defaulting off

**Shape.** A new `__reflex_ivm_reference` column (e.g. `reader_priority BOOLEAN`, default `FALSE`,
written through a `reflex_set_reader_priority(name, bool)` setter mirroring
`reflex_set_wipe_threshold`). When set, `reflex_reconcile`'s partitioned branch
(`src/reconcile.rs:432-508`) and the dependent-cascade branches (1) and (3) at
`src/partition.rs:1805-1828` use the `DELETE`+`INSERT` statement shape instead of the DETACH/ATTACH
swap.

**It reuses an existing, already-trusted statement shape.** `build_scoped_cascade_reconcile`
(`src/partition.rs:1858-1919`) already emits exactly
`DELETE FROM <intermediate> WHERE <key> IN (…); INSERT INTO <intermediate> <spliced base>;
DELETE FROM <target> WHERE <key> IN (…); INSERT INTO <target> …`, with a self-healing
`EXCEPTION WHEN OTHERS THEN PERFORM reflex_reconcile(child)` branch. This is a mode flag selecting
between two existing statement builders — **not a new mechanism**.

**Measured.** A hand-run of that exact statement shape rebuilding **all 6** partitions of `v1`
(9 M source rows re-aggregated), against the swap path on the identical fixture:

| | swap (`reflex_reconcile`) | DML (`DELETE`+`INSERT`) |
|---|---|---|
| wall clock | 5.31 s / 5.72 s | **7.04 s** |
| root `AccessExclusive` window | 4.39 s | **none — 0 samples in the whole trace** |
| blocked reader iterations (`lock_timeout='2s'`) | 1 cancellation + 1 near-timeout | **0** |
| lock modes observed on IMV relations | `AccessExclusive` | only `ShareRowExclusive` (from `DISABLE TRIGGER USER`), which does not conflict with readers |

**≈ +25 %, and genuinely reader-free.**

**Known costs and open items.**

* **Bloat is unquantified.** Each rebuild leaves one dead tuple per row in both the target and the
  intermediate (on this fixture: target children 23 MB, intermediate children 53 MB, source 448 MB),
  so a full-partition DML rebuild roughly doubles both until autovacuum catches up. This is the one
  number a decision should not be taken without; it was not measured here.
* **`TRUNCATE` is not an escape.** **Measured:** `TRUNCATE <leaf>` takes `AccessExclusiveLock`
  (plus `ShareLock`) on the leaf. Any "just truncate the partition instead of detaching it" variant
  reintroduces the block.
* **Default must stay off.** The swap remains the right default for IMVs nobody reads
  interactively; this mode buys latency-insensitivity at a 25 % throughput cost and should be opted
  into per IMV, as `wipe_threshold` already is.

**Why it is worth the complexity, under CLAUDE.md's ordering.** Correctness-neutral (the DML shape
is already the trusted cascade path, with its own self-healing fallback); simple (a flag between two
existing builders, no new primitive); and it costs 25 % only where an operator asks for it. It is
the only option in §2 that meets the requirement unconditionally.

---

## 6. Why the deferred DDL burst was rejected

**The idea.** Since the fill already happens on a detached table
(`create_swap_*` / `fill_swap_*`, `src/partition.rs:2005-2049`), batch every
`DETACH`/`ATTACH`/`DROP`/`RENAME` to the end of the transaction. This is a reordering of existing
statements, not a new mechanism.

**Measured** — the 6 swap tables of `v1` pre-filled outside the timed window, then the burst alone:

* burst wall clock **0.865 s**
* root `v1` `AccessExclusive` **0.83 s** (vs **4.39 s** today on the identical fixture — a
  **5.3×** reduction), and independent of data volume
* the concurrent reader stalled **0.850 s** on one iteration; no timeout at 2 s

**Rejected for three reasons.**

1. **Brief, but not zero.** The requirement is "reader blocking is unacceptable". 0.83 s of hard
   blocking on a central IMV is not zero, and the DML path (§5) achieves zero.
2. **It is O(partitions), not O(1).** The four DDL statements per child cost **~144 ms/partition**
   here (`DETACH` ~60 ms, `ATTACH` ~70 ms, `DROP` ~60 ms, `RENAME` ~17 ms), dominated by catalog
   work and the physical unlink in `DROP`. **Inferred** by extrapolation: ~8.6 s for a 60-leaf
   monthly-range IMV, ~14 s at N=100 — the same order as today's window on wide fan-outs. The
   optimisation evaporates exactly where the IMV is largest.
3. **The killer: dependent IMVs would be computed from pre-swap rows.** **Measured**
   (`src/partition.rs:1690-1828`): the cascade to dependents runs *after* the parent's swap loop and
   reads the parent's live tables. Deferring the parent's swap past the cascade means every
   dependent is computed from the parent's **old** contents — silent wrong data, which is the one
   failure direction this package must never take.

   Of the two resolutions:
   * **Dependency-ordered burst** (swap A → compute B → swap B) is correct but gives most of the
     benefit back: A's root stays locked from A's swap through B's entire computation to commit. On
     the measured chain that is **~85 % of the window retained**, and the absolute figure scales
     with the dependents' cost, which in production chains dominates.
   * **Have B read A's swap tables** via a `replace_source_with_transition`-style rewrite:
     **does not transfer.** That rewriter substitutes **one relation with one table**. Here A's
     post-swap contents are spread across N swap tables **plus** the live children that were not
     swapped — a heterogeneous union whose membership is only known mid-transaction, and which must
     be bound-correct per child or B's result silently changes. This is a materially different
     rewrite, not a reuse. **Inferred** from reading the rewriter; not prototyped.

**Storage, for completeness.** Measured on `v1`: holding all swap tables simultaneously is
**+76 MB** (target children 23 MB + intermediate children 53 MB), ~17 % of the 448 MB source.
Today's design already holds one partition's worth. Not a practical blocker at IMV scale, but it is
a full-IMV-sized transient.

**Deadlock risk, assessed as low.** `reflex_reconcile` acquires the two-key
`pg_advisory_xact_lock(hashtext($1), hashtext(reverse($1)))` transitively — `reconcile_one` calls
`reflex_sync_partitions_impl` first (`src/reconcile.rs:400`), which takes it at
`src/partition.rs:1116` — transaction-scoped, so it is held across the whole fill→swap window. Two
sessions maintaining the same IMV serialize; two sessions maintaining different IMVs touch disjoint
relations. A stable DDL ordering (by qualified name) is still worth having as defence in depth.

---

## 7. Dead end: taking a weaker lock early and upgrading — do not re-explore

The recurring proposal is to take a weak lock (e.g. `ShareUpdateExclusive`) on the partitions about
to be replaced, so that a deferred burst is safe for dependent IMVs. **Three independent reasons it
cannot work, each verified:**

1. **`ShareUpdateExclusive` does not block writers.** **Measured on bare PG 17.7:** with one session
   holding `LOCK TABLE dl IN SHARE UPDATE EXCLUSIVE MODE`, a second session's
   `INSERT INTO dl VALUES (1)` succeeded immediately (`INSERT 0 1`), as did a `SELECT`. The only
   mode that blocks writers while admitting readers is `SHARE` — **measured:** under
   `LOCK TABLE dl IN SHARE MODE` the `INSERT` was refused and the `SELECT` succeeded.
2. **The dependent-staleness problem is visibility, not concurrency.** A lock cannot make B *see* a
   not-yet-attached swap table; it can only make B wait. If A's swap has not happened, B computes
   from A's old rows regardless of what mode anyone holds.
3. **A transaction never blocks itself.** The writes at risk are our own cascade updates later in
   the same transaction, so no lock we take can guard them.

**And the active hazard — lock upgrades deadlock.** **Measured on PG 17.7**, two sessions each
running `BEGIN; LOCK TABLE dl IN SHARE MODE; …; LOCK TABLE dl IN ACCESS EXCLUSIVE MODE; COMMIT;`:

```
ERROR:  deadlock detected
DETAIL: Process 11539 waits for AccessExclusiveLock on relation 160803; blocked by process 11532.
        Process 11532 waits for AccessExclusiveLock on relation 160803; blocked by process 11539.
```

"Take a weak lock early, upgrade at commit" is therefore **strictly worse** than acquiring
`AccessExclusive` at the point of need: it converts a wait into a deadlock.

**Where locking *is* the right tool:** the **cross-session** case — another session's reflex
maintenance writing to A between our fill and our swap. That is real, and is already handled by the
two-key `pg_advisory_xact_lock(hashtext(name), hashtext(reverse(name)))`, confirmed in §6 to be held
for the whole fill→swap window.

**Conclusion to carry forward: the dependent-IMV constraint is an ordering problem, not a locking
one.** The only two candidate resolutions remain (a) process in dependency order, or (b) have B read
A's swap tables via a source-substitution rewrite — and (b) does not follow from the existing
rewriter.

---

## 8. Acceptance criteria for whichever option is built

With a partitioned IMV at mirror depth 1 **and** depth 2, each with at least one dependent IMV, and
a second session polling `SELECT count(*) FROM <imv> WHERE <partkey> = <untouched value>` at
`lock_timeout='2s'`:

1. Under the chosen mode, neither `reflex_reconcile(<imv>)` nor an ordinary bulk `UPDATE` that
   dirties every partition may place `AccessExclusiveLock` on the IMV **root**, at either depth, and
   the reader must never block.
2. `reflex_reconcile_partition(<imv>, <key>)` must not place `AccessExclusiveLock` on any
   **dependent** IMV's root.
3. Correctness by the bidirectional `EXCEPT ALL` / `assert_imv_correct` oracle at every step — the
   whole point of the DML path is that it is correctness-neutral, and that must be pinned, not
   assumed.
4. All assertions must be shown to go **RED** when the mode is disabled. They must sample `pg_locks`
   **from a second session**; a same-session probe sees its own locks and is a false green.
5. The fixture must assert `reltuples > 0` on the source children before exercising the dispatch —
   `reltuples` is `-1` before `ANALYZE` and `GREATEST(-1, 1000) = 1000`, so an unanalysed fixture
   silently classifies everything cold and the test passes for the wrong reason.
