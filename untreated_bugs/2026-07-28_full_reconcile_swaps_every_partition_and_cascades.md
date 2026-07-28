# 2026-07-28 — a full `reflex_reconcile` of a partitioned IMV holds `AccessExclusive` on the IMV **root** from the first child's DETACH to COMMIT, and the dependent cascade freezes the whole IMV closure

**Status: PARTIALLY FIXED — narrowed at integration.** `reflex_reconcile` now resolves mirror
leaves, so each swap DETACHes from the leaf's immediate parent. At mirror depth ≥ 2 that is a
branch and the IMV root is never taken `AccessExclusive` — pinned by
`full_reconcile_never_locks_imv_root_depth2`, which asserts both directions (no
`AccessExclusiveLock` on the root, `AccessExclusiveLock` present on the branch).

**Residual, still open:**

* **At mirror depth 1** the leaf's immediate parent IS the root, so a full reconcile still holds
  `AccessExclusive` on it to commit and every reader blocks.
* **Even at depth ≥ 2 the reconcile is not reader-free** — plan-time expansion locks the branches
  a query reaches, so a reader still blocks behind whichever branch is mid-swap.
* **The cascade / dependent half of this report is untouched.** See
  `2026-07-28_partitioned_reconcile_destroys_dependent_imvs.md`, which is the more severe
  finding on that path (destruction, not staleness).
* The **superlinearity** measured below is pre-existing and unchanged by the fix: the reconcile
  path fits O(N^1.42) both before and after (measured at N = 10/50/100), and the leaf-resolution
  change adds a flat ~5%. The likely cause is tracked separately in
  `2026-07-25_partition_swap_orphan_probe_quadratic.md`.

The decision document for the reader-blocking requirement is
`2026-07-28_reader_priority_mode_for_central_imvs.md`.

Original report follows.

**Mechanism confirmed by direct measurement** (PostgreSQL 16.11 under pgrx,
pg_reflex 1.11.2 @ `2f8b786`; bare PostgreSQL 17.7 for the lock matrix and the dead-end probes).
Availability defect, **no correctness risk on this path**. Sibling of
`2026-07-27_sync_partition_add_holds_accessexclusive_on_imv_root.md`, which explicitly deferred
this case ("Out of scope: making the *reconcile of an existing partition* reader-free … File
separately if it matters"). It matters.

Every number below is labelled **measured** or **inferred**. Raw traces were produced by sampling
`pg_locks` at 100 ms from a second session while the maintenance transaction was open, with a
third session polling a reader at `lock_timeout='2s'`.

---

## 1. Verdicts

| # | Hypothesis | Verdict |
|---|---|---|
| H1 | A full `reflex_reconcile` of a partitioned IMV ends with every partition branch locked until commit | **CONFIRMED, and worse than stated** — the lock lands on the **root**, not on branches |
| H2 | The doc comment at `src/reconcile.rs:424-431` is false | **CONFIRMED false**, at both depth 1 and depth 2 |
| H3 | Reachable from an ordinary bulk load, via `lib.rs:1312-1325` Path B | **Conclusion right, mechanism wrong** — `lib.rs:1312` is not a bulk-load path. The real route is the dispatch trip-cap |
| H4 | Amplifies across chained IMVs | **CONFIRMED for the cascade path; REFUTED for full `reflex_reconcile`** (which does not cascade at all) |
| H4-sub | Each hop evaluates Path B against a different denominator | **REFUTED as stated** — the relevant gate is per-partition, per-IMV, and evaluated against each child's own `reltuples` |

---

## 2. Mechanism

### 2.1 The swap loop always DETACHes from the root

`src/reconcile.rs:445-449` walks the source's partition children and swaps each:

```rust
// Walk every source partition child and swap each.
let src_children = crate::partition::list_partition_children(client, &plan.anchor_source);
for src in &src_children {
    ... execute_partition_swap_for_child(client, view_name, &schema, &src.bare_name, ...)
}
```

`list_partition_children` (`src/partition.rs:158-191`) returns **immediate children only**
(`WHERE i.inhparent = to_regclass($1)`). So `src_child_bare` is always a *top-level* source child,
the derived target child is always a *top-level* IMV child, and
`read_immediate_parent_qual` (`src/partition.rs:1977-1979`) therefore always resolves to the IMV
**root**. The statements built at `src/partition.rs:1988-2001` are

```
ALTER TABLE <IMV root>              DETACH PARTITION <top-level child>
ALTER TABLE <intermediate root>     DETACH PARTITION <top-level int child>
```

**Measured on bare PostgreSQL 17.7** (`lockprobe` db, no extension): a plain
`ALTER TABLE parent DETACH PARTITION child` takes `AccessExclusiveLock` on **both** the child and
the **parent**, and PostgreSQL holds DDL locks to **commit**, not to end-of-statement.

Therefore the **first** child's DETACH locks the IMV root `AccessExclusive` for the remainder of
the transaction — every `SELECT` on the IMV blocks, including one pruning to a partition that has
not been touched yet and one pruning to a partition that will never be touched.

This is a different, strictly worse lock than the one `reflex_reconcile_partition` takes. That
primitive resolves to *leaves* (`src/partition.rs:1578-1610`) and so locks the swapped leaf's
immediate parent — which at depth ≥ 2 is a branch, not the root. `reflex_reconcile` bypasses that
entirely.

### 2.2 The load-bearing comment is false

`src/reconcile.rs:424-431`:

> This keeps the AccessExclusiveLock window on the parent to per-child DDL only (**microseconds**)
> instead of holding it for the entire rebuild duration. **Readers pruning to a not-yet-swapped
> partition stay live throughout.**

Both sentences are false, and this comment is the entire stated justification for choosing the
swap design over the `TRUNCATE`-on-parent design it replaced. Measured windows: **4.39 s of a
5.31 s transaction** (depth 1) and **5.41 s of a 7.73 s transaction** (depth 2). Readers pruning to
a not-yet-swapped partition did **not** stay live; they blocked from the first child's DETACH.

### 2.3 How an ordinary bulk load reaches it (H3, corrected)

The brief's anchor, `src/lib.rs:1312-1325`, is inside
`public.reflex_apply_partition_delta(_imv, _source, _op, _child, _trans)` — the helper that applies
an **attached/detached partition child to an UNPARTITIONED IMV**. It is a DDL-driven path, and for
an unpartitioned IMV `reflex_reconcile` never enters the swap branch at all. That anchor does not
reach this defect.

The real routes into `reflex_reconcile(<partitioned IMV>)` from ordinary DML are the **trip-caps**
in the partition-aware trigger dispatch:

* aggregate dispatch — `src/trigger/dispatch.rs:337-341`
* passthrough dispatch — `src/trigger/dispatch.rs:566-570`

```
IF _hot_count > _partition_total / 2 THEN
    PERFORM public.reflex_reconcile('{view}');
    RETURN;
END IF;
```

with hotness classified at `src/trigger/dispatch.rs:326-328`:

```
(pc.dirty::NUMERIC / GREATEST(c.reltuples::NUMERIC, _floor::NUMERIC) >= _thr) AS hot
```

and `_thr` / `_floor` resolved at `src/trigger/dispatch.rs:276-279` from
`__reflex_ivm_reference.wipe_threshold` → GUC `reflex.wipe_threshold` → compiled `0.5`, and
`wipe_floor_rows` → GUC `reflex.wipe_floor_rows` → compiled `1000`.

**Threshold arithmetic, explicitly.** With defaults, a child is *hot* when
`dirty_rows / max(child.reltuples, 1000) >= 0.5`, i.e. when a statement changes at least half of a
partition's rows. When **more than half the partitions** are hot, the trip-cap fires and the entire
IMV is rebuilt by the swap loop above. Below that, hot children still swap — via
`reflex_reconcile_partition` at `src/trigger/dispatch.rs:344-347` — which at mirror depth 1 also
lands `AccessExclusive` on the root (§4.1, run L1).

`reltuples` is `-1` on a never-`ANALYZE`d relation, and `GREATEST(-1, 1000) = 1000`, so an
unanalysed fixture with < 500 dirty rows per child silently classifies everything cold and
reproduces nothing. **The reproduction below therefore `ANALYZE`s.**

### 2.4 The cascade (H4)

`reflex_reconcile` itself does **not** cascade to dependent IMVs — the partitioned branch returns
at `src/reconcile.rs:507` before any dependent handling, and `reflex_reconcile_with_orphans`
(`src/reconcile.rs:804-847`) only recurses *downward* into generated sub-IMVs. **Measured:** after
`SELECT reflex_reconcile('v1')`, the `last_update_date` of all three dependents was unchanged; the
only signal was a `WARNING: source table public.v1 was altered; IMV <dep> may be stale — run
SELECT reflex_rebuild_imv(...)`. So the chain does not amplify through this entry point; it simply
stops.

> **Follow-up investigation has since shown this is worse than "stops".** The dependents are not
> merely left stale — a full `reflex_reconcile` of a partitioned IMV **empties them and corrupts
> their partition mirror**. Filed as
> `untreated_bugs/2026-07-28_partitioned_reconcile_destroys_dependent_imvs.md`.

The amplification is real on the **partition-scoped** entry point. `reflex_reconcile_partition_impl`
cascades at `src/partition.rs:1805-1828`, choosing per dependent:

1. `same_part` (dependent partitioned on the same column) → `reflex_reconcile_partition(dep, keys)`
   → **swap → `AccessExclusive` on the dependent's root/branch**;
2. `build_scoped_cascade_reconcile` (`src/partition.rs:1858-1919`) → literal-pruned
   `DELETE`+`INSERT`, **no DDL, reader-free**;
3. otherwise → `reflex_reconcile(dep)` → for an unpartitioned dependent the `TRUNCATE` path
   (**`AccessExclusive`**, measured §"lock matrix"); for a partitioned dependent, this whole defect
   again.

**How often does the safe path (2) actually rescue this? Measured: never, in the plain
two-hop topology.** See §4.3. The reason (inferred from `src/create_ivm/mod.rs:664-740`) is that
**auto-mirroring makes paths (1) and (2) mutually exclusive**: when exactly one real source is
partitioned and the partition column is a bare projected output column, `create_reflex_ivm`
*automatically* partitions the dependent on that column. But "the partition column is a bare
projected output column" is essentially the same predicate that
`build_scoped_cascade_reconcile` tests (`group_by_columns` contains `part_col`), and that function
returns `None` the moment `dep_partition_cols` is non-empty (`src/partition.rs:1866-1868`).
So a dependent that qualifies for the reader-free path has already been auto-partitioned out of it.
The residual reachable cases (inferred) are dependents reading **two or more partitioned sources**
(`partitioned_sources.len() != 1` at `src/create_ivm/mod.rs:673` declines auto-mirroring) and
dependents whose partition column is projected through an expression at the parent hop but bare in
the `GROUP BY` of the dependent.

### 2.5 H4-sub — refuted

The brief's sub-question (each hop deciding Path B against its own `_source::regclass` `reltuples`,
so a delta could stay incremental at hop 1 and escalate at hop 2 where the denominator is smaller)
does not apply: that `reltuples` read is in `reflex_apply_partition_delta`
(`src/lib.rs:1315`), the DDL helper of §2.3, not in the DML dispatch. The DML dispatch's denominator
is **per partition child** (`c.reltuples` of the intermediate child, `dispatch.rs:327`), and each
dependent's dispatch runs against its own children. The multi-hop escalation that was *measured*
has a simpler cause: the cascade at `partition.rs:1805` hands the dependent straight to
`reflex_reconcile_partition` / `reflex_reconcile` with **no threshold test at all**.

---

## 3. Reproduction (verified to reproduce as written)

PostgreSQL 16.11, pg_reflex 1.11.2. `~9 M` source rows; expect ~2 min including load.

```sql
CREATE TABLE s1 (k TEXT NOT NULL, bucket INT NOT NULL, id BIGINT, amt NUMERIC)
  PARTITION BY LIST (k);
CREATE TABLE s1_a PARTITION OF s1 FOR VALUES IN ('A');
CREATE TABLE s1_b PARTITION OF s1 FOR VALUES IN ('B');
CREATE TABLE s1_c PARTITION OF s1 FOR VALUES IN ('C');
CREATE TABLE s1_d PARTITION OF s1 FOR VALUES IN ('D');
CREATE TABLE s1_e PARTITION OF s1 FOR VALUES IN ('E');
CREATE TABLE s1_f PARTITION OF s1 FOR VALUES IN ('F');

INSERT INTO s1 (k, bucket, id, amt)
SELECT k, (g % 50000), g, (g % 97)::numeric
FROM generate_series(1, 1500000) g
CROSS JOIN (VALUES ('A'),('B'),('C'),('D'),('E'),('F')) v(k);

ANALYZE s1;                              -- REQUIRED: reltuples = -1 otherwise

SELECT create_reflex_ivm(
  'v1',
  'SELECT k, bucket, SUM(amt) AS total, COUNT(*) AS n FROM s1 GROUP BY k, bucket',
  NULL, NULL, NULL, NULL, ARRAY['k']);
ANALYZE v1;
```

Session B, continuously, on a partition the operator would expect to be unaffected:

```sql
SET lock_timeout = '2s';
SELECT count(*) FROM v1 WHERE k = 'F';   -- 15-20 ms when idle
```

Session A, either form:

```sql
SELECT reflex_reconcile('v1');           -- direct
-- or, with no DDL and no reflex call at all:
UPDATE s1 SET amt = amt + 1;             -- 6/6 children hot > 6/2 -> trip-cap -> full reconcile
```

Session C, sampling `pg_locks` every 100 ms:

```sql
SELECT clock_timestamp(), l.relation::regclass::text, l.mode
  FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
 WHERE l.locktype = 'relation' AND l.mode = 'AccessExclusiveLock'
   AND a.pid <> pg_backend_pid();
```

---

## 4. Measured results

### 4.1 Depth 1 — `v1`, 6 partitions, 9 M source rows, 300 k IMV rows

`SELECT reflex_reconcile('v1')`: **11:44:17.621 → 11:44:22.933 = 5.31 s.**

| relation | first `AccessExclusive` | released |
|---|---|---|
| `__reflex_intermediate_v1` (root) | 11:44:18.403 | 11:44:22.929 (commit) |
| **`v1` (root)** | **11:44:18.536** | **11:44:22.929 (commit)** |
| `v1_s1_a` | 11:44:18.536 | commit |
| `v1_s1_b` | 11:44:19.313 | commit |
| `v1_s1_c` | 11:44:20.077 | commit |
| `v1_s1_d` | 11:44:21.033 | commit |
| `v1_s1_e` | 11:44:21.840 | commit |
| `v1_s1_f` | 11:44:22.660 | commit |

The root is locked **0.92 s into the transaction** and stays locked for **4.39 s = 83 % of it**.

Reader on `k = 'F'` — the **last** child swapped, untouched until t+5.0 s: normal latency
13-20 ms; blocked continuously from 11:44:18.47 to commit; **1 hard `lock_timeout` cancellation**
plus one 2.02 s wait that only completed at commit.

**Run L1 — no `reflex_*` call, no DDL, one partition only.** `UPDATE s5 SET amt = amt+1 WHERE k='A'`
on a 6 × 200 k fixture with default threshold. Server log:
`pg_reflex partition dispatch: hot=1 total=6 thr=0.5 floor=1000` →
`1 hot partitions for v5 → reflex_reconcile_partition`. Sampled locks include
**`v5 AccessExclusiveLock`**. At mirror depth 1, changing **one** partition freezes the whole IMV.

**Run L3 — trip-cap.** `UPDATE s5 SET amt = amt + 1` (all 6 children hot). Server log:
`INFO: pg_reflex: reconciled IMV 'v5' (partitioned, 6 children swapped)` — the trip-cap fired and
called full `reflex_reconcile`. 9.01 s, root `AccessExclusive`.

### 4.2 Depth 2 — `v4`, passthrough over a `LIST(k) → RANGE(d)` source, 4 branches × 3 leaves, 2.8 M rows

Created with `partition_by => ARRAY['k','d']` (a single-column `partition_by` yields a **depth-1**
mirror regardless of source depth — `partition_depth = partition_columns.len()`,
`src/create_ivm/mod.rs:649`).

`SELECT reflex_reconcile('v4')`: **11:48:16.553 → 11:48:24.294 = 7.73 s.**

| relation | first `AccessExclusive` | released |
|---|---|---|
| **`v4` (root)** | **11:48:18.808** | **11:48:24.219 (commit)** |
| `v4_s3_a` + its 3 leaves | 11:48:18.810 | commit |
| `v4_s3_b` + its 3 leaves | 11:48:20.578 | commit |
| `v4_s3_c` + its 3 leaves | 11:48:22.471 | commit |
| `v4_s3_d` + its 3 leaves | 11:48:24.083 | commit |

Root locked for **5.41 s = 70 %** of the transaction. Reader pruning to
`k='D' AND d='2026-03-05'` — a branch not touched until t+7.5 s — took **2 consecutive
`lock_timeout` cancellations**.

**This refutes the brief's depth-≥2 expectation.** Readers do *not* "drain gradually as branches
lock in turn": because the full-reconcile loop DETACHes **top-level** children from the root, the
first DETACH blocks everyone immediately, exactly as at depth 1. Depth buys nothing on this path.

> A separate and more serious defect was discovered while running this experiment: the depth-2
> mirror does not survive the reconcile, and a later `reflex_sync_partitions` then **empties the
> IMV**. Filed as
> `untreated_bugs/2026-07-28_swap_flattens_subpartitioned_child_then_sync_empties_imv.md`.

### 4.3 Chained IMVs — the cascade freezes the whole closure

Dependents of `v1`, all created with the plain 3-argument `create_reflex_ivm` (no `partition_by`):

| IMV | definition | `partition_columns` after create |
|---|---|---|
| `b_same` | `SELECT k, SUM(total) AS t FROM v1 GROUP BY k` | `{k}` — **auto-mirrored** |
| `c_other` | `SELECT bucket, SUM(total) AS t FROM v1 GROUP BY bucket` | `∅` |
| `d_part` | `SELECT k, bucket, SUM(total) AS t FROM v1 GROUP BY k, bucket` (explicit `ARRAY['k']`) | `{k}` |

`SELECT reflex_reconcile_partition('v1','B')` — **one partition of one IMV** — 1.70 s:

| relation | first `AccessExclusive` | released |
|---|---|---|
| `__reflex_intermediate_v1`, `…_v1_s1_b` | 11:59:23.754 | 11:59:24.559 (commit) |
| **`v1`**, `v1_s1_b` | 11:59:23.880 | commit |
| **`b_same`**, `b_same_v1_s1_b`, `__reflex_intermediate_b_same`, `…_b_same_v1_s1_b` | 11:59:23.880 | commit |
| **`c_other`**, `__reflex_intermediate_c_other` | 11:59:24.014 | commit |
| **`d_part`**, `d_part_v1_s1_b`, `__reflex_intermediate_d_part`, `…_d_part_v1_s1_b` | 11:59:24.014 | commit |

**Every IMV root in the dependent closure went `AccessExclusive`, all released only at commit.**
`b_same` and `d_part` via cascade path (1) (swap); `c_other` via path (3), whose
`INFO: pg_reflex: reconciled IMV 'c_other'` and root-level `AccessExclusive` are the unpartitioned
`TRUNCATE` rebuild. **The reader-free scoped-cascade path (2) was taken by none of them.**

Blast radius, therefore: not "one IMV's partitions" but **the transitive closure of dependent IMVs**,
each frozen root-wide, for the duration of the slowest hop.

### 4.4 Lock matrix (bare PostgreSQL 17.7, `lockprobe` db, no extension) — all measured

| statement | locks taken |
|---|---|
| `DELETE` + `INSERT` on a leaf | `RowExclusive` on the leaf, `AccessShare` on the root — **does not conflict with readers** |
| `TRUNCATE <leaf>` | `AccessExclusive` (+`Share`) — **not a substitute for the DML path** |
| `ALTER TABLE parent DETACH PARTITION child` | **`AccessExclusive` on child AND on parent** |
| `ALTER TABLE parent ATTACH PARTITION child` | `ShareUpdateExclusive` on parent |
| `ALTER TABLE … DISABLE TRIGGER USER` | `ShareRowExclusive` — measured not to block readers (0 blocked iterations in §5.2) |
| `LOCK TABLE … IN SHARE UPDATE EXCLUSIVE MODE` | does **not** block `INSERT` (measured: the `INSERT` succeeded) or `SELECT` |
| `LOCK TABLE … IN SHARE MODE` | blocks `INSERT`, allows `SELECT` |

---

## 5. Options

> The reader-lock findings in this section are consolidated, with the requirement they serve and the
> recommendation, in `untreated_bugs/2026-07-28_reader_priority_mode_for_central_imvs.md`. That note
> is the decision document; this section is its evidence base.

### 5.1 The lever available today, with no code change

**Yes, a partitioned IMV can be made reader-free today for ordinary DML — by disabling the wipe
escalation.** The hot/cold classifier and the trip-cap read the *same* `_thr`
(`dispatch.rs:276-279`, `:326-328`, `:337-341`), so one knob governs both, and
`__reflex_ivm_reference.wipe_threshold` carries no `CHECK` constraint (`src/lib.rs:127`), so an
arbitrarily large value is accepted:

```sql
SELECT reflex_set_wipe_threshold('<imv>', 1000000000::numeric);   -- per IMV
-- and on every dependent IMV in the closure
```
(`reflex_set_wipe_floor_rows` is a second, equivalent knob — it inflates the denominator.
`SET reflex.wipe_threshold` works session-wide but only for maintenance done in that session.)

**Measured, run L4** — identical whole-table `UPDATE` to run L3, `wipe_threshold = 1e9`:
`pg_reflex partition dispatch: hot=0 total=6 thr=1000000000 floor=1000`; **no
`AccessExclusiveLock` on `v5` or its intermediate at any sample**; 9.53 s vs 9.01 s for the swap
path — **6 % slower on the case most favourable to the swap** (every row of every partition
changed).

**Measured, chained** — `wipe_threshold = 1e9` on `v1`, `b_same`, `c_other`, `d_part`;
`UPDATE s1 SET amt = amt+1 WHERE k='C'` (1.5 M rows): `hot=0 total=6`; **zero
`AccessExclusiveLock` rows across the entire closure** (only `ShareUpdateExclusive` from `ANALYZE`);
**zero blocked reader iterations**; 14.13 s.

**What the lever does NOT cover — measured.** With `wipe_threshold = 1e9` still set on `v5`,
`ALTER TABLE s5 ATTACH PARTITION s5_g_new FOR VALUES IN ('G')` still produced
**`v5 AccessExclusiveLock`**. The DDL path (`ddl_command_end` sync + the `DEFERRABLE INITIALLY
DEFERRED` COMMIT-time flush, `src/lib.rs:1256`ff) never consults `wipe_threshold`. Nor do explicit
`reflex_reconcile` / `reflex_rebuild_imv` / `reflex_doctor` repairs. That gap is the subject of the
sibling report.

**Answer, plainly: yes for ordinary DML, no for partition DDL and no for manual repairs.**
Cost measured at 6 % on a worst case; the honest caveat is that the MERGE path's cost grows with
delta size while the swap's grows with partition size, so on a *small* delta the lever is free and
on a *whole-partition* delta it is roughly break-even — the crossover the threshold was invented to
find. Setting it to `1e9` gives up that optimisation permanently, which is the intended trade.

### 5.2 The DML path — genuinely zero reader lock

pg_reflex already contains this machinery: `build_scoped_cascade_reconcile`
(`src/partition.rs:1858-1919`, reached at `:1812`) emits exactly
`DELETE … WHERE <key> IN (…)` / `INSERT …` against the intermediate and the target, no DDL.

**Measured** — a hand-run of that exact statement shape rebuilding **all 6** partitions of `v1`
(9 M source rows re-aggregated):

| | swap (`reflex_reconcile`) | DML (`DELETE`+`INSERT`) |
|---|---|---|
| wall clock | 5.31 s / 5.72 s | **7.04 s** |
| root `AccessExclusive` | 4.39 s | **none — 0 samples** |
| blocked reader iterations | 1 timeout + 1 near-timeout | **0** |

≈ **25 % slower** for a genuine full rebuild, and **reader-free**. Bloat is the real cost and is
**unquantified**: each rebuild leaves one dead tuple per row in both the target and the
intermediate (`v1` target children 23 MB, intermediate children 53 MB on this fixture), so a
full-partition DML rebuild roughly doubles both until autovacuum catches up. `TRUNCATE` is **not**
an escape (§4.4).

### 5.3 The deferred DDL burst — brief, not zero

Because the fill already happens on a detached table (`create_swap_*` / `fill_swap_*` at
`src/partition.rs:2005-2049`), batching all DETACH/ATTACH/DROP/RENAME to the end of the transaction
is a reordering of existing statements, not a new mechanism.

**Measured** — the 6 swap tables of `v1` pre-filled outside the timed window, then the burst:

* burst wall clock **0.865 s** (11:54:49.161 → 11:54:50.026)
* root `v1` `AccessExclusive` **11:54:49.194 → 11:54:50.020 = 0.83 s**
* reader on `k='F'`: one iteration stalled **0.850 s**; no timeout at 2 s

versus **4.39 s** on the identical fixture today — a **5.3× reduction**, and the window becomes
independent of data volume.

**But it is O(partitions), not O(1).** The 4 DDL statements per child cost ~144 ms/partition here
(`DETACH` ~60 ms, `ATTACH` ~70 ms, `DROP` ~60 ms, `RENAME` ~17 ms), dominated by catalog work and
the physical unlink in `DROP`. Extrapolated (**inferred**): a 60-leaf monthly-range IMV would
freeze ~8.6 s; a 100-partition IMV ~14 s. For the maintainer's stated requirement — reader
blocking on a central IMV is unacceptable — **a burst is only acceptable on IMVs with few
partitions**, and its worst case is the same order as today's on wide fan-outs.

Constraints, assessed:

1. **Post-fill writes.** Real. The burst must be genuinely last. The `DEFERRABLE INITIALLY
   DEFERRED` constraint trigger on `__reflex_partition_pending` (`src/lib.rs:1256`ff) provides a
   natural end-of-transaction hook, but it is *itself* what invokes the reconcile, so it is a
   "last" point for DDL enqueued during the statement, **not** for work the reconcile's own cascade
   generates. **Inferred: insufficient on its own.**
2. **Dependent IMVs — the crux.** Fatal in the naive form. Measured (§4.3): the cascade runs
   *after* the parent's swap in `reflex_reconcile_partition_impl` and computes each dependent from
   the parent's table. Defer the parent's swap past that and every dependent is computed from
   pre-swap rows — **silent wrong data**, the one failure direction this package must never take.
   Of the two resolutions:
   * **Dependency-ordered burst** (swap A → compute B → swap B) is correct but gives most of the
     benefit back: A's root stays locked from A's swap through B's entire computation to commit. On
     the §4.3 chain that is 11:59:23.880 → commit, i.e. **~85 % of the window is retained**.
     Measured proportion; the absolute number scales with the dependents' cost, which in production
     chains dominates.
   * **Have B read A's swap table** via a `replace_source_with_transition`-style rewrite:
     **assessed as not reusable.** That rewriter substitutes *one* relation with *one* transition
     table. Here A's post-swap contents are spread over N swap tables **plus** the live children
     that were not swapped, so the substitution target is a heterogeneous union whose membership is
     only known mid-transaction — a materially different rewrite, and one that has to be
     bound-correct per child or it silently changes B's result. **Inferred from reading
     `src/trigger/`'s rewriter; not prototyped.**
3. **Storage.** Measured on `v1`: target children 23 MB + intermediate children 53 MB = **+76 MB**
   held simultaneously, ~17 % of the 448 MB source. Today's design already holds one partition's
   worth; the burst holds all of them. **Not a practical blocker at IMV scale**, but it is a full
   IMV-sized transient and should be stated in any design note.
4. **Deadlock.** Low. `reflex_reconcile` acquires the two-key
   `pg_advisory_xact_lock(hashtext($1), hashtext(reverse($1)))` transitively — `reconcile_one`
   calls `reflex_sync_partitions_impl` first (`src/reconcile.rs:400`), which takes it at
   `src/partition.rs:1116` — and it is transaction-scoped, so it is held across the whole
   fill→swap window. Two sessions maintaining the **same** IMV are serialized; two sessions
   maintaining **different** IMVs touch disjoint relation sets. A stable ordering (by qualified
   name) is still worth having as defence in depth. **Confirmed the advisory lock covers the
   window in question.**

### 5.4 `DETACH PARTITION CONCURRENTLY` — unavailable on every pg_reflex path

`DETACH CONCURRENTLY` takes only `ShareUpdateExclusive`, so it looks like the obvious fix. It is
not available:

```
BEGIN; ALTER TABLE lm DETACH PARTITION lm1 CONCURRENTLY;
ERROR:  ALTER TABLE ... DETACH CONCURRENTLY cannot run inside a transaction block
DO $$ BEGIN EXECUTE 'ALTER TABLE lm DETACH PARTITION lm1 CONCURRENTLY'; END $$;
ERROR:  ALTER TABLE ... DETACH CONCURRENTLY cannot be executed from a function
```

**Measured on both PostgreSQL 17.7 (bare) and 16.11 (pgrx).** The second restriction is the
decisive one: every pg_reflex reconcile runs inside a SQL-callable function
(`#[pg_extern] fn reflex_reconcile`, `src/lib.rs:724`), so the restriction applies to the
**manually invoked** path too, not just the COMMIT-time one. There is no partial fix here. This
closes the idea completely.

### 5.5 Comparison

| option | reader lock | correctness risk | complexity | cost | available |
|---|---|---|---|---|---|
| **`wipe_threshold` = huge** (config) | **none** for DML; unchanged for partition DDL and manual repairs | none | zero | **+6 %** measured worst case | **today** |
| **DML path everywhere** (opt-in per IMV) | **none** | none — path already trusted for cascade (2) | low: reuse `build_scoped_cascade_reconcile`'s statement shape, add a per-IMV mode column | **+25 %** measured, plus unquantified bloat/vacuum | code change |
| **Deferred DDL burst** | **~0.14 s × N partitions** (0.83 s at N=6, ~14 s at N=100 inferred) | **high in the naive form** — dependents computed from pre-swap rows; needs dependency ordering, which returns ~85 % of the window | medium; plus a full-IMV-sized transient | fastest | code change |
| **`DETACH CONCURRENTLY`** | would be none | — | — | — | **impossible** (§5.4) |

**Recommendation.** Ship nothing clever. In priority order:

1. **Document the `wipe_threshold` lever now** (release notes / README), including that it must be
   set on every IMV in the closure and that it does not cover partition DDL. It is measured,
   costs 6 %, and needs no release.
2. **Add a per-IMV `reader_priority` (force-DML) mode, defaulting off** — a new
   `__reflex_ivm_reference` column that makes `reflex_reconcile`'s partitioned branch and the
   cascade's paths (1) and (3) use the `DELETE`+`INSERT` statement shape instead of the swap. This
   is worth the complexity under CLAUDE.md's ordering: it is *correctness-neutral* (the DML shape
   is already the trusted cascade path), it is *simple* (a mode flag selecting between two existing
   statement builders — no new mechanism), and it costs 25 % only for IMVs that opt in. It is the
   only option that satisfies "reader blocking is unacceptable" without qualification.
3. **Reject the deferred burst** unless someone first solves the dependent-ordering problem
   without giving the window back. Its best case (0.83 s at N=6) is worse than the DML path's zero,
   its worst case (N large) is the same order as today, and its failure mode is silent wrong data.
4. **Fix the false comment at `src/reconcile.rs:424-431` regardless.** It currently tells the next
   reader that this path is already solved.

A cheap partial fix worth considering independently: make the full-reconcile loop resolve **leaves**
the way `reflex_reconcile_partition_impl` does (`src/partition.rs:1578-1610`) instead of top-level
children. That does not remove the lock, but at mirror depth ≥ 2 it moves it from the root to the
branches, which is what the §2.2 comment already claims happens.

---

## 6. Dead ends — do not re-explore

**Taking a weaker lock (e.g. `ShareUpdateExclusive`) on the partitions about to be replaced, to
make the deferred burst safe for dependent IMVs.** Three independent reasons, each verified:

1. **`ShareUpdateExclusive` does not block writers.** Measured on bare PG 17.7: with one session
   holding `LOCK TABLE dl IN SHARE UPDATE EXCLUSIVE MODE`, a second session's
   `INSERT INTO dl VALUES (1)` succeeded immediately (`INSERT 0 1`), as did a `SELECT`. Blocking
   writers while admitting readers requires `SHARE` mode — measured: under `LOCK TABLE … IN SHARE
   MODE` the `INSERT` was refused and the `SELECT` succeeded.
2. **The dependent-staleness problem is visibility, not concurrency.** A lock cannot make B *see*
   a not-yet-attached swap table; it can only make B wait. If A's swap has not happened, B computes
   from A's old rows no matter what mode is held.
3. **A transaction never blocks itself.** The writes at risk are our own cascade updates later in
   the same transaction, so no lock we take can guard them.

**And the active hazard: lock upgrades deadlock.** Measured on PG 17.7 — two sessions each running
`BEGIN; LOCK TABLE dl IN SHARE MODE; …; LOCK TABLE dl IN ACCESS EXCLUSIVE MODE; COMMIT;`:

```
ERROR:  deadlock detected
DETAIL: Process 11539 waits for AccessExclusiveLock on relation 160803; blocked by process 11532.
        Process 11532 waits for AccessExclusiveLock on relation 160803; blocked by process 11539.
```

"Take a weak lock early, upgrade at commit" is therefore **strictly worse** than acquiring
`AccessExclusive` at the point of need: it converts a wait into a deadlock.

Where locking *is* the right tool: the **cross-session** case — another session's reflex
maintenance writing to A between our fill and our swap — is real, and is already handled by the
two-key `pg_advisory_xact_lock(hashtext(name), hashtext(reverse(name)))`, confirmed above
(§5.3.4) to be held for the whole fill→swap window.

**Conclusion to carry forward: the dependent-IMV constraint is an ordering problem, not a locking
one.** The only two candidate resolutions remain (a) process in dependency order, or (b) have B
read A's swap table via a source-substitution rewrite.

---

## 7. What was ruled out

* **"Path B in `reflex_apply_partition_delta` escalates ordinary bulk loads."** It does not — that
  function serves partition ATTACH/DETACH against **unpartitioned** IMVs (§2.3).
* **"A full `reflex_reconcile` cascades into dependents and compounds."** It does not cascade at
  all; it warns and leaves them stale (§2.4, measured).
* **"At depth ≥ 2 readers drain gradually."** They do not; the root is locked by the first child's
  DETACH at any depth (§4.2, measured).
* **"Each hop escalates against a smaller denominator."** Not the mechanism (§2.5).
* **"The scoped-cascade path usually rescues real topologies."** Measured: taken by none of three
  natural dependent shapes; auto-mirroring makes it and the swap path mutually exclusive (§2.4).
* **"`DETACH CONCURRENTLY` is available at least on the manual path."** It is not — it cannot run
  from a function at all (§5.4, measured on 16.11 and 17.7).
* **"`TRUNCATE` could replace the swap."** `TRUNCATE` takes `AccessExclusive` (§4.4, measured).
* **Correctness of the results themselves.** Row counts and values matched throughout; this is an
  availability defect on this path. (The *separate* correctness defect found alongside it is filed
  as its own report — see §4.2.)

---

## 8. Severity

**High availability impact, no correctness risk on this path.**

* Reachable with **no DDL and no `reflex_*` call** — an ordinary `UPDATE`/bulk load that dirties
  ≥ 50 % of a partition (`dispatch.rs:326-328`), which is the normal shape of a period reload.
* At mirror depth 1 — which is what a single-column `partition_by` produces, and what every
  aggregate IMV gets regardless of source depth (§4.2) — **one** hot partition is enough
  (run L1, measured).
* The freeze covers the IMV **root**, so partition pruning gives readers no protection.
* The freeze propagates to the **entire dependent IMV closure** (§4.3, measured).
* Duration is the rebuild duration: 4.4 s on a 9 M-row toy; the sibling report measured 13.7 s and
  23.7 s on production-shaped data.

---

## 9. Acceptance test

With a partitioned IMV at mirror depth 1 **and** depth 2, each with at least one dependent IMV,
and a second session polling `SELECT count(*) FROM <imv> WHERE <partkey> = <untouched value>` at
`lock_timeout='2s'`:

1. A full `reflex_reconcile(<imv>)` must never place `AccessExclusiveLock` on the IMV **root**, at
   either depth, and the reader must never block.
2. `reflex_reconcile_partition(<imv>, <key>)` must never place `AccessExclusiveLock` on any
   **dependent** IMV's root.
3. Both assertions must be shown to go **RED** when the fix is reverted (per the methodology, a
   green-under-mutation assertion here is a false green — note that these must assert on `pg_locks`
   sampled from a second session, since a same-session probe sees its own locks).
4. The `reltuples`/`ANALYZE` trap in §2.3 must be pinned by the fixture, not assumed: assert
   `reltuples > 0` on the source children before exercising the dispatch, or the test silently
   classifies everything cold and passes for the wrong reason.
