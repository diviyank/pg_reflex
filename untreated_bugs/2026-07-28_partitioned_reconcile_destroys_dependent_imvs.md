# 2026-07-28 — a full `reflex_reconcile` of a **partitioned** IMV does not propagate to its dependents; it **empties them and corrupts their partition mirror**

**Status: untreated. Reproduced deterministically from a clean database** (PostgreSQL 16.11 under
pgrx, pg_reflex 1.11.2 @ `2f8b786`). **Silent wrong data on every dependent IMV.**

Filed at the maintainer's request after the measurement in
`2026-07-28_full_reconcile_swaps_every_partition_and_cascades.md` §2.4 showed that
`reflex_reconcile` does not fan out to dependents the way `reflex_reconcile_partition` does
(`src/partition.rs:1805-1828`). Investigating *why* turned up something worse than staleness.

Every claim is marked **measured** or **inferred**.

---

## 1. Headline

The maintainer expected an IMV update to propagate to its children. For an **unpartitioned** parent
it does — the hypothesis that this is structurally impossible is **refuted** (§3). For a
**partitioned** parent it does not, and the dependent does not merely go stale: it ends the
transaction with **zero rows** and a partition set containing a child named after a swap table that
no longer exists.

| parent shape | rebuild mechanism | dependent after `reflex_reconcile(parent)` |
|---|---|---|
| unpartitioned | `TRUNCATE` + `INSERT` (`src/reconcile.rs:567-579`, `:702-713`) | **correct** — measured |
| partitioned | DETACH/ATTACH swap (`src/reconcile.rs:445-449`) | **emptied + mirror corrupted** — measured |

---

## 2. Reproduction (clean database, ~20 s)

```sql
CREATE EXTENSION pg_reflex;

CREATE TABLE p1 (k TEXT NOT NULL, bucket INT NOT NULL, amt NUMERIC) PARTITION BY LIST (k);
CREATE TABLE p1_a PARTITION OF p1 FOR VALUES IN ('A');
CREATE TABLE p1_b PARTITION OF p1 FOR VALUES IN ('B');
CREATE TABLE p1_c PARTITION OF p1 FOR VALUES IN ('C');
INSERT INTO p1 SELECT k, (g % 50), (g % 97)::numeric
  FROM generate_series(1,5000) g CROSS JOIN (VALUES ('A'),('B'),('C')) v(k);
ANALYZE p1;

SELECT create_reflex_ivm('pa','SELECT k, bucket, SUM(amt) AS total FROM p1 GROUP BY k, bucket',
                         NULL, NULL, NULL, NULL, ARRAY['k']);
SELECT create_reflex_ivm('pb','SELECT k, SUM(total) AS t FROM pa GROUP BY k');
ANALYZE pa; ANALYZE pb;
```

`pb` is created with the plain 3-argument form and is nevertheless **auto-partitioned** on `k`
(`INFO: pg_reflex: auto-mirroring partition column 'k' from source (depth Some(1))`,
`src/create_ivm/mod.rs:664-740`). This is the default shape, not a contrived one.

```sql
-- baseline: parent, dependent and the oracle agree
SELECT sum(total) FROM pa;                                                    -- 716661
SELECT sum(t)     FROM pb;                                                    -- 716661
SELECT sum(total) FROM (SELECT k,bucket,SUM(amt) AS total FROM p1 GROUP BY k,bucket) o;  -- 716661

-- drift the parent with its triggers ENABLED, so the dependent follows it
UPDATE pa SET total = total + 1;
SELECT sum(total) FROM pa;   -- 716811
SELECT sum(t)     FROM pb;   -- 716811   <- trigger propagation works for DML

SELECT reflex_reconcile('pa');
SELECT sum(total) FROM pa;   -- 716661   <- parent repaired
SELECT sum(t)     FROM pb;   -- NULL     <- dependent EMPTY
```

**Measured, exactly as printed above.** `count(*) FROM pb` = **0**;
`count(*) FROM __reflex_intermediate_pb` = **0**; `known_stale` on `pb` = **`f`**.

And the mirror is left structurally wrong:

```sql
SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = 'pb'::regclass ORDER BY 1;
```
```
 pb___reflex_swap_tgt_pa_p1_c     <- mirrors a swap table that no longer exists
 pb_pa_p1_a
 pb_pa_p1_b                       <- pb_pa_p1_c is GONE
```

while `pa`'s own children are the correct `pa_p1_a`, `pa_p1_b`, `pa_p1_c`.

Repeating `SELECT reflex_reconcile('pa')` re-empties `pb` every time (measured on a second run:
`pb` → 0 rows again). This is deterministic, not a race.

---

## 3. What the coordinator's reading got right and wrong

> `TRUNCATE` does not fire row triggers, and DETACH/ATTACH is DDL — so trigger-driven propagation
> is **structurally impossible** on that path.

* **Half right.** For the **partitioned** path the reasoning holds and is confirmed: the swap
  moves data with `CREATE TABLE … AS` into a *detached* table and then `DETACH`/`ATTACH`/`DROP`/
  `RENAME` (`src/partition.rs:2005-2060`). No DML ever touches the live target, so no data trigger
  can fire. Propagation there is indeed structurally impossible, not merely omitted.
* **Wrong for the unpartitioned path.** pg_reflex installs an **`AFTER TRUNCATE … FOR EACH
  STATEMENT`** trigger on every source (`src/schema_builder.rs:705-711` and `:834-839`, body in
  `sql/trigger_truncate_body.plpgsql.in`) precisely so a `TRUNCATE` propagates, and the following
  `INSERT` fires the `AFTER INSERT … FOR EACH STATEMENT … REFERENCING NEW TABLE` trigger.
  **Measured** on an unpartitioned parent `a_un` with dependent `b_un`: drift `a_un` 959307 →
  965307 (dependent follows to 965307), then `reflex_reconcile('a_un')` → `a_un` 959307 **and
  `b_un` 959307**. Propagation is implemented and works.

> Triggers are additionally suppressed deliberately in `reconcile_generated_child_without_propagating`.

**Confirmed** (`src/reconcile.rs:1144-1162`): `ALTER TABLE … DISABLE TRIGGER USER` around
`reconcile_one`, correctly, and only for *generated* sub-IMVs of the chain being rebuilt. It is not
the cause here — `pb` is a user-declared dependent and is never routed through that function.

---

## 4. Mechanism (inferred from code, corroborated by the NOTICE trail)

The swap loop issues, per child, `ALTER TABLE pa ATTACH PARTITION pa___reflex_swap_tgt_pa_p1_c …`
(`src/partition.rs:1996-2001`). Every one of those `ALTER TABLE`s fires the `ddl_command_end` event
trigger `__reflex_on_ddl_command_end` (`src/lib.rs:1055`, event trigger at `:1251-1254`), whose
auto-sync branch runs `reflex_sync_partitions` on **every IMV that depends on `pa`** — while `pa`'s
child set is in its transient, mid-swap state.

The dependent's sync therefore:

* creates `pb___reflex_swap_tgt_pa_p1_c` to mirror the transient child, and
* drops `pb_pa_p1_c` as a **confirmed orphan** — observed verbatim on the larger fixture:
  `NOTICE: pg_reflex: dropped confirmed orphan partition 'b_same_v1_s1_f' (bounds matched incoming
  child 'b_same___reflex_swap_tgt_v1_s1_f')` — the swap's F3 orphan heal
  (`src/partition.rs:2060-2140`) calling `drop_bound_collision_orphan`
  (`src/partition.rs:1420`ff, NOTICE at `:1451`).

When the swap then renames `pa`'s child back to `pa_p1_c`, nothing revisits `pb`. The dependent is
left with a mirror of a vanished relation, and because the full reconcile never cascades
(`src/reconcile.rs:507` returns before any dependent handling), nothing refills it.

Note this makes the defect **self-inflicted**: pg_reflex's own DDL is what trips pg_reflex's own
orphan heal against a dependent.

---

## 5. The three questions asked

### 5.1 Which warning fires, and does it name a remedy?

**Exact text, measured** — emitted **8 times** for a 3-partition IMV (once per qualifying
`ALTER TABLE` on the tracked source):

```
WARNING:  pg_reflex: source table public.pa was altered; IMV pb may be stale —
          run SELECT reflex_rebuild_imv('pb') to recover
```

Source: `src/lib.rs:1237`, inside the **generic alter-source alarm** — the `warn`/`error` policy
branch for non-partition `ALTER TABLE` on tracked sources. It is **not** a purpose-built
dependent-staleness signal; it fires as a side effect of the swap issuing DDL on a relation that
happens to be another IMV's source.

**`rebuild_convergence_advisory` (`src/reconcile.rs:964`) is a different concern and must not be
conflated** — confirmed by reading it: it fires only for `ignore_sources`-on-partitioned
(archive residue) and matview-fed (`requires_explicit_refresh`) IMVs. Neither applies here, so it
does not fire. The coordinator's worry that the advisory might be the *only* signal is therefore
unfounded, but the actual signal is weaker than it looks:

* **It understates the damage by an order of magnitude.** "may be stale" describes drift. The
  dependent is *empty* and its partition mirror is *corrupted*.
* **It is an accident of the mechanism.** An unpartitioned parent's reconcile issues no
  `ALTER TABLE`, so no warning fires there — which is correct only because propagation happens to
  work on that path (§3). Nothing ties the warning to the condition it is standing in for.
* **It has a suppression hole (inferred, not measured).** The alarm is skipped when the dependent's
  name matches the in-flight reconcile root, including the pattern
  `split_part(_imv.name,'.',2) LIKE split_part(_reconcile_root,'.',2) || '\_\_%'`
  (`src/lib.rs:1226-1234`). A **user-declared** dependent named `<parent>__<something>` would
  therefore be destroyed with no warning at all. The suppression is aimed at generated sub-IMVs and
  cannot distinguish them from a user IMV that shares the naming shape.
* **Under the non-default `error` policy the reconcile is blocked outright.** Measured:
  ```
  SET pg_reflex.alter_source_policy='error';
  SELECT reflex_reconcile('pa');
  ERROR:  pg_reflex: ALTER blocked by pg_reflex.alter_source_policy='error' on tracked source(s);
          affected: public.pa -> pb
  HINT:   Set pg_reflex.alter_source_policy = 'warn' (default) or drop_reflex_ivm() first.
  ```
  The package's own primary recovery primitive is refused by the package's own guard, and the hint
  suggests **dropping the IMV**. Any operator running `alter_source_policy = 'error'` cannot
  reconcile a partitioned IMV that has dependents.

### 5.2 Does the prescribed remedy converge?

**Yes — measured.** `SELECT reflex_rebuild_imv('pb')` on the damaged IMV:

```
INFO:  pg_reflex: reconciled IMV 'pb' (partitioned, 3 children swapped)
pb rows | 3 | 716661        <- matches the oracle
pb children | pb_pa_p1_a, pb_pa_p1_b, pb_pa_p1_c   <- swap-table orphan gone, pb_pa_p1_c restored
```

One call, full convergence, mirror repaired. **CLAUDE.md's "don't print a remedy that can't clear
its own finding" rule is not violated.** The defect is that the operator must (a) see 8 identical
WARNINGs in a log, (b) read them as "your dependent is empty" rather than "may be stale", and
(c) act — for every dependent, after every reconcile of a partitioned parent.

### 5.3 How do damaged dependents get repaired today?

**`reflex_doctor` detects it** — measured, 5 findings on `pb` from a plain `SELECT * FROM
reflex_doctor()`:

```
F5/F6 WARNING pb  Partition pa_p1_a is empty but the IMV definition would populate it (archive residue)
                  -> SELECT reflex_reconcile_partition('pb', '', 'pa_p1_a');
F5/F6 WARNING pb  Partition pa_p1_b is empty but the IMV definition would populate it …
F3    WARNING pb  Intermediate is missing child partitions: __reflex_intermediate_pb_pa_p1_c
                  Target has unexpected child partitions: pb___reflex_swap_tgt_pa_p1_c …
                  -> SELECT reflex_sync_partitions('pb');
F3    WARNING pb  Partition drift: source leaf maps to IMV leaf 'pb_pa_p1_c' which is missing
F3    WARNING pb  Partition drift: IMV leaf 'pb___reflex_swap_tgt_pa_p1_c' has no source counterpart
```

Note the F5/F6 finding text says **"archive residue"**, which is the wrong diagnosis — this is
swap residue. An operator following that lead investigates `ignore_sources` and finds nothing.

**`reflex_doctor(fix => true, drop_orphans => true)` converges, but only on the second pass** —
measured:

| pass | outcomes | `pb` after |
|---|---|---|
| 1 | 2 × F5/F6 `fixed`; 3 × F3 `reported` (not fixed) | **2 rows, 477774 — silently wrong** |
| 2 | 1 × F5/F6 (`pa_p1_c`) `fixed` | **3 rows, 716661 — correct** |
| 3 | no findings | 716661 |

Pass 1 is the problem: it reports two findings **`fixed`**, the finding list shrinks, and the IMV is
left serving a wrong total with no indication. An operator who runs the doctor once and sees
progress will conclude the repair succeeded.

**Without a doctor run there is no sweeper.** `known_stale` stays `f`, `last_update_date` on the
dependent is not touched, and no background process revisits it. The damage is invisible until
someone reads wrong data or runs the doctor.

---

## 6. Severity

**High — silent wrong data on every dependent IMV of every partitioned IMV, from a routine
operation.**

* The trigger is `reflex_reconcile(<partitioned imv>)` — the first thing any operator reaches for,
  and also reachable with **no DDL and no `reflex_*` call** via the trigger-dispatch trip-cap
  (`src/trigger/dispatch.rs:337-341`, `:566-570`); see the companion report §2.3.
* The dependent shape that gets destroyed is the **default** one: `create_reflex_ivm` auto-mirrors
  the parent's partition column, so an ordinary `GROUP BY k` roll-up is partitioned and lands in
  this path.
* Failure mode is zero rows, not drift — a dashboard reads empty, a downstream join drops rows.
* `known_stale` is not set; the only automatic signal is a generic "may be stale" WARNING that
  understates the damage and has a naming-based suppression hole.
* Both remedies **do** converge, which caps the severity below the two data-loss reports filed
  today — but only for an operator who notices.

---

## 7. Fix direction

1. **Make the full reconcile cascade, like the partition-scoped one already does.** The fan-out at
   `src/partition.rs:1805-1828` exists and is tested; `reflex_reconcile`'s partitioned branch
   returns at `src/reconcile.rs:507` without it. Reusing that block — after the swap loop, with
   `affected_keys` = all keys — repairs the dependents in the same transaction. This is the
   smallest correct change.
2. **Stop the swap's DDL from re-syncing dependents mid-swap.** The auto-sync in
   `__reflex_on_ddl_command_end` already ignores `__reflex_`-owned relations for the *pending queue*
   (`NOT LIKE '%__reflex_%'`); the dependent auto-sync branch needs the same guard, so that an
   `ALTER TABLE <imv> ATTACH PARTITION <imv>___reflex_swap_tgt_…` does not cause a dependent to
   mirror a transient name and drop its real child. Without this, fix 1 alone still leaves the
   `pb___reflex_swap_tgt_*` orphan for the cascade to clean up.
3. **Fix the F5/F6 finding text**: "archive residue" is a specific diagnosis and is wrong for this
   shape. It should name swap residue, or be generic.
4. **Make `reflex_doctor(fix => true)` converge in one pass, or say it did not.** Reporting `fixed`
   while the IMV is still wrong is the failure mode CLAUDE.md's remedy rule exists to prevent, even
   though a second pass does converge. Simplest: after applying repairs, re-run the checks and
   report the residual, or count a pass that left findings as `partial`.
5. **Consider whether `alter_source_policy = 'error'` should exempt pg_reflex's own maintenance
   DDL.** Today it turns `reflex_reconcile` on a partitioned IMV with dependents into a hard error
   whose hint recommends dropping the IMV.

Fixes 1 and 2 are the correctness fixes; 3-5 are operability.

---

## 8. What was ruled out

* **"Trigger propagation is structurally impossible after `TRUNCATE`."** Refuted — an
  `AFTER TRUNCATE … FOR EACH STATEMENT` trigger exists for exactly this
  (`src/schema_builder.rs:705-711`), and the unpartitioned path was measured to propagate correctly.
* **"`reconcile_generated_child_without_propagating` is suppressing the dependent."** No — it is
  only reached for *generated* sub-IMVs of the chain being rebuilt (`src/reconcile.rs:1144-1162`);
  `pb` is user-declared and never routed through it.
* **"`rebuild_convergence_advisory` is the (only) warning."** No — it covers `ignore_sources`
  archive residue and matview-fed IMVs (`src/reconcile.rs:941-991`) and does not fire here. The
  warning that fires is `src/lib.rs:1237`.
* **"The remedy cannot clear its own finding."** Refuted — `reflex_rebuild_imv` converges in one
  call, `reflex_doctor(fix => true, drop_orphans => true)` in two passes (both measured).
* **"It is invisible."** Not quite — `reflex_doctor` reports it, with the wrong diagnosis label.
  It is invisible only to an operator who does not run the doctor.
* **"It is a race / mid-swap timing artifact."** No — reproduced identically on two consecutive
  reconciles of the same IMV, and on a second independent fixture (`v1` → `b_same`/`c_other`/
  `d_part`).

---

## 9. Acceptance test

Real IMVs over a real partitioned source (never hand-inserted registry rows), parent partitioned,
with **three** dependents: auto-partitioned on the parent key, unpartitioned grouping by another
column, and one unpartitioned parent as a control.

1. After `reflex_reconcile(<partitioned parent>)`, every dependent must satisfy the bidirectional
   `EXCEPT ALL` / `assert_imv_correct` oracle — not merely be non-empty.
2. Every dependent's partition set must contain no relation whose name matches
   `%__reflex_swap_%`, and must equal the expected mirror of the parent's post-swap children.
3. The unpartitioned control must keep passing (guards against a fix that breaks the working path).
4. `reflex_doctor()` must report **no** findings immediately after the reconcile.
5. All four must be shown to go **RED** when the fix is reverted — assertion 1 currently yields
   0 rows and assertion 2 currently yields `pb___reflex_swap_tgt_pa_p1_c`, which are the mutation
   signals.
