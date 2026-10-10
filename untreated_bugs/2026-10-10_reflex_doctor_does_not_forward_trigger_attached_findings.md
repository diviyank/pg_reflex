# `reflex_doctor` returns no row for a source that lost a trigger; only `reflex_audit` reports it — S2 (maintenance broken, doctor silent)

Pre-existing: identical on `0324f47` and on `fix/full-review-r3` (pg16). Met while making the
audit report partitions that lack their triggers; out of that fix's scope.

## Reproduction (pg16)

```sql
CREATE TABLE dz(id INT, g TEXT, v INT);
SELECT create_reflex_ivm('dz_v', 'SELECT g, SUM(v) AS sv FROM dz GROUP BY g');
DROP TRIGGER "__reflex_trigger_ins_on_dz" ON dz;
SELECT reflex_audit('dz_v');     -- [ERROR] dz_v trigger-attached: Source dz is missing trigger(s): …
SELECT * FROM reflex_doctor();   -- no row names dz or dz_v
INSERT INTO dz VALUES (1,'a',1); -- silently not maintained
```

## Expected vs actual

Expected: a doctor row for the finding, with the audit's remedy as its action. Actual: none. The
same holds for the findings added in 1.12.0 for a partition without its write, truncate or guard
trigger, and for `trigger-mode-matches`: an operator who runs only `reflex_doctor` is told nothing
while writes are lost.

## Mechanism

`src/doctor.rs` forwards audit findings by category: `archive_residue`, `bare_name_ambiguity`,
the `orphan-*` family, `partition-mirror`, `partition-tree-drift`, `duplicate-function` and
`ignore-soundness`. Every other category falls into `_ => continue`.

## What was ruled out

The audit side is complete after the 1.12.0 fix: the finding is printed, and its remedy
(`reflex_rebuild_triggers('<source>')` then `reflex_reconcile('<imv>')`) is proven to leave a clean
audit and correct IMVs (`src/tests/pg_test_v112_partition_review.rs`).

## Fix direction (hypothesis to falsify)

Map `trigger-attached` and `trigger-mode-matches` to a doctor check id (a new one: the existing ids
are documented), report-only at first. Running the two-statement remedy under `fix => TRUE` needs
`apply_doctor_repair` to run each line, and a decision on whether the doctor may reconcile an IMV
on its own. Test: the doctor row appears, and after the action is run by hand it is gone.
