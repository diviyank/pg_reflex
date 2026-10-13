# `reflex_rebuild_chain` of an IMV created before 1.12.0 fails when 1.12.0 refuses its definition — S3 (loud, rolled back; the upgrade remedy does not apply to these IMVs)


**Narrowed 2026-10-13 by the packaging upgrade test (pg15, `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/packaging-report.md`, "Phase 3"):**
on an upgraded instance `reflex_rebuild_imv` does NOT fail on such an IMV: it reconciles in
place and returns `RECONCILED` (observed twice). Only `reflex_rebuild_chain` raises and keeps the
IMV. Whether `reflex_rebuild_imv` keeping a definition that 1.12.0 would refuse is itself a
problem (it keeps maintaining a shape now considered unsafe) is not established.
1.12.0 refuses at create several definitions that 1.11.5 accepted. The rebuild calls recreate an
IMV through the same validation, so on such an IMV they raise and roll back. Not possible on
1.11.5 (the refusals are new). Sources, in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`: `packaging-report.md` §4.4
(pg17.11, upgraded instance), `final-review.md` (u01, pg17), `p3-a-report.md` rounds 1 and 2.

## Reproduction

Observed, an IMV calling an unlabelled (so VOLATILE) function (`final-review.md`, u01: create
the IMV while the function is IMMUTABLE, make it volatile, rebuild; and the packaging run, where
the IMV was created on 1.11.5). The scripts are not printed; the outcome is:

```
reflex_rebuild_chain: failed to recreate IMV '<v>': ERROR: [reflex-unsupported] Volatile function(s) …
```

The IMV is still registered afterwards. `create_reflex_ivm_if_not_exists`: skipped. After
`ALTER FUNCTION … IMMUTABLE` the same call returns `REBUILT CHAIN`.

Not run, read from the code:
- an IMV that reads a partition of a partitioned table (refused as a source since 1.12.0):
  "`reflex_rebuild_imv` / `reflex_rebuild_chain` on an IMV that already reads a partition will
  now fail at the recreate step (the rebuild raises and rolls back its drop). Read from the
  code, not run" (`p3-a-report.md`, round 2, concern 2);
- two IMVs over sources that share trigger names (`s.t` and `s_t`): "the second recreate is then
  refused" (`p3-a-report.md`, round 1, concern 2). See
  `2026-10-12_imvs_over_two_sources_sharing_trigger_names_created_before_1_12_0_stay_stale.md`.

## Expected vs actual

The behaviour is the safe one: the call refuses loudly, names the cause and leaves the IMV as it
was. What is missing is on the operator's side: the release notes say "recreate every IMV after
upgrading", and for these IMVs the recreate cannot succeed until the definition is changed, while
the old IMV keeps running on its 1.11.5 artefacts (see
`2026-10-12_imvs_created_before_1_12_0_keep_their_plan_and_trigger_functions_after_the_upgrade.md`).
Nothing lists them in advance.

## What is established and what is not

Established: the volatile-function case, including that the IMV survives and that the printed
remedy converges. Through `reflex_rebuild_chain` the message carries the prefix above, so the
remedy starts later in the text (with 10-character names the word IMMUTABLE ends near character
261; the doctor cap is 400). Not established: the two "read from the code" cases; the other new
refusals (REAL `SUM` / `AVG`, `SUM(money)`, inheritance members, a column alias list that renames,
reprinted expressions) through a rebuild; whether a failed rebuild inside a larger chain leaves
the IMVs below it rebuilt or rolls the whole chain back.

## Fix direction (hypothesis to falsify)

A read-only pre-upgrade check: a function that runs the create validation over every registered
`sql_query` and returns the IMVs 1.12.0 would refuse with their messages, to be run before or
right after `ALTER EXTENSION … UPDATE`. Test it on an instance upgraded from 1.11.5 with one IMV
per new refusal.
