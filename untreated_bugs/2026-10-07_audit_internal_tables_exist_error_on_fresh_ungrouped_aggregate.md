# reflex_audit reports `internal-tables-exist` ERROR on a freshly created ungrouped aggregate IMV — S3 (false finding; remedy untested)

Pre-existing: same on the 1.11.5 base and on the 1.12.0 branch. Found by the Task 4b reviewer
(adjacent A7).

## Reproduction

Reproduction not recorded as a script. The review states: `reflex_audit('<imv>')` on a freshly
created ungrouped aggregate IMV reports

```
[ERROR] internal-tables-exist ... "__reflex_affected_<imv>"
```

with remedy `reflex_rebuild_imv`.

## Expected vs actual

Expected: a fresh IMV audits clean. Actual: an ERROR finding naming `__reflex_affected_<imv>`.

## What is established and what is not

Established: the finding text on a fresh ungrouped aggregate IMV. Not established: why the table
is missing or is not expected, and whether the printed remedy clears the finding ("Whether the
remedy clears it was not tested"). Per the project's rule, an audit remedy that cannot clear its
own finding sends operators into a retry loop, so that has to be checked first.

## Fix direction (hypothesis to falsify)

First determine whether an ungrouped aggregate IMV is meant to have an `__reflex_affected_`
table (a sentinel group has no group keys to record). If not, the audit check should skip it; if
so, creation is missing it. Then run the printed remedy and confirm the finding clears.
