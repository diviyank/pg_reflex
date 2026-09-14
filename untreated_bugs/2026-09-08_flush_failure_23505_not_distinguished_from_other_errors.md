# 2026-09-08 — a unique-violation flush failure is reported like any other error

**Status: untreated — deliberately deferred from 1.11.4.** Raised by the final
whole-branch review of `reflex-1.11.4`.

Severity: **low.** No information is lost, only its actionability.

---

## Mechanism

The deferred flush's per-IMV handler (`src/trigger/deferred.rs`) and the partition flush's
per-root handler (`src/partition.rs`) catch `WHEN OTHERS` and record
`SQLERRM (SQLSTATE …)` plus the generic remedy "run `reflex_reconcile('<imv>')`".

For `23505` (unique_violation) that remedy cannot converge while the cause remains:
duplicates in a source on the IMV's unique key make the reconcile's own rebuild fail
the same way. This is exactly what wedged `nvg.sales_simulation` and
`omc.sales_simulation` on db_dev (2026-08-03 / 2026-09-07): the error named the
internal `__reflex_swap_tgt_*` index, not the source or the key.

## Ruled out for 1.11.4

The SQLSTATE is already recorded, so the class is diagnosable by an operator who knows
to look for it.

## Fix direction

A dedicated `WHEN unique_violation` branch that names the IMV's unique key columns and
prescribes finding the duplicate source rows (`GROUP BY <key> HAVING COUNT(*) > 1`)
before any reconcile.
