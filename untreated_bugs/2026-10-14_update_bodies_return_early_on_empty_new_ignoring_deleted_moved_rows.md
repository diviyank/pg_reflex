# UPDATE bodies return early when NEW is empty, missing rows deleted by a cancelled row movement

**Severity:** silent wrong result. Pre-existing.
**Found:** 2026-10-14 by the D′ review (ledger `Dprime-review.txt`, F2).

Both UPDATE trigger bodies return early when the NEW transition table is empty, without checking OLD.
A cross-partition UPDATE whose destination BEFORE INSERT trigger returns NULL for every moved row
removes those rows from the source, but no IMV is maintained (IMMEDIATE probe: 2 mismatches, with and
without D′). Fix direction: early return only when NOT (EXISTS(OLD) OR EXISTS(NEW)).
