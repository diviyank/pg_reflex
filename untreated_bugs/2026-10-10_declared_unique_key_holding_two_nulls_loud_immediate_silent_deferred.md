# A declared unique key that the data breaks with two NULLs: IMMEDIATE raises, DEFERRED keeps the old row — S2 (contract question)

Reported by the adversarial review of the 1.12.0 fix wave (finding 12, same on `0324f47`); not
re-run on the fix branch.

## Reproduction (as reported)

A passthrough IMV `SELECT k, v FROM t` created with `k` declared as its unique key, over a
table where `k` has no constraint and one row already holds `k IS NULL`; then
`UPDATE t SET k = NULL` for a second row.

IMMEDIATE: the UPDATE raises a duplicate key error. DEFERRED: no error at the write or at the
flush, and the IMV keeps the old row.

## Expected vs actual

The caller declared a key the data does not honour, so no outcome is "correct"; but the two
modes should agree, and the silent one is the wrong one to keep (refuse loudly, never no-op
silently).

## What is established and what is not

Established by the review: the two outcomes. Not established: whether the DEFERRED flush
reports anything in its return string; whether a duplicate non-NULL key behaves the same;
which statement of the flush drops the row (a key join with `=` does not match NULL).

## Fix direction (hypothesis to falsify)

Either make the DEFERRED flush raise as IMMEDIATE does, or refuse at create a declared key
whose columns are nullable in the source and not covered by a unique index (remedy: declare the
columns NOT NULL, or pass no key). The second is a new rejection of definitions that work today
whenever the data does honour the key, so the first is the safer direction.
