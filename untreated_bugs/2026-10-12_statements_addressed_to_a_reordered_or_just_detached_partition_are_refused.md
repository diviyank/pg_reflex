# A write addressed to a partition whose columns are in another order, or to a table in the transaction that detached it, is refused — limitation (new in 1.12.0, on purpose); foreign-table partitions are skipped untested

Introduced with the per-partition write triggers of 1.12.0 (`8d2c69c`). Source:
`fold-g8b1-report.md` ("Two refusals", "Concerns / residuals") and `full-review-r3.md` in the
ledger `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`; pg18 and pg16. On 1.11.5
these statements were accepted and silently not maintained, so this is an improvement that
blocks a write; it is filed so that the limitation and its open ends are recorded. Grouped: the
three points are the cases where a partition does not get the ordinary write triggers.

## 1. Partition whose columns are in another order than the source's

A table created independently, then attached. No script is printed; what the report gives: with
the write triggers installed on such a partition, a keyed passthrough stored `(221,'r',1)` as
`(1,'r',221)`, silently, because the transition tables have the partition's column order and the
passthrough copies them positionally (`INSERT INTO __reflex_pt_new… SELECT * FROM <transition>`,
`trigger/ops.rs`). So the partition gets the guard and no write trigger:

```
pg_reflex: INSERT on partition p cannot maintain the incremental views of s: the columns of this
partition are in a different order …   HINT: address the statement to s
```

A WARNING says so when the trigger is installed. The remedy is proven: the same writes through
the source are correct.

## 2. Table detached in the running transaction

It keeps its triggers until COMMIT. Capturing its writes was correct in IMMEDIATE but not in
DEFERRED (the staged row was applied after the flush had removed the partition's rows: an extra
row `(295,l,5)` in the keyed and MIN/MAX IMVs); ignoring them corrupts the detach delta. The
guard refuses:

```
… the table is no longer a partition of s and carries its triggers until the transaction that
detached it commits
```

After the COMMIT the table is plain again.

## 3. Open ends

- **A statement touching no row** on a reordered or detached table is refused too (BEFORE
  trigger). An `UPDATE … WHERE false` or an empty `INSERT … SELECT` in a load script fails.
- **Foreign-table partitions are skipped** (no transition tables); "not tried". A write
  addressed to a foreign partition is presumably not maintained, as on 1.11.5: not verified.
- The usual load pattern "DETACH, write the detached table, ATTACH it back, one transaction" is
  refused at its second step. The report names "a detach-write-attach sequence" as the case the
  refusal protects; whether a remedy other than splitting the transaction exists is not stated.

## What is established and what is not

Established: both refusals and their messages, the silent wrong row that motivated the first,
the wrong DEFERRED row that motivated the second (tests in `pg_test_v112_partition_dml.rs`, the
reordered and detached-refusal cells). Not established: the three open ends above beyond what is
quoted.

## Fix direction (hypothesis to falsify)

1: make the positional copy name-based when the partition's column order differs (the generator
has no catalog access and its output is cached per IMV, which is why it was not done); or
generate one function per distinct column order. 2: in DEFERRED, stage the write of a detached
table against the detach delta instead of refusing. Both need the oracle over the report's
matrices. The no-row case could be let through by moving the refusal to a row-count check in an
AFTER statement trigger, at the cost of refusing later.
