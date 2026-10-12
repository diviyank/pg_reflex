# `create_reflex_ivm(…, 'DEFERRED')` is refused when the same transaction already staged a write to one of its sources — limitation (new in 1.12.0, replaces a silent double apply)

New in 1.12.0 (`cdeeadb` round of the cross-schema lock work). Source: `p3-c-report.md`, round 2,
in the ledger `.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`; PostgreSQL 15.

## What the source gives

No script is printed. Before the change every DEFERRED create ran a `DROP TRIGGER` on the shared
pending table, which PostgreSQL refuses when the transaction has pending events, so a create
after a staged write failed with PostgreSQL's error. Once that DDL was removed, such a create
"went through and the new IMV received the staged write twice at COMMIT: silent, 2 mismatches in
the first run of the test (`logs/d-c2-green1.log`)". The create is now refused before any DDL
when this transaction staged a write to one of the IMV's sources
(`source_written_deferred_by_this_transaction`, `create_ivm/mod.rs`); the message names the
source and the remedy (commit, then create), and the test runs that remedy against the oracle.
A write to another source does not refuse.

The shape, as described (an existing DEFERRED IMV on `s` is what makes the write staged):

```sql
BEGIN;
INSERT INTO s …;                                   -- staged for an existing DEFERRED IMV of s
SELECT create_reflex_ivm('new_v', 'SELECT … FROM s …', NULL, 'UNLOGGED', 'DEFERRED');
-- refused: tagged message naming s; remedy: commit, then create
```

## Expected vs actual

Expected by a migration script that loads a table and creates its views in one transaction: the
create succeeds and the IMV equals its query. Actual: refused; the script has to commit between
the write and the create. The refusal is the safe floor against the double apply.

## What is established and what is not

Established: the double apply without the refusal (one test run), the refusal, its remedy. Not
established: the same with an IMMEDIATE create after a staged write (the new IMV is built from
the table, and the staged rows are flushed to the older DEFERRED IMVs only: presumably correct,
not stated); a DEFERRED create over an IMV whose own source has staged rows.

## Fix direction (hypothesis to falsify)

Build the new IMV from the source as it is (which already contains the staged rows) and mark the
rows staged before the create as not applicable to it, e.g. by recording at create the highest
staged row id of each source and having the flush skip older rows for this IMV. Test: the
refused sequence with the oracle, one and several sources, and a rollback to a savepoint taken
before the create.
