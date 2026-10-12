# The remedy printed when a HASH partition is refused as a source names the parent table by OID — limitation (the definition does not survive a dump and restore)

New in 1.12.0 with the refusal of a source that is itself a partition (`d3d27c4`). Declared by
the author (`p3-a-report.md`, item 1 and round 2 concern 3) and repeated by the review
(`p3-ac-review.md`, finding 11), in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`. pg17 / pg18.

## What the sources give

No script is printed. For a source that is a HASH partition, the refusal's remedy is the parent
with the partition's bound as a WHERE, and for HASH that bound is what
`pg_get_partition_constraintdef` returns:

```
… FROM <parent> WHERE satisfies_hash_partition('<oid>'::oid, <modulus>, <remainder>, <column>)
```

The remedy is created as printed and maintained (test
`v112_cr_hash_partition_source_remedy_is_maintained`; rows, ungrouped aggregate, grouped
aggregate, both modes, compared with its own query and with the same query over the partition).

## Why it is filed

`'<oid>'::oid` is the parent's OID in this database. A dump and restore, or the same DDL replayed
in another environment, gives the parent another OID. Expected consequence, **not run**: the IMV
definition, replayed elsewhere, either fails (`satisfies_hash_partition` raises for an OID that
is not a hash-partitioned table) or names another table. An IMV definition kept in a migration
file is exactly what gets replayed.

## What is established and what is not

Established: the text of the remedy and that it is maintained where it was printed. Not
established: the behaviour after a dump and restore of a database holding such an IMV (the
registry stores the query text with the OID), and whether `'<parent>'::regclass` in its place is
accepted by `create_reflex_ivm` and maintained.

## Fix direction (hypothesis to falsify)

Print `'<schema>.<parent>'::regclass` instead of the OID literal (PostgreSQL accepts a regclass
where an oid is expected) and run the existing remedy test on it. If the analyzer does not accept
the cast, print no predicate for HASH, as is already done for a multi-column RANGE parent, and
say "read the parent".
