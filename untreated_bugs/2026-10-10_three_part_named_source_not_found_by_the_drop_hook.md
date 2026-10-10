# A source written `db.schema.table` is not found by the drop hook — S3 (LOUD on the next write)

Reported by the adversarial review of the 1.12.0 fix wave (finding 9, probe p28, same on
`0324f47`); not re-run on the fix branch.

## Reproduction (as reported)

An IMMEDIATE IMV created over `FROM <current database>.public.t`, then `DROP TABLE t`: the IMV
stays registered, and the registry row then points at a source that no longer exists.

## What is established and what is not

Established by the review: the IMV survives the drop. The other half of finding 9, a table with
a non-ASCII letter in its name (`é_t`), is fixed in 1.12.0: `__reflex_on_sql_drop` now also
matches the unquoted `schema.name` of the dropped table when that text reads back unquoted
(`v112_sr_drop_of_a_table_registered_bare_but_printed_quoted_drops_its_imvs`). Not established:
what spelling `depends_on` holds for a three-part name, and whether the other readers of
`depends_on` (truncate trigger lookup, partition bookkeeping) find it.

## Fix direction (hypothesis to falsify)

Drop the database part when the source is canonicalised at create
(`canonicalize_analysis_sources`, `src/create_ivm/soundness.rs`), so that one relation has one
registered spelling. See also
`2026-07-25_three_part_column_ref_survives_transition_table_rewrite.md` for the column
references written with three parts.
