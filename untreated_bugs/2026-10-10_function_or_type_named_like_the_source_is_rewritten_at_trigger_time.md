# A function or a type named like the source table is rewritten to the transition table — S1 for the type (SILENT, DEFERRED), S3 otherwise

Pre-existing in the trigger-time rewriter; met while building the keyword-relation matrix of
the 1.12.0 fix round 3 (pg17). The create-time respelling leaves these words alone
(`v112_sr_kwref_only_references_to_the_relation_are_respelled` pins the stored text); the
failure is later, where the source is swapped for its transition table.

## Reproduction (pg17; the shapes of temporary probes, written out by hand, not committed cells)

```sql
CREATE TABLE "position"(id INT PRIMARY KEY, v TEXT);
SELECT create_reflex_ivm('x', 'SELECT id, position(''a'' in v) AS p FROM "position"');
INSERT INTO "position" VALUES (1, 'cat');     -- IMMEDIATE: raises

CREATE TABLE "time"(id INT PRIMARY KEY, v TEXT);
SELECT create_reflex_ivm('y', 'SELECT id, v::time AS t FROM "time"', NULL, NULL, 'DEFERRED');
INSERT INTO "time" VALUES (1, '10:00');
SELECT reflex_flush_deferred('"time"');        -- no error; y differs from the query
```

## What is established and what is not

Established: IMMEDIATE raises on the first write in both shapes; the DEFERRED IMV with the
`::time` cast was silently wrong after the flush. Not established: the DEFERRED outcome of the
function shape; whether a plain (non-keyword) table that shares its name with a user function
or type (`CREATE FUNCTION t(int)`, `SELECT t(v) FROM t`) fails the same way, which is the
likely general case; the behaviour on 1.11.5.

## Fix direction (hypothesis to falsify)

`replace_source_with_transition` (`src/sql_writer/identifier.rs`) replaces the source name as
text. It should skip a name followed by `(` and a name after `::` / inside `CAST(… AS …)`, or
work from the parsed statement and rewrite relation names and column qualifiers only. Same
function as `2026-07-25_three_part_column_ref_survives_transition_table_rewrite.md`.
