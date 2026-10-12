# `ALTER TABLE` of a source with DEFERRED IMVs makes the next write fail — S2 (write-blocking, loud)

Met on PostgreSQL 15 while extending the cross-schema lock tests of 1.12.0; not reduced and not
checked on 1.11.5. Recorded so it is not lost: the reproduction below is the shape that was
observed, not a committed test.

## Observed

A source with at least one DEFERRED IMV gets a column added (`ALTER TABLE <source> ADD COLUMN
note0 …`). The next write to the source raises `column "note0" does not exist`: the staging
table that holds the DEFERRED delta rows of that source is not altered with it.

## Not established

- Which `alter_source_policy` was in force, and whether the policy that rebuilds the IMVs after
  an ALTER also rebuilds the staging table.
- Whether the write fails for every kind of ALTER or only for an added column.
- Whether `reflex_rebuild_triggers('<source>')` or recreating the IMVs clears it.

## Fix direction (hypothesis)

The ALTER handling of the `ddl_command_end` event trigger should rebuild (or alter) the
DEFERRED staging table of the source together with its triggers, or refuse the ALTER under the
policies that cannot.
