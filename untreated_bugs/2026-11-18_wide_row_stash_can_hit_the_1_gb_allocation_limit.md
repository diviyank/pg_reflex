# The two-source stash of very wide rows can hit the 1 GB allocation limit — S3 (loud error; reasoned, not reproduced)

Found by the 1.12.5 final review (M2); was the "whole-row stash" bullet of
`2026-11-17_minor_followups_from_the_1_12_5_batch.md`.

## Mechanism

When a statement writes two sources X and S of an IMMEDIATE join IMV, X's delta keeps X's
transition rows for S's correction (`stash_rows_sql`, src/trigger/joint.rs): whole rows,
`reflex_joint_stash_text(array_agg(row.*))`, stored as text in a transaction-local setting.
`STASH_ROW_LIMIT` (10 000 rows) bounds the row count, not the bytes. 10 000 rows of a source with a
TOASTed 100 kB column make about 1 GB of text; `array_agg`, the array's text output or
`set_config` then raise `invalid memory alloc request size`, and the user's statement aborts.

## What was ruled out

- Not a wrong result: the statement fails loudly. 1.12.4 silently double counted the same
  statement, so this is not a regression in correctness.
- The row cap cannot be lowered to fix it without losing exact corrections of ordinary narrow
  statements.
- Not seen in the field; db_dev's two-source shapes have narrow rows.

## Reproduction (to build)

Source `wx (k INT PRIMARY KEY, g INT, pad TEXT)` with `pad` 100 kB per row, `wy (k INT PRIMARY KEY,
b INT)`, an IMMEDIATE `SUM(wy.b) … FROM wx JOIN wy ON wy.k = wx.k GROUP BY wx.g`, then
`WITH x AS (INSERT INTO wx SELECT g, g % 10, repeat('x', 100000) FROM generate_series(1, 10000) g
RETURNING 1) INSERT INTO wy SELECT g, g FROM generate_series(1, 10000) g`. The stashed side
depends on trigger firing order; write the wide rows to the side fired first.

## Severity

S3: a statement that 1.12.4 completed (wrongly) now fails with an allocation error.

## Fix direction

Either cap the stash by bytes (sum `octet_length` of the rows' text with the row count, poison the
stash past a few tens of MB, so the IMV is recomputed), or stash only the columns the IMV reads
from X (join keys, grouped and aggregated columns). The byte cap is the smaller change and fails
toward recompute.
