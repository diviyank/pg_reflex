# An IMV created inside a statement that writes one of its sources double counts that statement's rows — S2 (silent wrong rows; unusual usage)

Pre-existing (present at 1.12.4 and at every 1.12.5 batch commit tested). Found by the round-4
review of the 1.12.5 two-source fix; it is not caused by that fix and not repaired by it.

## Mechanism

`create_reflex_ivm` builds the new IMV from the sources as the creating statement sees them after a
command-counter increment: rows the enclosing statement already wrote are in the build. When the
source already has reflex statement triggers (another IMV over it), the statement's AFTER STATEMENT
trigger then fires, finds the new IMV registered in `__reflex_ivm_reference`, and applies the
statement's transition rows to it a second time.

## Reproduction (verified on 1.12.5, pg17)

```sql
CREATE TABLE sb (k INT PRIMARY KEY, g INT NOT NULL, v INT NOT NULL);
INSERT INTO sb SELECT i, i % 5, i FROM generate_series(1, 100) i;
SELECT create_reflex_ivm('sb_first', 'SELECT g, SUM(v) AS s FROM sb GROUP BY g',
                         NULL, NULL, 'IMMEDIATE');           -- installs sb's triggers
WITH ins AS (INSERT INTO sb VALUES (1001, 1, 7), (1002, 2, 9) RETURNING 1)
SELECT create_reflex_ivm('sb_new', 'SELECT g, SUM(v) AS s, COUNT(*) AS n FROM sb GROUP BY g',
                         NULL, NULL, 'IMMEDIATE')
FROM (SELECT count(*) FROM ins) c;
-- bidirectional EXCEPT ALL of sb_new against its query: 4 rows (groups 1 and 2 counted twice);
-- sb_first: 0. A later INSERT into group 3: still 4.
```

The round-4 reviewer's probe (a two-source variant) reported 2 mismatched rows after the creating
statement, and still 2 after a later statement writing both sources: the later statement's delta
does not touch the corrupted groups, so nothing recomputes them. `reflex_reconcile('sb_new')`
repairs the IMV.

## What was ruled out

- Not caused by the 1.12.5 write-counter baseline: identical with the baseline call removed.
- An IMV created in a *later* statement of the same transaction is correct
  (`imv_created_after_its_sources_were_written`).
- A source with no reflex triggers yet is not affected (no trigger fires for the statement).

## Severity

S2: silent wrong rows that persist, but only for an IMV created from inside a data-modifying
statement (writable CTE, or a trigger/function that runs `create_reflex_ivm` while its statement
writes a source) — rare in practice.

## Fix direction

Refuse loudly: `create_reflex_ivm` can detect that a source it reads has pending AFTER STATEMENT
trigger events in the current statement (e.g. the 1.12.5 per-transaction write counters moved
during the current command, or `GetCurrentCommandId` vs the source's last-write command) and raise
with a remedy ("create the IMV in its own statement"). Alternatively record the creating command id
in the registry and have the trigger skip an IMV created by the statement whose delta it applies.
