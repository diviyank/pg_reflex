# Aggregate over a UNION ALL subquery whose operands name their columns differently: a write to a later operand's table raises — S3 (loud)

Pre-existing (same on `903efbc` and on the G1 head `8ef9743`). Found by the G1 (joins)
adversarial review, probe cells `uaplain` and `dtua`, pg17.

## Reproduction (pg17)

```sql
CREATE TABLE dim(id SERIAL PRIMARY KEY, g TEXT, w INT);
CREATE TABLE grp(g TEXT PRIMARY KEY, z INT);
INSERT INTO dim(g, w) VALUES ('g0',5),('g1',7);
INSERT INTO grp VALUES ('g0',1),('g1',2);
SELECT create_reflex_ivm('v', 'SELECT u.g, COUNT(*) AS c, SUM(u.w) AS w
  FROM (SELECT d.g, d.w FROM dim d UNION ALL SELECT k.g, k.z FROM grp k) u GROUP BY u.g');
INSERT INTO grp VALUES ('g2',4);
-- ERROR:  column u.w does not exist
```

Create succeeds and the IMV is correct after create. Writes to `dim` (the first operand) are
correct. Control: aliasing the second operand's column to the first operand's name
(`SELECT k.g, k.z AS w FROM grp k`) is correct for writes to both tables (`uasame`).

## Expected vs actual

Expected: the subquery's column names are those of its first operand, whatever the later
operands call theirs. Actual: the delta for a write to `grp` keeps only the operand that reads
`grp`, which becomes the first operand of the pruned subquery, so the subquery's columns are now
`g, z` and `u.w` no longer exists.

## What is known about the mechanism

By reading: `scoped_delta_query` (`src/trigger/union_delta.rs`) prunes a `UNION ALL` subquery to
the operands that reference the written source (`scope_setexpr`, `rebuild_union_all`) and does
not carry the first operand's output names over to the surviving operand. Not traced in a
debugger.

## Fix direction (hypothesis)

When pruning removes the first operand, alias the projection of the new first operand with the
original first operand's output names (or give the derived table an explicit column list). If
the names cannot be derived (a wildcard), fall back to the full recompute. Add oracle tests for
two and three operands with different names, writes to each operand's table.
