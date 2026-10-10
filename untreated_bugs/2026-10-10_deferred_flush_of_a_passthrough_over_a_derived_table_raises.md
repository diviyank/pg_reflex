# DEFERRED flush of a passthrough IMV over a derived table raises — S3 (LOUD at flush)

Pre-existing: met with plain table names while building the keyword-relation matrix of the
1.12.0 fix round 3 (pg17), so it does not come from the respelling.

## Reproduction (pg17, one transaction; the shape of the removed matrix cell, written out by hand)

```sql
CREATE TABLE t(id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,10),(2,20);
SELECT create_reflex_ivm('x', 'SELECT q.id, q.v FROM (SELECT public.t.id, public.t.v FROM public.t WHERE public.t.v IS NOT NULL) q',
                         NULL, NULL, 'DEFERRED');
INSERT INTO t VALUES (3,30);
SELECT reflex_flush_deferred('t');
-- ERROR: relation "__reflex_delta_<subquery:q>" does not exist
```

The IMMEDIATE IMV over the same query is maintained and correct
(`v112_sr_kwref_*`, cell `derived`, which is why that cell runs IMMEDIATE only:
`V112_SR_KEYWORD_REFERENCES_IMMEDIATE_ONLY`).

## What is established and what is not

Established: the error above on the flush, with a plain and with a keyword table name.
Not established: whether the pending rows stay queued after the failed flush (the flush is
expected to roll back), whether an aggregate over a derived table fails the same way, and the
behaviour on 1.11.5.

## Fix direction (hypothesis to falsify)

The flush builds a delta table name from the analysis source `<subquery:q>`, a placeholder that
is not a relation. Either the DEFERRED flush should walk the real sources of the subquery as the
IMMEDIATE trigger does, or create should refuse DEFERRED for this shape with a tagged message
naming the remedy (write the subquery as a CTE, which the flush chain handles).
