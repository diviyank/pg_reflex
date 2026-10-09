# A passthrough over a derived table infers the source primary key from an output that is not that column — raw error at create, or blocked writes

Residual of two reports closed on the 1.12.0 branch for the direct and CTE spellings: "a
SELECT item aliased like the primary key is taken for the key" and "a set-returning function
in the SELECT list is given the source key". Both still fail when the renamed or multiplied
column comes out of a derived table (`FROM (SELECT ...) x`), on 1.11.5 and on the branch alike.

## Reproduction (pg15, executed by the adversarial review of the branch)

```sql
-- t(id serial primary key, g text, v int, arr int[])
SELECT create_reflex_ivm('a', 'SELECT x.id, x.g FROM (SELECT v AS id, g FROM t) x');
-- raises: could not create unique index "__reflex_uk_a" (when two rows share v)

SELECT create_reflex_ivm('b', 'SELECT x.id, x.e FROM (SELECT id, unnest(arr) AS e FROM t) x');
-- raises the same error when some arr holds two elements at create time.
-- When no arr holds two elements yet, the IMV is created with key (id), and the next
-- INSERT INTO t of a two-element array fails with
--   duplicate key value violates unique constraint "__reflex_uk_b"
-- so the source table refuses that write until the IMV is dropped.
```

The CTE spelling of both queries is keyless and correct on the branch.

## Severity

Loud in every observed case: a raw error at create, or a failed source write. No wrong row was
observed. The second form is the worse one, because the create succeeds and a later, valid write
to the source is refused. When `v` happens to be unique at create time the first form is created
with a key that the source does not guarantee, and a later duplicate `v` fails the same way.

## Root cause

The key inference reads the outer SELECT list: an outer item `x.id` is a bare reference output
under its own name, and the source's primary key column is called `id`. It does not check that
the derived table's `id` is the source's `id` column, nor that the derived table yields one row
per source row.

## Fix direction

Infer the key through a derived table only when the inner SELECT list itself outputs the primary
key columns under their own names and calls no set-returning function; otherwise leave the IMV
keyless, as the CTE spelling is. A tagged rejection is not needed: keyless is correct.
