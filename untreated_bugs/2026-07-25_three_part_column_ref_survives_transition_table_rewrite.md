# 2026-07-25 — a 3-part qualified column reference is not rewritten to the transition table, aborting every source DML

**Status: untreated, confirmed by reproduction.** Found while confirming the (now fixed)
qualified-GROUP-BY stability-qualifier bug. Independent of that fix: it lives in
`replace_source_with_transition` (`src/sql_writer/identifier.rs`), on the mainstream
delta path, and fails loudly rather than silently.

## The mechanism

`replace_source_with_transition` swaps the mutated source for its transition table by
replacing the identifier `src` (quoted, schema-qualified, and bare spellings) with
`"__reflex_new_src"`. A column reference of the form `schema.src.col` is left untouched —
its `src` token is preceded by a `.`, so the identifier pass (correctly, for column names)
skips it. The FROM entry is rewritten, the 3-part reference is not, and the resulting
delta query references a table that is no longer in its own FROM clause.

## Reproduction (pg17, verified)

```sql
CREATE TABLE qgbx_fa (id INT PRIMARY KEY, k INT);
CREATE TABLE qgbx_fb (k INT PRIMARY KEY, w INT);
INSERT INTO qgbx_fa VALUES (1,5);
INSERT INTO qgbx_fb VALUES (5,50);
SELECT create_reflex_ivm('qgbx_v',
  'SELECT qgbx_fa.k AS k, COUNT(*) AS n
     FROM qgbx_fa JOIN qgbx_fb ON public.qgbx_fa.k = qgbx_fb.k
    GROUP BY qgbx_fa.k');
INSERT INTO qgbx_fa VALUES (2,5);
-- ERROR: missing FROM-clause entry for table "qgbx_fa"
```

Creation succeeds (the base query is valid SQL). The generated scratch-fill statement is:

```sql
INSERT INTO "__reflex_scratch_qgbx_v"
SELECT "__reflex_new_qgbx_fa".k AS "k", COUNT(*) AS "__count_star", COUNT(*) AS __ivm_count
FROM "__reflex_new_qgbx_fa" JOIN qgbx_fb ON public.qgbx_fa.k = qgbx_fb.k
GROUP BY "__reflex_new_qgbx_fa".k
```

Note the ON clause still says `public.qgbx_fa.k`.

## Impact

Any IMV whose base query contains a 3-part reference to one of its sources — anywhere:
SELECT list, ON clause, WHERE, GROUP BY — is created successfully and then aborts every
INSERT/UPDATE/DELETE on that source with a cryptic `missing FROM-clause entry`. The IMV
is unusable and the failure is only discovered at first write, not at create.

Severity: high nuisance, low corruption risk — the failure is loud and rolls the DML back,
so no wrong data is written. The 3-part spelling is unusual in hand-written SQL, which is
why this has not been reported from the field.

## What was ruled out

- Not a PostgreSQL restriction: `public.t.c` is a legal reference to an unaliased FROM
  entry `t`, and the IMV's own base query runs fine.
- Not specific to outer joins, aggregates, or the GROUP BY clause — reproduced above with
  an INNER JOIN and a 3-part reference in the ON clause only.
- Not the same defect as the qualified-GROUP-BY stability bug fixed on
  `fix/qualified-group-by-stability-qualifier` (that one caused silently wrong results via
  scope misclassification; this one is a codegen rewrite gap and always errors).

## Fix direction

Two candidates, in order of preference:

1. **Refuse loudly at create.** Reject (or normalise) a base query containing a 3-part
   reference to one of the IMV's own sources, with a message telling the user to write
   `t.c` or an alias. Cheapest, and consistent with "refuse loudly, never no-op silently".
2. **Rewrite the reference.** Teach `replace_source_with_transition` to also match
   `schema.src.` (and `"schema"."src".`) prefixes and replace the `schema.src` part.
   Needs care not to rewrite a genuine `<something>.src.col` where `src` is a column of a
   composite type.

Related, not a bug: `resolve_column_source` (`src/create_ivm/soundness.rs`) splits on the
first `.` too, so a 3-part reference resolves to table `public`, column `fb.k`. Its only
caller is `column_base_not_null`, whose catalog probe then finds nothing and returns
`false` — a missed NOT-NULL inference, which is the safe direction.
