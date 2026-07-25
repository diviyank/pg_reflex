# 2026-07-25 — a 3+ part qualified GROUP BY column can be misclassified as stable by outer-join-secondary scoping

**Status: untreated, low reachability.** Found by adversarial review of the ljgroup FULL JOIN
fix (`5345d74`); pre-existing since at least 1.4.6, not introduced by that fix — the fix inherits
this gap rather than causing it.

## The mechanism

Both `join_key_scope_is_sound` and the STABLE-column fallback in `outer_join_secondary_stmts`
(`src/trigger/ops.rs`) decide whether a GROUP BY column is "stable" (safe to scope a targeted
recompute on) by taking its qualifier via `gb.split_once('.')` and checking whether that
qualifier is the table just mutated. `split_once` returns only the FIRST `.`-delimited segment,
so a genuinely 3-part qualified column reference (e.g. `public.fb.k`, a schema-qualified table
alias) yields qualifier `public` — never a member of `secondary_ref_identifiers`'s result set —
and is therefore always classified STABLE regardless of which table it actually belongs to.

## Impact

If a base query's GROUP BY ever uses a schema-qualified (not just table/alias-qualified)
reference to a column that is actually on the SECONDARY (mutated) side, this bug would let the
fast/fallback scoped-recompute path treat it as stable, silently missing groups that migrate on
a secondary mutation — the same class of bug as PS-11/PS-5/the LEFT-JOIN groupby fix, but from a
qualifier-parsing gap rather than a join-safety gap.

## Reachability, unconfirmed

Whether `base_query` (the AST-regenerated form for aggregate IMVs — see `sql_analyzer.rs`'s
`generate_base_query`) ever actually emits a 3-part schema-qualified column reference in GROUP BY,
as opposed to always using bare aliases, was not checked. If the codegen path never produces
that shape, this is dead-in-practice, matching the class of finding already filed for
`join_key_scope_is_sound`'s superseded internal FULL JOIN check.

## Fix direction, if confirmed reachable

Use the LAST `.`-delimited segment as the qualifier (or match against known table aliases
explicitly) instead of the first, consistent between `join_key_scope_is_sound` and the
STABLE-column fallback (both currently share the same `split_once('.')` pattern and would need
the same fix to stay consistent).
