# `reflex_flush_deferred('t')` is a silent no-op when the IMV registered the source as `public.t` — S3 (manual call only)

Pre-existing: same behaviour on commit 8ef9743 and on the `fix/fold-g2-having` head, pg16. Found by
the adversarial review of the HAVING fold (its P6) while it built a DEFERRED harness; not re-run
when this report was written.

## Reproduction

Recorded by the review without a full script: a DEFERRED IMV whose `depends_on` holds the source as
`public.t`; after a write to `t`, `SELECT reflex_flush_deferred('t')` returns without an error and
the IMV stays stale; `SELECT reflex_flush_deferred('public.t')` flushes it.

## Expected vs actual

Expected: the flush resolves the name the way the create did, or says that no DEFERRED IMV is
registered on the name it was given. Actual: no error, nothing flushed.

## What is established and what is not

Established: the match on `depends_on` is on the exact text. The COMMIT-time flush is not
affected (it is driven by the registered name), so this concerns a manual call inside a
transaction. Not established: which spellings a create stores in which session settings (inside
`cargo pgrx test` the sources of this group's tests are stored bare).

## Fix direction (hypothesis to falsify)

Canonicalise the argument with the function the create uses for sources before matching, and
return a message when nothing is registered under the name.
