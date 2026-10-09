# `reflex_flush_deferred('t')` flushes nothing when the IMV registered the source as `public.t` — S3 (manual call only)

Pre-existing: same behaviour on commit 8ef9743 and on the `fix/fold-g2-having` head, pg16. Found by
the adversarial review of the HAVING fold (its P6) while it built a DEFERRED harness; not re-run
when this report was written.

## Reproduction

Reproduced twice by the review (pg16), without a full script recorded. A DEFERRED IMV whose
`depends_on` holds the source as `public.t` (the query was written `FROM public.t`): after a write
to `t`, `SELECT reflex_flush_deferred('t')` returns `NO DEFERRED IMVS` and the IMV stays stale;
`SELECT reflex_flush_deferred('public.t')` flushes it. The mirror holds too: registered as `t`
(the query was written `FROM t`), `reflex_flush_deferred('public.t')` returns `NO DEFERRED IMVS`.

## Expected vs actual

Expected: the flush resolves the name the way PostgreSQL would and flushes the IMVs on that
table. Actual: nothing is flushed. The call is not an error and not silent: it returns
`NO DEFERRED IMVS`, which a caller that does not read the result takes for a flush.

## What is established and what is not

Established: the match on `depends_on` is on the exact text, and a create stores the spelling the
query was written with. The COMMIT-time flush is not affected (it is driven by the registered
name), so this concerns a manual call inside a transaction.

## Fix direction (hypothesis to falsify)

Canonicalise the argument with the function the create uses for sources before matching, and
return a message when nothing is registered under the name.
