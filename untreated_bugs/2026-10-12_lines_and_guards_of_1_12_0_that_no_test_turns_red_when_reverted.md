# Lines and guards of 1.12.0 whose reversion leaves the suite green — test gap (false-green risk; no wrong result shown)

The bug-resolution method of this repository requires every "must refuse / must stay unchanged"
assertion to go RED under mutation. The reviews and fix reports of 1.12.0 name the mutations
below as surviving or not shown RED, and none was closed afterwards as far as the ledger says.
Grouped because they are one kind of finding with one remedy (a pinning cell each); they are
independent and can be closed one by one. Ledger:
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`. **None was re-checked against
the release head**, except where a line says so; a later test may already pin some of them.

| # | Mutation that stays green | Where | Source |
|---|---|---|---|
| 1 | the schema-suffix arm of `is_the_same_qualified_column` removed | `soundness.rs` (`:1058` at the time) | `fix-wave-review.md` finding 7 (pg17, `ffdca63`), mutation table "P1 schema-suffix arm: green" |
| 2 | the `unaliased_tables` arm of `binds_an_alias_twice` removed (although `v112_sr_p2_alias_named_like_the_outer_table` exists) | `sql_analyzer.rs` (`:2535`) | same |
| 3 | the quoted-expression arm reverted (`i4_expr_lower`) | join-key comparison of the I4 / P1 fix | same |
| 4 | `IF _rebuilt THEN _announced := NULL` removed from the BEFORE TRUNCATE trigger | generated truncate function | same. The R2 fix author adds: the counter rolls back with a failed statement or savepoint, "I see no way to leave it raised; not established" |
| 5 | the `to_regclass` half of the identity check of the trigger-name guard dropped | `source_relations.rs` | `p3-a-report.md` (r6): "I could not build two different registered spellings of one relation with the same suffix" |
| 6 | "whole WHERE checked" instead of the printed conjuncts: the `exists_subquery` cell alone stays green (`exists_beside_a_filter` catches it) | `v112_cr_*` cells | `p3-a-report.md` round 3, "Weak spot" |
| 7 | the "HAVING that fails to parse is kept" path | HAVING reader | `fold-g2-review.md` M3 (pg16): not reachable with 60+ exotic HAVING texts, so untested either way |
| 8 | the parse-failure fallback of the bare-keyword unquoting (the query is left as written) | keyword unquoting before the parse | `fold-g8b2-report.md` concern 2: "Item 2 fallback is untested: … two spellings would be possible again. No keyword reaches it in the probe" |
| 9 | `__reflex_shrunk_` filtered out of `ImvRow::group_capture_tables` (the audit finding for a missing shrunk-capture table) | `audit/mod.rs` | `fold-g67-review.md` N2 (proved by construction instead). On the release head `src/tests/pg_test_audit_gaps.rs` mentions the shrunk table; whether it pins this finding was not checked |
| 10 | "no sync in the partition flush" for `…later_partitions_deferred` | partition flush | `fold-g8a-report.md`: the mutation run lost its connection before that test reported, "so that one test is not shown RED under (b)" |
| 11 | the WARNING of a manual `reflex_flush_deferred` that matches nothing | message | `fold-g8a-report.md`: "The WARNING itself is not asserted" |

Also recorded as coverage limits rather than mutants: the collation cells of the G3 work
discriminate only through ICU (database default `C.UTF-8`; the mirror case, a linguistic default
with a `COLLATE "C"` argument, "is covered by the same code but not run"; nondeterministic
collations not tested), `fold-g3-report.md`; the X and Y mutation runs of the per-partition
triggers combine three mutations each, "which of them turns a later matrix RED is not
separated", `fold-g8b1-report.md`.

## Expected vs actual

Expected: each line that exists for correctness has a cell that fails without it, or is deleted
as dead. Actual: eleven places where the suite cannot tell.

## What is established and what is not

Established: the mutation outcomes as reported by their authors and reviewers, on the trees and
versions named. For 1 to 4 the reviewer adds "each is either dead or untested; none could be made
to produce a wrong result here". Not established: for each row, whether the line is dead (then
delete it) or needs an input nobody built yet; the state on the release head.

## Fix direction

Per row: build the input that needs the line and add the cell (real IMV, oracle), or delete the
line and show the suite and the review corpora unchanged. Rows 4 and 10 need only a re-run of
the mutation. Row 11: assert the message with the existing notice-capture helper where one
exists.
