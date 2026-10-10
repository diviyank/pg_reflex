# Quoted names PostgreSQL accepts fail at create with a raw error, or are created and raise on write — S3

Reported by the adversarial review of the 1.12.0 fix wave (finding 10, same on `0324f47`); not
re-run on the fix branch. Beside
`2026-10-07_five_untagged_raw_errors_at_create_found_by_g4_review.md` (other shapes).

## Reproduction (as reported)

| Shape | Outcome |
|---|---|
| `… GROUP BY "Id" % 2` | `zero-length delimited identifier` at create (a08) |
| `WITH "C" AS (…) SELECT … FROM "C"` | `Invalid view name` at create (p54) |
| source table named `"a.b"` | syntax error at create (f09) |
| source table named `"my table"` | syntax error at create (f10) |
| source table named `"x""y"` | created; every write to the source raises (f11) |

## Expected vs actual

Expected: created and maintained, or a tagged `[reflex-unsupported]` rejection with a remedy
(rename the table / the CTE; give the grouped expression an alias). Actual: untagged errors
from generated SQL, and for `"x""y"` an IMV that blocks writes to its source.

## What is established and what is not

Established by the review: the outcomes above. Not established: whether anything is left
behind by the failed creates; whether the `"x""y"` writes are rolled back cleanly (a raise
inside a statement trigger does abort the statement).

## Fix direction (hypothesis to falsify)

The last three are one cause: generated names are built by stripping the quotes of the source
name (`source_trigger_suffix`, the `__reflex_new_…` / `__reflex_delta_…` names), which is not
an identifier for a name holding a space, a dot or a quote. Reject such source names at create
with a tagged message until the generated names go through `safe_identifier`. `"x""y"` first:
it is the only row that is not loud at create.
