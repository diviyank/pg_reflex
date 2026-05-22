# IMV migration sweep

Differential check of every real materialized view against a pg_reflex IMV on a
live database (e.g. `db_clone`). Complements the in-CI fuzz harness
(`src/tests/pg_test_fuzz.rs`, `fuzz_differential_exact`) by exercising REAL view
shapes instead of generated ones.

## Prerequisites

- A Postgres with the `pg_reflex` extension installed and the real views present.
- `pip install psycopg2-binary`.

## Run

```bash
python3 scripts/imv_sweep.py --dsn 'host=localhost dbname=db_clone user=postgres'
# optional:
python3 scripts/imv_sweep.py --dsn '...' --sql-dir /path/to/base_db/sql --unique-key id
```

## Output (per view)

| status | meaning |
| --- | --- |
| `PASS` | IMV contents identical to the MV. |
| `LIMITATION` | pg_reflex cleanly rejected the shape (tagged `[reflex-unsupported]`) — expected, not a bug. |
| `CODEGEN-BUG` | Postgres raised while building/maintaining the IMV (generated SQL is wrong). |
| `DIVERGED` | IMV built but its contents differ from the MV. |

Exit code is non-zero if any `CODEGEN-BUG` / `DIVERGED` is found.

## Handling findings

Each `CODEGEN-BUG` / `DIVERGED` is a real finding. Workflow (same as the fuzzer's):

1. Reduce it to a minimal repro.
2. Add it to `docs/fuzz-findings.md` (Finding #N) and an `#[ignore]`'d `#[pg_test]`
   in `src/tests/pg_test_fuzz.rs` (`mod findings`).
3. Fix on a branch; remove `#[ignore]` once green.
4. Never weaken the comparator to make a finding disappear.

## Notes

- The script runs everything inside a `SAVEPOINT` and rolls back — it leaves no
  artifacts in the database.
- `discover_views()` uses a naive `CREATE MATERIALIZED VIEW … AS … ;` regex.
  The real registry layout may differ (templated DDL, schema qualification,
  trailing index/`WITH DATA` clauses); refine the regex on first run if it finds
  zero or malformed views.
- The unique-key heuristic defaults to the MV's first column; pass `--unique-key`
  when that is wrong.
