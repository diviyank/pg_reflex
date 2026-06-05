# pg_reflex gap matrix

Sweeps shape × CTE × mode × partition × op × size against the fair
`bare DML + REFRESH MV` baseline; writes every cell to `bench_gap_results`.

## Run (synthetic)
    ./run.sh smoke   # ~1 min, 100k base, every code path
    ./run.sh full    # tens of min, 1M base + 10M scaling sweep

## Run (real-data overlay, against db_clone)
    psql -U postgres -h localhost -d db_clone -f 00_harness.sql
    psql -U postgres -h localhost -d db_clone -v RUN_TS="<ts>" -f 30_dbclone_overlay.sql
    psql -U postgres -h localhost -d db_clone -f 40_report.sql

## Reading results
- `mismatches <> 0` ⇒ the cell is INVALID (correctness failure); its timing is meaningless.
- `advantage_pct < 0` ⇒ a GAP (IMV slower than bare+REFRESH). The report ranks
  these and tags a root-cause hypothesis.

## Files
- `00_harness.sql`  results sink + `gap_measure` procedure
- `10_synthetic_setup.sql`  8-shape config-driven build
- `20_synthetic_driver.sql`  core + cascade (+ scaling under `-v SCALING=1`) sweeps
- `30_dbclone_overlay.sql`  real `db_clone` views, scratch-isolated
- `40_report.sql`  ranked gaps + raw matrix
