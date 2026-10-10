# A CTE over a partitioned source does not follow a newly created partition — S1 (DEFERRED silent) / S2 (IMMEDIATE raises)

Pre-existing. Versions: 1.11.5 (6946400) and the 1.12.0 pre-release (0324f47), pg17. Found by the non-regression review of 1.12.0 (full-review-r1, 650 definitions x IMMEDIATE / DEFERRED, every DML step its own committed transaction); same outcome on both trees, so not a regression.

## Reproduction (probe fixture)
```sql
CREATE TABLE sales_part (id INT NOT NULL, region TEXT NOT NULL, amount NUMERIC, sale_date DATE, qty INT,
  PRIMARY KEY (id, region)) PARTITION BY LIST (region);
CREATE TABLE sales_part_us  PARTITION OF sales_part FOR VALUES IN ('US');
CREATE TABLE sales_part_eu  PARTITION OF sales_part FOR VALUES IN ('EU');
CREATE TABLE sales_part_row PARTITION OF sales_part FOR VALUES IN ('ASIA', 'LATAM');
-- seed rows, then:
SELECT create_reflex_ivm('r_c19',
  'WITH per_day AS (SELECT region, sale_date, SUM(amount) AS total FROM sales_part GROUP BY region, sale_date)
   SELECT region, MAX(total) AS best_day, COUNT(*) AS days FROM per_day GROUP BY region');
CREATE TABLE sales_part_apac PARTITION OF sales_part FOR VALUES IN ('APAC');
INSERT INTO sales_part VALUES (9100, 'APAC', ...);   -- the review does not print the row; DEFERRED result shows (APAC,6.5,2)
```
Registry: `r_c19{IMMEDIATE,2} r_c19__cte_per_day{IMMEDIATE,1}`.

## Observed vs expected
- IMMEDIATE: `ERROR: no partition of relation "__reflex_intermediate_r_c19" found for row DETAIL: Partition key of the failing row contains (region) = (APAC). CONTEXT: SQL statement "MERGE INTO "__reflex_interme...`. Raised on "ins new partition" and on "move to new partition" — the insert is refused.
- DEFERRED: no error; after the insert imv has `` (empty), query has `(APAC,6.5,2)`; after "move to new partition" imv `(US,46.0,10)` vs query `(US,46.0,9) ; (APAC,38.0,3)`; the IMV stays wrong through every later step.
- Expected: the new partition's rows appear in the IMV, as for the plain aggregate.

## What was ruled out
- Not a regression (identical on 6946400).
- The plain aggregate over the same source (d35, t03) and the IMV-over-IMV form (z03: `part_daily` then `part_region` over it) follow the new partition correctly.
- Failure is specific to the CTE's sub-IMV intermediate `__reflex_intermediate_r_c19`, which is partitioned like the source but is not given the new partition.

## Fix direction (hypothesis to falsify)
Partition maintenance (the sync that mirrors a new source partition into the intermediate of a partitioned IMV) does not visit the intermediate of the CTE sub-IMV. Extend it to the sub-IMVs of a CTE, or make the intermediate fall back to a DEFAULT partition. Test: the oracle after `CREATE TABLE ... PARTITION OF`, an insert, and a move into the new partition, both modes; also check whether the DEFERRED silent result can be turned into a loud refusal.
