# 2026-10-22 — creating an IMV whose `partition_by` reaches a HASH source sub-level fails with "is not partitioned"

**Status: untreated. PRE-EXISTING — reproduced identically on `main` @ `83d43c9`
and on `perf/fresh-node-fill`. Found by that branch's final review.**

## Symptom

```sql
CREATE TABLE rvh_ss (id BIGINT NOT NULL, dp BIGINT NOT NULL, order_date DATE NOT NULL,
                     product_id BIGINT, region TEXT, qty INT) PARTITION BY LIST (dp);
CREATE TABLE rvh_ss_1 PARTITION OF rvh_ss FOR VALUES IN (1) PARTITION BY HASH (product_id);
CREATE TABLE rvh_ss_1_h0 PARTITION OF rvh_ss_1 FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE rvh_ss_1_h1 PARTITION OF rvh_ss_1 FOR VALUES WITH (MODULUS 3, REMAINDER 1);
CREATE TABLE rvh_ss_1_h2 PARTITION OF rvh_ss_1 FOR VALUES WITH (MODULUS 3, REMAINDER 2);
SELECT create_reflex_ivm('rvh_v', 'SELECT id, dp, order_date, product_id, region, qty FROM rvh_ss',
                         'id,dp,product_id', NULL, NULL, NULL, ARRAY['dp','product_id']);
-- ERROR:  "rvh_v_rvh_ss_1" is not partitioned   (raised from tablecmds.c)
```

## Severity

Loud refusal, no wrong data. A LIST → HASH source cannot be mirrored two levels
deep, so such IMVs must use a one-level `partition_by`. The mirror DDL appears to
create the LIST-level node without `PARTITION BY HASH (...)` and then attach HASH
children to it. That part is not confirmed in code.

## Fix direction

Step 0: find where the mirror node DDL derives `sub_strategy`/`sub_columns` for a
HASH level (`list_partition_tree` returns `HASH` correctly; the tree query was proven
identical on HASH nodes) and why the node is created unpartitioned. Then either
mirror the HASH sub-level or refuse it at create time with a clean message naming
the one-level alternative.

## What was ruled out

* Not caused by the `list_partition_tree` rewrite: the old and new queries return
  identical rows on a HASH node keyed on two columns.
