-- Investigate MERGE join strategy: Nested Loop vs Hash Join
\timing on

DROP EXTENSION IF EXISTS pg_reflex CASCADE;
CREATE EXTENSION pg_reflex;
DROP TABLE IF EXISTS inv_src CASCADE;
CREATE TABLE inv_src (id SERIAL PRIMARY KEY, account_id INTEGER NOT NULL, amount NUMERIC NOT NULL);
INSERT INTO inv_src (account_id, amount)
SELECT 1 + (i % 100000), ROUND((random() * 1000)::numeric, 2)
FROM generate_series(1, 1000000) AS i;
ANALYZE inv_src;

SELECT create_reflex_ivm('inv_view',
    'SELECT account_id, SUM(amount) AS total, COUNT(*) AS cnt FROM inv_src GROUP BY account_id');

-- Baseline: 100K batch with default settings (Nested Loop due to bad row estimate)
CREATE TEMP TABLE staged_batch AS
SELECT (1 + (i % 100000))::integer AS account_id,
       ROUND((random() * 500)::numeric, 2) AS amount
FROM generate_series(1, 100000) AS i;

\echo ''
\echo '=== 100K MERGE — Default (Nested Loop, low work_mem) ==='
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, BUFFERS ON, FORMAT TEXT)
MERGE INTO __reflex_intermediate_inv_view AS t
USING (SELECT account_id AS "account_id", SUM(amount) AS "__sum_amount", COUNT(*) AS "__count_star", COUNT(*) AS __ivm_count
       FROM staged_batch GROUP BY account_id) AS d
ON t."account_id" = d."account_id"
WHEN MATCHED THEN UPDATE SET
    "__sum_amount" = t."__sum_amount" + d."__sum_amount",
    "__count_star" = t."__count_star" + d."__count_star",
    __ivm_count = t.__ivm_count + d.__ivm_count
WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count)
    VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count);

-- Recreate batch (MERGE consumed rows)
DROP TABLE staged_batch;
CREATE TEMP TABLE staged_batch AS
SELECT (1 + (i % 100000))::integer AS account_id,
       ROUND((random() * 500)::numeric, 2) AS amount
FROM generate_series(1, 100000) AS i;

\echo ''
\echo '=== 100K MERGE — Hash Join forced + high work_mem ==='
SET LOCAL enable_nestloop = off;
SET LOCAL work_mem = '256MB';
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, BUFFERS ON, FORMAT TEXT)
MERGE INTO __reflex_intermediate_inv_view AS t
USING (SELECT account_id AS "account_id", SUM(amount) AS "__sum_amount", COUNT(*) AS "__count_star", COUNT(*) AS __ivm_count
       FROM staged_batch GROUP BY account_id) AS d
ON t."account_id" = d."account_id"
WHEN MATCHED THEN UPDATE SET
    "__sum_amount" = t."__sum_amount" + d."__sum_amount",
    "__count_star" = t."__count_star" + d."__count_star",
    __ivm_count = t.__ivm_count + d.__ivm_count
WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count)
    VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count);

-- Now test at 10K batch (typical OLTP)
RESET enable_nestloop;
RESET work_mem;

DROP TABLE staged_batch;
CREATE TEMP TABLE staged_batch AS
SELECT (1 + (i % 100000))::integer AS account_id,
       ROUND((random() * 500)::numeric, 2) AS amount
FROM generate_series(1, 10000) AS i;

\echo ''
\echo '=== 10K MERGE — Default ==='
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, BUFFERS ON, FORMAT TEXT)
MERGE INTO __reflex_intermediate_inv_view AS t
USING (SELECT account_id AS "account_id", SUM(amount) AS "__sum_amount", COUNT(*) AS "__count_star", COUNT(*) AS __ivm_count
       FROM staged_batch GROUP BY account_id) AS d
ON t."account_id" = d."account_id"
WHEN MATCHED THEN UPDATE SET
    "__sum_amount" = t."__sum_amount" + d."__sum_amount",
    "__count_star" = t."__count_star" + d."__count_star",
    __ivm_count = t.__ivm_count + d.__ivm_count
WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count)
    VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count);

DROP TABLE staged_batch;
CREATE TEMP TABLE staged_batch AS
SELECT (1 + (i % 100000))::integer AS account_id,
       ROUND((random() * 500)::numeric, 2) AS amount
FROM generate_series(1, 10000) AS i;

\echo ''
\echo '=== 10K MERGE — Hash Join forced ==='
SET LOCAL enable_nestloop = off;
SET LOCAL work_mem = '256MB';
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, BUFFERS ON, FORMAT TEXT)
MERGE INTO __reflex_intermediate_inv_view AS t
USING (SELECT account_id AS "account_id", SUM(amount) AS "__sum_amount", COUNT(*) AS "__count_star", COUNT(*) AS __ivm_count
       FROM staged_batch GROUP BY account_id) AS d
ON t."account_id" = d."account_id"
WHEN MATCHED THEN UPDATE SET
    "__sum_amount" = t."__sum_amount" + d."__sum_amount",
    "__count_star" = t."__count_star" + d."__count_star",
    __ivm_count = t.__ivm_count + d.__ivm_count
WHEN NOT MATCHED THEN INSERT ("account_id", "__sum_amount", "__count_star", __ivm_count)
    VALUES (d."account_id", d."__sum_amount", d."__count_star", d.__ivm_count);

-- Cleanup
SELECT drop_reflex_ivm('inv_view');
DROP TABLE IF EXISTS inv_src CASCADE;
