-- Test: do we capture inner table names from subqueries?
SELECT * FROM (SELECT * FROM inner_table) AS sub;
