\timing on
SET search_path TO alp, public;
SET work_mem = '256MB';
SET maintenance_work_mem = '2GB';

\echo === Listing registered IMVs ===
SELECT name,
       pg_size_pretty(pg_total_relation_size(format('alp.%I', name)::regclass)) AS size,
       COALESCE((SELECT reltuples::bigint FROM pg_class
                 WHERE relname = r.name AND relnamespace = 'alp'::regnamespace), 0) AS rows
FROM public.__reflex_ivm_reference r
WHERE name NOT LIKE '%\_\_cte\_%'
ORDER BY graph_depth, name;
