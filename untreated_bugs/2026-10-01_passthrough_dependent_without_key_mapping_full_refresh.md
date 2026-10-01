# A passthrough dependent with no key mapping for a source fully refreshes on every statement — S3 (perf)

`build_passthrough_key_mappings` (src/create_ivm/soundness.rs:90) records a key mapping for a
source only when it owns every key column or a JOIN equality maps it onto a key column. A
secondary source joined on a column outside the dependent's unique key gets no entry
(soundness.rs:174), and every statement on it then takes the full-refresh fallback:
`outer_join_secondary_stmts` (src/trigger/ops.rs:710, any operation on an outer-join
secondary) and `passthrough_op_stmts` (ops.rs:1154 DELETE, ops.rs:1251 UPDATE).

Since 1.11.5 that fallback is `reflex_rebuild_target_rows` (a row diff, so the dependent's own
dependents see only real changes), but it still evaluates the dependent's whole base query and
diffs the whole target per statement. This is the case 1.11.5 does not make proportional: an
upstream IMV rebuild now reaches such a dependent as a small row diff, and the dependent then
recomputes all of itself.

## Reproduction (1.11.5)
```sql
CREATE TABLE prod (k INT PRIMARY KEY, fk INT, v INT);
CREATE TABLE rel (pk INT PRIMARY KEY, attr TEXT);
SELECT create_reflex_ivm('up', 'SELECT pk, attr FROM rel', 'pk');
SELECT create_reflex_ivm('dep', 'SELECT p.k, p.v, u.attr FROM prod p LEFT JOIN up u ON u.pk = p.fk', 'k');
SELECT aggregations->'passthrough_key_mappings' FROM __reflex_ivm_reference WHERE name = 'dep';
-- {"prod": [["k", "k"]]}   (no entry for up)
SELECT reflex_build_delta_sql('dep', 'up', op, base_query, end_query, aggregations::text, base_query)
       ~ 'reflex_rebuild_target_rows'
  FROM __reflex_ivm_reference, unnest(ARRAY['INSERT','UPDATE','DELETE']) op WHERE name = 'dep';
-- t, t, t
```

Correctness is unaffected; the cost is a full base-query evaluation per statement on `up`.

## Ruled out
Not the incident shape: `sop_forecast_view` joins `current_assortment_activity_view` on
`(product_id, location_id)`, both in its key, so it has a mapping and takes the keyed path.

## Fix direction
Scope by the join columns instead of the key: delete / re-insert the dependent rows whose join
column values (`p.fk`) match the changed upstream rows (`u.pk`), through an index on those
columns. Sound for an outer-join secondary because a change to it can only alter rows joined
on those values. Keep the full refresh for FULL JOIN and whenever the join columns cannot be
resolved.
