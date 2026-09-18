# 2026-09-18 — a partitioned IMV created by bare name under a non-public `search_path` cannot be created

**Status: untreated.** Found during the 1.11.4 prod-readiness rehearsal (base-db view
definitions replayed against a `db_clone`-shaped `alp` schema). **Not a 1.11.4
regression:** reproduced identically on a real 1.11.3 install (pgrx pg16) and on 1.11.4
(pgrx pg17).

Severity: **medium — availability, no wrong data.** The create fails and rolls back; nothing
is left half-built. But every partitioned IMV is uncreatable through this path.

## Reproduction

```sql
CREATE SCHEMA alp;
SET search_path = alp, public;
CREATE TABLE ss (dem_plan_id BIGINT NOT NULL, order_date DATE NOT NULL, qty INT NOT NULL)
  PARTITION BY LIST (dem_plan_id);
CREATE TABLE ss_1 PARTITION OF ss FOR VALUES IN (1);
INSERT INTO ss VALUES (1, '2026-06-10', 5);
SELECT create_reflex_ivm('p_imv', 'SELECT dem_plan_id, order_date, qty FROM ss',
                         'dem_plan_id,order_date', 'UNLOGGED', 'DEFERRED', NULL,
                         ARRAY['dem_plan_id']);
-- ERROR:  relation "public.p_imv" does not exist
```

The real-world shape was base-db's `sop_forecast_view` (`partition_by: [dem_plan_id,
order_date]`) created as `create_reflex_ivm(view_name => 'sop_forecast_view', …)` under
`SET search_path = alp, public`. The minimal repro above was run verbatim on 1.11.4 (pgrx
pg17): the bare `p_imv` fails as shown, and the same create as `'alp.q_imv'` succeeds.

## Mechanism

`create_reflex_ivm` creates the target unqualified, so it lands in the first
`search_path` schema (`alp`); `create_ivm/mod.rs:1898` already WARNs that bare names under a
non-public `search_path` are fragile. The partition-mirror DDL does not follow it:
`build_partition_node_ddl_pair` (`src/partition.rs:579-580`) resolves the schema with
`split_qualified_name(view_name).0.unwrap_or("public")`, so a top-level child is created
`PARTITION OF "public"."<imv>"`, which does not exist. The same `unwrap_or("public")`
default appears at 11 sites in `src/partition.rs` and `src/create_ivm/`, identical in
1.11.3 and 1.11.4.

## Who hits it

base-db's async `recreate_views` / `create_missing_views` render creates with no schema
(`_render_create_imv(spec)`), i.e. bare names under `SET search_path = <company>, public`,
so they cannot create any partitioned IMV. Before base-db `bba4fbc` that path also
ignored pg_reflex's `ERROR: …` return and logged success; it now reports the failure. The
sync executor passes the schema (qualified names) and is unaffected, as are existing IMVs
and `ALTER EXTENSION … UPDATE`.

## Ruled out

- **A 1.11.4 regression:** same error on 1.11.3.
- **Qualified names:** the same create as `create_reflex_ivm('alp.q_imv', …)` succeeds
  (verified), and schema-qualified partitioned IMVs are covered by existing tests (e.g.
  `ish_writer_without_public_schema_usage_can_write_the_ignored_source`).

## Fix direction

Resolve a bare IMV name to the schema its target was actually created in (the registry's
`target_schema`, or `current_schema()` at create time) once, at the top of create, and
pass the qualified name down, instead of defaulting to `public` at each site. The
create-time WARN at `create_ivm/mod.rs:1898` was the 2026-06 guardrail for bare names
under a non-public `search_path`, shipped because threading the schema through was the
larger change; this report is the concrete failure that makes that change necessary.
Alternatively, base-db can pass the schema on the async
path too; that removes the trigger but not the defect.

Test: a real partitioned IMV created by bare name under `SET search_path = <other>,
public` succeeds, its children live in `<other>`, and it passes `assert_imv_correct`
after a source INSERT; must be RED before the fix.
