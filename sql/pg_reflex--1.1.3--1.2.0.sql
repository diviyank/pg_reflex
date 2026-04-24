-- Migration: pg_reflex 1.1.3 → 1.2.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.2.0';
--
-- Changes in 1.2.0:
--
-- Theme 1 — MIN/MAX retraction
--   The recompute path that fires on DELETE/UPDATE-old for MIN/MAX aggregates
--   is now scoped to the affected-groups table (__reflex_affected_<view>)
--   rather than re-aggregating the full source. Code-gen only — the emitted
--   SQL changes shape but existing IMVs need no schema migration.
--
-- Theme 2 — correctness bug fixes
--   Transitive cycle detection in create_reflex_ivm, catalog-lookup WARNING
--   in resolve_column_type, 64-bit advisory-lock keys, STRICT-safe
--   reflex_build_delta_sql signature, and other bug fixes from
--   journal/2026-04-22_bug_report.md.
--
-- Theme 3 — operational safety
--   reflex_rebuild_imv SPI (exposed as an alias over reflex_reconcile) to
--   rebuild an IMV from scratch. sql_drop event trigger auto-drops IMVs
--   whose source table is dropped. ddl_command_end event trigger raises a
--   WARNING on ALTER TABLE of a tracked source so operators can rerun
--   reflex_rebuild_imv. Per-IMV SAVEPOINT around each flush so one bad IMV
--   doesn't abort a cascade.
--
-- Theme 4 — observability
--   __reflex_ivm_reference gains four columns: last_flush_ms, last_flush_rows,
--   flush_count, last_error. These are populated by reflex_flush_deferred.
--   New SPIs: reflex_ivm_status(), reflex_ivm_stats(view_name),
--   reflex_explain_flush(view_name).

-- === Registry columns (Theme 4.1) ===
--
-- 1.1.3 shipped without these columns; 1.2.0 tables declare them in the
-- bootstrap DDL, but existing installations need ALTERs so upgraded
-- installations match a fresh install. IF NOT EXISTS keeps this idempotent.
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS last_flush_ms BIGINT,
    ADD COLUMN IF NOT EXISTS last_flush_rows BIGINT,
    ADD COLUMN IF NOT EXISTS flush_count BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

-- === Function signature updates (Theme 2 / bug #13) ===
--
-- Any signature renegotiation of reflex_build_delta_sql lands here in a
-- future migration. 1.2.0 keeps the 1.1.3 signature unchanged (seven TEXT
-- args) — no ALTER needed.

-- Nothing else to do: Theme 1's change is code-gen only (emitted SQL shape),
-- so installed triggers pick up the new behavior on the next flush; Theme 3
-- event triggers are installed by extension_sql in lib.rs during UPDATE;
-- Theme 4 SPIs are new #[pg_extern]s auto-registered by the upgrade.
