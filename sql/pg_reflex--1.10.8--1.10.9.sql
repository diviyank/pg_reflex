-- Migration: pg_reflex 1.10.8 → 1.10.9
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.10.9';
--
-- Replace the module (.so) BEFORE running this. 1.10.9 is a module-only
-- reflex_doctor / reflex_audit hardening release: the reflex_doctor and
-- reflex_audit signatures and the catalog are unchanged, so this migration
-- installs no DDL. The behavioural fixes ship entirely in the recompiled
-- module:
--
--   * Schema-qualified partition-child queries in the archive_residue audit
--     check — no more `relation "…" does not exist` crash when auditing an IMV
--     registered in a non-public schema under a narrow search_path.
--   * Engine-level per-check isolation: a check that raises a Postgres error now
--     yields a `check-errored` finding instead of aborting reflex_audit /
--     reflex_doctor; one partition's bad where_predicate degrades to
--     "could not verify" rather than forfeiting the whole check.
--   * reflex_doctor archive-residue rows now carry the real IMV name and an
--     executable reflex_reconcile_partition(...) command, fix => TRUE actually
--     reconciles residue, and > 3 residual partitions collapse to a single
--     reflex_reconcile(<imv>). Prose ("could not verify" / bare-name) findings
--     are report-only.
--   * reflex_doctor also surfaces orphan aux tables (F9) and duplicate trigger
--     functions (F11) that reflex_audit detects (report-only).

DO $migrate$ BEGIN
    RAISE NOTICE 'pg_reflex 1.10.9: reflex_doctor/reflex_audit hardening (module-only; no DDL changes).';
END $migrate$;
