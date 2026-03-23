-- Migration: pg_reflex 1.0.0 → 1.0.1
--
-- New functions for materialized view refresh support.

-- Refresh a single IMV by rebuilding from source.
CREATE FUNCTION "refresh_reflex_imv"(
	"view_name" TEXT
) RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'refresh_reflex_imv_wrapper';

-- Refresh ALL IMVs that depend on a given source table or materialized view.
CREATE FUNCTION "refresh_imv_depending_on"(
	"source" TEXT
) RETURNS TEXT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'refresh_imv_depending_on_wrapper';
