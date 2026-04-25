-- Migration: pg_reflex 1.2.1 → 1.3.0
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '1.3.0';
--
-- Changes in 1.3.0:
--
-- Theme A — Top-K MIN/MAX heap (R3)
--   New `topk` integer parameter on create_reflex_ivm. When > 0, MIN/MAX
--   intermediate columns gain a sibling __<name>_topk array column that
--   maintains the K extremum values per group. Retraction uses multi-set
--   subtraction (cheap path), with fallback to scoped recompute when the
--   array underflows.
--
--   New SQL helper:
--     public.__reflex_array_subtract_multiset(anyarray, anyarray) -> anyarray
--
--   No schema migration on existing IMVs — top-K is opt-in per IMV at
--   create time. Retrofit SPI (reflex_enable_topk) is tracked for 1.3.1.
--
-- Theme B — Flush histogram (R6)
--   __reflex_ivm_reference gains `flush_ms_history BIGINT[]` (ring buffer,
--   size 64). Populated by reflex_flush_deferred. New SPI:
--     reflex_ivm_histogram(view_name) -> (p50_ms, p95_ms, p99_ms, max_ms, samples)
--
-- Theme C — pg_stat_statements correlation
--   Each per-IMV flush body sets application_name = 'reflex_flush:<view>'
--   for its duration so operators with track_application_name = on can
--   filter pg_stat_statements / log_line_prefix entries by IMV.
--
-- Theme D — scalar MIN/MAX is a tested supported case
--   No DDL change. The 1.0.x sentinel-row path already supported scalar
--   MIN/MAX without GROUP BY; 1.3.0 adds dedicated correctness tests
--   covering the case (audit unsupported §2). With topk=K, scalar
--   MIN/MAX retraction becomes O(K) instead of O(N) when the heap is
--   well-stocked.

-- === 1.3.0-A: SQL helper for top-K multi-set subtraction ===
CREATE OR REPLACE FUNCTION public.__reflex_array_subtract_multiset(
    arr anyarray, remove anyarray
) RETURNS anyarray
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $REFLEX$
DECLARE
    i INT;
    pos INT;
BEGIN
    IF arr IS NULL THEN RETURN NULL; END IF;
    IF remove IS NULL THEN RETURN arr; END IF;
    FOR i IN 1..COALESCE(cardinality(remove), 0) LOOP
        pos := array_position(arr, remove[i]);
        IF pos IS NOT NULL THEN
            arr := arr[1:pos-1] || arr[pos+1:];
        END IF;
    END LOOP;
    RETURN arr;
END;
$REFLEX$;

-- === 1.3.0-C: flush_ms_history ring buffer ===
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS flush_ms_history BIGINT[] DEFAULT ARRAY[]::BIGINT[];

-- === 1.3.0 SPIs ===
-- create_reflex_ivm overload + reflex_ivm_histogram are auto-registered by
-- pgrx schema generation. No manual SQL needed here.
