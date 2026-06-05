\set ON_ERROR_STOP on
\timing off
\if :{?MAXVOL}
\else
\set MAXVOL 100000
\endif
\if :{?RUN_TS}
\else
\set RUN_TS '2026-06-05 12:00:00+00'
\endif
-- bench.base_rows is set by 10_synthetic_setup.sql in its own session; this is
-- a separate psql session, so re-establish it here. MUST match the BASE_ROWS the
-- sources were built with, or the id-lanes miss the seeded rows.
\if :{?BASE_ROWS}
\else
\set BASE_ROWS 1000000
\endif
SET bench.maxvol = :MAXVOL;
SET bench.base_rows = :BASE_ROWS;

-- Given op + lane cursor, returns the [lo,hi] id window for an edit of size v.
-- Lanes carve disjoint id ranges so ops never collide:
--   DELETE  : [1 .. 0.2*base)        UPDATE : [0.2*base .. 0.5*base)
--   FLIP    : [0.5*base .. 0.8*base) INSERT : [base+1 .. )
-- A per-(op) cursor advances by v each call.
CREATE OR REPLACE FUNCTION gap_window(_op text, _base bigint, _v bigint, INOUT _cursor bigint, OUT lo bigint, OUT hi bigint)
LANGUAGE plpgsql AS $$
DECLARE lane_lo bigint; lane_hi bigint;
BEGIN
    CASE _op
        WHEN 'DELETE'  THEN lane_lo := 1;                  lane_hi := (_base*2)/10;
        WHEN 'UPDATE'  THEN lane_lo := (_base*2)/10 + 1;   lane_hi := (_base*5)/10;
        WHEN 'FLIP_OUT' THEN lane_lo := (_base*5)/10 + 1;  lane_hi := (_base*8)/10;
        WHEN 'FLIP_IN'  THEN lane_lo := (_base*5)/10 + 1;  lane_hi := (_base*8)/10; -- same lane as FLIP_OUT (re-enters)
        WHEN 'INSERT'  THEN lane_lo := _base + 1;          lane_hi := _base*100;     -- effectively unbounded
        ELSE RAISE EXCEPTION 'unknown op %', _op;
    END CASE;
    IF _cursor IS NULL OR _cursor < lane_lo THEN _cursor := lane_lo; END IF;
    lo := _cursor; hi := _cursor + _v - 1;
    IF _op <> 'INSERT' AND hi > lane_hi THEN
        -- wrap within the lane if exhausted (acceptable: re-touches rows, still correct)
        _cursor := lane_lo; lo := lane_lo; hi := lane_lo + _v - 1;
    END IF;
    _cursor := hi + 1;
END $$;

-- Build the edit SQL for a given op against a source table.
CREATE OR REPLACE FUNCTION gap_edit_sql(_op text, _tbl text, _lo bigint, _hi bigint)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE _op
        WHEN 'INSERT'  THEN format('INSERT INTO %I SELECT g, g%%20, g%%1000, (g%%99)+1 FROM generate_series(%s,%s) g', _tbl,_lo,_hi)
        WHEN 'UPDATE'  THEN format('UPDATE %I SET qty=qty+1 WHERE id BETWEEN %s AND %s', _tbl,_lo,_hi)
        WHEN 'DELETE'  THEN format('DELETE FROM %I WHERE id BETWEEN %s AND %s', _tbl,_lo,_hi)
        WHEN 'FLIP_OUT' THEN format('UPDATE %I SET qty = -abs(qty) WHERE id BETWEEN %s AND %s', _tbl,_lo,_hi)
        WHEN 'FLIP_IN'  THEN format('UPDATE %I SET qty = abs(qty)+1 WHERE id BETWEEN %s AND %s', _tbl,_lo,_hi)
    END;
$$;

-- Core sweep: every shape, both modes, ops × edit sizes, at a fixed base size.
-- NOTE: gap_measure COMMITs internally. Driving the outer loop with a
-- `FOR s IN SELECT * FROM gap_shape` query would hold a portal across that
-- COMMIT and raise "cannot commit while a portal is pinned". So we iterate a
-- text[] of shape keys (in-memory, no portal) and fetch each shape by key.
CREATE OR REPLACE PROCEDURE gap_core_sweep(_run_ts timestamptz) LANGUAGE plpgsql AS $$
DECLARE
    skeys text[]; k text; s gap_shape; m text; op text; v bigint;
    maxv bigint := current_setting('bench.maxvol')::bigint;
    base bigint := current_setting('bench.base_rows')::bigint;
    vols bigint[] := ARRAY[1000, 10000, 100000];
    ops  text[]  := ARRAY['INSERT','UPDATE','DELETE','FLIP_OUT','FLIP_IN'];
    imv_sql text; mv text; lo bigint; hi bigint;
    cur_del bigint; cur_upd bigint; cur_flo bigint; cur_fli bigint; cur_ins bigint;
    cur bigint; flush_src text;
BEGIN
    SELECT array_agg(skey ORDER BY skey) INTO skeys FROM gap_shape;
    FOREACH k IN ARRAY skeys LOOP
        SELECT * INTO s FROM gap_shape WHERE skey = k;
        cur_del:=NULL; cur_upd:=NULL; cur_flo:=NULL; cur_fli:=NULL; cur_ins:=NULL;
        FOREACH m IN ARRAY ARRAY['IMMEDIATE','DEFERRED'] LOOP
            -- (Re)create the IMV for this (shape, mode) on the *_imv source.
            CALL gap_drop_imv(s.skey||'_v');
            imv_sql := format(s.body_tmpl, s.skey||'_imv');
            IF s.part_by IS NULL THEN
                PERFORM create_reflex_ivm(s.skey||'_v', imv_sql, s.unique_cols, NULL, m, NULL);
            ELSE
                PERFORM create_reflex_ivm(s.skey||'_v', imv_sql, s.unique_cols, NULL, m, NULL, ARRAY[s.part_by]);
            END IF;
            EXECUTE format('ANALYZE %I', s.skey||'_v');
            mv := 'mv_'||s.skey;
            flush_src := s.skey||'_imv';

            FOREACH op IN ARRAY ops LOOP
                FOREACH v IN ARRAY vols LOOP
                    IF v > maxv THEN CONTINUE; END IF;
                    -- pick the right per-op cursor
                    cur := CASE op WHEN 'DELETE' THEN cur_del WHEN 'UPDATE' THEN cur_upd
                                   WHEN 'FLIP_OUT' THEN cur_flo WHEN 'FLIP_IN' THEN cur_fli
                                   ELSE cur_ins END;
                    SELECT w.lo, w.hi, w._cursor INTO lo, hi, cur
                      FROM gap_window(op, base, v, cur) w;
                    CASE op WHEN 'DELETE' THEN cur_del:=cur;
                            WHEN 'UPDATE' THEN cur_upd:=cur;
                            WHEN 'FLIP_OUT' THEN cur_flo:=cur;
                            WHEN 'FLIP_IN' THEN cur_fli:=cur;
                            ELSE cur_ins:=cur;
                    END CASE;

                    CALL gap_measure(
                        _run_ts, 'synthetic',
                        format('%s/%s/%s/%s v=%s', s.shape, CASE WHEN s.cte THEN 'CTE' ELSE 'noCTE' END, m, op, v),
                        s.shape, s.cte, m, s.partitioned, op, base, v,
                        gap_edit_sql(op, s.skey||'_imv', lo, hi),
                        gap_edit_sql(op, s.skey||'_base', lo, hi),
                        CASE WHEN m='DEFERRED' AND NOT s.cte THEN flush_src ELSE NULL END,
                        s.skey||'_v', mv, imv_sql);
                END LOOP;
            END LOOP;
        END LOOP;
    END LOOP;
END $$;

\echo '=== core sweep starting ==='
CALL gap_core_sweep(:'RUN_TS'::timestamptz);
\echo '=== core sweep done ==='
SELECT count(*) AS cells, count(*) FILTER (WHERE mismatches<>0) AS invalid,
       count(*) FILTER (WHERE advantage_pct<0) AS gaps
  FROM bench_gap_results WHERE run_ts = :'RUN_TS'::timestamptz;

-- Cascade sweep: UPDATE on a CTE shape's source. Only meaningful for CTE shapes;
-- non-CTE shapes are pruned and logged. Creates a fresh IMV, then measures
-- a cascade operation (which drives the CTE source through deferred batching).
CREATE OR REPLACE PROCEDURE gap_cascade_sweep(_run_ts timestamptz) LANGUAGE plpgsql AS $$
DECLARE
    skeys text[]; k text; s gap_shape;
    v bigint := LEAST(10000, current_setting('bench.maxvol')::bigint);
    base bigint := current_setting('bench.base_rows')::bigint;
    lo bigint; hi bigint; cur bigint := NULL; imv_sql text;
BEGIN
    SELECT array_agg(skey ORDER BY skey) INTO skeys FROM gap_shape;
    FOREACH k IN ARRAY skeys LOOP
        SELECT * INTO s FROM gap_shape WHERE skey = k;
        IF NOT s.cte THEN
            INSERT INTO bench_gap_results VALUES (
                _run_ts,'synthetic',format('%s/%s/cascade',s.shape,s.skey),s.shape,s.cte,
                NULL,s.partitioned,'CASCADE',base,NULL,NULL,NULL,NULL,NULL,NULL,'pruned: cascade requires CTE');
            CONTINUE;
        END IF;
        CALL gap_drop_imv(s.skey||'_v');
        imv_sql := format(s.body_tmpl, s.skey||'_imv');
        IF s.part_by IS NULL THEN
            PERFORM create_reflex_ivm(s.skey||'_v', imv_sql, s.unique_cols, NULL, 'DEFERRED', NULL);
        ELSE
            PERFORM create_reflex_ivm(s.skey||'_v', imv_sql, s.unique_cols, NULL, 'DEFERRED', NULL, ARRAY[s.part_by]);
        END IF;
        SELECT w.lo, w.hi INTO lo, hi FROM gap_window('UPDATE', base, v, cur) w;
        CALL gap_measure(_run_ts,'synthetic',format('%s/%s/cascade v=%s',s.shape,s.skey,v),
            s.shape,s.cte,'DEFERRED',s.partitioned,'CASCADE',base,v,
            gap_edit_sql('UPDATE', s.skey||'_imv', lo, hi),
            gap_edit_sql('UPDATE', s.skey||'_base', lo, hi),
            NULL, s.skey||'_v', 'mv_'||s.skey, imv_sql);
    END LOOP;
END $$;

-- Scaling sweep: 4 representative shapes at base sizes {100k,1M,10M}, fixed v=10k.
-- Rebuilds the *_imv/*_base/mv for each size in-place. Only run with -v SCALING=1.
CREATE OR REPLACE PROCEDURE gap_scaling_sweep(_run_ts timestamptz, _sizes bigint[]) LANGUAGE plpgsql AS $$
DECLARE
    keys text[] := ARRAY['p_nc_np','p_nc_lp','c_nc_np','c_nc_lp'];
    k text; s gap_shape; n bigint; lo bigint; hi bigint; cur bigint; imv_sql text; src text; p int;
    v bigint := 10000;
BEGIN
    FOREACH n IN ARRAY _sizes LOOP
        FOREACH k IN ARRAY keys LOOP
            SELECT * INTO s FROM gap_shape WHERE skey=k;
            -- rebuild both sources at size n
            FOREACH src IN ARRAY ARRAY['imv','base'] LOOP
                EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', k||'_'||src);
                IF s.partitioned='LIST' THEN
                    EXECUTE format('CREATE TABLE %I (id int NOT NULL, region int NOT NULL, grp int NOT NULL, qty int) PARTITION BY LIST (region)', k||'_'||src);
                    FOR p IN 0..19 LOOP EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%s)', k||'_'||src||'_'||p, k||'_'||src, p); END LOOP;
                ELSE
                    EXECUTE format('CREATE TABLE %I (id int PRIMARY KEY, region int NOT NULL, grp int NOT NULL, qty int)', k||'_'||src);
                END IF;
                EXECUTE format('INSERT INTO %I SELECT i, i%%20, i%%1000, (random()*99)::int+1 FROM generate_series(1,%s) i', k||'_'||src, n);
                EXECUTE format('ANALYZE %I', k||'_'||src);
            END LOOP;
            EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %I', 'mv_'||k);
            EXECUTE format('CREATE MATERIALIZED VIEW %I AS %s', 'mv_'||k, format(s.body_tmpl, k||'_base'));
            CALL gap_drop_imv(k||'_v');
            imv_sql := format(s.body_tmpl, k||'_imv');
            IF s.part_by IS NULL THEN
                PERFORM create_reflex_ivm(k||'_v', imv_sql, s.unique_cols, NULL, 'DEFERRED', NULL);
            ELSE
                PERFORM create_reflex_ivm(k||'_v', imv_sql, s.unique_cols, NULL, 'DEFERRED', NULL, ARRAY[s.part_by]);
            END IF;
            cur := NULL;
            SELECT w.lo, w.hi INTO lo, hi FROM gap_window('UPDATE', n, v, cur) w;
            CALL gap_measure(_run_ts,'synthetic',format('%s/%s/scale base=%s',s.shape,k,n),
                s.shape,s.cte,'DEFERRED',s.partitioned,'UPDATE',n,v,
                gap_edit_sql('UPDATE', k||'_imv', lo, hi),
                gap_edit_sql('UPDATE', k||'_base', lo, hi),
                k||'_imv', k||'_v', 'mv_'||k, imv_sql);
        END LOOP;
    END LOOP;
END $$;

\echo '=== cascade sweep ==='
CALL gap_cascade_sweep(:'RUN_TS'::timestamptz);

\if :{?SCALING}
\echo '=== scaling sweep ==='
CALL gap_scaling_sweep(:'RUN_TS'::timestamptz, ARRAY[100000,1000000,10000000]::bigint[]);
\endif
