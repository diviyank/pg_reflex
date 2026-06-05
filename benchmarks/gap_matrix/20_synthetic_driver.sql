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
SET bench.maxvol = :MAXVOL;

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
