CREATE OR REPLACE FUNCTION public.__reflex_ins_trigger_on_upg_a()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
        _skip_cols TEXT; _skip_pred TEXT; _skip_pred_clause TEXT; _skip_sql TEXT; _filter_skip BOOLEAN;
        _old_has BOOLEAN; _new_has BOOLEAN; _directional_op TEXT;
BEGIN
    SELECT EXISTS(SELECT 1 FROM "__reflex_new_upg_a" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;
    FOR _rec IN
      SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate, ignored_sources
      FROM public.__reflex_ivm_reference
      WHERE 'upg_a' = ANY(depends_on) AND enabled = TRUE
      ORDER BY graph_depth, name
    LOOP
      IF _rec.ignored_sources IS NOT NULL AND ('upg_a' = ANY(_rec.ignored_sources) OR 'upg_a' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
      IF _rec.where_predicate IS NOT NULL THEN
               EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '__reflex_new_upg_a', _rec.where_predicate) INTO _pred_match;
               IF NOT _pred_match THEN CONTINUE; END IF;
             END IF;

      
      
      
      PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
      _sql := public.reflex_build_delta_sql(_rec.name, 'upg_a', 'INSERT', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
      IF _sql <> '' THEN
        FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
          IF _stmt <> '' THEN EXECUTE _stmt; END IF;
        END LOOP;
      END IF;
    END LOOP;
    RETURN NULL;
END;
 $function$

-- next function
CREATE OR REPLACE FUNCTION public.__reflex_del_trigger_on_upg_a()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
        _skip_cols TEXT; _skip_pred TEXT; _skip_pred_clause TEXT; _skip_sql TEXT; _filter_skip BOOLEAN;
        _old_has BOOLEAN; _new_has BOOLEAN; _directional_op TEXT;
BEGIN
    SELECT EXISTS(SELECT 1 FROM "__reflex_old_upg_a" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;
    FOR _rec IN
      SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate, ignored_sources
      FROM public.__reflex_ivm_reference
      WHERE 'upg_a' = ANY(depends_on) AND enabled = TRUE
      ORDER BY graph_depth, name
    LOOP
      IF _rec.ignored_sources IS NOT NULL AND ('upg_a' = ANY(_rec.ignored_sources) OR 'upg_a' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
      IF _rec.where_predicate IS NOT NULL THEN
               EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '__reflex_old_upg_a', _rec.where_predicate) INTO _pred_match;
               IF NOT _pred_match THEN CONTINUE; END IF;
             END IF;

      
      
      
      PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
      _sql := public.reflex_build_delta_sql(_rec.name, 'upg_a', 'DELETE', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
      IF _sql <> '' THEN
        FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
          IF _stmt <> '' THEN EXECUTE _stmt; END IF;
        END LOOP;
      END IF;
    END LOOP;
    RETURN NULL;
END;
 $function$

-- next function
CREATE OR REPLACE FUNCTION public.__reflex_upd_trigger_on_upg_a()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
        _skip_cols TEXT; _skip_pred TEXT; _skip_pred_clause TEXT; _skip_sql TEXT; _filter_skip BOOLEAN;
        _old_has BOOLEAN; _new_has BOOLEAN; _directional_op TEXT;
BEGIN
    SELECT EXISTS(SELECT 1 FROM "__reflex_new_upg_a" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;
    FOR _rec IN
      SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate, ignored_sources
      FROM public.__reflex_ivm_reference
      WHERE 'upg_a' = ANY(depends_on) AND enabled = TRUE
      ORDER BY graph_depth, name
    LOOP
      IF _rec.ignored_sources IS NOT NULL AND ('upg_a' = ANY(_rec.ignored_sources) OR 'upg_a' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
      IF _rec.where_predicate IS NOT NULL THEN
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1) OR EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)',
                          '__reflex_new_upg_a', _rec.where_predicate, '__reflex_old_upg_a', _rec.where_predicate) INTO _pred_match;
           IF NOT _pred_match THEN CONTINUE; END IF;
         END IF;

      SELECT string_agg(
              CASE WHEN t.typname IN ('json','xml')
                   THEN format('%I::text', sel.col)
                   ELSE format('%I', sel.col)
              END,
              ', ' ORDER BY sel.col)
              INTO _skip_cols
              FROM jsonb_array_elements_text(
                   COALESCE(_rec.aggregations::jsonb->'imv_relevant_columns'->'upg_a', '[]'::jsonb)
              ) AS sel(col)
              JOIN pg_attribute a
                ON a.attname = sel.col
               AND a.attrelid = 'upg_a'::regclass
               AND a.attnum > 0 AND NOT a.attisdropped
              JOIN pg_type t ON t.oid = a.atttypid;
         IF _skip_cols IS NOT NULL AND _skip_cols <> '' THEN
           _skip_pred := _rec.aggregations::jsonb->'imv_relevant_where'->>'upg_a';
           IF _skip_pred IS NULL OR _skip_pred = '' THEN
             _skip_pred_clause := '';
           ELSE
             _skip_pred_clause := ' WHERE ' || _skip_pred;
           END IF;
           _skip_sql := format(
             'WITH _o AS (SELECT %s FROM %I%s), _n AS (SELECT %s FROM %I%s) '
             || 'SELECT NOT EXISTS(TABLE _o EXCEPT ALL TABLE _n) AND NOT EXISTS(TABLE _n EXCEPT ALL TABLE _o)',
             _skip_cols, '__reflex_old_upg_a', _skip_pred_clause,
             _skip_cols, '__reflex_new_upg_a', _skip_pred_clause
           );
           EXECUTE _skip_sql INTO _filter_skip;
           IF _filter_skip THEN CONTINUE; END IF;
         END IF;

      _directional_op := 'UPDATE';
         IF _skip_cols IS NOT NULL AND _skip_cols <> '' THEN
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I%s LIMIT 1)',
                          '__reflex_old_upg_a', _skip_pred_clause) INTO _old_has;
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I%s LIMIT 1)',
                          '__reflex_new_upg_a', _skip_pred_clause) INTO _new_has;
           IF (NOT _old_has) AND _new_has THEN
             _directional_op := 'INSERT_PROMOTED';
           ELSIF _old_has AND (NOT _new_has) THEN
             _directional_op := 'DELETE_PROMOTED';
           END IF;
         END IF;

      IF _directional_op = 'INSERT_PROMOTED' THEN
           DECLARE _pc_sql TEXT; _pc_plan TEXT; _pc_est BIGINT; _pc_imv_rows BIGINT;
                   _pc_thr NUMERIC; _pc_per_imv NUMERIC; _pc_ratio NUMERIC;
                   _pc_int_tbl TEXT; _pc_scratch_tbl TEXT; _pc_target_qv TEXT;
                   _pc_idx_def TEXT; _pc_end_q TEXT;
           BEGIN
             _pc_sql := public.reflex_build_path_c_explain_sql(_rec.name, 'upg_a');
             IF _pc_sql <> '' THEN
               EXECUTE 'EXPLAIN (FORMAT JSON) ' || _pc_sql INTO _pc_plan;
               _pc_est := (_pc_plan::jsonb -> 0 -> 'Plan' ->> 'Plan Rows')::BIGINT;
               -- Resolve derived relation names through the same Rust helpers
               -- that built them, so bare IMV names and long names truncated
               -- by `safe_identifier` are handled identically here and at
               -- create time. The previous implementation re-derived the
               -- names by parsing the IMV name with a `.` split + raw string
               -- concat, which silently broke on bare names and on view
               -- names whose intermediate-table name exceeded PG's 63-char
               -- NAMEDATALEN limit.
               _pc_int_tbl     := public.reflex_intermediate_table_name(_rec.name);
               _pc_scratch_tbl := public.reflex_delta_scratch_table_name(_rec.name);
               _pc_target_qv   := public.reflex_quote_identifier(_rec.name);
               BEGIN
                 EXECUTE format('SELECT reltuples::BIGINT FROM pg_class WHERE oid = %L::regclass', _pc_int_tbl) INTO _pc_imv_rows;
               EXCEPTION WHEN OTHERS THEN
                 _pc_imv_rows := NULL;
               END;
               IF _pc_imv_rows IS NULL OR _pc_imv_rows <= 0 THEN
                 BEGIN
                   EXECUTE format('SELECT reltuples::BIGINT FROM pg_class WHERE oid = %L::regclass', _rec.name) INTO _pc_imv_rows;
                 EXCEPTION WHEN OTHERS THEN
                   _pc_imv_rows := NULL;
                 END;
               END IF;
               SELECT wipe_threshold INTO _pc_per_imv FROM public.__reflex_ivm_reference WHERE name = _rec.name;
               _pc_thr := COALESCE(_pc_per_imv, NULLIF(current_setting('reflex.wipe_threshold', true), '')::NUMERIC, 0.5);
               IF _pc_imv_rows IS NOT NULL AND _pc_imv_rows > 0 AND _pc_est IS NOT NULL THEN
                 _pc_ratio := _pc_est::NUMERIC / _pc_imv_rows;
                 IF _pc_ratio >= _pc_thr THEN
                   RAISE DEBUG 'pg_reflex Path C smart bulk-INSERT: % (est=% imv=% ratio=% thr=%)',
                                _rec.name, _pc_est, _pc_imv_rows, _pc_ratio, _pc_thr;
                   EXECUTE format('TRUNCATE %s', _pc_scratch_tbl);
                   EXECUTE format('INSERT INTO %s %s', _pc_scratch_tbl, _pc_sql);
                   -- Look up the unique index on the intermediate table by
                   -- relation OID (so the lookup uses the same canonical
                   -- name as the relation we just resolved), and by
                   -- `indisunique` (an `indexdef ILIKE '%UNIQUE%'` match
                   -- false-positives on comments / column names).
                   SELECT pg_get_indexdef(ix.indexrelid) INTO _pc_idx_def
                     FROM pg_index ix
                     WHERE ix.indrelid = _pc_int_tbl::regclass
                       AND ix.indisunique
                     LIMIT 1;
                   IF _pc_idx_def IS NOT NULL THEN
                     EXECUTE format('DROP INDEX %s', (
                       SELECT format('%I.%I', n.nspname, i.relname)
                       FROM pg_index ix
                       JOIN pg_class i ON i.oid = ix.indexrelid
                       JOIN pg_namespace n ON n.oid = i.relnamespace
                       WHERE ix.indrelid = _pc_int_tbl::regclass
                         AND ix.indisunique
                       LIMIT 1
                     ));
                   END IF;
                   EXECUTE format('INSERT INTO %s SELECT * FROM %s', _pc_int_tbl, _pc_scratch_tbl);
                   IF _pc_idx_def IS NOT NULL THEN
                     EXECUTE _pc_idx_def;
                   END IF;
                   _pc_end_q := REPLACE(_rec.end_query, _pc_int_tbl, _pc_scratch_tbl);
                   EXECUTE format('INSERT INTO %s %s', _pc_target_qv, _pc_end_q);
                   EXECUTE format('ANALYZE %s', _pc_int_tbl);
                   UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = _rec.name;
                   CONTINUE;
                 END IF;
               END IF;
             END IF;
           EXCEPTION WHEN OTHERS THEN
             RAISE WARNING 'pg_reflex Path C smart bulk-INSERT failed for %: %, falling through to standard incremental', _rec.name, SQLERRM;
           END;
         END IF;

      PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
      _sql := public.reflex_build_delta_sql(_rec.name, 'upg_a', _directional_op, _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
      IF _sql <> '' THEN
        FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
          IF _stmt <> '' THEN EXECUTE _stmt; END IF;
        END LOOP;
      END IF;
    END LOOP;
    RETURN NULL;
END;
 $function$

-- next function
CREATE OR REPLACE FUNCTION public.__reflex_ins_trigger_on_upg_b()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
        _skip_cols TEXT; _skip_pred TEXT; _skip_pred_clause TEXT; _skip_sql TEXT; _filter_skip BOOLEAN;
        _old_has BOOLEAN; _new_has BOOLEAN; _directional_op TEXT;
BEGIN
    SELECT EXISTS(SELECT 1 FROM "__reflex_new_upg_b" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;
    FOR _rec IN
      SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate, ignored_sources
      FROM public.__reflex_ivm_reference
      WHERE 'upg_b' = ANY(depends_on) AND enabled = TRUE
      ORDER BY graph_depth, name
    LOOP
      IF _rec.ignored_sources IS NOT NULL AND ('upg_b' = ANY(_rec.ignored_sources) OR 'upg_b' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
      IF _rec.where_predicate IS NOT NULL THEN
               EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '__reflex_new_upg_b', _rec.where_predicate) INTO _pred_match;
               IF NOT _pred_match THEN CONTINUE; END IF;
             END IF;

      
      
      
      PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
      _sql := public.reflex_build_delta_sql(_rec.name, 'upg_b', 'INSERT', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
      IF _sql <> '' THEN
        FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
          IF _stmt <> '' THEN EXECUTE _stmt; END IF;
        END LOOP;
      END IF;
    END LOOP;
    RETURN NULL;
END;
 $function$

-- next function
CREATE OR REPLACE FUNCTION public.__reflex_del_trigger_on_upg_b()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
        _skip_cols TEXT; _skip_pred TEXT; _skip_pred_clause TEXT; _skip_sql TEXT; _filter_skip BOOLEAN;
        _old_has BOOLEAN; _new_has BOOLEAN; _directional_op TEXT;
BEGIN
    SELECT EXISTS(SELECT 1 FROM "__reflex_old_upg_b" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;
    FOR _rec IN
      SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate, ignored_sources
      FROM public.__reflex_ivm_reference
      WHERE 'upg_b' = ANY(depends_on) AND enabled = TRUE
      ORDER BY graph_depth, name
    LOOP
      IF _rec.ignored_sources IS NOT NULL AND ('upg_b' = ANY(_rec.ignored_sources) OR 'upg_b' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
      IF _rec.where_predicate IS NOT NULL THEN
               EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '__reflex_old_upg_b', _rec.where_predicate) INTO _pred_match;
               IF NOT _pred_match THEN CONTINUE; END IF;
             END IF;

      
      
      
      PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
      _sql := public.reflex_build_delta_sql(_rec.name, 'upg_b', 'DELETE', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
      IF _sql <> '' THEN
        FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
          IF _stmt <> '' THEN EXECUTE _stmt; END IF;
        END LOOP;
      END IF;
    END LOOP;
    RETURN NULL;
END;
 $function$

-- next function
CREATE OR REPLACE FUNCTION public.__reflex_upd_trigger_on_upg_b()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_rows BOOLEAN; _pred_match BOOLEAN;
        _skip_cols TEXT; _skip_pred TEXT; _skip_pred_clause TEXT; _skip_sql TEXT; _filter_skip BOOLEAN;
        _old_has BOOLEAN; _new_has BOOLEAN; _directional_op TEXT;
BEGIN
    SELECT EXISTS(SELECT 1 FROM "__reflex_new_upg_b" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;
    FOR _rec IN
      SELECT name, base_query, end_query, aggregations::text AS aggregations, where_predicate, ignored_sources
      FROM public.__reflex_ivm_reference
      WHERE 'upg_b' = ANY(depends_on) AND enabled = TRUE
      ORDER BY graph_depth, name
    LOOP
      IF _rec.ignored_sources IS NOT NULL AND ('upg_b' = ANY(_rec.ignored_sources) OR 'upg_b' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
      IF _rec.where_predicate IS NOT NULL THEN
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1) OR EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)',
                          '__reflex_new_upg_b', _rec.where_predicate, '__reflex_old_upg_b', _rec.where_predicate) INTO _pred_match;
           IF NOT _pred_match THEN CONTINUE; END IF;
         END IF;

      SELECT string_agg(
              CASE WHEN t.typname IN ('json','xml')
                   THEN format('%I::text', sel.col)
                   ELSE format('%I', sel.col)
              END,
              ', ' ORDER BY sel.col)
              INTO _skip_cols
              FROM jsonb_array_elements_text(
                   COALESCE(_rec.aggregations::jsonb->'imv_relevant_columns'->'upg_b', '[]'::jsonb)
              ) AS sel(col)
              JOIN pg_attribute a
                ON a.attname = sel.col
               AND a.attrelid = 'upg_b'::regclass
               AND a.attnum > 0 AND NOT a.attisdropped
              JOIN pg_type t ON t.oid = a.atttypid;
         IF _skip_cols IS NOT NULL AND _skip_cols <> '' THEN
           _skip_pred := _rec.aggregations::jsonb->'imv_relevant_where'->>'upg_b';
           IF _skip_pred IS NULL OR _skip_pred = '' THEN
             _skip_pred_clause := '';
           ELSE
             _skip_pred_clause := ' WHERE ' || _skip_pred;
           END IF;
           _skip_sql := format(
             'WITH _o AS (SELECT %s FROM %I%s), _n AS (SELECT %s FROM %I%s) '
             || 'SELECT NOT EXISTS(TABLE _o EXCEPT ALL TABLE _n) AND NOT EXISTS(TABLE _n EXCEPT ALL TABLE _o)',
             _skip_cols, '__reflex_old_upg_b', _skip_pred_clause,
             _skip_cols, '__reflex_new_upg_b', _skip_pred_clause
           );
           EXECUTE _skip_sql INTO _filter_skip;
           IF _filter_skip THEN CONTINUE; END IF;
         END IF;

      _directional_op := 'UPDATE';
         IF _skip_cols IS NOT NULL AND _skip_cols <> '' THEN
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I%s LIMIT 1)',
                          '__reflex_old_upg_b', _skip_pred_clause) INTO _old_has;
           EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I%s LIMIT 1)',
                          '__reflex_new_upg_b', _skip_pred_clause) INTO _new_has;
           IF (NOT _old_has) AND _new_has THEN
             _directional_op := 'INSERT_PROMOTED';
           ELSIF _old_has AND (NOT _new_has) THEN
             _directional_op := 'DELETE_PROMOTED';
           END IF;
         END IF;

      IF _directional_op = 'INSERT_PROMOTED' THEN
           DECLARE _pc_sql TEXT; _pc_plan TEXT; _pc_est BIGINT; _pc_imv_rows BIGINT;
                   _pc_thr NUMERIC; _pc_per_imv NUMERIC; _pc_ratio NUMERIC;
                   _pc_int_tbl TEXT; _pc_scratch_tbl TEXT; _pc_target_qv TEXT;
                   _pc_idx_def TEXT; _pc_end_q TEXT;
           BEGIN
             _pc_sql := public.reflex_build_path_c_explain_sql(_rec.name, 'upg_b');
             IF _pc_sql <> '' THEN
               EXECUTE 'EXPLAIN (FORMAT JSON) ' || _pc_sql INTO _pc_plan;
               _pc_est := (_pc_plan::jsonb -> 0 -> 'Plan' ->> 'Plan Rows')::BIGINT;
               -- Resolve derived relation names through the same Rust helpers
               -- that built them, so bare IMV names and long names truncated
               -- by `safe_identifier` are handled identically here and at
               -- create time. The previous implementation re-derived the
               -- names by parsing the IMV name with a `.` split + raw string
               -- concat, which silently broke on bare names and on view
               -- names whose intermediate-table name exceeded PG's 63-char
               -- NAMEDATALEN limit.
               _pc_int_tbl     := public.reflex_intermediate_table_name(_rec.name);
               _pc_scratch_tbl := public.reflex_delta_scratch_table_name(_rec.name);
               _pc_target_qv   := public.reflex_quote_identifier(_rec.name);
               BEGIN
                 EXECUTE format('SELECT reltuples::BIGINT FROM pg_class WHERE oid = %L::regclass', _pc_int_tbl) INTO _pc_imv_rows;
               EXCEPTION WHEN OTHERS THEN
                 _pc_imv_rows := NULL;
               END;
               IF _pc_imv_rows IS NULL OR _pc_imv_rows <= 0 THEN
                 BEGIN
                   EXECUTE format('SELECT reltuples::BIGINT FROM pg_class WHERE oid = %L::regclass', _rec.name) INTO _pc_imv_rows;
                 EXCEPTION WHEN OTHERS THEN
                   _pc_imv_rows := NULL;
                 END;
               END IF;
               SELECT wipe_threshold INTO _pc_per_imv FROM public.__reflex_ivm_reference WHERE name = _rec.name;
               _pc_thr := COALESCE(_pc_per_imv, NULLIF(current_setting('reflex.wipe_threshold', true), '')::NUMERIC, 0.5);
               IF _pc_imv_rows IS NOT NULL AND _pc_imv_rows > 0 AND _pc_est IS NOT NULL THEN
                 _pc_ratio := _pc_est::NUMERIC / _pc_imv_rows;
                 IF _pc_ratio >= _pc_thr THEN
                   RAISE DEBUG 'pg_reflex Path C smart bulk-INSERT: % (est=% imv=% ratio=% thr=%)',
                                _rec.name, _pc_est, _pc_imv_rows, _pc_ratio, _pc_thr;
                   EXECUTE format('TRUNCATE %s', _pc_scratch_tbl);
                   EXECUTE format('INSERT INTO %s %s', _pc_scratch_tbl, _pc_sql);
                   -- Look up the unique index on the intermediate table by
                   -- relation OID (so the lookup uses the same canonical
                   -- name as the relation we just resolved), and by
                   -- `indisunique` (an `indexdef ILIKE '%UNIQUE%'` match
                   -- false-positives on comments / column names).
                   SELECT pg_get_indexdef(ix.indexrelid) INTO _pc_idx_def
                     FROM pg_index ix
                     WHERE ix.indrelid = _pc_int_tbl::regclass
                       AND ix.indisunique
                     LIMIT 1;
                   IF _pc_idx_def IS NOT NULL THEN
                     EXECUTE format('DROP INDEX %s', (
                       SELECT format('%I.%I', n.nspname, i.relname)
                       FROM pg_index ix
                       JOIN pg_class i ON i.oid = ix.indexrelid
                       JOIN pg_namespace n ON n.oid = i.relnamespace
                       WHERE ix.indrelid = _pc_int_tbl::regclass
                         AND ix.indisunique
                       LIMIT 1
                     ));
                   END IF;
                   EXECUTE format('INSERT INTO %s SELECT * FROM %s', _pc_int_tbl, _pc_scratch_tbl);
                   IF _pc_idx_def IS NOT NULL THEN
                     EXECUTE _pc_idx_def;
                   END IF;
                   _pc_end_q := REPLACE(_rec.end_query, _pc_int_tbl, _pc_scratch_tbl);
                   EXECUTE format('INSERT INTO %s %s', _pc_target_qv, _pc_end_q);
                   EXECUTE format('ANALYZE %s', _pc_int_tbl);
                   UPDATE public.__reflex_ivm_reference SET last_update_date = NOW() WHERE name = _rec.name;
                   CONTINUE;
                 END IF;
               END IF;
             END IF;
           EXCEPTION WHEN OTHERS THEN
             RAISE WARNING 'pg_reflex Path C smart bulk-INSERT failed for %: %, falling through to standard incremental', _rec.name, SQLERRM;
           END;
         END IF;

      PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
      _sql := public.reflex_build_delta_sql(_rec.name, 'upg_b', _directional_op, _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
      IF _sql <> '' THEN
        FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
          IF _stmt <> '' THEN EXECUTE _stmt; END IF;
        END LOOP;
      END IF;
    END LOOP;
    RETURN NULL;
END;
 $function$

-- next function
CREATE OR REPLACE FUNCTION public.__reflex_ins_trigger_on_upg_d()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_deferred BOOLEAN := FALSE; _has_rows BOOLEAN; _pred_match BOOLEAN;
BEGIN
    SELECT EXISTS(SELECT 1 FROM "__reflex_new_upg_d" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;
    FOR _rec IN
      SELECT name, base_query, end_query, aggregations::text AS aggregations,
             COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, where_predicate, ignored_sources
      FROM public.__reflex_ivm_reference
      WHERE 'upg_d' = ANY(depends_on) AND enabled = TRUE
      ORDER BY graph_depth, name
    LOOP
      IF _rec.ignored_sources IS NOT NULL AND ('upg_d' = ANY(_rec.ignored_sources) OR 'upg_d' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
      IF _rec.where_predicate IS NOT NULL THEN
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '__reflex_new_upg_d', _rec.where_predicate) INTO _pred_match;
        IF NOT _pred_match THEN CONTINUE; END IF;
      END IF;
      IF _rec.refresh_mode = 'IMMEDIATE' THEN
        PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
        _sql := public.reflex_build_delta_sql(_rec.name, 'upg_d', 'INSERT', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
        IF _sql <> '' THEN
          FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
            IF _stmt <> '' THEN EXECUTE _stmt; END IF;
          END LOOP;
        END IF;
      ELSE
        _has_deferred := TRUE;
      END IF;
    END LOOP;
    IF _has_deferred THEN
      INSERT INTO "__reflex_delta_upg_d" (__reflex_op, "k", "g", "v") SELECT 'I', "k", "g", "v" FROM "__reflex_new_upg_d";
      INSERT INTO public.__reflex_deferred_pending (source_table, operation)
        VALUES ('upg_d', 'INSERT');
    END IF;
    RETURN NULL;
END;
 $function$

-- next function
CREATE OR REPLACE FUNCTION public.__reflex_del_trigger_on_upg_d()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_deferred BOOLEAN := FALSE; _has_rows BOOLEAN; _pred_match BOOLEAN;
BEGIN
    SELECT EXISTS(SELECT 1 FROM "__reflex_old_upg_d" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;
    FOR _rec IN
      SELECT name, base_query, end_query, aggregations::text AS aggregations,
             COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, where_predicate, ignored_sources
      FROM public.__reflex_ivm_reference
      WHERE 'upg_d' = ANY(depends_on) AND enabled = TRUE
      ORDER BY graph_depth, name
    LOOP
      IF _rec.ignored_sources IS NOT NULL AND ('upg_d' = ANY(_rec.ignored_sources) OR 'upg_d' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
      IF _rec.where_predicate IS NOT NULL THEN
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '__reflex_old_upg_d', _rec.where_predicate) INTO _pred_match;
        IF NOT _pred_match THEN CONTINUE; END IF;
      END IF;
      IF _rec.refresh_mode = 'IMMEDIATE' THEN
        PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
        _sql := public.reflex_build_delta_sql(_rec.name, 'upg_d', 'DELETE', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
        IF _sql <> '' THEN
          FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
            IF _stmt <> '' THEN EXECUTE _stmt; END IF;
          END LOOP;
        END IF;
      ELSE
        _has_deferred := TRUE;
      END IF;
    END LOOP;
    IF _has_deferred THEN
      INSERT INTO "__reflex_delta_upg_d" (__reflex_op, "k", "g", "v") SELECT 'D', "k", "g", "v" FROM "__reflex_old_upg_d";
      INSERT INTO public.__reflex_deferred_pending (source_table, operation)
        VALUES ('upg_d', 'DELETE');
    END IF;
    RETURN NULL;
END;
 $function$

-- next function
CREATE OR REPLACE FUNCTION public.__reflex_upd_trigger_on_upg_d()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ DECLARE _rec RECORD; _sql TEXT; _stmt TEXT; _has_deferred BOOLEAN := FALSE; _has_rows BOOLEAN; _pred_match BOOLEAN;
BEGIN
    SELECT EXISTS(SELECT 1 FROM "__reflex_new_upg_d" LIMIT 1) INTO _has_rows;
    IF NOT _has_rows THEN RETURN NULL; END IF;
    DECLARE _read TEXT; _unchanged BOOLEAN := FALSE;
    BEGIN
     BEGIN
      WITH consumer AS (
        SELECT aggregations, where_predicate,
               CASE WHEN jsonb_typeof(aggregations->'imv_relevant_columns'->'upg_d') = 'array'
                    THEN aggregations->'imv_relevant_columns'->'upg_d'
                    ELSE '[]'::jsonb END AS read_columns
        FROM public.__reflex_ivm_reference
        WHERE 'upg_d' = ANY(depends_on) AND enabled = TRUE
          AND NOT COALESCE('upg_d' = ANY(ignored_sources) OR 'upg_d' = ANY(ignored_sources), FALSE)
      ),
      read_column AS (
        SELECT DISTINCT c.name FROM consumer CROSS JOIN LATERAL jsonb_array_elements_text(consumer.read_columns) AS c(name)
      ),
      read_value AS (
        SELECT CASE WHEN t.typname IN ('json', 'xml') THEN format('%I::text', c.name) ELSE format('%I', c.name) END AS expr,
               a.attnum IS NULL AS unresolved
        FROM read_column c
        LEFT JOIN pg_attribute a ON a.attrelid = 'upg_d'::regclass
                                AND a.attname = c.name AND a.attnum > 0 AND NOT a.attisdropped
        LEFT JOIN pg_type t ON t.oid = a.atttypid
        UNION
        SELECT format('((%s) IS TRUE)', p.predicate), FALSE
        FROM consumer
        CROSS JOIN LATERAL (VALUES (consumer.aggregations->'imv_relevant_where'->>'upg_d'),
                                   (consumer.where_predicate)) AS p(predicate)
        WHERE p.predicate <> ''
      )
      SELECT CASE
               WHEN NOT EXISTS (SELECT 1 FROM consumer) THEN NULL
               WHEN EXISTS (SELECT 1 FROM consumer WHERE jsonb_array_length(read_columns) = 0) THEN NULL
               WHEN EXISTS (SELECT 1 FROM read_value WHERE unresolved) THEN NULL
               ELSE (SELECT string_agg(expr, ', ' ORDER BY expr) FROM read_value)
             END
      INTO _read;
      IF _read IS NOT NULL THEN
        EXECUTE format(
          'SELECT (SELECT array_agg(ROW(%1$s) ORDER BY ROW(%1$s)) FROM (SELECT * FROM %2$I LIMIT 100) o) '
          || 'IS NOT DISTINCT FROM (SELECT array_agg(ROW(%1$s) ORDER BY ROW(%1$s)) FROM (SELECT * FROM %3$I LIMIT 100) n)',
          _read, '__reflex_old_upg_d', '__reflex_new_upg_d') INTO _unchanged;
        IF _unchanged THEN
          EXECUTE format(
            'SELECT (SELECT count(*) FROM %2$I) = (SELECT count(*) FROM %3$I) '
            || 'AND NOT EXISTS (SELECT %1$s FROM %3$I EXCEPT ALL SELECT %1$s FROM %2$I)',
            _read, '__reflex_old_upg_d', '__reflex_new_upg_d') INTO _unchanged;
        END IF;
        IF _unchanged AND EXISTS (SELECT 1 FROM "__reflex_delta_upg_d") THEN
          _unchanged := FALSE;
        END IF;
      END IF;
     EXCEPTION WHEN OTHERS THEN
      _unchanged := FALSE;
     END;
     IF _unchanged THEN RETURN NULL; END IF;
    END;

    FOR _rec IN
      SELECT name, base_query, end_query, aggregations::text AS aggregations,
             COALESCE(refresh_mode, 'IMMEDIATE') AS refresh_mode, where_predicate, ignored_sources
      FROM public.__reflex_ivm_reference
      WHERE 'upg_d' = ANY(depends_on) AND enabled = TRUE
      ORDER BY graph_depth, name
    LOOP
      IF _rec.ignored_sources IS NOT NULL AND ('upg_d' = ANY(_rec.ignored_sources) OR 'upg_d' = ANY(_rec.ignored_sources)) THEN CONTINUE; END IF;
      IF _rec.where_predicate IS NOT NULL THEN
        -- An UPDATE is relevant if the NEW image enters/stays in the filter OR
        -- the OLD image was in it (a row leaving the filter must be DELETEd).
        -- Checking NEW only silently drops row-leaves-filter updates.
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1) OR EXISTS(SELECT 1 FROM %I WHERE %s LIMIT 1)', '__reflex_new_upg_d', _rec.where_predicate, '__reflex_old_upg_d', _rec.where_predicate) INTO _pred_match;
        IF NOT _pred_match THEN CONTINUE; END IF;
      END IF;
      IF _rec.refresh_mode = 'IMMEDIATE' THEN
        PERFORM pg_advisory_xact_lock(hashtext(_rec.name), hashtext(reverse(_rec.name)));
        _sql := public.reflex_build_delta_sql(_rec.name, 'upg_d', 'UPDATE', _rec.base_query, _rec.end_query, _rec.aggregations, _rec.base_query);
        IF _sql <> '' THEN
          FOREACH _stmt IN ARRAY string_to_array(_sql, E'\n--<<REFLEX_SEP>>--\n') LOOP
            IF _stmt <> '' THEN EXECUTE _stmt; END IF;
          END LOOP;
        END IF;
      ELSE
        _has_deferred := TRUE;
      END IF;
    END LOOP;
    IF _has_deferred THEN
      INSERT INTO "__reflex_delta_upg_d" (__reflex_op, "k", "g", "v") SELECT 'U_OLD', "k", "g", "v" FROM "__reflex_old_upg_d";
      INSERT INTO "__reflex_delta_upg_d" (__reflex_op, "k", "g", "v") SELECT 'U_NEW', "k", "g", "v" FROM "__reflex_new_upg_d";
      INSERT INTO public.__reflex_deferred_pending (source_table, operation)
        VALUES ('upg_d', 'UPDATE');
    END IF;
    RETURN NULL;
END;
 $function$

