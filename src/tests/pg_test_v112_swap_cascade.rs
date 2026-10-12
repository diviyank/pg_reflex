// A source leaf swapped the way the application does it (build detached, DETACH
// old / ATTACH new / DROP old / RENAME), committed for real through a `dblink`
// worker on a private database (helpers of `pg_test_partition_attach_locks.rs`,
// `pg_test_rebuild_commit.rs` and `pg_test_v112_deferred_partition.rs`).
//
// The chain mirrors the field one: a LIST(plan) x RANGE(month) source, a keyed
// passthrough mirroring both levels, a CTE dependent partitioned by plan only
// that ignores the passthrough, an unpartitioned aggregate dependent, and a
// dependent of the CTE dependent.

const SC_PLANS: [i32; 4] = [1, 2, 3, 4];
const SC_MONTHS: [i32; 4] = [1, 2, 3, 4];

const SC_SFV_SQL: &str = "SELECT ss.plan, ss.d, ss.yr, ss.product, ss.loc, \
       ss.base AS forecast_base, ss.qty AS quantity, \
       ss.qty * COALESCE(p.base_price, 0) AS turnover, \
       COALESCE(c.is_active, FALSE) AS in_assortment \
     FROM sc_ss AS ss \
       INNER JOIN sc_dp AS dp ON dp.id = ss.plan \
       INNER JOIN sc_loc AS l ON l.id = ss.loc \
       LEFT JOIN sc_pricing AS p ON dp.assortment_id = p.assortment_id \
         AND p.canal_id = l.canal_id AND ss.product = p.product \
       LEFT JOIN sc_caav AS c ON c.product = ss.product AND c.loc = ss.loc";

/// The CTEs of the analysis view over `{sfv}`.
const SC_FAV_CTES: &str = "WITH history_bounds AS ( \
       SELECT MAX(h.d) - 365 AS year_cutoff, MAX(h.d) - 7 AS week_cutoff FROM sc_hist AS h), \
     forecast_bounds AS ( \
       SELECT sf.plan, MIN(sf.d) AS min_d, MAX(sf.d) AS max_d FROM {sfv} AS sf GROUP BY sf.plan), \
     date_limits AS ( \
       SELECT fb.plan, GREATEST(hb.year_cutoff, fb.min_d) AS min_date, \
              LEAST(fb.max_d - 1, hb.week_cutoff) AS max_date, \
              CASE WHEN fb.plan IN (SELECT cur.plan FROM sc_cur AS cur) \
                   THEN LEAST(fb.max_d - 1, hb.week_cutoff) ELSE fb.max_d - 1 END AS forecast_max_date \
       FROM forecast_bounds AS fb CROSS JOIN history_bounds AS hb \
       WHERE fb.plan IN (SELECT cur.plan FROM sc_cur AS cur) \
          OR fb.plan IN (SELECT dp.id FROM sc_dp AS dp WHERE dp.status = 'archive')), \
     history_buckets AS ( \
       SELECT h.product, h.loc, h.d, SUM(h.quantity) AS quantity, SUM(h.turnover) AS turnover \
       FROM sc_hist AS h CROSS JOIN history_bounds AS bounds \
       WHERE h.d >= bounds.year_cutoff AND (h.quantity <> 0 OR h.turnover <> 0) \
       GROUP BY h.product, h.loc, h.d), \
     history_sales AS ( \
       SELECT dl.plan, hb.product, hb.loc, hb.d, hb.quantity, hb.turnover \
       FROM history_buckets AS hb \
         JOIN date_limits AS dl ON hb.d >= dl.min_date AND hb.d <= dl.max_date), \
     forecast_sales AS ( \
       SELECT dl.plan, sf.product, sf.loc, sf.d, SUM(sf.forecast_base) AS forecast_base, \
              SUM(sf.quantity) AS quantity, SUM(sf.turnover) AS turnover \
       FROM {sfv} AS sf \
         JOIN date_limits AS dl ON sf.plan = dl.plan AND sf.d >= dl.min_date \
           AND sf.d <= dl.forecast_max_date \
       WHERE sf.quantity <> 0 OR sf.forecast_base <> 0 OR sf.turnover <> 0 \
       GROUP BY dl.plan, sf.product, sf.loc, sf.d), \
     active_assortment AS (SELECT product, loc FROM sc_caav WHERE is_active) ";

const SC_FAV_SELECT: &str = "SELECT COALESCE(f.plan, hs.plan) AS plan, \
       COALESCE(f.product, hs.product) AS product, COALESCE(f.loc, hs.loc) AS loc, \
       COALESCE(f.d, hs.d) AS d, \
       CAST(EXTRACT(YEAR FROM COALESCE(f.d, hs.d)) AS INT) AS yr, \
       COALESCE(hs.quantity, 0) AS sales, COALESCE(f.quantity, 0) AS forecast, \
       COALESCE(f.turnover, 0) AS forecast_turnover, \
       (NOT a.product IS NULL) AS in_assortment \
     FROM forecast_sales AS f \
       FULL JOIN history_sales AS hs ON f.plan = hs.plan AND f.product = hs.product \
         AND f.loc = hs.loc AND f.d = hs.d \
       LEFT JOIN active_assortment AS a ON a.product = COALESCE(f.product, hs.product) \
         AND a.loc = COALESCE(f.loc, hs.loc)";

const SC_FAV_KEYS: &str = "plan, product, loc, d ; date_limits : plan ; \
     history_buckets : product, loc, d ; history_sales : plan, product, loc, d ; \
     forecast_sales : plan, product, loc, d";

const SC_FDYA_SQL: &str =
    "SELECT plan, yr, MIN(d) AS min_date, MAX(d) AS max_date FROM {sfv} GROUP BY plan, yr";

const SC_ADYA_SQL: &str =
    "SELECT plan, yr, MIN(d) AS min_date, MAX(d) AS max_date FROM {fav} GROUP BY plan, yr";

/// A plan's rows next to plan 1's quantity for the same key, over the days the
/// history reaches: the passthrough is read twice, and a sub-IMV is generated.
const SC_VERSUS_CTES: &str =
    "WITH horizon AS (SELECT MAX(h.d) - 400 AS first_day FROM sc_hist AS h) ";
const SC_VERSUS_SELECT: &str = "SELECT a.plan, a.product, a.loc, a.d, a.quantity, \
       b.quantity AS reference_quantity \
     FROM {sfv} AS a JOIN {sfv} AS b ON b.plan = 1 AND b.product = a.product \
       AND b.loc = a.loc AND b.d = a.d \
       CROSS JOIN horizon AS hz WHERE a.d > hz.first_day";

/// Plan 1's rows shown under every plan of `sc_map`: the partition column
/// comes from the joined table.
const SC_MAPPED_SQL: &str = "SELECT m.plan, sf.product, sf.loc, sf.d, sf.quantity \
     FROM {sfv} AS sf JOIN sc_map AS m ON m.src_plan = sf.plan";

/// The fixture's relations under the field's names, long enough that a mirror
/// partition of a generated sub-IMV exceeds the identifier limit.
fn sc_field_names(sql: &str) -> String {
    sql.replace("sc_ss", "sales_simulation")
        .replace("sc_sfv", "sop_forecast_view")
        .replace("sc_fav", "forecast_analysis_view")
        .replace("sc_fdya", "forecast_dp_year_agg")
        .replace("sc_adya", "analysis_dp_year_agg")
}

/// One chain on the worker's database: under the field's names or short ones,
/// with or without the dependents whose rows depend on another plan's.
#[derive(Clone, Copy)]
struct ScChain {
    field_names: bool,
    cross_plan: bool,
}

impl ScChain {
    fn named(&self, sql: &str) -> String {
        if self.field_names {
            sc_field_names(sql)
        } else {
            sql.to_string()
        }
    }

    fn exec(&self, sql: &str) {
        worker_exec(&self.named(sql));
    }

    fn nodes(&self) -> Vec<(String, String)> {
        sc_chain(self.cross_plan)
            .into_iter()
            .map(|(node, fresh)| (self.named(&node), self.named(&fresh)))
            .collect()
    }

    /// Commits `statements` (which may return rows) as one implicit
    /// transaction that records its id first; the error text when it raises.
    fn commit(&self, statements: &str) -> Result<(), String> {
        worker_exec("TRUNCATE sc_trace, sc_xact");
        let committed = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM dblink('reflex_lock_worker', {}, false) AS t(v text)",
            sql_lit(&self.named(&format!(
                "INSERT INTO sc_xact SELECT pg_current_xact_id()::text; {statements}; \
                 SELECT 'committed'"
            )))
        ))
        .expect("dblink")
        .unwrap_or(0);
        if committed == 1 {
            return Ok(());
        }
        let error = Spi::get_one::<String>("SELECT dblink_error_message('reflex_lock_worker')")
            .expect("dblink_error_message")
            .unwrap_or_default();
        let _ =
            Spi::get_one::<String>("SELECT dblink_exec('reflex_lock_worker', 'ROLLBACK', false)");
        Err(error)
    }

    fn create(&self, call: &str) {
        let call = self.named(call);
        assert_eq!(rbc_select(&call), "CREATE REFLEX INCREMENTAL VIEW", "{call}");
    }
}

fn sc_sfv_inline() -> String {
    format!("({SC_SFV_SQL})")
}

fn sc_fav_fresh(select: &str) -> String {
    format!("{}{select}", SC_FAV_CTES.replace("{sfv}", &sc_sfv_inline()))
}

fn sc_fav_inline() -> String {
    format!("({})", sc_fav_fresh(SC_FAV_SELECT))
}

/// Every stored node of the chain with the query that recomputes it from the
/// base tables.
fn sc_chain(with_cross_plan: bool) -> Vec<(String, String)> {
    let mut chain = vec![
        ("sc_sfv".to_string(), SC_SFV_SQL.to_string()),
        ("sc_fav".to_string(), sc_fav_fresh(SC_FAV_SELECT)),
        ("sc_fdya".to_string(), SC_FDYA_SQL.replace("{sfv}", &format!("{} AS inlined", sc_sfv_inline()))),
        ("sc_adya".to_string(), SC_ADYA_SQL.replace("{fav}", &format!("{} AS inlined", sc_fav_inline()))),
    ];
    for cte in [
        "history_bounds",
        "forecast_bounds",
        "date_limits",
        "history_buckets",
        "history_sales",
        "forecast_sales",
        "active_assortment",
    ] {
        chain.push((
            format!("sc_fav__cte_{cte}"),
            sc_fav_fresh(&format!("SELECT * FROM {cte}")),
        ));
    }
    if with_cross_plan {
        chain.push((
            "sc_versus".to_string(),
            format!(
                "{SC_VERSUS_CTES}{}",
                SC_VERSUS_SELECT.replace("{sfv}", &sc_sfv_inline())
            ),
        ));
        chain.push((
            "sc_versus__cte_horizon".to_string(),
            format!("{SC_VERSUS_CTES}SELECT * FROM horizon"),
        ));
        chain.push((
            "sc_mapped".to_string(),
            SC_MAPPED_SQL.replace("{sfv}", &sc_sfv_inline()),
        ));
    }
    chain
}

fn sc_leaf(plan: i32, month: i32) -> String {
    format!("sc_ss_p_{plan}_2027_{month:02}")
}

fn sc_month_bounds(month: i32) -> (String, String) {
    (
        format!("2027-{month:02}-01"),
        if month == 12 {
            "2028-01-01".to_string()
        } else {
            format!("2027-{:02}-01", month + 1)
        },
    )
}

/// The rows of one leaf: `days` days from the first of the month, every product
/// and location, quantities shifted by `bump`.
fn sc_leaf_rows(plan: i32, month: i32, days: i32, bump: i32) -> String {
    format!(
        "SELECT {plan}, DATE '2027-{month:02}-01' + day, 2027, product, loc, \
                {plan} * 1000 + {month} * 100 + product * 10 + loc + day + {bump}, \
                {plan} * 500 + product + day \
         FROM generate_series(0, {days} - 1) AS day, generate_series(1, 5) AS product, \
              generate_series(1, 3) AS loc"
    )
}

/// The application's swap of one month leaf: a detached replacement filled by
/// `rows`, then DETACH old / ATTACH new / DROP old / RENAME.
fn sc_swap(plan: i32, month: i32, rows: &str) -> String {
    let leaf = sc_leaf(plan, month);
    let (lo, hi) = sc_month_bounds(month);
    format!(
        "CREATE TABLE {leaf}_swap (LIKE sc_ss_p_{plan} INCLUDING DEFAULTS); \
         INSERT INTO {leaf}_swap {rows}; \
         ALTER TABLE {leaf}_swap ADD CONSTRAINT {leaf}_swap_chk \
           CHECK (d >= '{lo}' AND d < '{hi}'); \
         ALTER TABLE sc_ss_p_{plan} DETACH PARTITION {leaf}; \
         ALTER TABLE sc_ss_p_{plan} ATTACH PARTITION {leaf}_swap \
           FOR VALUES FROM ('{lo}') TO ('{hi}'); \
         ALTER TABLE {leaf}_swap DROP CONSTRAINT {leaf}_swap_chk; \
         DROP TABLE {leaf}; \
         ALTER TABLE {leaf}_swap RENAME TO {leaf}"
    )
}

/// Builds the sources and the chain on a private database, every IMV in `mode`.
fn sc_open(dbname: &str, mode: &str, chain: ScChain) {
    probe_db_open(dbname);
    for ddl in [
        "CREATE TABLE sc_dp (id INT PRIMARY KEY, assortment_id INT NOT NULL, status TEXT NOT NULL)",
        "INSERT INTO sc_dp VALUES (1, 1, 'current'), (2, 1, 'archive'), (3, 2, 'archive'), \
           (4, 2, 'draft'), (5, 1, 'archive')",
        "CREATE TABLE sc_loc (id INT PRIMARY KEY, canal_id INT NOT NULL)",
        "INSERT INTO sc_loc VALUES (1, 1), (2, 1), (3, 2)",
        "CREATE TABLE sc_pricing (assortment_id INT NOT NULL, canal_id INT NOT NULL, \
           product INT NOT NULL, base_price INT NOT NULL, \
           PRIMARY KEY (assortment_id, canal_id, product))",
        "INSERT INTO sc_pricing SELECT a, c, p, a * 3 + c + p \
           FROM generate_series(1, 2) a, generate_series(1, 2) c, generate_series(1, 4) p",
        "CREATE TABLE sc_caav (product INT NOT NULL, loc INT NOT NULL, is_active BOOL NOT NULL, \
           PRIMARY KEY (product, loc))",
        "INSERT INTO sc_caav SELECT p, l, (p + l) % 2 = 0 \
           FROM generate_series(1, 5) p, generate_series(1, 2) l",
        "CREATE TABLE sc_cur (plan INT PRIMARY KEY)",
        "INSERT INTO sc_cur VALUES (1)",
        "CREATE TABLE sc_hist_raw (product INT NOT NULL, loc INT NOT NULL, d DATE NOT NULL, \
           quantity INT NOT NULL, turnover INT NOT NULL)",
        "INSERT INTO sc_hist_raw SELECT p, l, DATE '2026-11-01' + day, (p + l + day) % 4, \
             ((p + l + day) % 4) * 7 \
           FROM generate_series(0, 111) day, generate_series(1, 6) p, generate_series(1, 3) l",
        "CREATE MATERIALIZED VIEW sc_hist AS SELECT * FROM sc_hist_raw",
        "CREATE TABLE sc_ss (plan INT NOT NULL, d DATE NOT NULL, yr INT NOT NULL, \
           product INT NOT NULL, loc INT NOT NULL, qty INT NOT NULL, base INT NOT NULL, \
           PRIMARY KEY (plan, product, loc, d)) PARTITION BY LIST (plan)",
    ] {
        chain.exec(ddl);
    }
    for plan in SC_PLANS {
        chain.exec(&format!(
            "CREATE TABLE sc_ss_p_{plan} PARTITION OF sc_ss FOR VALUES IN ({plan}) \
             PARTITION BY RANGE (d)"
        ));
        for month in SC_MONTHS {
            let (lo, hi) = sc_month_bounds(month);
            chain.exec(&format!(
                "CREATE TABLE {} PARTITION OF sc_ss_p_{plan} FOR VALUES FROM ('{lo}') TO ('{hi}')",
                sc_leaf(plan, month)
            ));
            chain.exec(&format!("INSERT INTO sc_ss {}", sc_leaf_rows(plan, month, 4, 0)));
        }
    }
    let mode = sql_lit(mode);
    chain.create(&format!(
        "create_reflex_ivm('sc_sfv', {}, 'plan, product, loc, d', NULL, {mode}, \
           '!sc_loc,!sc_pricing,!sc_dp', ARRAY['plan', 'd'])",
        sql_lit(SC_SFV_SQL)
    ));
    chain.create(&format!(
        "create_reflex_ivm('sc_fav', {}, {}, NULL, {mode}, '!sc_sfv,!sc_caav,!sc_dp', \
           ARRAY['plan'])",
        sql_lit(&format!("{}{SC_FAV_SELECT}", SC_FAV_CTES.replace("{sfv}", "sc_sfv"))),
        sql_lit(SC_FAV_KEYS)
    ));
    chain.create(&format!(
        "create_reflex_ivm('sc_fdya', {}, 'plan, yr', NULL, {mode}, NULL, ARRAY[]::text[])",
        sql_lit(&SC_FDYA_SQL.replace("{sfv}", "sc_sfv"))
    ));
    chain.create(&format!(
        "create_reflex_ivm('sc_adya', {}, 'plan, yr', NULL, {mode}, NULL, ARRAY[]::text[])",
        sql_lit(&SC_ADYA_SQL.replace("{fav}", "sc_fav"))
    ));
    if chain.cross_plan {
        chain.exec(
            "CREATE TABLE sc_map (src_plan INT NOT NULL, plan INT NOT NULL, \
               PRIMARY KEY (src_plan, plan))",
        );
        chain.exec("INSERT INTO sc_map SELECT 1, p FROM generate_series(1, 4) p");
        chain.create(&format!(
            "create_reflex_ivm('sc_versus', {}, 'plan, product, loc, d', NULL, {mode}, \
               '!sc_sfv', ARRAY['plan'])",
            sql_lit(&format!(
                "{SC_VERSUS_CTES}{}",
                SC_VERSUS_SELECT.replace("{sfv}", "sc_sfv")
            ))
        ));
        chain.create(&format!(
            "create_reflex_ivm('sc_mapped', {}, 'plan, product, loc, d', NULL, {mode}, \
               '!sc_sfv', ARRAY['plan'])",
            sql_lit(&SC_MAPPED_SQL.replace("{sfv}", "sc_sfv"))
        ));
    }
    for ddl in [
        "CREATE TABLE sc_trace (seq SERIAL, name TEXT NOT NULL, context TEXT NOT NULL)",
        "CREATE TABLE sc_xact (id TEXT NOT NULL)",
        "CREATE FUNCTION sc_note_refresh() RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
           DECLARE stack TEXT; \
           BEGIN GET DIAGNOSTICS stack = PG_CONTEXT; \
             INSERT INTO sc_trace (name, context) VALUES (NEW.name, stack); RETURN NULL; END $$",
        "CREATE TRIGGER sc_note_refresh AFTER UPDATE OF last_update_date \
           ON public.__reflex_ivm_reference FOR EACH ROW EXECUTE FUNCTION sc_note_refresh()",
    ] {
        chain.exec(ddl);
    }
}

fn sc_text_rows(query: &str) -> Vec<String> {
    Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT v FROM dblink('reflex_lock_worker', {}) AS t(v text)",
                    sql_lit(query)
                ),
                None,
                &[],
            )
            .expect("worker rows")
            .filter_map(|row| row.get_by_name::<String, _>("v").ok().flatten())
            .collect()
    })
}

impl ScChain {
    /// The explicit refreshes the last committed transaction ran, in order, as
    /// `(node, kind)`: `full` for a whole-IMV reconcile, `partitions` for a
    /// partition-scoped one, `groups` for a key-scoped one. The application of
    /// a staged delta is not a refresh.
    fn refreshes(&self) -> Vec<(String, String)> {
        sc_text_rows(
            "SELECT name || '|' || kind FROM ( \
               SELECT seq, name, CASE \
                 WHEN context LIKE '%last_update_date = clock_timestamp()%' THEN 'full' \
                 WHEN context LIKE '%reflex_scoped_cascade%' THEN 'groups' \
                 WHEN context LIKE '%SET last_update_date = NOW() WHERE name = $1%' \
                   THEN 'partitions' \
                 ELSE 'delta' END AS kind FROM sc_trace) t \
             WHERE kind <> 'delta' ORDER BY seq",
        )
        .into_iter()
        .map(|row| {
            let (node, kind) = row.split_once('|').expect("node|kind");
            (node.to_string(), kind.to_string())
        })
        .collect()
    }

    fn expected_refreshes(&self, refreshes: &[(&str, &str)]) -> Vec<(String, String)> {
        refreshes
            .iter()
            .map(|(node, kind)| (self.named(node), kind.to_string()))
            .collect()
    }

    /// The nodes in which the last committed transaction wrote a row of a plan
    /// outside `plans`, or any row when the node has no plan.
    fn written_outside(&self, plans: &[i32]) -> Vec<String> {
        let allowed = plans
            .iter()
            .map(|plan| plan.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        self.nodes()
            .iter()
            .filter_map(|(node, _)| {
                let has_plan = worker_scalar_i64(&format!(
                    "SELECT count(*)::int8 FROM pg_attribute \
                     WHERE attrelid = '{node}'::regclass AND attname = 'plan'"
                )) == 1;
                let outside = if has_plan {
                    format!("plan NOT IN ({allowed})")
                } else {
                    "TRUE".to_string()
                };
                let written = worker_scalar_i64(&format!(
                    "SELECT count(*)::int8 FROM {node} \
                     WHERE xmin::text::int8 >= (SELECT id::int8 FROM sc_xact) AND {outside}"
                ));
                (written != 0).then(|| format!("{node}: {written} rows"))
            })
            .collect()
    }

    fn wrong_nodes(&self, except: &[&str]) -> Vec<String> {
        let except: Vec<String> = except.iter().map(|node| self.named(node)).collect();
        self.nodes()
            .iter()
            .filter(|(node, _)| !except.contains(node))
            .filter_map(|(node, fresh)| {
                let mismatches = rbc_mismatch(node, fresh);
                (mismatches != 0).then(|| format!("{node}: {mismatches} mismatches"))
            })
            .collect()
    }
}

fn sc_three_leaves(plan: i32, bump: i32) -> String {
    [1, 2, 3]
        .map(|month| sc_swap(plan, month, &sc_leaf_rows(plan, month, 4, bump)))
        .join("; ")
}

/// The application's replacement of a whole plan: a detached twin partitioned
/// by month, then DETACH old / ATTACH twin / DROP old / RENAME twin and months.
fn sc_swap_plan(plan: i32, bump: i32) -> String {
    let mut statements = vec![format!(
        "CREATE TABLE sc_ss_p_{plan}_swap (LIKE sc_ss INCLUDING DEFAULTS) PARTITION BY RANGE (d)"
    )];
    for month in SC_MONTHS {
        let (lo, hi) = sc_month_bounds(month);
        statements.push(format!(
            "CREATE TABLE sc_ss_p_{plan}_swap_2027_{month:02} PARTITION OF sc_ss_p_{plan}_swap \
             FOR VALUES FROM ('{lo}') TO ('{hi}')"
        ));
        statements.push(format!(
            "INSERT INTO sc_ss_p_{plan}_swap {}",
            sc_leaf_rows(plan, month, 4, bump)
        ));
    }
    statements.push(format!(
        "ALTER TABLE sc_ss_p_{plan}_swap ADD CONSTRAINT sc_ss_p_{plan}_swap_chk \
         CHECK (plan = {plan})"
    ));
    statements.push(format!("ALTER TABLE sc_ss DETACH PARTITION sc_ss_p_{plan}"));
    statements.push(format!(
        "ALTER TABLE sc_ss ATTACH PARTITION sc_ss_p_{plan}_swap FOR VALUES IN ({plan})"
    ));
    statements.push(format!(
        "ALTER TABLE sc_ss_p_{plan}_swap DROP CONSTRAINT sc_ss_p_{plan}_swap_chk"
    ));
    statements.push(format!("DROP TABLE sc_ss_p_{plan}"));
    statements.push(format!("ALTER TABLE sc_ss_p_{plan}_swap RENAME TO sc_ss_p_{plan}"));
    for month in SC_MONTHS {
        statements.push(format!(
            "ALTER TABLE sc_ss_p_{plan}_swap_2027_{month:02} RENAME TO {}",
            sc_leaf(plan, month)
        ));
    }
    statements.join("; ")
}

/// A month leaf attached with its rows to a plan that has no such month.
fn sc_attach_month(plan: i32, month: i32) -> String {
    let leaf = sc_leaf(plan, month);
    let (lo, hi) = sc_month_bounds(month);
    format!(
        "CREATE TABLE {leaf} (LIKE sc_ss_p_{plan} INCLUDING DEFAULTS); \
         INSERT INTO {leaf} {}; \
         ALTER TABLE sc_ss_p_{plan} ATTACH PARTITION {leaf} FOR VALUES FROM ('{lo}') TO ('{hi}')",
        sc_leaf_rows(plan, month, 4, 0)
    )
}

/// A plan the source does not hold yet, attached with one filled month.
fn sc_attach_plan(plan: i32, month: i32) -> String {
    format!(
        "CREATE TABLE sc_ss_p_{plan} PARTITION OF sc_ss FOR VALUES IN ({plan}) \
           PARTITION BY RANGE (d); {}",
        sc_attach_month(plan, month)
    )
}

const SC_FLUSH: &str = "SELECT reflex_flush_partition_source('sc_ss')";

/// One committed transaction of a scenario and the nodes its oracle leaves out.
struct ScStep {
    label: &'static str,
    statements: String,
    compared: bool,
    not_compared: &'static [&'static str],
}

fn sc_step(label: &'static str, statements: String) -> ScStep {
    ScStep { label, statements, compared: true, not_compared: &[] }
}

fn sc_swap_scenarios() -> Vec<ScStep> {
    let some_rows = "SELECT plan, d, yr, product, loc, \
                       qty + CASE WHEN product = 1 THEN 3 ELSE 0 END, base FROM sc_ss_p_2_2027_02";
    let added_and_removed = format!(
        "SELECT * FROM ({}) AS r(plan, d, yr, product, loc, qty, base) WHERE product <> 5",
        sc_leaf_rows(2, 3, 6, 0)
    );
    vec![
        sc_step("every row of a leaf changed", sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7))),
        sc_step("some rows of a leaf changed", sc_swap(2, 2, some_rows)),
        sc_step("rows added to and removed from a leaf", sc_swap(2, 3, &added_and_removed)),
        sc_step("identical data", sc_swap(1, 1, "SELECT * FROM sc_ss_p_1_2027_01")),
        sc_step("emptied leaf", sc_swap(3, 2, "SELECT * FROM sc_ss WHERE FALSE")),
        sc_step("leaf filled from empty", sc_swap(3, 2, &sc_leaf_rows(3, 2, 4, 2))),
        sc_step("last month extended", sc_swap(3, 4, &sc_leaf_rows(3, 4, 9, 0))),
        sc_step("first month emptied", sc_swap(1, 1, "SELECT * FROM sc_ss WHERE FALSE")),
        sc_step("three leaves of one plan", sc_three_leaves(2, 4)),
        sc_step(
            "leaves of two plans",
            format!(
                "{}; {}",
                sc_swap(1, 2, &sc_leaf_rows(1, 2, 4, 5)),
                sc_swap(3, 1, &sc_leaf_rows(3, 1, 4, 5))
            ),
        ),
        // The analysis view ignores the assortment table the passthrough observes.
        ScStep {
            label: "leaf swap and a write to another source of the passthrough",
            statements: format!(
                "{}; UPDATE sc_caav SET is_active = NOT is_active WHERE product = 2",
                sc_swap(2, 4, &sc_leaf_rows(2, 4, 4, 6))
            ),
            compared: true,
            not_compared: &["sc_fav", "sc_fav__cte_active_assortment"],
        },
        sc_step(
            "reconcile of the view that ignores the written source",
            "SELECT reflex_reconcile('sc_fav')".to_string(),
        ),
        sc_step(
            "one leaf, explicit flush",
            format!("{}; {SC_FLUSH}", sc_swap(1, 3, &sc_leaf_rows(1, 3, 4, 8))),
        ),
        sc_step("brand-new month of a plan", sc_attach_month(3, 5)),
        sc_step("new plan attached with data", sc_attach_plan(5, 5)),
        sc_step("whole plan replaced", sc_swap_plan(2, 5)),
        sc_step(
            "whole plan replaced, explicit flush",
            format!("{}; {SC_FLUSH}", sc_swap_plan(1, 6)),
        ),
        ScStep {
            label: "ignored source changed",
            statements: "UPDATE sc_dp SET assortment_id = 2 WHERE id = 2".to_string(),
            compared: false,
            not_compared: &[],
        },
        ScStep {
            label: "leaf swap after the ignored source changed",
            statements: sc_swap(2, 1, &sc_leaf_rows(2, 1, 4, 9)),
            compared: false,
            not_compared: &[],
        },
        sc_step("heal of the ignored change", "SELECT reflex_heal_ignored_sources()".to_string()),
        // The dependents of a mirror are not told of a partition it drops:
        // untreated_bugs/2026-10-10_detach_partition_leaves_dependents_of_partition_mirroring_imvs_stale.md
        ScStep {
            label: "leaf swap then detach of its plan",
            statements: format!(
                "{}; ALTER TABLE sc_ss DETACH PARTITION sc_ss_p_4",
                sc_swap(4, 1, &sc_leaf_rows(4, 1, 4, 3))
            ),
            compared: true,
            not_compared: &["sc_fdya", "sc_fav__cte_forecast_bounds"],
        },
    ]
}

fn sc_assert_swaps_keep_the_chain_correct(dbname: &str, mode: &str, field_names: bool) {
    let chain = ScChain { field_names, cross_plan: false };
    sc_open(dbname, mode, chain);
    let created_wrong = chain.wrong_nodes(&[]);
    let mut failures = Vec::new();
    for step in sc_swap_scenarios() {
        if let Err(error) = chain.commit(&step.statements) {
            failures.push(format!("{}: raised {error}", step.label));
            continue;
        }
        if step.compared {
            for wrong in chain.wrong_nodes(step.not_compared) {
                failures.push(format!("{}: {wrong}", step.label));
            }
        }
    }
    probe_db_close(dbname);
    assert!(created_wrong.is_empty(), "after create: {}", created_wrong.join(", "));
    assert!(failures.is_empty(), "{mode}\n{}", failures.join("\n"));
}

#[pg_test]
fn v112_sc_swaps_keep_the_chain_correct_deferred_field_names() {
    sc_assert_swaps_keep_the_chain_correct("reflex_sc_ok_df", "DEFERRED", true);
}

#[pg_test]
fn v112_sc_swaps_keep_the_chain_correct_deferred_short_names() {
    sc_assert_swaps_keep_the_chain_correct("reflex_sc_ok_ds", "DEFERRED", false);
}

#[pg_test]
fn v112_sc_swaps_keep_the_chain_correct_immediate_field_names() {
    sc_assert_swaps_keep_the_chain_correct("reflex_sc_ok_if", "IMMEDIATE", true);
}

#[pg_test]
fn v112_sc_swaps_keep_the_chain_correct_immediate_short_names() {
    sc_assert_swaps_keep_the_chain_correct("reflex_sc_ok_is", "IMMEDIATE", false);
}

/// A dependent whose rows of one plan are computed from another plan's rows is
/// correct after a swap of that other plan's leaf, of its own, and of several.
fn sc_assert_cross_plan_dependents_stay_correct(dbname: &str, mode: &str) {
    let chain = ScChain { field_names: false, cross_plan: true };
    sc_open(dbname, mode, chain);
    let mut failures = chain.wrong_nodes(&[]);
    for (label, statements) in [
        ("leaf of the plan the others read", sc_swap(1, 2, &sc_leaf_rows(1, 2, 4, 7))),
        ("leaf of another plan", sc_swap(3, 2, &sc_leaf_rows(3, 2, 4, 7))),
        ("leaves of the read plan and of another", format!(
            "{}; {}",
            sc_swap(1, 3, &sc_leaf_rows(1, 3, 6, 1)),
            sc_swap(2, 3, &sc_leaf_rows(2, 3, 4, 1))
        )),
        ("emptied leaf of the read plan", sc_swap(1, 2, "SELECT * FROM sc_ss WHERE FALSE")),
        ("read plan's leaf, explicit flush", format!(
            "{}; {SC_FLUSH}",
            sc_swap(1, 4, &sc_leaf_rows(1, 4, 4, 9))
        )),
    ] {
        if let Err(error) = chain.commit(&statements) {
            failures.push(format!("{label}: raised {error}"));
            continue;
        }
        for wrong in chain.wrong_nodes(&[]) {
            failures.push(format!("{label}: {wrong}"));
        }
    }
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{mode}\n{}", failures.join("\n"));
}

#[pg_test]
fn v112_sc_cross_plan_dependents_stay_correct_deferred() {
    sc_assert_cross_plan_dependents_stay_correct("reflex_sc_xp_d", "DEFERRED");
}

#[pg_test]
fn v112_sc_cross_plan_dependents_stay_correct_immediate() {
    sc_assert_cross_plan_dependents_stay_correct("reflex_sc_xp_i", "IMMEDIATE");
}

const SC_ONE_LEAF_REFRESHES: &[(&str, &str)] = &[
    ("sc_sfv", "partitions"),
    ("sc_fav__cte_forecast_bounds", "partitions"),
    ("sc_fav__cte_forecast_sales", "partitions"),
];

/// Swaps of populated leaves refresh the passthrough and the two sub-IMVs that
/// ignore it, once each and within the swapped plans, whatever the number of
/// leaves and whichever flush runs; the other nodes receive the row diff.
fn sc_assert_leaf_swaps_stay_within_their_plans(dbname: &str, field_names: bool) {
    let chain = ScChain { field_names, cross_plan: false };
    sc_open(dbname, "DEFERRED", chain);
    let expected = chain.expected_refreshes(SC_ONE_LEAF_REFRESHES);
    let mut failures = Vec::new();
    for (label, statements, plans) in [
        ("one leaf", sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7)), vec![2]),
        (
            "one leaf, explicit flush",
            format!("{}; {SC_FLUSH}", sc_swap(2, 3, &sc_leaf_rows(2, 3, 4, 7))),
            vec![2],
        ),
        ("three leaves of one plan", sc_three_leaves(3, 3), vec![3]),
        (
            "leaves of two plans",
            format!(
                "{}; {}",
                sc_swap(1, 2, &sc_leaf_rows(1, 2, 4, 5)),
                sc_swap(3, 4, &sc_leaf_rows(3, 4, 4, 5))
            ),
            vec![1, 3],
        ),
    ] {
        chain.commit(&statements).unwrap_or_else(|error| panic!("{label}: {error}"));
        let refreshes = chain.refreshes();
        if refreshes != expected {
            failures.push(format!("{label}: refreshed {refreshes:?}"));
        }
        let outside = chain.written_outside(&plans);
        if !outside.is_empty() {
            failures.push(format!("{label}: wrote outside plans {plans:?}: {outside:?}"));
        }
        for wrong in chain.wrong_nodes(&[]) {
            failures.push(format!("{label}: {wrong}"));
        }
    }
    probe_db_close(dbname);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[pg_test]
fn v112_sc_leaf_swaps_stay_within_their_plans_field_names() {
    sc_assert_leaf_swaps_stay_within_their_plans("reflex_sc_cost_f", true);
}

#[pg_test]
fn v112_sc_leaf_swaps_stay_within_their_plans_short_names() {
    sc_assert_leaf_swaps_stay_within_their_plans("reflex_sc_cost_s", false);
}

/// A plan the chain has never seen reaches every node through partitions no
/// row diff can describe: each node is refreshed once, for that plan only.
fn sc_assert_new_plan_refreshes_each_node_once(dbname: &str, field_names: bool, explicit: bool) {
    let chain = ScChain { field_names, cross_plan: false };
    sc_open(dbname, "DEFERRED", chain);
    let statements = if explicit {
        format!("{}; {SC_FLUSH}", sc_attach_plan(5, 5))
    } else {
        sc_attach_plan(5, 5)
    };
    chain.commit(&statements).expect("attach plan");
    let refreshes = chain.refreshes();
    let outside = chain.written_outside(&[5]);
    let wrong = chain.wrong_nodes(&[]);
    probe_db_close(dbname);

    assert!(wrong.is_empty(), "{}", wrong.join(", "));
    let mut refreshed: Vec<&String> = refreshes.iter().map(|(node, _)| node).collect();
    refreshed.sort();
    let distinct = refreshed.len();
    refreshed.dedup();
    assert_eq!(refreshed.len(), distinct, "a node refreshed more than once: {refreshes:?}");
    assert!(
        refreshes.iter().all(|(_, kind)| kind != "full"),
        "a node refreshed in full: {refreshes:?}"
    );
    assert!(outside.is_empty(), "wrote outside plan 5: {outside:?}\n{refreshes:?}");
    for node in ["sc_sfv", "sc_fav", "sc_fav__cte_forecast_sales", "sc_fav__cte_history_sales"] {
        assert!(
            refreshes.contains(&(chain.named(node), "partitions".to_string())),
            "{node} not refreshed by partition: {refreshes:?}"
        );
    }
}

#[pg_test]
fn v112_sc_new_plan_refreshes_each_node_once_field_names() {
    sc_assert_new_plan_refreshes_each_node_once("reflex_sc_new_f", true, false);
}

#[pg_test]
fn v112_sc_new_plan_refreshes_each_node_once_short_names() {
    sc_assert_new_plan_refreshes_each_node_once("reflex_sc_new_s", false, false);
}

#[pg_test]
fn v112_sc_new_plan_refreshes_each_node_once_explicit_flush() {
    sc_assert_new_plan_refreshes_each_node_once("reflex_sc_new_e", true, true);
}

/// A dependent whose plans cannot be shown to follow the swapped source's is
/// refreshed whole, once, without its generated sub-IMV, whichever flush runs;
/// the provable ones keep their partition scope.
#[pg_test]
fn v112_sc_unprovable_dependents_are_refreshed_whole_once() {
    let chain = ScChain { field_names: false, cross_plan: true };
    sc_open("reflex_sc_whole", "DEFERRED", chain);
    let mut expected = chain.expected_refreshes(SC_ONE_LEAF_REFRESHES);
    expected.push(("sc_versus".to_string(), "full".to_string()));
    expected.push(("sc_mapped".to_string(), "full".to_string()));
    expected.sort();
    let mut failures = Vec::new();
    for (label, statements) in [
        ("commit", sc_swap(2, 2, &sc_leaf_rows(2, 2, 4, 7))),
        (
            "explicit flush",
            format!("{}; {SC_FLUSH}", sc_swap(2, 3, &sc_leaf_rows(2, 3, 4, 7))),
        ),
    ] {
        chain.commit(&statements).unwrap_or_else(|error| panic!("{label}: {error}"));
        let mut refreshes = chain.refreshes();
        refreshes.sort();
        if refreshes != expected {
            failures.push(format!("{label}: refreshed {refreshes:?}"));
        }
        for wrong in chain.wrong_nodes(&[]) {
            failures.push(format!("{label}: {wrong}"));
        }
    }
    probe_db_close("reflex_sc_whole");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Ordinary writes do not reach the sub-IMVs that ignore the passthrough; the
/// next swap of a leaf of that plan refreshes them, even one that changes no
/// row of the passthrough.
#[pg_test]
fn v112_sc_unchanged_leaf_still_refreshes_the_ignoring_sub_imvs() {
    let chain = ScChain { field_names: true, cross_plan: false };
    sc_open("reflex_sc_same", "DEFERRED", chain);
    chain
        .commit("INSERT INTO sc_ss VALUES (3, '2027-02-20', 2027, 1, 1, 5, 5)")
        .expect("insert");
    let behind_before = chain.wrong_nodes(&[]);
    chain
        .commit(&sc_swap(3, 1, "SELECT * FROM sc_ss_p_3_2027_01"))
        .expect("swap");
    let refreshes = chain.refreshes();
    let outside = chain.written_outside(&[3]);
    let wrong = chain.wrong_nodes(&[]);
    probe_db_close("reflex_sc_same");

    assert!(
        behind_before.iter().any(|node| node.contains("forecast_sales")),
        "fixture: the ignoring sub-IMV is behind after an ordinary write: {behind_before:?}"
    );
    assert!(wrong.is_empty(), "{}", wrong.join(", "));
    assert_eq!(refreshes, chain.expected_refreshes(SC_ONE_LEAF_REFRESHES));
    assert!(outside.is_empty(), "wrote outside plan 3: {outside:?}");
}

/// A partition of a source other than the anchor names the slice under the
/// same bound; a relation that is no partition of a source is refused.
#[pg_test]
fn v112_sc_partition_of_a_non_anchor_source_names_its_slice() {
    let chain = ScChain { field_names: true, cross_plan: false };
    sc_open("reflex_sc_bound", "DEFERRED", chain);
    let partition_of_plan_2 = |imv: &str| {
        sc_text_rows(&chain.named(&format!(
            "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
             WHERE i.inhparent = '{imv}'::regclass \
               AND pg_get_expr(c.relpartbound, c.oid) = 'FOR VALUES IN (2)'"
        )))
        .pop()
        .expect("partition of plan 2")
    };
    let date_limits = partition_of_plan_2("sc_fav__cte_date_limits");
    let forecast_sales = partition_of_plan_2("sc_fav__cte_forecast_sales");
    let by_other_source = rbc_select(&chain.named(&format!(
        "reflex_reconcile_partition('sc_fav__cte_forecast_sales', '', 'public.{date_limits}')"
    )));
    let by_unrelated_relation = rbc_select(&chain.named(
        "reflex_reconcile_partition('sc_fav__cte_forecast_sales', '', 'sc_loc')",
    ));
    let wrong = chain.wrong_nodes(&[]);
    probe_db_close("reflex_sc_bound");

    assert_eq!(by_other_source, format!("RECONCILED partitions: {forecast_sales}"));
    assert!(
        by_unrelated_relation.starts_with("ERROR")
            && by_unrelated_relation.contains("is not a partition of a source of"),
        "{by_unrelated_relation}"
    );
    assert!(wrong.is_empty(), "{}", wrong.join(", "));
}

/// The partition scope is kept only for a query that takes its partition
/// column from the one row of the rebuilt source each output row is built on.
#[pg_test]
fn v112_sc_scope_is_proven_only_for_aligned_queries() {
    let unproven = |sql: &str, reads: &[&str], source: &str| {
        let reads: Vec<String> = reads.iter().map(|r| r.to_string()).collect();
        crate::cascade::unproven_partition_alignment(sql, &reads, source, "plan")
    };
    for (sql, reads, source) in [
        ("SELECT plan, yr, MIN(d) AS lo FROM src GROUP BY plan, yr", &["src"][..], "src"),
        ("SELECT sf.plan, MIN(sf.d) AS lo FROM src AS sf GROUP BY sf.plan", &["src"][..], "src"),
        (
            "SELECT dl.plan, sf.x FROM src AS sf JOIN other AS dl ON sf.plan = dl.plan \
               AND sf.d >= dl.lo",
            &["src", "other"][..],
            "src",
        ),
        (
            "SELECT dl.plan, hb.x FROM other AS hb JOIN src AS dl ON hb.d >= dl.lo",
            &["other", "src"][..],
            "src",
        ),
        (
            "SELECT COALESCE(f.plan, hs.plan) AS plan, f.x FROM src AS f \
               FULL JOIN other AS hs ON f.plan = hs.plan AND f.k = hs.k \
               LEFT JOIN third AS a ON a.k = COALESCE(f.k, hs.k)",
            &["src", "other", "third"][..],
            "src",
        ),
        (
            "SELECT COALESCE(f.plan, hs.plan) AS plan, f.x FROM other AS f \
               FULL JOIN src AS hs ON f.plan = hs.plan",
            &["other", "src"][..],
            "src",
        ),
        (
            "SELECT t.plan, s.x FROM other AS t LEFT JOIN src AS s ON s.plan = t.plan",
            &["other", "src"][..],
            "src",
        ),
        (
            "SELECT sf.plan, sf.x FROM \"tenant\".\"src\" AS sf",
            &["tenant.src"][..],
            "tenant.src",
        ),
        (
            "SELECT COALESCE(forecast.plan, \"v__cte_hs\".plan) AS plan, forecast.x \
             FROM \"v__cte_fs\" AS forecast \
               FULL JOIN \"v__cte_hs\" ON forecast.plan = \"v__cte_hs\".plan \
               LEFT JOIN \"v__cte_aa\" AS caav ON caav.k = forecast.k",
            &["\"v__cte_fs\"", "\"v__cte_hs\"", "\"v__cte_aa\""][..],
            "v__cte_hs",
        ),
        (
            "SELECT fb.plan, fb.lo FROM \"v__cte_bounds\" AS fb CROSS JOIN one AS o \
             WHERE fb.plan IN (SELECT c.plan FROM cur AS c)",
            &["\"v__cte_bounds\"", "one", "cur"][..],
            "v__cte_bounds",
        ),
    ] {
        assert_eq!(unproven(sql, reads, source), None, "{sql}");
    }
    for (sql, reads, source) in [
        (
            "SELECT a.plan, b.x FROM src AS a JOIN src AS b ON b.plan = 1 AND b.k = a.k",
            &["src", "src"][..],
            "src",
        ),
        (
            "SELECT m.plan, sf.x FROM src AS sf JOIN map AS m ON m.src_plan = sf.plan",
            &["src", "map"][..],
            "src",
        ),
        (
            "SELECT s.plan, t.x FROM other AS t LEFT JOIN src AS s ON s.k = t.k",
            &["other", "src"][..],
            "src",
        ),
        (
            "SELECT dl.plan, sf.x FROM src AS sf LEFT JOIN other AS dl ON sf.plan = dl.plan",
            &["src", "other"][..],
            "src",
        ),
        (
            "SELECT sf.plan, RANK() OVER (ORDER BY sf.q) AS r FROM src AS sf",
            &["src"][..],
            "src",
        ),
        (
            "SELECT dl.plan, sf.x FROM src AS sf JOIN other AS dl ON sf.plan = dl.plan \
               OR sf.k = dl.k",
            &["src", "other"][..],
            "src",
        ),
        (
            "SELECT f.plan, f.x FROM src AS f FULL JOIN other AS hs ON f.plan = hs.plan",
            &["src", "other"][..],
            "src",
        ),
        (
            "SELECT COALESCE(f.plan, hs.alt) AS plan, f.x FROM src AS f \
               FULL JOIN other AS hs ON f.k = hs.k",
            &["src", "other"][..],
            "src",
        ),
        ("SELECT sf.plan + 1 AS plan, sf.x FROM src AS sf", &["src"][..], "src"),
        ("SELECT sf.alt AS plan, sf.x FROM src AS sf", &["src"][..], "src"),
        (
            "SELECT dl.alt AS plan, sf.x FROM src AS sf JOIN other AS dl ON sf.alt = dl.alt",
            &["src", "other"][..],
            "src",
        ),
        (
            "SELECT t.plan, t.x FROM other AS t WHERE t.k IN (SELECT s.k FROM src AS s)",
            &["other", "src"][..],
            "src",
        ),
        (
            "SELECT sf.plan, o.x FROM src AS sf, other AS o WHERE o.k = sf.k",
            &["src", "other"][..],
            "src",
        ),
        (
            "SELECT sf.plan, o.x FROM src AS sf RIGHT JOIN other AS o ON o.k = sf.k",
            &["src", "other"][..],
            "src",
        ),
        (
            "SELECT plan, x FROM src JOIN other ON src.k = other.k",
            &["src", "other"][..],
            "src",
        ),
        (
            "SELECT DISTINCT ON (sf.k) sf.plan, sf.k FROM src AS sf ORDER BY sf.k, sf.q",
            &["src"][..],
            "src",
        ),
        (
            "SELECT sf.plan FROM src AS sf UNION ALL SELECT o.plan FROM other AS o",
            &["src", "other"][..],
            "src",
        ),
    ] {
        assert!(unproven(sql, reads, source).is_some(), "{sql}");
    }
}

#[pg_test]
fn v112_sc_scopes_reaching_one_dependent_merge_to_the_wider() {
    use crate::cascade::RefreshScope::{Groups, PartitionKeys, SourcePartitions, Whole};
    let set = |items: &[&str]| items.iter().map(|i| i.to_string()).collect::<std::collections::BTreeSet<String>>();
    let whole = || Whole("unproven".to_string());
    for narrower in [
        SourcePartitions(set(&["public.p_1"])),
        PartitionKeys(set(&["1"])),
        Groups { column: "plan".to_string(), keys: set(&["1"]) },
    ] {
        assert_eq!(whole().merged_with(narrower.clone()), whole());
        assert_eq!(narrower.clone().merged_with(whole()), whole());
    }
    assert_eq!(
        PartitionKeys(set(&["1"])).merged_with(PartitionKeys(set(&["2"]))),
        PartitionKeys(set(&["1", "2"]))
    );
    assert_eq!(
        SourcePartitions(set(&["public.p_1"])).merged_with(SourcePartitions(set(&["public.p_2"]))),
        SourcePartitions(set(&["public.p_1", "public.p_2"]))
    );
    assert_eq!(
        Groups { column: "plan".to_string(), keys: set(&["1"]) }
            .merged_with(Groups { column: "plan".to_string(), keys: set(&["2"]) }),
        Groups { column: "plan".to_string(), keys: set(&["1", "2"]) }
    );
    for (one, other) in [
        (PartitionKeys(set(&["1"])), SourcePartitions(set(&["public.p_1"]))),
        (
            Groups { column: "plan".to_string(), keys: set(&["1"]) },
            Groups { column: "yr".to_string(), keys: set(&["1"]) },
        ),
        (PartitionKeys(set(&["1"])), Groups { column: "plan".to_string(), keys: set(&["1"]) }),
    ] {
        assert!(matches!(one.merged_with(other), Whole(_)));
    }
}
