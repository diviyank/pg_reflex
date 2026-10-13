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
        ScStep {
            label: "leaf swap then detach of its plan",
            statements: format!(
                "{}; ALTER TABLE sc_ss DETACH PARTITION sc_ss_p_4",
                sc_swap(4, 1, &sc_leaf_rows(4, 1, 4, 3))
            ),
            compared: true,
            not_compared: &[],
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

/// A partition reconcile serves partitions of the source the IMV mirrors. A
/// partition of another source, or a relation that is no partition, is refused
/// and nothing is rebuilt.
#[pg_test]
fn v112_sc_partition_of_a_source_that_is_not_mirrored_is_refused() {
    let chain = ScChain { field_names: true, cross_plan: false };
    sc_open("reflex_sc_bound", "DEFERRED", chain);
    let date_limits = sc_text_rows(&chain.named(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = 'sc_fav__cte_date_limits'::regclass \
           AND pg_get_expr(c.relpartbound, c.oid) = 'FOR VALUES IN (2)'",
    ))
    .pop()
    .expect("partition of plan 2");
    let by_other_source = rbc_select(&chain.named(&format!(
        "reflex_reconcile_partition('sc_fav__cte_forecast_sales', '', 'public.{date_limits}')"
    )));
    let by_unrelated_relation = rbc_select(&chain.named(
        "reflex_reconcile_partition('sc_fav__cte_forecast_sales', '', 'sc_loc')",
    ));
    let wrong = chain.wrong_nodes(&[]);
    probe_db_close("reflex_sc_bound");

    for refusal in [&by_other_source, &by_unrelated_relation] {
        assert!(
            refusal.starts_with("ERROR") && refusal.contains("is not a partition of"),
            "{refusal}"
        );
    }
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
        (
            "SELECT COALESCE(f.plan, hs.plan) AS plan, f.x FROM src AS f \
               JOIN other AS hs ON f.plan = hs.plan",
            &["src", "other"][..],
            "src",
        ),
        (
            "SELECT COALESCE(t.plan, s.plan) AS plan, t.x FROM other AS t \
               LEFT JOIN src AS s ON s.plan = t.plan",
            &["other", "src"][..],
            "src",
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
        (
            "SELECT r.plan, sf.x FROM src AS sf JOIN other AS r ON r.k = sf.k \
               LEFT JOIN third AS z ON z.plan = sf.plan AND r.plan = sf.plan",
            &["src", "other", "third"][..],
            "src",
        ),
        (
            "SELECT COALESCE(f.plan, hs.plan) AS plan, f.x FROM src AS f \
               FULL JOIN other AS hs ON f.plan = hs.plan \
               FULL JOIN third AS t ON t.k = f.k",
            &["src", "other", "third"][..],
            "src",
        ),
        (
            "SELECT COALESCE(f.plan, hs.plan) AS plan, f.x FROM src AS f \
               FULL JOIN other AS hs ON f.plan = hs.plan \
               JOIN third AS t ON t.k = f.k",
            &["src", "other", "third"][..],
            "src",
        ),
        (
            "SELECT COALESCE(f.plan, hs.plan) AS plan, f.x FROM third AS t \
               JOIN src AS f ON f.k = t.k FULL JOIN other AS hs ON f.plan = hs.plan",
            &["third", "src", "other"][..],
            "src",
        ),
        (
            "SELECT COALESCE(f.plan, hs.plan) AS plan, f.x FROM other AS f \
               FULL JOIN third AS hs ON f.plan = hs.plan \
               LEFT JOIN src AS v ON v.k = f.k",
            &["other", "third", "src"][..],
            "src",
        ),
        (
            "SELECT COALESCE(f.plan, f.alt) AS plan, f.x FROM src AS f \
               FULL JOIN other AS hs ON f.plan = f.alt",
            &["src", "other"][..],
            "src",
        ),
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
    use crate::cascade::RefreshScope::{Groups, Partitions, Whole};
    let set = |items: &[&str]| {
        items.iter().map(|i| i.to_string()).collect::<std::collections::BTreeSet<String>>()
    };
    let partitions = |source_partitions: &[&str], keys: &[&str]| Partitions {
        source_partitions: set(source_partitions),
        keys: set(keys),
    };
    let groups = |column: &str, keys: &[&str]| Groups { column: column.to_string(), keys: set(keys) };
    let whole = || Whole("unproven".to_string());
    for narrower in [partitions(&["public.p_1"], &[]), partitions(&[], &["1"]), groups("plan", &["1"])] {
        assert_eq!(whole().merged_with(narrower.clone()), whole());
        assert_eq!(narrower.clone().merged_with(whole()), whole());
    }
    assert_eq!(
        partitions(&["public.p_1"], &["1"]).merged_with(partitions(&["public.p_2"], &["2"])),
        partitions(&["public.p_1", "public.p_2"], &["1", "2"])
    );
    assert_eq!(
        partitions(&["public.p_1"], &[]).merged_with(partitions(&[], &["2"])),
        partitions(&["public.p_1"], &["2"])
    );
    assert_eq!(
        groups("plan", &["1"]).merged_with(groups("plan", &["2"])),
        groups("plan", &["1", "2"])
    );
    for (one, other) in [
        (groups("plan", &["1"]), groups("yr", &["1"])),
        (partitions(&[], &["1"]), groups("plan", &["1"])),
    ] {
        assert!(matches!(one.merged_with(other), Whole(_)));
    }
}

/// Only a bound that lists plain values names the keys of its partition.
#[pg_test]
fn v112_sc_only_a_plain_list_bound_names_its_keys() {
    let listed = |bound: &str| crate::cascade::keys_listed_by(bound);
    let keys = |items: &[&str]| Some(items.iter().map(|i| i.to_string()).collect::<Vec<_>>());
    assert_eq!(listed("FOR VALUES IN (7)"), keys(&["7"]));
    assert_eq!(listed("FOR VALUES IN (1, 2)"), keys(&["1", "2"]));
    assert_eq!(listed("FOR VALUES IN ('-1', 2.50, -3)"), keys(&["-1", "2.50", "-3"]));
    assert_eq!(listed("FOR VALUES IN ('a, b', 'it''s')"), keys(&["a, b", "it's"]));
    assert_eq!(listed("FOR VALUES IN ('2027-01-01')"), keys(&["2027-01-01"]));
    for unlisted in [
        "DEFAULT",
        "FOR VALUES IN (NULL, 5)",
        "FOR VALUES IN (NULL)",
        "FOR VALUES IN (true)",
        "FOR VALUES IN ('a'::text)",
        "FOR VALUES FROM (1) TO (5)",
        "FOR VALUES FROM ('2027-01-01') TO ('2027-02-01')",
        "FOR VALUES WITH (modulus 4, remainder 0)",
        "FOR VALUES IN (1",
        "",
    ] {
        assert_eq!(listed(unlisted), None, "{unlisted}");
    }
}

// ---------------------------------------------------------------------------
// Shapes in which a narrowed refresh is complete only under a condition: a
// DEFAULT partition, a dependent mirroring another root, a leaf holding several
// keys or the NULL key, a dependent of a group-refreshed node, a second
// partition level taken from another relation, and queries the alignment proof
// must refuse. Every IMV is real; every scenario is committed on the worker and
// followed by the oracle on every IMV.

thread_local! {
    static SC_NOTICES: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

unsafe extern "C-unwind" fn sc_keep_reflex_notices(edata: *mut pgrx::pg_sys::ErrorData) {
    const NOTICE: i32 = 18;
    if edata.is_null() {
        return;
    }
    let (level, message) = unsafe { ((*edata).elevel, (*edata).message) };
    if level != NOTICE || message.is_null() {
        return;
    }
    let text = unsafe { std::ffi::CStr::from_ptr(message) }.to_string_lossy().to_string();
    if text.starts_with("pg_reflex:") {
        SC_NOTICES.with(|notices| notices.borrow_mut().push(text));
    }
}

/// Starts keeping the `pg_reflex:` NOTICEs of the calling backend.
#[pg_extern]
fn sc_keep_notices() -> bool {
    unsafe {
        pgrx::pg_sys::emit_log_hook = Some(sc_keep_reflex_notices);
    }
    true
}

/// The NOTICEs kept since the last call, one per row.
#[pg_extern]
fn sc_kept_notices() -> Vec<String> {
    SC_NOTICES.with(|notices| notices.borrow_mut().drain(..).collect())
}

#[derive(Clone, Copy)]
struct SxImv {
    name: &'static str,
    /// Its query; `{name}` stands for another IMV of the fixture.
    sql: &'static str,
    keys: &'static str,
    ignore: &'static str,
    partition_by: &'static [&'static str],
}

const SX_YD: &str = "SELECT v.plan, v.k, v.q, t.w FROM {yv} AS v \
     JOIN a_t AS t ON t.plan = v.plan AND t.k = v.k";
const SX_PLAN_SUMS_Y: &str =
    "SELECT v.plan, SUM(v.q) AS sq, COUNT(*) AS n FROM {yv} AS v GROUP BY v.plan";
const SX_DAY_SUMS: &str = "SELECT d, SUM(q) AS sq, COUNT(*) AS n FROM {gv} AS gv GROUP BY d";
const SX_PLAN_SUMS_L: &str =
    "SELECT plan, SUM(q) AS sq, COUNT(*) AS n FROM {lv} AS lv GROUP BY plan";
const SX_PLAN_SUMS_M: &str =
    "SELECT plan, SUM(q) AS sq, MIN(d) AS lo, COUNT(*) AS n FROM {mv} AS mv GROUP BY plan";
const SX_OTHER_DAY: &str =
    "SELECT mv.plan, o.d, mv.k, mv.q, mv.d AS od FROM {mv} AS mv JOIN mo AS o ON o.k = mv.k";
const SX_TWO_PATHS: &str = "SELECT v.plan, v.k, v.d, v.q, xm.q AS mq FROM {mv} AS v \
     JOIN {m_map} AS xm ON xm.plan = v.plan AND xm.k = v.k AND xm.d = v.d";

const SX_IMVS: &[SxImv] = &[
    // A source with a DEFAULT partition, and dependents mirroring another root.
    SxImv { name: "yv", sql: "SELECT s.plan, s.k, s.q FROM ys AS s", keys: "plan, k", ignore: "", partition_by: &["plan"] },
    SxImv { name: "yd", sql: SX_YD, keys: "plan, k", ignore: "!yv", partition_by: &["plan"] },
    SxImv { name: "yd_obs", sql: SX_YD, keys: "plan, k", ignore: "", partition_by: &["plan"] },
    SxImv { name: "yg", sql: SX_PLAN_SUMS_Y, keys: "plan", ignore: "!yv", partition_by: &[] },
    SxImv { name: "yp", sql: "SELECT v.plan, v.k, v.q FROM {yv} AS v", keys: "plan, k", ignore: "!yv", partition_by: &["plan"] },
    // Mirrors a root that has no partition for plan 5.
    SxImv {
        name: "yx",
        sql: "SELECT v.plan, v.k, v.q, x.r FROM {yv} AS v \
              JOIN x2_t AS x ON x.plan = v.plan AND x.k = v.k",
        keys: "plan, k",
        ignore: "!yv",
        partition_by: &["plan"],
    },
    // Partitioned first by a column of another source, of the same type as plan.
    SxImv {
        name: "yr_region",
        sql: "SELECT v.plan, x.region, v.k, v.q FROM {yv} AS v JOIN x_t AS x ON x.k = v.k",
        keys: "plan, k, region",
        ignore: "!yv",
        partition_by: &["region"],
    },
    // Its plan is the joined table's; the equality with the view's plan sits in
    // the ON of an outer join of a third table and constrains nothing.
    SxImv {
        name: "y_loose",
        sql: "SELECT r.plan, v.plan AS vplan, v.k, v.q, z.z FROM {yv} AS v \
              JOIN a_t AS r ON r.k = v.k \
              LEFT JOIN zt AS z ON z.plan = v.plan AND r.plan = v.plan",
        keys: "plan, vplan, k",
        ignore: "!yv",
        partition_by: &["plan"],
    },
    // COALESCE over the two sides of a FULL JOIN neither of which is the view.
    SxImv {
        name: "y_beside",
        sql: "SELECT COALESCE(f.plan, h.plan) AS plan, COALESCE(f.k, h.k) AS k, f.w, h.r, v.q \
              FROM a_t AS f FULL JOIN x2_t AS h ON f.plan = h.plan AND f.k = h.k \
              LEFT JOIN {yv} AS v ON v.k = COALESCE(f.k, h.k) AND v.plan = 1",
        keys: "plan, k",
        ignore: "!yv",
        partition_by: &["plan"],
    },
    // COALESCE over a FULL JOIN followed by a second FULL JOIN, whose unmatched
    // rows carry no plan.
    SxImv {
        name: "y_two_full",
        sql: "SELECT COALESCE(v.plan, h.plan) AS plan, COALESCE(v.k, h.k) AS k, v.q, h.w, t.z \
              FROM {yv} AS v FULL JOIN a_t AS h ON v.plan = h.plan AND v.k = h.k \
              FULL JOIN zk AS t ON t.k = COALESCE(v.k, h.k)",
        keys: "",
        ignore: "!yv",
        partition_by: &["plan"],
    },
    // Sources of another column or joined on another column.
    SxImv {
        name: "nd",
        sql: "SELECT t.plan, t.k, t.w, x.r FROM a_t AS t JOIN x_t AS x ON x.k = t.k",
        keys: "plan, k",
        ignore: "",
        partition_by: &["plan"],
    },
    SxImv {
        name: "nd2",
        sql: "SELECT t.plan, t.k, t.w, x.r FROM a_t AS t JOIN x2_t AS x ON x.k = t.k",
        keys: "plan, k",
        ignore: "",
        partition_by: &["plan"],
    },
    // A RANGE leaf holds several days.
    SxImv { name: "gv", sql: "SELECT s.d, s.k, s.q FROM gs AS s", keys: "d, k", ignore: "", partition_by: &["d"] },
    SxImv { name: "gg", sql: SX_DAY_SUMS, keys: "d", ignore: "!gv", partition_by: &[] },
    SxImv { name: "gg_obs", sql: SX_DAY_SUMS, keys: "d", ignore: "", partition_by: &[] },
    SxImv { name: "gg_dep", sql: "SELECT g.d, g.sq FROM {gg} AS g", keys: "d", ignore: "!gg", partition_by: &[] },
    SxImv { name: "gg_dep_obs", sql: "SELECT g.d, g.sq FROM {gg} AS g", keys: "d", ignore: "", partition_by: &[] },
    // A LIST leaf holds two plans.
    SxImv { name: "lv", sql: "SELECT s.plan, s.k, s.q FROM ls AS s", keys: "plan, k", ignore: "", partition_by: &["plan"] },
    SxImv { name: "lg", sql: SX_PLAN_SUMS_L, keys: "plan", ignore: "!lv", partition_by: &[] },
    SxImv { name: "lg_obs", sql: SX_PLAN_SUMS_L, keys: "plan", ignore: "", partition_by: &[] },
    // A leaf holds the NULL key.
    SxImv { name: "nv", sql: "SELECT s.plan, s.k, s.q FROM ns AS s", keys: "", ignore: "", partition_by: &["plan"] },
    SxImv {
        name: "ng",
        sql: "SELECT plan, SUM(q) AS sq, COUNT(*) AS n FROM {nv} AS nv GROUP BY plan",
        keys: "",
        ignore: "!nv",
        partition_by: &[],
    },
    // The NULL key in a leaf below a top-level partition that is not replaced.
    SxImv { name: "nv2", sql: "SELECT s.plan, s.d, s.k, s.q FROM ns2 AS s", keys: "plan, d, k", ignore: "", partition_by: &["plan", "d"] },
    SxImv {
        name: "ng2",
        sql: "SELECT plan, SUM(q) AS sq, COUNT(*) AS n FROM {nv2} AS nv2 GROUP BY plan",
        keys: "",
        ignore: "!nv2",
        partition_by: &[],
    },
    // Months first, then a list: a leaf's days are not listed by any bound.
    SxImv { name: "tv", sql: "SELECT s.d, s.k, s.q FROM ts AS s", keys: "d, k", ignore: "", partition_by: &["d", "k"] },
    SxImv { name: "tg", sql: "SELECT d, SUM(q) AS sq, COUNT(*) AS n FROM {tv} AS tv GROUP BY d", keys: "d", ignore: "!tv", partition_by: &[] },
    SxImv { name: "tg_obs", sql: "SELECT d, SUM(q) AS sq, COUNT(*) AS n FROM {tv} AS tv GROUP BY d", keys: "d", ignore: "", partition_by: &[] },
    // Two levels.
    SxImv { name: "mv", sql: "SELECT s.plan, s.d, s.k, s.q FROM ms AS s", keys: "plan, k, d", ignore: "", partition_by: &["plan", "d"] },
    SxImv { name: "mg", sql: SX_PLAN_SUMS_M, keys: "plan", ignore: "!mv", partition_by: &[] },
    SxImv { name: "mg_obs", sql: SX_PLAN_SUMS_M, keys: "plan", ignore: "", partition_by: &[] },
    SxImv {
        name: "mg2",
        sql: "SELECT plan, d, SUM(q) AS sq FROM {mv} AS mv GROUP BY plan, d",
        keys: "plan, d",
        ignore: "!mv",
        partition_by: &[],
    },
    SxImv {
        name: "m_rename",
        sql: "SELECT k AS plan, plan AS orig, SUM(q) AS sq FROM {mv} AS mv GROUP BY k, plan",
        keys: "plan, orig",
        ignore: "!mv",
        partition_by: &[],
    },
    SxImv { name: "mg_dep", sql: "SELECT g.plan, g.sq FROM {mg} AS g", keys: "plan", ignore: "!mg", partition_by: &[] },
    SxImv { name: "m_other", sql: SX_OTHER_DAY, keys: "plan, k, od, d", ignore: "!mv", partition_by: &["plan", "d"] },
    SxImv { name: "m_other_obs", sql: SX_OTHER_DAY, keys: "plan, k, od, d", ignore: "", partition_by: &["plan", "d"] },
    SxImv {
        name: "m_pd",
        sql: "SELECT mv.plan, mv.d, mv.k, mv.q FROM {mv} AS mv",
        keys: "plan, k, d",
        ignore: "!mv",
        partition_by: &["plan", "d"],
    },
    // The view read a second time, in a sub-query.
    SxImv {
        name: "m_scalar",
        sql: "SELECT v.plan, v.k, v.d, v.q, (SELECT MAX(v2.q) FROM {mv} AS v2) AS mx FROM {mv} AS v",
        keys: "plan, k, d",
        ignore: "!mv",
        partition_by: &["plan"],
    },
    SxImv {
        name: "m_above_mean",
        sql: "SELECT v.plan, v.k, v.d, v.q FROM {mv} AS v \
              WHERE v.q >= (SELECT AVG(v2.q) FROM {mv} AS v2)",
        keys: "plan, k, d",
        ignore: "!mv",
        partition_by: &["plan"],
    },
    // Reached by an aligned path and by one that is refreshed whole.
    SxImv {
        name: "m_map",
        sql: "SELECT m.plan, v.k, v.d, v.q FROM {mv} AS v JOIN zmap AS m ON m.src_plan = v.plan",
        keys: "plan, k, d",
        ignore: "!mv",
        partition_by: &["plan"],
    },
    SxImv { name: "m_two_paths", sql: SX_TWO_PATHS, keys: "plan, k, d", ignore: "!mv,!m_map", partition_by: &["plan"] },
    // Mirrors the view's months and reads a per-plan total of it: a changed month
    // changes the total, so every month of that plan.
    SxImv {
        name: "zz_tot",
        sql: "SELECT plan, SUM(q) AS sq FROM {mv} AS mv GROUP BY plan",
        keys: "plan",
        ignore: "!mv",
        partition_by: &["plan"],
    },
    SxImv {
        name: "m_with_total",
        sql: "SELECT v.plan, v.d, v.k, v.q, t.sq FROM {mv} AS v JOIN {zz_tot} AS t ON t.plan = v.plan",
        keys: "plan, k, d",
        ignore: "!mv,!zz_tot",
        partition_by: &["plan", "d"],
    },
    // Readers of the passthrough over ys that observe it.
    SxImv { name: "o_flat", sql: "SELECT v.plan, v.k, v.q FROM {yv} AS v", keys: "plan, k", ignore: "", partition_by: &[] },
    SxImv { name: "o_pass", sql: "SELECT v.plan, v.k, v.q FROM {yv} AS v", keys: "plan, k", ignore: "", partition_by: &["plan"] },
    SxImv { name: "o_agg", sql: SX_PLAN_SUMS_Y, keys: "plan", ignore: "", partition_by: &[] },
    SxImv { name: "o_agg_dep", sql: "SELECT g.plan, g.sq FROM {o_agg} AS g", keys: "plan", ignore: "", partition_by: &[] },
    // A dependent reached by keys whose partition column has another type than
    // the rebuilt IMV's: numeric into integer, varchar into text, timestamp
    // into timestamptz.
    SxImv { name: "tn_v", sql: "SELECT s.c, s.k, s.q FROM tn_s AS s", keys: "c, k", ignore: "", partition_by: &["c"] },
    SxImv {
        name: "tn_d",
        sql: "SELECT a.c, v.k, v.q, a.w FROM {tn_v} AS v JOIN tn_a AS a ON a.c = v.c AND a.k = v.k",
        keys: "c, k",
        ignore: "!tn_v",
        partition_by: &["c"],
    },
    SxImv { name: "tt_v", sql: "SELECT s.c, s.k, s.q FROM tt_s AS s", keys: "c, k", ignore: "", partition_by: &["c"] },
    SxImv {
        name: "tt_d",
        sql: "SELECT a.c, v.k, v.q, a.w FROM {tt_v} AS v JOIN tt_a AS a ON a.c = v.c AND a.k = v.k",
        keys: "c, k",
        ignore: "!tt_v",
        partition_by: &["c"],
    },
    SxImv { name: "tz_v", sql: "SELECT s.c, s.k, s.q FROM tz_s AS s", keys: "c, k", ignore: "", partition_by: &["c"] },
    SxImv {
        name: "tz_d",
        sql: "SELECT a.c, v.k, v.q, a.w FROM {tz_v} AS v JOIN tz_a AS a ON a.c = v.c AND a.k = v.k",
        keys: "c, k",
        ignore: "!tz_v",
        partition_by: &["c"],
    },
];

fn sx_query(imv: &SxImv, inline: bool) -> String {
    let mut sql = imv.sql.to_string();
    for other in SX_IMVS {
        let placeholder = format!("{{{}}}", other.name);
        if sql.contains(&placeholder) {
            let replacement = if inline {
                format!("({})", sx_query(other, true))
            } else {
                other.name.to_string()
            };
            sql = sql.replace(&placeholder, &replacement);
        }
    }
    sql
}

fn sx_month_bounds(month: i32) -> (String, String) {
    (format!("2027-{month:02}-01"), format!("2027-{:02}-01", month + 1))
}

fn sx_ms_rows(plan: i32, month: i32, first_day: i32, bump: i32) -> String {
    format!(
        "SELECT {plan}, DATE '2027-{month:02}-01' + day, k, \
                {plan} * 1000 + {month} * 100 + k * 10 + day + {bump} \
         FROM generate_series({first_day}, {first_day} + 2) day, generate_series(1, 3) k"
    )
}

fn sx_open(dbname: &str, mode: &str) {
    probe_db_open(dbname);
    let mut setup: Vec<String> = [
        "CREATE TABLE ys (plan INT NOT NULL, k INT NOT NULL, q INT NOT NULL, PRIMARY KEY (plan, k)) \
           PARTITION BY LIST (plan)",
        "CREATE TABLE ys_p1 PARTITION OF ys FOR VALUES IN (1)",
        "CREATE TABLE ys_p5 PARTITION OF ys FOR VALUES IN (5)",
        "CREATE TABLE ys_def PARTITION OF ys DEFAULT",
        "INSERT INTO ys SELECT p, k, p * 100 + k FROM unnest(ARRAY[1, 2, 3, 5]) p, generate_series(1, 4) k",
        "CREATE TABLE a_t (plan INT NOT NULL, k INT NOT NULL, w INT NOT NULL, PRIMARY KEY (plan, k)) \
           PARTITION BY LIST (plan)",
        "CREATE TABLE a_t_p1 PARTITION OF a_t FOR VALUES IN (1)",
        "CREATE TABLE a_t_p2 PARTITION OF a_t FOR VALUES IN (2)",
        "CREATE TABLE a_t_def PARTITION OF a_t DEFAULT",
        "INSERT INTO a_t SELECT p, k, p * 10 + k FROM generate_series(1, 5) p, generate_series(1, 4) k",
        "CREATE TABLE x_t (region INT NOT NULL, k INT NOT NULL, r INT NOT NULL, \
           PRIMARY KEY (region, k)) PARTITION BY LIST (region)",
        "CREATE TABLE x_t_r1 PARTITION OF x_t FOR VALUES IN (1)",
        "CREATE TABLE x_t_r2 PARTITION OF x_t FOR VALUES IN (2)",
        "INSERT INTO x_t VALUES (1, 1, 11), (1, 2, 12), (2, 3, 23), (2, 4, 24)",
        "CREATE TABLE x2_t (plan INT NOT NULL, k INT NOT NULL, r INT NOT NULL, \
           PRIMARY KEY (plan, k)) PARTITION BY LIST (plan)",
        "CREATE TABLE x2_t_p1 PARTITION OF x2_t FOR VALUES IN (1)",
        "CREATE TABLE x2_t_p2 PARTITION OF x2_t FOR VALUES IN (2)",
        "INSERT INTO x2_t VALUES (1, 1, 11), (1, 2, 12), (2, 3, 23), (2, 4, 24)",
        "CREATE TABLE zt (plan INT PRIMARY KEY, z INT NOT NULL)",
        "INSERT INTO zt SELECT p, p * 3 FROM generate_series(1, 5) p",
        "CREATE TABLE gs (d DATE NOT NULL, k INT NOT NULL, q INT NOT NULL, PRIMARY KEY (d, k)) \
           PARTITION BY RANGE (d)",
        "CREATE TABLE gs_m01 PARTITION OF gs FOR VALUES FROM ('2027-01-01') TO ('2027-02-01')",
        "CREATE TABLE gs_m02 PARTITION OF gs FOR VALUES FROM ('2027-02-01') TO ('2027-03-01')",
        "INSERT INTO gs SELECT DATE '2027-01-01' + day, k, day * 10 + k \
           FROM generate_series(0, 3) day, generate_series(1, 3) k",
        "INSERT INTO gs SELECT DATE '2027-02-01' + day, k, 500 + day * 10 + k \
           FROM generate_series(0, 3) day, generate_series(1, 3) k",
        "CREATE TABLE ls (plan INT NOT NULL, k INT NOT NULL, q INT NOT NULL, PRIMARY KEY (plan, k)) \
           PARTITION BY LIST (plan)",
        "CREATE TABLE ls_a PARTITION OF ls FOR VALUES IN (1, 2)",
        "CREATE TABLE ls_b PARTITION OF ls FOR VALUES IN (3)",
        "INSERT INTO ls SELECT p, k, p * 100 + k FROM generate_series(1, 3) p, generate_series(1, 4) k",
        "CREATE TABLE ns (plan INT, k INT NOT NULL, q INT NOT NULL) PARTITION BY LIST (plan)",
        "CREATE TABLE ns_a PARTITION OF ns FOR VALUES IN (1)",
        "CREATE TABLE ns_n PARTITION OF ns FOR VALUES IN (NULL, 5)",
        "INSERT INTO ns SELECT p, k, COALESCE(p, 9) * 100 + k \
           FROM (VALUES (1), (5), (NULL)) AS plans(p), generate_series(1, 3) k",
        "CREATE TABLE ms (plan INT NOT NULL, d DATE NOT NULL, k INT NOT NULL, q INT NOT NULL, \
           PRIMARY KEY (plan, k, d)) PARTITION BY LIST (plan)",
        "CREATE TABLE mo (k INT PRIMARY KEY, d DATE NOT NULL)",
        "INSERT INTO mo VALUES (1, '2027-02-10'), (2, '2027-01-10'), (3, '2027-02-11')",
        "CREATE TABLE zk (k INT PRIMARY KEY, z INT NOT NULL)",
        "INSERT INTO zk SELECT k, k * 11 FROM unnest(ARRAY[1, 2, 3, 4, 9]) k",
        "CREATE TABLE ns2 (plan INT, d DATE NOT NULL, k INT NOT NULL, q INT NOT NULL) \
           PARTITION BY LIST (plan)",
        "CREATE TABLE ns2_a PARTITION OF ns2 FOR VALUES IN (1) PARTITION BY RANGE (d)",
        "CREATE TABLE ns2_a_m01 PARTITION OF ns2_a FOR VALUES FROM ('2027-01-01') TO ('2027-02-01')",
        "CREATE TABLE ns2_n PARTITION OF ns2 FOR VALUES IN (NULL, 5) PARTITION BY RANGE (d)",
        "CREATE TABLE ns2_n_m01 PARTITION OF ns2_n FOR VALUES FROM ('2027-01-01') TO ('2027-02-01')",
        "INSERT INTO ns2 SELECT p, DATE '2027-01-05', k, COALESCE(p, 9) * 100 + k \
           FROM (VALUES (1), (5), (NULL)) AS plans(p), generate_series(1, 3) k",
        "CREATE TABLE ts (d DATE NOT NULL, k INT NOT NULL, q INT NOT NULL, PRIMARY KEY (d, k)) \
           PARTITION BY RANGE (d)",
        "CREATE TABLE ts_m01 PARTITION OF ts FOR VALUES FROM ('2027-01-01') TO ('2027-02-01') \
           PARTITION BY LIST (k)",
        "CREATE TABLE ts_m01_k1 PARTITION OF ts_m01 FOR VALUES IN (1)",
        "CREATE TABLE ts_m01_k2 PARTITION OF ts_m01 FOR VALUES IN (2)",
        "INSERT INTO ts SELECT DATE '2027-01-01' + day, k, day * 10 + k \
           FROM generate_series(0, 3) day, generate_series(1, 2) k",
        "CREATE TABLE zmap (src_plan INT NOT NULL, plan INT NOT NULL, PRIMARY KEY (src_plan, plan))",
        "INSERT INTO zmap VALUES (1, 1), (1, 2)",
        "CREATE TABLE tn_s (c NUMERIC NOT NULL, k INT NOT NULL, q INT NOT NULL, PRIMARY KEY (c, k)) \
           PARTITION BY LIST (c)",
        "CREATE TABLE tn_s_p1 PARTITION OF tn_s FOR VALUES IN (1.0)",
        "CREATE TABLE tn_s_p2 PARTITION OF tn_s FOR VALUES IN (2.0)",
        "INSERT INTO tn_s SELECT p + 0.0, k, p * 100 + k FROM generate_series(1, 2) p, generate_series(1, 4) k",
        "CREATE TABLE tn_a (c INT NOT NULL, k INT NOT NULL, w INT NOT NULL, PRIMARY KEY (c, k)) \
           PARTITION BY LIST (c)",
        "CREATE TABLE tn_a_p1 PARTITION OF tn_a FOR VALUES IN (1)",
        "CREATE TABLE tn_a_p2 PARTITION OF tn_a FOR VALUES IN (2)",
        "INSERT INTO tn_a SELECT p, k, p + k FROM generate_series(1, 2) p, generate_series(1, 4) k",
        "CREATE TABLE tt_s (c VARCHAR(5) NOT NULL, k INT NOT NULL, q INT NOT NULL, PRIMARY KEY (c, k)) \
           PARTITION BY LIST (c)",
        "CREATE TABLE tt_s_a PARTITION OF tt_s FOR VALUES IN ('a ')",
        "CREATE TABLE tt_s_b PARTITION OF tt_s FOR VALUES IN ('b')",
        "INSERT INTO tt_s SELECT c, k, k * 7 FROM unnest(ARRAY['a ', 'b']) c, generate_series(1, 4) k",
        "CREATE TABLE tt_a (c TEXT NOT NULL, k INT NOT NULL, w INT NOT NULL, PRIMARY KEY (c, k)) \
           PARTITION BY LIST (c)",
        "CREATE TABLE tt_a_a PARTITION OF tt_a FOR VALUES IN ('a ')",
        "CREATE TABLE tt_a_b PARTITION OF tt_a FOR VALUES IN ('b')",
        "INSERT INTO tt_a SELECT c, k, k FROM unnest(ARRAY['a ', 'b']) c, generate_series(1, 4) k",
        "CREATE TABLE tz_s (c TIMESTAMP NOT NULL, k INT NOT NULL, q INT NOT NULL, PRIMARY KEY (c, k)) \
           PARTITION BY LIST (c)",
        "CREATE TABLE tz_s_1 PARTITION OF tz_s FOR VALUES IN ('2027-01-01 00:00')",
        "CREATE TABLE tz_s_2 PARTITION OF tz_s FOR VALUES IN ('2027-02-01 00:00')",
        "INSERT INTO tz_s SELECT c, k, k * 3 FROM unnest(ARRAY['2027-01-01'::timestamp, '2027-02-01']) c, \
           generate_series(1, 4) k",
        "CREATE TABLE tz_a (c TIMESTAMPTZ NOT NULL, k INT NOT NULL, w INT NOT NULL, PRIMARY KEY (c, k)) \
           PARTITION BY LIST (c)",
        "CREATE TABLE tz_a_1 PARTITION OF tz_a FOR VALUES IN ('2027-01-01 00:00')",
        "CREATE TABLE tz_a_2 PARTITION OF tz_a FOR VALUES IN ('2027-02-01 00:00')",
        "INSERT INTO tz_a SELECT c, k, k FROM unnest(ARRAY['2027-01-01'::timestamptz, '2027-02-01']) c, \
           generate_series(1, 4) k",
    ]
    .iter()
    .map(|ddl| ddl.to_string())
    .collect();
    for plan in 1..=2 {
        setup.push(format!(
            "CREATE TABLE ms_p{plan} PARTITION OF ms FOR VALUES IN ({plan}) PARTITION BY RANGE (d)"
        ));
        for month in 1..=2 {
            let (lo, hi) = sx_month_bounds(month);
            setup.push(format!(
                "CREATE TABLE ms_p{plan}_m{month:02} PARTITION OF ms_p{plan} \
                 FOR VALUES FROM ('{lo}') TO ('{hi}')"
            ));
            setup.push(format!("INSERT INTO ms {}", sx_ms_rows(plan, month, 0, 0)));
        }
    }
    for ddl in &setup {
        worker_exec(ddl);
    }
    for imv in SX_IMVS {
        let optional = |text: &str| {
            if text.is_empty() {
                "NULL".to_string()
            } else {
                sql_lit(text)
            }
        };
        let partition_by: Vec<String> = imv.partition_by.iter().map(|column| sql_lit(column)).collect();
        let call = format!(
            "create_reflex_ivm({}, {}, {}, NULL, {}, {}, ARRAY[{}]::text[])",
            sql_lit(imv.name),
            sql_lit(&sx_query(imv, false)),
            optional(imv.keys),
            sql_lit(mode),
            optional(imv.ignore),
            partition_by.join(", ")
        );
        assert_eq!(rbc_select(&call), "CREATE REFLEX INCREMENTAL VIEW", "{call}");
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
        "SET log_min_messages = notice",
    ] {
        worker_exec(ddl);
    }
    assert_eq!(rbc_select("tests.sc_keep_notices()"), "true");
}

fn sx_wrong() -> Vec<String> {
    SX_IMVS
        .iter()
        .filter_map(|imv| {
            let mismatches = rbc_mismatch(imv.name, &sx_query(imv, true));
            (mismatches != 0).then(|| format!("{}: {mismatches} mismatches", imv.name))
        })
        .collect()
}

/// A flat leaf replaced the way the application does it.
fn sx_swap(root: &str, leaf: &str, bound: &str, rows: &str) -> String {
    format!(
        "CREATE TABLE {leaf}_swap (LIKE {root} INCLUDING DEFAULTS); \
         INSERT INTO {leaf}_swap {rows}; \
         ALTER TABLE {root} DETACH PARTITION {leaf}; \
         ALTER TABLE {root} ATTACH PARTITION {leaf}_swap {bound}; \
         DROP TABLE {leaf}; \
         ALTER TABLE {leaf}_swap RENAME TO {leaf}"
    )
}

fn sx_swap_month(plan: i32, month: i32, rows: &str) -> String {
    let (lo, hi) = sx_month_bounds(month);
    sx_swap(
        &format!("ms_p{plan}"),
        &format!("ms_p{plan}_m{month:02}"),
        &format!("FOR VALUES FROM ('{lo}') TO ('{hi}')"),
        rows,
    )
}

struct SxStep {
    label: &'static str,
    statements: String,
    /// `(node, kind)` refreshes the step must run exactly once each.
    refreshed: &'static [(&'static str, &'static str)],
    /// A text one of the step's NOTICEs must hold.
    notice: Option<&'static str>,
    /// Nodes an IMMEDIATE chain refreshes exactly once, of any kind.
    once_when_immediate: &'static [&'static str],
}

fn sx_steps() -> Vec<SxStep> {
    let gs_m01 = "FOR VALUES FROM ('2027-01-01') TO ('2027-02-01')";
    let gs_m02 = "FOR VALUES FROM ('2027-02-01') TO ('2027-03-01')";
    let step = |label, statements: String| SxStep {
        label,
        statements,
        refreshed: &[],
        notice: None,
        once_when_immediate: &[],
    };
    vec![
        SxStep {
            refreshed: &[
                ("yd", "full"),
                ("yd_obs", "full"),
                ("yg", "full"),
                ("yp", "partitions"),
                ("y_loose", "full"),
                ("y_beside", "full"),
                ("y_two_full", "full"),
            ],
            notice: Some(
                "pg_reflex: 'yd' is refreshed whole, not by partition, after the partition rebuild \
                 of 'yv': partition 'yv_ys_def' was created or altered in this transaction and its \
                 bound (DEFAULT) does not list the \"plan\" values its rows held",
            ),
            ..step(
                "default leaf changed",
                sx_swap("ys", "ys_def", "DEFAULT", "SELECT plan, k, q + 1000 FROM ys_def"),
            )
        },
        step(
            "default leaf loses a plan and gains one",
            sx_swap(
                "ys",
                "ys_def",
                "DEFAULT",
                "SELECT CASE WHEN plan = 3 THEN 4 ELSE plan END, k, q + 7 FROM ys_def",
            ),
        ),
        SxStep {
            refreshed: &[
                ("yd", "partitions"),
                ("yd_obs", "partitions"),
                ("yp", "partitions"),
                ("yr_region", "full"),
            ],
            ..step(
                "listed leaf changed",
                sx_swap("ys", "ys_p1", "FOR VALUES IN (1)", "SELECT plan, k, q + 50 FROM ys_p1"),
            )
        },
        step(
            "listed leaf gains a key",
            sx_swap(
                "ys",
                "ys_p1",
                "FOR VALUES IN (1)",
                "SELECT plan, k, q + 1 FROM ys_p1 UNION ALL SELECT 1, 9, 109",
            ),
        ),
        SxStep {
            refreshed: &[("yx", "full")],
            notice: Some(
                "pg_reflex: 'yx' is refreshed whole, not by partition, after the partition \
                 rebuild of 'yv': its partition-scoped refresh returned: reconcile_partition: \
                 no children matched",
            ),
            ..step(
                "leaf of a plan the other root has no partition for",
                sx_swap("ys", "ys_p5", "FOR VALUES IN (5)", "SELECT plan, k, q + 9 FROM ys_p5"),
            )
        },
        step(
            "range leaf with other days",
            sx_swap(
                "gs",
                "gs_m01",
                gs_m01,
                "SELECT DATE '2027-01-01' + day, k, 900 + day * 10 + k \
                   FROM generate_series(2, 5) day, generate_series(1, 3) k",
            ),
        ),
        step(
            "one range leaf emptied, another changed",
            format!(
                "{}; {}",
                sx_swap("gs", "gs_m01", gs_m01, "SELECT * FROM gs WHERE FALSE"),
                sx_swap("gs", "gs_m02", gs_m02, "SELECT d, k, q + 1 FROM gs_m02")
            ),
        ),
        SxStep {
            refreshed: &[("gg", "full")],
            ..step(
                "range leaf refilled",
                sx_swap(
                    "gs",
                    "gs_m01",
                    gs_m01,
                    "SELECT DATE '2027-01-01' + day, k, 40 + day * 10 + k \
                       FROM generate_series(0, 3) day, generate_series(1, 3) k",
                ),
            )
        },
        SxStep {
            refreshed: &[("lg", "groups")],
            ..step(
                "two-value leaf loses a plan",
                sx_swap(
                    "ls",
                    "ls_a",
                    "FOR VALUES IN (1, 2)",
                    "SELECT plan, k, q + 5 FROM ls_a WHERE plan = 1",
                ),
            )
        },
        step(
            "two-value leaf swaps its plan",
            sx_swap("ls", "ls_a", "FOR VALUES IN (1, 2)", "SELECT 2, k, q + 9 FROM ls_a"),
        ),
        SxStep {
            refreshed: &[("ng", "full")],
            notice: Some("pg_reflex: 'ng' is refreshed whole, not by partition, after the partition rebuild of 'nv': "),
            ..step(
                "leaf holding the null key changed",
                sx_swap("ns", "ns_n", "FOR VALUES IN (NULL, 5)", "SELECT plan, k, q + 3 FROM ns_n"),
            )
        },
        SxStep {
            refreshed: &[("ng2", "full")],
            notice: Some("holds rows whose \"plan\" is NULL"),
            ..step(
                "month holding the null key changed",
                sx_swap(
                    "ns2_n",
                    "ns2_n_m01",
                    "FOR VALUES FROM ('2027-01-01') TO ('2027-02-01')",
                    "SELECT plan, d, k, q + 3 FROM ns2_n_m01",
                ),
            )
        },
        SxStep {
            refreshed: &[("tg", "groups")],
            ..step(
                "list leaf below a month, with other days",
                sx_swap(
                    "ts_m01",
                    "ts_m01_k1",
                    "FOR VALUES IN (1)",
                    "SELECT DATE '2027-01-01' + day, 1, 700 + day FROM generate_series(2, 5) day",
                ),
            )
        },
        SxStep {
            refreshed: &[
                ("mg", "groups"),
                ("m_pd", "partitions"),
                ("m_other", "partitions"),
                ("m_scalar", "full"),
                ("m_above_mean", "full"),
                ("m_map", "full"),
                ("m_rename", "full"),
                ("zz_tot", "partitions"),
                ("m_with_total", "partitions"),
            ],
            once_when_immediate: &["m_two_paths"],
            ..step("one month changed", sx_swap_month(1, 1, &sx_ms_rows(1, 1, 0, 7)))
        },
        step(
            "a month of one plan emptied, a month of another changed",
            format!(
                "{}; {}",
                sx_swap_month(1, 2, "SELECT * FROM ms WHERE FALSE"),
                sx_swap_month(2, 1, &sx_ms_rows(2, 1, 0, 9))
            ),
        ),
        step("a month with other days", sx_swap_month(2, 2, &sx_ms_rows(2, 2, 5, 1))),
        step(
            "one month, explicit flush",
            format!(
                "{}; SELECT reflex_flush_partition_source('ms')",
                sx_swap_month(1, 1, &sx_ms_rows(1, 1, 3, 2))
            ),
        ),
        step(
            "new month attached with rows",
            "CREATE TABLE ms_p1_m03 (LIKE ms_p1 INCLUDING DEFAULTS); \
             INSERT INTO ms_p1_m03 SELECT 1, DATE '2027-03-01' + day, k, 7000 + k * 10 + day \
               FROM generate_series(0, 2) day, generate_series(1, 3) k; \
             ALTER TABLE ms_p1 ATTACH PARTITION ms_p1_m03 \
               FOR VALUES FROM ('2027-03-01') TO ('2027-04-01')"
                .to_string(),
        ),
        step(
            "a plan emptied entirely and another changed",
            format!(
                "{}; {}; {}",
                sx_swap_month(1, 1, "SELECT * FROM ms WHERE FALSE"),
                sx_swap_month(1, 2, "SELECT * FROM ms WHERE FALSE"),
                sx_swap_month(2, 1, &sx_ms_rows(2, 1, 1, 4))
            ),
        ),
        SxStep {
            refreshed: &[("tn_d", "full")],
            ..step(
                "numeric key into an integer partition column",
                sx_swap("tn_s", "tn_s_p1", "FOR VALUES IN (1.0)", "SELECT c, k, q + 11 FROM tn_s_p1"),
            )
        },
        SxStep {
            refreshed: &[("tt_d", "full")],
            ..step(
                "varchar key with a trailing space into a text partition column",
                sx_swap("tt_s", "tt_s_a", "FOR VALUES IN ('a ')", "SELECT c, k, q + 1 FROM tt_s_a"),
            )
        },
        SxStep {
            refreshed: &[("tz_d", "full")],
            ..step(
                "timestamp key into a timestamptz partition column",
                sx_swap(
                    "tz_s",
                    "tz_s_1",
                    "FOR VALUES IN ('2027-01-01 00:00')",
                    "SELECT c, k, q + 1 FROM tz_s_1",
                ),
            )
        },
        step(
            "listed leaf swapped for rows of other keys",
            sx_swap("ys", "ys_p1", "FOR VALUES IN (1)", "SELECT plan, k + 100, q FROM ys_p1"),
        ),
        step(
            "listed leaf swapped for an empty table",
            sx_swap("ys", "ys_p1", "FOR VALUES IN (1)", "SELECT * FROM ys WHERE FALSE"),
        ),
        step(
            "default leaf swapped for an empty table",
            sx_swap("ys", "ys_def", "DEFAULT", "SELECT * FROM ys WHERE FALSE"),
        ),
        step(
            "leaf with rows detached and dropped",
            "ALTER TABLE ys DETACH PARTITION ys_p5; DROP TABLE ys_p5".to_string(),
        ),
    ]
}

fn sx_assert_shapes_stay_correct(dbname: &str, mode: &str) {
    sx_open(dbname, mode);
    let chain = ScChain { field_names: false, cross_plan: false };
    let mut failures = sx_wrong()
        .into_iter()
        .map(|wrong| format!("after create: {wrong}"))
        .collect::<Vec<_>>();
    for step in sx_steps() {
        let label = step.label;
        let _ = sc_text_rows("SELECT unnest(tests.sc_kept_notices())");
        if let Err(error) = chain.commit(&step.statements) {
            failures.push(format!("{label}: raised {error}"));
            continue;
        }
        let refreshes = chain.refreshes();
        for (node, kind) in step.refreshed {
            let kinds: Vec<&str> = refreshes
                .iter()
                .filter(|(refreshed, _)| refreshed == node)
                .map(|(_, kind)| kind.as_str())
                .collect();
            if kinds != [*kind] {
                failures.push(format!("{label}: {node} refreshed {kinds:?}, not once by {kind}"));
            }
        }
        if mode == "IMMEDIATE" {
            for node in step.once_when_immediate {
                let times = refreshes.iter().filter(|(refreshed, _)| refreshed == node).count();
                if times != 1 {
                    failures.push(format!("{label}: {node} refreshed {times} times, not once"));
                }
            }
        }
        if let Some(expected) = step.notice {
            let notices = sc_text_rows("SELECT unnest(tests.sc_kept_notices())");
            if !notices.iter().any(|notice| notice.contains(expected)) {
                let whole: Vec<&String> =
                    notices.iter().filter(|notice| notice.contains("refreshed whole")).collect();
                failures.push(format!("{label}: no NOTICE <{expected}> in {whole:?}"));
            }
        }
        for wrong in sx_wrong() {
            let (name, _) = wrong.split_once(':').expect("name: mismatches");
            let repaired = rbc_select(&format!("reflex_reconcile('{name}')"));
            failures.push(format!("{label}: {wrong} (then {repaired})"));
        }
    }
    for (view, partition) in [("nd", "x_t_r1"), ("nd2", "x2_t_p1")] {
        let refusal =
            rbc_select(&format!("reflex_reconcile_partition('{view}', '', '{partition}')"));
        if !(refusal.starts_with("ERROR") && refusal.contains("is not a partition of")) {
            failures.push(format!("{view} by {partition}: {refusal}"));
        }
    }
    for wrong in sx_wrong() {
        failures.push(format!("after the refused calls: {wrong}"));
    }
    probe_db_close(dbname);
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

#[pg_test]
fn v112_sc_shapes_stay_correct_immediate() {
    sx_assert_shapes_stay_correct("reflex_sx_i", "IMMEDIATE");
}

#[pg_test]
fn v112_sc_shapes_stay_correct_deferred() {
    sx_assert_shapes_stay_correct("reflex_sx_d", "DEFERRED");
}

/// A partition reconcile run from inside a statement's trigger (here a user
/// trigger; the hot-partition dispatch of an IMMEDIATE IMV is the internal one)
/// reaches a DEFERRED aggregate by key. That refresh cannot run while the
/// statement may still stage a delta, so it is left to the COMMIT-time
/// reconcile, which must find it listed.
#[pg_test]
fn v112_sc_group_refresh_inside_a_trigger_is_left_to_commit() {
    probe_db_open("reflex_sc_hot");
    let chain = ScChain { field_names: false, cross_plan: false };
    for ddl in [
        "CREATE TABLE hs (id INT NOT NULL, region TEXT NOT NULL, amount INT NOT NULL) \
           PARTITION BY LIST (region)",
        "CREATE TABLE hs_a PARTITION OF hs FOR VALUES IN ('A')",
        "CREATE TABLE hs_b PARTITION OF hs FOR VALUES IN ('B')",
        "INSERT INTO hs SELECT g, r, g FROM unnest(ARRAY['A', 'B']) r, generate_series(1, 20) g",
        "CREATE TABLE zr (r TEXT PRIMARY KEY, w INT NOT NULL)",
        "INSERT INTO zr VALUES ('A', 1), ('B', 2)",
        "CREATE TABLE kick (n INT)",
        "CREATE TABLE sc_trace (seq SERIAL, name TEXT NOT NULL, context TEXT NOT NULL)",
        "CREATE TABLE sc_xact (id TEXT NOT NULL)",
    ] {
        worker_exec(ddl);
    }
    for call in [
        "create_reflex_ivm('hv', 'SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM hs \
           GROUP BY region', NULL, NULL, 'IMMEDIATE', NULL, ARRAY['region'])",
        "create_reflex_ivm('hg', 'SELECT v.region, SUM(v.total) AS t, SUM(z.w) AS sw \
           FROM hv AS v JOIN zr AS z ON z.r = v.region GROUP BY region', \
           NULL, NULL, 'DEFERRED', '!hv', ARRAY[]::text[])",
    ] {
        assert_eq!(rbc_select(call), "CREATE REFLEX INCREMENTAL VIEW", "{call}");
    }
    for ddl in [
        "CREATE FUNCTION kick_reconcile() RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
           BEGIN PERFORM public.reflex_reconcile_partition('hv', 'A'); RETURN NULL; END $$",
        "CREATE TRIGGER kick_reconcile AFTER INSERT ON kick \
           FOR EACH STATEMENT EXECUTE FUNCTION kick_reconcile()",
        "CREATE FUNCTION sc_note_refresh() RETURNS TRIGGER LANGUAGE plpgsql AS $$ \
           DECLARE stack TEXT; BEGIN GET DIAGNOSTICS stack = PG_CONTEXT; \
             INSERT INTO sc_trace (name, context) VALUES (NEW.name, stack); RETURN NULL; END $$",
        "CREATE TRIGGER sc_note_refresh AFTER UPDATE OF last_update_date \
           ON public.__reflex_ivm_reference FOR EACH ROW EXECUTE FUNCTION sc_note_refresh()",
    ] {
        worker_exec(ddl);
    }
    let outcome = chain
        .commit("UPDATE hs SET amount = amount + 7 WHERE region = 'A'; INSERT INTO kick VALUES (1)");
    let refreshes = chain.refreshes();
    let mismatches = rbc_mismatch(
        "hg",
        "SELECT v.region, SUM(v.total) AS t, SUM(z.w) AS sw FROM \
           (SELECT region, SUM(amount) AS total FROM hs GROUP BY region) AS v \
           JOIN zr AS z ON z.r = v.region GROUP BY region",
    );
    probe_db_close("reflex_sc_hot");
    assert_eq!(outcome, Ok(()));
    assert!(
        refreshes.iter().any(|(node, kind)| node == "hg" && kind == "full"),
        "the key refresh of hg is left to the COMMIT-time reconcile: {refreshes:?}"
    );
    assert_eq!(mismatches, 0, "hg");
}

/// Detaching source partitions removes the rows of the IMVs that mirror them;
/// the IMVs reading those lose the rows too, whether they observe them or are
/// refreshed for them.
fn sc_assert_detached_rows_reach_every_reader(dbname: &str, mode: &str) {
    probe_db_open(dbname);
    let chain = ScChain { field_names: false, cross_plan: false };
    for ddl in [
        "CREATE TABLE s (id INT NOT NULL, g TEXT NOT NULL, v INT) PARTITION BY LIST (g)",
        "CREATE TABLE s_pa PARTITION OF s FOR VALUES IN ('a', 'b')",
        "CREATE TABLE s_pc PARTITION OF s FOR VALUES IN ('c')",
        "CREATE TABLE s_pd PARTITION OF s FOR VALUES IN ('d')",
        "CREATE TABLE s_pe PARTITION OF s FOR VALUES IN ('e')",
        "INSERT INTO s VALUES (1,'a',1),(2,'a',2),(3,'b',5),(4,'c',7),(5,'d',9),(6,'e',11)",
        "CREATE TABLE sc_trace (seq SERIAL, name TEXT NOT NULL, context TEXT NOT NULL)",
        "CREATE TABLE sc_xact (id TEXT NOT NULL)",
    ] {
        worker_exec(ddl);
    }
    let mode = sql_lit(mode);
    let imvs = [
        ("agg", "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM s GROUP BY g", "NULL", "NULL", "ARRAY['g']"),
        ("keyed", "SELECT id, g, v FROM s", "'id, g'", "NULL", "ARRAY['g']"),
        ("dep_agg", "SELECT COUNT(*) AS n, SUM(sv) AS t FROM agg", "NULL", "NULL", "ARRAY[]::text[]"),
        ("dep_keyed", "SELECT g, COUNT(*) AS c FROM keyed GROUP BY g", "NULL", "NULL", "ARRAY[]::text[]"),
        ("ign_keyed", "SELECT g, COUNT(*) AS c FROM keyed GROUP BY g", "NULL", "'!keyed'", "ARRAY[]::text[]"),
    ];
    for (name, sql, keys, ignore, partition_by) in imvs {
        let call = format!(
            "create_reflex_ivm('{name}', {}, {keys}, NULL, {mode}, {ignore}, {partition_by})",
            sql_lit(sql)
        );
        assert_eq!(rbc_select(&call), "CREATE REFLEX INCREMENTAL VIEW", "{call}");
    }
    let fresh = |name: &str| match name {
        "agg" => "SELECT g, COUNT(*) AS c, SUM(v) AS sv FROM s GROUP BY g".to_string(),
        "keyed" => "SELECT id, g, v FROM s".to_string(),
        "dep_agg" => {
            "SELECT COUNT(*) AS n, SUM(sv) AS t FROM (SELECT SUM(v) AS sv FROM s GROUP BY g) x"
                .to_string()
        }
        _ => "SELECT g, COUNT(*) AS c FROM s GROUP BY g".to_string(),
    };
    let mut failures = Vec::new();
    // ign_keyed is told nothing of a write to keyed, by its ignore list: the
    // step that writes is checked on the IMVs that observe the write.
    let observing_the_write = |name: &&str| *name != "ign_keyed";
    for (label, statements, write) in [
        ("detach", "ALTER TABLE s DETACH PARTITION s_pa", false),
        ("detach and drop", "ALTER TABLE s DETACH PARTITION s_pc; DROP TABLE s_pc", false),
        ("detach and write", "ALTER TABLE s DETACH PARTITION s_pd; INSERT INTO s VALUES (7, 'e', 13)", true),
        ("two detaches", "ALTER TABLE s DETACH PARTITION s_pe; ALTER TABLE s ATTACH PARTITION s_pa FOR VALUES IN ('a', 'b'); ALTER TABLE s DETACH PARTITION s_pa", false),
    ] {
        if let Err(error) = chain.commit(statements) {
            failures.push(format!("{label}: raised {error}"));
            continue;
        }
        for name in imvs.map(|(name, ..)| name).iter().filter(|name| !write || observing_the_write(name)) {
            let mismatches = rbc_mismatch(name, &fresh(name));
            if mismatches != 0 {
                failures.push(format!(
                    "{label}: {name}: {mismatches} mismatches; holds {:?}, query {:?}",
                    sc_text_rows(&format!("SELECT to_jsonb(t)::text AS v FROM {name} t ORDER BY 1")),
                    sc_text_rows(&format!(
                        "SELECT to_jsonb(t)::text AS v FROM ({}) t ORDER BY 1",
                        fresh(name)
                    )),
                ));
                rbc_select(&format!("reflex_reconcile('{name}')"));
            }
        }
        if write {
            rbc_select("reflex_reconcile('ign_keyed')");
        }
    }
    probe_db_close(dbname);
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

#[pg_test]
fn v112_sc_detached_rows_reach_every_reader_immediate() {
    sc_assert_detached_rows_reach_every_reader("reflex_sc_detach_i", "IMMEDIATE");
}

#[pg_test]
fn v112_sc_detached_rows_reach_every_reader_deferred() {
    sc_assert_detached_rows_reach_every_reader("reflex_sc_detach_d", "DEFERRED");
}
