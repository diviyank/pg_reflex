# pg_reflex production-readiness rollout plan

## Context

`journal/audit-production-readiness.md` (2026-04-24) verdicted pg_reflex 1.2.0 as
**ready for controlled production use** but **not yet a drop-in REFRESH replacement**.
It enumerated 8 risks (R1-R8) and a prioritised follow-up list:

1. R1 — source DROP orphans intermediate/target tables.
2. R3 — MIN/MAX retraction cliff (top-K heap deferred from 1.2.0).
3. R7 — no automated drift detection.
4. R2 — source ALTER TABLE warns but does not block.
5. R5 — passthrough `unique_columns` should be inferable from source PK.

The audit also flags R6 (no `pg_stat_statements` integration) and a documentation gap:
README is API-focused, with no troubleshooting runbook, no per-shape SLO table, and
no published 1.2.0 perf numbers. The `discussion*.md` and `journal/` files hold
contributor-grade design history but are not operator-friendly.

Goal of this plan:

- **1.2.0 (unreleased)** — fold in the smallest audit follow-up that complements 1.2.0's
  operational-safety theme (R1) without expanding scope.
- **1.2.1 (patch)** — close the remaining ergonomics/ops items (R2, R5, R7)
  with low-risk code + docs.
- **1.3.0 (feature)** — close the headline perf gap (R3 top-K) and observability gap
  (R6 histogram).
- **Documentation site** — replace the single README with a Material-for-MkDocs site
  that operators, integrators and contributors can each navigate to the slice they need.
  Color palette: `#FCFCFC` / `#4169E1` / `#001556` / `#121212`.

The audit is the authoritative spec; this plan translates it into concrete file-level work.

---

## 1.2.0 (unreleased) — fold-in scope

Tag: ship as part of `v1.2.0` (already in flight). The 1.2.0 theme is "operational
safety + observability + scoped MIN/MAX recompute". R1 is the only audit item
that fits cleanly without scope creep — it is small (~50 lines of plpgsql in an
event trigger), zero schema migration, and directly extends the existing
`reflex_on_sql_drop` event trigger.

### 1.2.0-A — Auto-drop IMV artifacts on source DROP (R1)

**Today** (`src/lib.rs:171-190`): `__reflex_on_sql_drop` deletes registry rows but leaves
the intermediate (`__reflex_intermediate_<view>`), affected-groups
(`__reflex_affected_<view>`), and target (`<view>`) tables in the schema.

**Change**: extend the event-trigger body to:

1. For each registry row matched by the drop, compute the artifact set
   (intermediate name, affected-groups name, target name, sub-IMV children if cascade
   marker present).
2. Issue `DROP TABLE IF EXISTS … CASCADE` on each artifact inside the event-trigger
   transaction so the DROP is atomic with the source DROP.
3. Cascade through `graph_child` so passthrough sub-IMVs and CTE-derived
   sub-IMVs go too.
4. Emit `RAISE NOTICE` listing every artifact dropped, so operators see exactly what
   happened in the same transaction.

**Files**:

- `src/lib.rs:169-224` — rewrite the `extension_sql!("pg_reflex_event_trigger", …)`
  body. Reuse the `bare_name` helper pattern from `src/introspect.rs:255` to compute
  intermediate / affected names from `name`. Skip orphans where the registry row is
  for a sub-IMV the parent will already cascade-drop (read `unlogged_tables` column
  for the precise artifact list — already populated by `create_ivm`).
- `sql/pg_reflex--1.1.3--1.2.0.sql` — append a `CREATE OR REPLACE FUNCTION` block
  for `public.__reflex_on_sql_drop` so existing 1.1.3 installations pick up the
  new body on `ALTER EXTENSION pg_reflex UPDATE TO '1.2.0'`. The event trigger
  itself was created in 1.2.0 (so no prior version exists).
- `src/tests/pg_test_drop.rs` — add 3 tests:
  1. `pg_test_source_drop_removes_aggregate_imv_artifacts` — create IMV, drop source,
     assert intermediate and target tables are gone via `pg_class`.
  2. `pg_test_source_drop_cascades_to_child_imvs` — L1→L2 chain, drop L1's source,
     assert both targets gone.
  3. `pg_test_source_drop_passthrough` — passthrough IMV (no intermediate), drop source,
     assert target gone.

**Risk**: Auto-dropping user tables from inside an event trigger has historically
been risky (the audit cites this as the reason it was deferred). Mitigation:
restrict the DROP set strictly to artifacts whose names appear in
`__reflex_ivm_reference.unlogged_tables` (set at create-time by pg_reflex), never to
arbitrary names. A `RAISE NOTICE` per drop is the audit trail.

### 1.2.0-B — README: add a "Troubleshooting" stub

**Why now**: 1.2.0 is the first release that makes `last_error`/`flush_count` visible.
Operators need a one-page runbook. The full docs site (later in this plan) will
supersede this, but shipping 1.2.0 with no troubleshooting guide leaves an obvious gap.

**Files**:

- `README.md` — add a "Troubleshooting" section after "Operational notes":
  - "Flush keeps failing on one IMV" → check `SELECT name, last_error FROM
    __reflex_ivm_reference WHERE last_error IS NOT NULL`, then `reflex_explain_flush(name)`.
  - "IMV drifted after a crash" → `SELECT reflex_rebuild_imv(name)`.
  - "Source ALTER TABLE warning" → `SELECT reflex_rebuild_imv(name)`.
  - "Cascade is slow" → check `graph_depth` and `last_flush_ms` per IMV.

The full docs site will lift this content into a longer runbook; the README stub is the
1.2.0 stopgap.

### 1.2.0 verification

- `cargo pgrx test pg17` — all 487 tests + 3 new drop tests pass.
- Manual: in a scratch DB, `CREATE TABLE src (...); SELECT create_reflex_ivm('v', 'SELECT a, sum(b) FROM src GROUP BY a'); DROP TABLE src;` — assert
  `\dt __reflex_*` lists nothing for `v`.
- `cargo clippy && cargo fmt --check` clean.
- Smoke: build + install via `./install.sh`, run `ALTER EXTENSION pg_reflex UPDATE
  TO '1.2.0'` on a 1.1.3 instance, assert event trigger body is the new one.

---

## 1.2.1 (patch) — operator ergonomics

Tag: `v1.2.1`. Three small features + one docs deliverable. New migration file
`sql/pg_reflex--1.2.0--1.2.1.sql`. No behaviour change for existing IMVs.

### 1.2.1-A — `pg_reflex.alter_source_policy` GUC (R2)

**Today** (`src/lib.rs:192-220`): the `__reflex_on_ddl_command_end` event trigger
always emits a `WARNING`. There is no way to escalate to `ERROR`.

**Change**: add a session/database-scoped GUC `pg_reflex.alter_source_policy` with
two values:

- `'warn'` (default) — current behaviour (warning, ALTER proceeds).
- `'error'` — `RAISE EXCEPTION`, ALTER is rolled back inside the
  `ddl_command_end` trigger.

The GUC is read from inside the plpgsql event-trigger body via
`current_setting('pg_reflex.alter_source_policy', true)`, with `coalesce(…,
'warn')` for un-set sessions.

**Files**:

- `src/lib.rs` — register the GUC with `GucRegistry::define_string_guc(…)` in a
  `_PG_init` hook (pgrx `#[pg_guard]` extern). Update the
  `__reflex_on_ddl_command_end` body to read the GUC and dispatch.
- `sql/pg_reflex--1.2.0--1.2.1.sql` — `CREATE OR REPLACE FUNCTION` for the
  updated body.
- `src/tests/pg_test_error.rs` — 2 tests:
  1. Default `warn` → ALTER succeeds, warning emitted.
  2. `SET pg_reflex.alter_source_policy = 'error'` → ALTER rolls back with
     `pg_reflex: ALTER blocked …`.

**Risk**: an `ERROR` raised from inside `ddl_command_end` rolls back the entire
ALTER transaction — legitimate operator workflows (renames, type widenings) will
fail under `error` mode. The default stays `warn` so opt-in is explicit.

### 1.2.1-B — Infer `unique_columns` from source PK (R5)

**Today** (`src/create_ivm.rs:735-736`): passthrough IMVs that handle
DELETE/UPDATE require the operator to pass `unique_columns` explicitly. If the
source table has a single-table SELECT and a usable PK, this is unnecessary
toil.

**Change**: in `create_reflex_ivm_impl`, when:

- the query is passthrough (no aggregation),
- `unique_columns` is the empty string,
- the FROM list resolves to a single base table (not a JOIN, not a subquery),

…look up `pg_index.indisprimary = true` for that table and adopt the PK columns
as the unique key. Emit a `RAISE INFO` so the operator sees the inference
happened. If lookup fails (no PK, or table not in `pg_class`), fall through to
the existing behaviour: unique_columns left empty, with the existing
"DELETE-capable passthrough requires unique_columns" guard kicking in only if a
delete-capable trigger fires.

**Files**:

- `src/create_ivm.rs` — new `infer_unique_columns_from_pk(source_table: &str) ->
  Option<Vec<String>>` helper (~40 lines, single SPI query against
  `pg_index`/`pg_attribute`).
- `src/sql_analyzer.rs` — extend the "single-source detection" path to expose the
  base-table name (already computed for trigger generation, just plumb it
  outwards).
- `src/tests/pg_test_passthrough.rs` — 2 tests:
  1. PK present → IMV created without explicit unique_columns, DELETE works.
  2. No PK → existing error path (unique_columns required).

**Risk**: A composite PK could be huge; `RAISE INFO` makes the inference
visible at create time so operators can override. JOINs are explicitly
excluded — the current per-source key mapping logic
(`src/create_ivm.rs` JOIN-key map) is the right tool there.

### 1.2.1-C — `reflex_scheduled_reconcile()` SPI + pg_cron recipe (R7)

**Today**: drift detection requires manually running `reflex_reconcile(name)`
per IMV. The audit suggests pg_cron as the deployment vector.

**Change**: add one SPI:

- `reflex_scheduled_reconcile(max_age_minutes INTEGER DEFAULT 60) -> TABLE(name TEXT, status TEXT, ms BIGINT)`
  iterates IMVs whose `last_update_date < now() - max_age_minutes` (or `flush_count = 0`),
  reconciles each, returns one row per attempted IMV with the status.

This is a plain SPI implementation — no background worker, no plpgsql cron
dependency. Operators schedule it via pg_cron:

```sql
SELECT cron.schedule('reflex-drift-scan', '*/15 * * * *',
    'SELECT * FROM reflex_scheduled_reconcile(60)');
```

**Files**:

- `src/reconcile.rs` — new `reflex_scheduled_reconcile` `#[pg_extern]` (~60 lines).
  Reuses existing `reflex_reconcile` per IMV. Wraps each call in a savepoint so
  one bad IMV does not abort the rest.
- `sql/pg_reflex--1.2.0--1.2.1.sql` — function registration handled by pgrx
  schema generation; no manual SQL needed.
- `src/tests/pg_test_reconcile.rs` — 1 test exercising the multi-IMV path.

### 1.2.1-D — Docs: pg_cron recipe + runbook expansion

Lifts the 1.2.0 README stub into a fuller runbook plus the pg_cron recipe. This
work happens in the docs-site phase (below) — the 1.2.1 release notes link to it
rather than expanding the README further.

### 1.2.1 verification

- `cargo pgrx test pg17` — all tests + 5 new ones pass.
- Manual: `SHOW pg_reflex.alter_source_policy` returns `warn`; `SET
  pg_reflex.alter_source_policy = 'error'; ALTER TABLE src ADD COLUMN x INT;` is
  rejected.
- Manual: passthrough IMV on a PK-bearing source created without
  `unique_columns`, then DELETE on source propagates correctly.
- Manual: `SELECT * FROM reflex_scheduled_reconcile(0)` reconciles every IMV.
- Migration smoke: `ALTER EXTENSION pg_reflex UPDATE TO '1.2.1'` from `'1.2.0'`,
  no error, all SPIs callable.

---

## 1.3.0 (minor) — perf parity

Tag: `v1.3.0`. Two anchor features (R3 top-K, R6 histogram) plus the auxiliary
unlocks they enable. Migration file `sql/pg_reflex--1.2.1--1.3.0.sql`. Adds
intermediate-table columns for IMVs that opt into top-K (existing IMVs keep the
1.2.0 scoped-recompute path as the fallback).

### 1.3.0-A — Bounded top-K heap for MIN/MAX (R3)

This is the audit's "headline perf gap" and the work originally scoped for 1.2.0
then deferred (`CHANGELOG.md:28-30`). The plan adopts the bounded-K design from
`journal/2026-04-22_optimization_ideas.md:23-35`: each MIN/MAX intermediate
column gets a sibling `<col>_topk` array column holding the K smallest (MIN) or
largest (MAX) values seen for that group, with K=16 by default and configurable
per-IMV.

**Maintenance algebra**:

- INSERT: heap-insert delta values, truncate to K, update `__min_x` to
  `topk[1]` (or `topk[K]` for MAX).
- DELETE: `array_remove` the deleted value from the heap. If `array_length(heap, 1) = 0`,
  fall back to a per-group recompute (current 1.2.0 path scoped via
  `__reflex_affected_<view>` already handles this — reuse it). Otherwise update
  `__min_x` from the new heap top.

**Files**:

- `src/aggregation.rs` — new `IntermediateColumn` variant
  `MinMaxWithTopK { k: usize, kind: MinOrMax }`. Default K=16. Plumbed through
  `column_definitions()` to emit
  `__min_<expr>_topk numeric[]` (or the source element type) alongside
  `__min_<expr>`.
- `src/query_decomposer.rs` — when generating the base_query for a MIN/MAX
  aggregate, emit a CTE that builds the per-group sorted-array companion via
  `(ARRAY_AGG(x ORDER BY x ASC))[1:K]` for MIN, `[K+1-len:]` for MAX. The
  existing `MIN()` projection stays as `topk[1]`.
- `src/trigger.rs::build_min_max_recompute_sql:279` — new branch: if the IMV's
  aggregation plan declares top-K, emit the heap-based delta path; only fall
  back to the existing scoped recompute when heap underflow is detected for a
  group.
- `src/schema_builder.rs` — extend the trigger bodies to set the per-group
  recompute predicate to `WHERE EXISTS(SELECT 1 FROM <intermediate> WHERE __min_x_topk = '{}'::numeric[])`
  (heap empty after delta) — only that subset re-runs the full-scan recompute.
- `sql/pg_reflex--1.2.1--1.3.0.sql` — for existing 1.2.x MIN/MAX IMVs, emit:
  - `ALTER TABLE __reflex_intermediate_<v> ADD COLUMN __min_x_topk <type>[]`
  - `UPDATE … SET __min_x_topk = (per-group ARRAY_AGG ORDER BY x LIMIT K)` from a
    one-shot scan of source.
  - This is opt-in: a new `topk` parameter on `create_reflex_ivm` (default
    NULL=disabled to keep migration cost bounded). Existing IMVs continue with
    the scoped-recompute path until the operator opts in via
    `reflex_enable_topk(view_name, k)`.
- `src/tests/unit_proptest.rs` — add proptest: random INSERT/DELETE
  sequences with up to 5K rows per group, asserting `MIN(x)` from the IMV equals
  `MIN(x)` from a fresh `SELECT MIN(x) FROM source`.
- `src/tests/pg_test_correctness.rs` — add 4 integration tests covering
  insert-only, delete-only, mixed, and full-group-retraction (heap-empty
  fallback).

**Acceptance**: bench against the three views currently kept as plain matviews
(`stock_chart_weekly_reflex`, `stock_chart_monthly_reflex`,
`forecast_stock_chart_monthly_reflex` per
`journal/2026-04-22_unsupported_views.md:81-88`). Target: 5-50× speedup on
DELETE/UPDATE flushes (the audit's stated payoff).

**Risk**: top-K maintenance is the most invasive change in this plan. Two
mitigations:

- Default K=16 is small enough that the heap stays cheap; configurable per IMV.
- Opt-in flag means operators can stage rollout per IMV instead of taking a
  cluster-wide migration cost.

### 1.3.0-B — Scalar / no-GROUP-BY MIN/MAX via top-K (extends R3)

Once top-K is in, the scalar MIN/MAX views (`max_order_date_reflex`, category §2 of
`journal/2026-04-22_unsupported_views.md`) become representable as a single-row
intermediate with a top-K array. The work is small once §A lands: relax the
sentinel-only branch in `build_min_max_recompute_sql:312-323` to use the heap
path with one implicit group.

**Files**:

- `src/trigger.rs:312-323` — replace the sentinel-only branch.
- `src/tests/pg_test_correctness.rs` — 1 test covering scalar MIN over a 100K
  source with mixed INSERT/DELETE.

### 1.3.0-C — Per-IMV flush histogram (R6)

**Today** (`src/introspect.rs:122-198`): `reflex_ivm_stats` exposes a single
`last_flush_ms` value. Operators have no percentile distribution.

**Change**: extend `__reflex_ivm_reference` with a fixed-size ring-buffer
column `flush_ms_history BIGINT[]` (size 64) plus a derived view over the registry
that exposes `p50_flush_ms`, `p95_flush_ms`, `p99_flush_ms`, `max_flush_ms`
computed via `percentile_cont`.

**Files**:

- `sql/pg_reflex--1.2.1--1.3.0.sql` — `ALTER TABLE __reflex_ivm_reference ADD
  COLUMN flush_ms_history BIGINT[] DEFAULT '{}'`. Create
  `reflex_ivm_histogram` view computed from the array.
- `src/trigger.rs:1340-…` — in the per-IMV SAVEPOINT block that already records
  `last_flush_ms`, also append to `flush_ms_history` and trim to 64 elements.
- `src/introspect.rs` — new SPI `reflex_ivm_histogram(view_name)` returning p50/p95/p99/max.
- `src/tests/pg_test_deferred.rs` — 1 test asserting the array fills up after 70 flushes
  and the percentiles stabilise.

The audit explicitly calls this out as "1.3.0 could add a histogram"; this is
the minimum useful surface without adding a `pg_stat_statements` dependency.

### 1.3.0-D — pg_stat_statements query tagging (optional within R6)

Use `set_config('application_name', 'reflex_flush:<view>', true)` inside each
flush SAVEPOINT so operators who run pg_stat_statements + log_line_prefix can
filter flush queries by IMV. No code dependency on the `pg_stat_statements`
extension itself — purely a tagging convention. ~5 lines in `src/trigger.rs`.

### 1.3.0 verification

- `cargo pgrx test pg17` — all tests + 6 new ones pass.
- Bench: re-run `benchmarks/bench_db_clone.sql` for the three previously-matview
  stock_chart views, assert flush time drops below the REFRESH baseline. Capture
  numbers in `journal/2026-05-XX_topk_bench.md`.
- Manual: `SELECT * FROM reflex_ivm_histogram('large_view')` returns sane
  percentiles after a populated workload.
- Manual: `pg_stat_statements` shows distinct entries per IMV when
  `track_application_name = on`.

---

## Documentation site — `docs/` Material for MkDocs

Status: net-new directory. Color palette `#FCFCFC` (background) / `#4169E1`
(primary accent) / `#001556` (deep navy / strong) / `#121212` (text + dark mode
background). Material for MkDocs (current 9.x generation) supports every plugin
needed; the site stays a static-build artifact deployed via GitHub Pages. Note:
"Material for Mkdocs 2.0" in the task is interpreted as the current
Material-for-MkDocs generation (the package itself is at 9.x; "2.0" likely
refers to the modern post-`material[recommended]` plugin set rather than a
package version). All plugins listed below are supported by current Material.

### Site structure

```
docs/
  index.md                       # Hero, what is pg_reflex, big-3 benchmark numbers
  getting-started/
    install.md                   # .deb + from source (lifted from README)
    first-imv.md                 # Quick start (lifted from README)
    upgrading.md                 # ALTER EXTENSION matrix
  concepts/
    architecture.md              # Source → intermediate → target diagram (mermaid)
    sufficient-statistics.md     # SUM/COUNT, AVG decomposition, top-K
    decomposition.md             # CTE / UNION / WINDOW splits
    delta-processing.md          # MERGE-based deltas, advisory locks, savepoints
    deferred-mode.md             # IMMEDIATE vs DEFERRED, flush_deferred semantics
  sql-reference/
    aggregates.md                # SUM/COUNT/AVG/MIN/MAX/BOOL_OR table + retraction cost
    clauses.md                   # GROUP BY / WHERE / JOIN / HAVING / DISTINCT
    set-ops.md                   # UNION / UNION ALL / INTERSECT / EXCEPT
    cte.md                       # WITH / WITH RECURSIVE limitation
    window-functions.md          # GROUP BY + RANK, passthrough LAG/LEAD
    distinct-on.md               # 1.1.1 decomposition path
    filters.md                   # FILTER (WHERE …)
  api/
    create_reflex_ivm.md         # Full param matrix
    drop_reflex_ivm.md
    reflex_reconcile.md          # + reflex_rebuild_imv alias + refresh_reflex_imv alias
    reflex_flush_deferred.md
    reflex_scheduled_reconcile.md  # 1.2.1
    reflex_ivm_status.md         # 1.2.0 SPIs
    reflex_ivm_stats.md
    reflex_explain_flush.md
    reflex_ivm_histogram.md      # 1.3.0
    reflex_enable_topk.md        # 1.3.0
    event-triggers.md            # __reflex_on_sql_drop / __reflex_on_ddl_command_end
    gucs.md                      # pg_reflex.alter_source_policy (1.2.1)
  operations/
    deployment-profile.md        # Green/Yellow/Red light table from audit
    monitoring.md                # reflex_ivm_status / pg_stat_statements tagging
    runbook.md                   # Stuck flush, drift, ALTER fallout
    pg-cron.md                   # reflex_scheduled_reconcile recipe
    schema-changes.md            # ALTER TABLE workflow + alter_source_policy GUC
    crash-recovery.md            # UNLOGGED + reconcile
    multi-tenant-guards.md       # R8 — gate behind RPC layer
  performance/
    benchmarks.md                # Production 5-table JOIN + synthetic + 1.3.0 top-K
    when-to-use.md               # IMV vs MV vs trigger-of-your-own
    cost-model.md                # Per-shape SLO table (audit gap closed)
  limitations/
    unsupported-shapes.md        # Lifts 2026-04-22_unsupported_views.md (categories 1-10)
    known-issues.md              # Passthrough duplicate-row collapse, etc.
  contributing/
    architecture-tour.md         # src/ module map
    testing.md                   # cargo pgrx test, proptest, EXCEPT ALL oracle
    release-process.md           # CHANGELOG, .deb build, ALTER EXTENSION
  changelog.md                   # `mkdocs-include-markdown-plugin` pulls CHANGELOG.md
  about/
    license.md                   # Apache 2.0
mkdocs.yml
overrides/
  partials/
    header.html                  # Custom header (logo + version)
  stylesheets/
    extra.css                    # Color palette overrides
.github/workflows/docs.yml       # Build + deploy on push to main
docs-requirements.txt            # pinned plugin versions
```

### `mkdocs.yml` outline

```yaml
site_name: pg_reflex
site_description: Incremental materialized view maintenance for PostgreSQL
site_url: https://diviyank.github.io/pg_reflex/
repo_url: https://github.com/diviyank/pg_reflex
repo_name: diviyank/pg_reflex
edit_uri: edit/main/docs/

theme:
  name: material
  custom_dir: overrides
  palette:
    - media: "(prefers-color-scheme: light)"
      scheme: default
      primary: custom
      accent: custom
      toggle: { icon: material/weather-night, name: Switch to dark mode }
    - media: "(prefers-color-scheme: dark)"
      scheme: slate
      primary: custom
      accent: custom
      toggle: { icon: material/weather-sunny, name: Switch to light mode }
  font:
    text: Inter
    code: JetBrains Mono
  features:
    - navigation.tabs
    - navigation.sections
    - navigation.expand
    - navigation.top
    - navigation.indexes
    - navigation.footer
    - toc.follow
    - search.suggest
    - search.highlight
    - content.code.copy
    - content.code.annotate
    - content.tabs.link
    - content.action.edit

plugins:
  - search
  - glightbox
  - git-revision-date-localized:
      enable_creation_date: true
  - include-markdown
  - mermaid2

markdown_extensions:
  - admonition
  - attr_list
  - def_list
  - footnotes
  - md_in_html
  - tables
  - toc: { permalink: true }
  - pymdownx.details
  - pymdownx.highlight: { anchor_linenums: true, line_spans: __span, pygments_lang_class: true }
  - pymdownx.inlinehilite
  - pymdownx.snippets
  - pymdownx.superfences:
      custom_fences:
        - { name: mermaid, class: mermaid, format: !!python/name:mermaid2.fence_mermaid_custom }
  - pymdownx.tabbed: { alternate_style: true }
  - pymdownx.tasklist: { custom_checkbox: true }
  - pymdownx.tilde
  - pymdownx.keys

extra_css:
  - stylesheets/extra.css
```

### `overrides/stylesheets/extra.css` — color palette

```css
:root {
  --md-primary-fg-color:       #001556;
  --md-primary-fg-color--light:#4169E1;
  --md-primary-fg-color--dark: #000A2A;
  --md-accent-fg-color:        #4169E1;
  --md-default-bg-color:       #FCFCFC;
  --md-default-fg-color:       #121212;
  --md-default-fg-color--light:#3A3A3A;
  --md-typeset-a-color:        #4169E1;
  --md-code-bg-color:          #F4F6FB;
  --md-code-fg-color:          #001556;
}

[data-md-color-scheme="slate"] {
  --md-default-bg-color:       #121212;
  --md-default-fg-color:       #FCFCFC;
  --md-primary-fg-color:       #4169E1;
  --md-primary-fg-color--light:#7A95E8;
  --md-accent-fg-color:        #7A95E8;
  --md-code-bg-color:          #1A1F2E;
  --md-code-fg-color:          #FCFCFC;
}

.md-header { background: linear-gradient(90deg, #001556 0%, #4169E1 100%); }
.md-tabs   { background: #001556; }
.md-typeset h1 { color: var(--md-primary-fg-color); border-bottom: 2px solid #4169E1; }
.md-typeset h2 { color: var(--md-primary-fg-color); }
.md-typeset code { font-feature-settings: "calt" 1, "liga" 1; }
.md-typeset .admonition.tip,    .md-typeset details.tip    { border-color: #4169E1; }
.md-typeset .admonition.warning,.md-typeset details.warning{ border-color: #001556; }
```

### Content-source rules (avoid duplicating canonical text)

- `getting-started/install.md`, `first-imv.md`, `upgrading.md` — reuse README sections via
  `mkdocs-include-markdown-plugin` (single source of truth: README is the
  GitHub-rendered landing, the docs site is the canonical operator surface).
- `changelog.md` — pure include of `CHANGELOG.md`.
- `limitations/unsupported-shapes.md` — lifts
  `journal/2026-04-22_unsupported_views.md` (the journal stays as the contributor
  reference; the docs page becomes the operator reference, with explicit
  callouts for "now supported in 1.3.0").
- `performance/benchmarks.md` — table from README + the new 1.3.0 top-K numbers
  to be captured during 1.3.0-A bench.
- `operations/deployment-profile.md` — green/yellow/red light tables lifted
  verbatim from `journal/audit-production-readiness.md:222-251`.

### `.github/workflows/docs.yml`

- Trigger: push to `main` touching `docs/**`, `mkdocs.yml`, or `CHANGELOG.md`.
- Steps: `setup-python@v5` → `pip install -r docs-requirements.txt` →
  `mkdocs build --strict` → `peaceiris/actions-gh-pages@v4` deploy to `gh-pages`.
- Manual dispatch enabled for force-rebuilds.

### `docs-requirements.txt`

Pin every plugin to a minor for reproducibility:

```
mkdocs-material~=9.5
mkdocs-glightbox~=0.4
mkdocs-include-markdown-plugin~=6.2
mkdocs-git-revision-date-localized-plugin~=1.2
mkdocs-mermaid2-plugin~=1.2
pymdown-extensions~=10.7
```

### Docs verification

- `pip install -r docs-requirements.txt && mkdocs serve` — local preview shows
  every nav entry rendering, mermaid diagrams compile, search index returns hits
  for "passthrough", "deferred", "top-K".
- `mkdocs build --strict` — zero warnings (strict catches broken cross-links).
- Visual check on light + dark palettes: header gradient (`#001556` → `#4169E1`)
  matches palette; body remains `#FCFCFC` light / `#121212` dark; links
  `#4169E1`.
- Color-contrast: WCAG AA verified for `#001556` on `#FCFCFC` (>15:1) and
  `#4169E1` on `#FCFCFC` (>4.5:1).
- Accessibility: every code block has a copy-button (Material default), every
  table has a caption, every mermaid diagram has alt text (Material renders
  `<figcaption>`).

---

## Cross-cutting verification (entire rollout)

1. **Migration chain** — `cargo pgrx test pg17` exercises every
   `ALTER EXTENSION pg_reflex UPDATE TO …` from 1.0.0 through 1.3.0 in one run.
   The release workflow asserts the chain is monotonic.
2. **EXCEPT ALL oracle** — every new test in this plan uses `assert_imv_correct`
   (`src/lib.rs:237`). Top-K correctness is the highest stake; proptest covers
   the random INSERT/DELETE space.
3. **`cargo clippy && cargo fmt --check`** clean before tagging each release.
4. **CHANGELOG** entries written before the tag for 1.2.0 (extend existing
   entry), 1.2.1, and 1.3.0 (new entries, follow the existing format).
5. **Docs CI** — `docs.yml` green on `main` before announcing 1.3.0; the docs
   site is the announcement surface for top-K and the histogram.
6. **`./install.sh`** — verify each release builds a `.deb` and installs into a
   fresh PG17 instance.

---

## Out-of-scope / explicit non-goals

- **R4** (DEFERRED single-session flush) — audit notes this is latency, not
  correctness; no architectural change planned. Documented as a known
  trade-off in the new operations docs.
- **R8** (multi-tenant adversarial SQL) — pg_reflex is admin-facing by design;
  the docs page `multi-tenant-guards.md` documents the boundary instead of
  changing code.
- **Category §4 / §7 / §9** unsupported views from
  `2026-04-22_unsupported_views.md` (FULL JOIN, UNION-ALL-in-CTE, ARRAY_AGG /
  catalog) — architectural items, beyond 1.3.0 scope. The docs page lists them
  with the unlock conditions.
- **Trigger-template caching** (`2026-04-22_optimization_ideas.md` #7) — separate
  perf track, no audit risk attached.
