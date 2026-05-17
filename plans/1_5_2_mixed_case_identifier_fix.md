# 1.5.2 — Mixed-case quoted identifier fix

## Bug

When a user creates an IMV that uses **quoted mixed-case** column names —
`"Grp"`, `"DisplayName"`, etc. — pg_reflex unconditionally lower-cases
those names when building the target/intermediate DDL, the trigger code,
and the persisted aggregation metadata. The target ends up with column
`grp` (lower-case), and any application query against the IMV with the
original `"Grp"` fails with `column "Grp" does not exist`.

Reproduction:

```sql
CREATE TABLE bug_src ("Id" INT PRIMARY KEY, "Grp" TEXT, v INT);
INSERT INTO bug_src VALUES (1,'a',10),(2,'a',20),(3,'b',30);

SELECT create_reflex_ivm(
  'bug_view',
  'SELECT "Grp", SUM(v) AS s FROM bug_src GROUP BY "Grp"',
  NULL, NULL, NULL, NULL);
-- INFO: data-probe added 1 effectively-NOT-NULL column(s) to 'bug_view': ["grp"]
-- INFO: created IMV 'bug_view'

SELECT * FROM bug_view WHERE "Grp" = 'a';  -- ERROR: column "Grp" does not exist
\d bug_view                                  -- columns are 'grp', 's' (both lower-case)
```

PostgreSQL identifier rules:

- Unquoted refs (`SELECT Grp`) are folded to lower-case **at parse time**.
- Quoted refs (`SELECT "Grp"`) are case-sensitive — the user explicitly
  asked for the literal mixed case.

pg_reflex's `query_decomposer::normalized_column_name` (line 218) is
written to "match PG's identifier folding," which is *correct for the
unquoted case* and *wrong for the quoted case*. It strips quotes
unconditionally, then lower-cases, losing the information the user
preserved by quoting.

## Scope of the bug

`normalized_column_name` is the *single* place where case is destroyed.
It is called from **18 sites** across 5 modules:

| Module                  | call sites |
| ----------------------- | :--------: |
| `trigger.rs`            | 11         |
| `schema_builder.rs`     | 10         |
| `aggregation.rs`        | 10         |
| `query_decomposer.rs`   |  7         |
| `create_ivm.rs`         |  7         |

Every site that reads a column name from the SQL parse tree, persists it
to the registry, or emits DDL/DML using it, routes through this single
function. Fixing the function fixes the whole pipeline.

## Resolution

### Step 1 — Change the contract of `normalized_column_name`

New contract: **match PostgreSQL's own folding rule, including the
quoted-identifier exception.**

- If the bare segment starts with `"` and ends with `"` (quoted) →
  strip quotes, preserve case verbatim.
- Otherwise (unquoted) → fold to lower-case (current behavior).

```rust
pub fn normalized_column_name(col: &str) -> String {
    let bare = bare_column_name(col);
    let is_quoted = bare.starts_with('"') && bare.ends_with('"') && bare.len() >= 2;
    let stripped = if is_quoted {
        // Quoted: PG keeps case verbatim. Also handle `""` → `"` escaping.
        bare[1..bare.len()-1].replace("\"\"", "\"")
    } else {
        // Unquoted: PG folds to lower-case at parse time.
        bare.to_lowercase()
    };
    if stripped.contains('(') {
        // Expression with parens — sanitize for use as an identifier suffix.
        stripped.chars()
            .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
            .collect::<String>()
            .trim_matches('_')
            .to_string()
    } else {
        stripped
    }
}
```

### Step 2 — Audit downstream comparisons

Several call sites compare normalized names case-insensitively against
catalog data (e.g. `cols.contains(&c.to_lowercase())` in
`create_ivm.rs:944` and `:1022` — the analyzer-over-attribution catalog
filter). With case-preserving normalization, those comparisons need to
become **case-preserving** for quoted-source refs too. The fix:

- Catalog filters currently use `.to_lowercase()` on both sides; change
  to a comparison that respects the `was_quoted` decision. Two options:
  - **Option A**: leave catalog reads as-is (they are case-preserving
    from `information_schema.columns`) and compare against the
    case-preserving normalized name. This works because PG stores the
    column name verbatim in the catalog when the DDL quoted it.
  - **Option B**: add a `normalize_for_catalog_compare` helper that
    folds to lower-case only when the original was unquoted.

Option A is simpler and matches PG's own semantics — catalog rows have
the user's case preserved.

### Step 3 — `bare_column_name` audit

`bare_column_name` uses `col.rsplit('.').next()` to take the rightmost
dotted segment. For `"My Schema"."My Col"` it would incorrectly split on
the embedded space-less dot — but quoted dots are never split by
sqlparser (it tokenises before we see the string). For
`schema."Col.With.Dots"` it would take the literal `Col.With.Dots"` —
quotes intact, still detected by Step 1. No change needed.

### Step 4 — Trigger / metadata regeneration on upgrade

Existing IMVs (1.5.0 / 1.5.1) created with mixed-case quoted columns
have already persisted lower-cased metadata and built lower-cased
targets. The fix changes the contract, so those IMVs will continue to
work *as-is* (target was created with lower-case, trigger SQL references
the same lower-case, end-to-end consistent — just doesn't match the
user's source query).

Migration:

- **Migration**: `sql/pg_reflex--1.5.1--1.5.2.sql` re-emits trigger
  function bodies for every source so the new codegen takes effect for
  source-side reads. The persisted `imv_relevant_columns` JSON for old
  IMVs stays lower-cased — fine for the run-time filter (it compares
  against the source's catalog, which was created by the user with the
  same case they used in their SQL). 
- **For mixed-case IMVs**: operators must `DROP` and `CREATE` the IMV
  fresh to get the case-preserved target shape. Document this in
  CHANGELOG. The number of affected IMVs in production is expected to be
  small (most schemas use unquoted lower-case-only identifiers).

### Step 5 — Add a knob: opt-out

Some operators may have application code that relies on the lower-cased
behavior. Add a per-IMV flag or session GUC to opt out and keep the old
behavior:

```sql
SELECT reflex_set_force_lowercase_columns('view_name', TRUE);
```

Or simpler: just document the new behavior; no opt-out (the new behavior
is what users have always assumed).

## Test plan

Already landed (4 `#[ignore]`-d tests in `src/tests/pg_test_coverage.rs`):

- `cov_bug_mixed_case_grouped_imv_target_preserves_case` — aggregate
  IMV with `"Grp"` GROUP BY column, asserts target column is `"Grp"`,
  initial materialisation correct, INSERT / UPDATE-flip-group / DELETE
  all work.
- `cov_bug_mixed_case_passthrough_imv` — passthrough IMV with `"Id"` PK
  and `"DisplayName"` projection, asserts both columns present with case
  preserved, UPDATE flows through.
- `cov_bug_mixed_case_aliased_aggregate_column` — `SUM(v) AS "TotalQty"`,
  asserts the alias preserves case (regardless of source column casing).
- `cov_bug_mixed_case_with_schema_qualified_source` — schema-qualified
  IMV name + quoted source column.

And one *regression* test that must stay green:

- `cov_bug_unquoted_mixed_case_still_lowercases` — `SELECT Grp` (no
  quotes) must still produce target column `grp`, matching PG's parse.

Once the fix lands, remove all `#[ignore]` attributes; the 4 currently
failing tests must turn green and the regression test must stay green.

## Acceptance criteria

1. All 5 `cov_bug_*` tests pass.
2. All 878 existing tests still pass (no regression).
3. `cargo clippy` clean, `cargo fmt --check` clean.
4. Migration script in `sql/pg_reflex--1.5.1--1.5.2.sql` ships and
   re-emits triggers.
5. CHANGELOG entry documents:
   - the fix,
   - the operator action ("DROP and CREATE IMVs that use mixed-case
     quoted source columns to get the case-preserved target shape"),
   - the migration script's behavior.

## Estimated complexity

- `normalized_column_name` change: 10–20 lines.
- Catalog-filter audit (Step 2): 4–10 lines across 2 sites.
- Migration script: ~30 lines (boilerplate trigger rebuild).
- CHANGELOG entry: ~30 lines.
- Total: ~100 lines of production change + 1 SQL migration file.

Risk: low. The function has one well-defined contract; the change is
narrow and the test matrix already characterises the desired behavior on
both the quoted and unquoted sides.

## Out of scope (deferred)

- Mixed-case **table names** as IMV source (`SELECT * FROM "MyTable"`).
  Less common; can be a follow-up. The current bug affects user-visible
  IMV columns, which is the high-leverage fix.
- Embedded-dot quoted identifiers (`"Sch.With.Dots"."Col"`). Not seen in
  any reported user schema; defer.
- Unicode / non-ASCII identifier handling beyond what PG does.
