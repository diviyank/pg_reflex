# Refactor: internal `sql_writer` module for SQL emission

## Context

The 2026-05-17 review (`journal/2026-05-17_code_and_architecture_review.md`)
identified that pg_reflex builds SQL through scattered `format!()` calls,
with three high-cost sites:

1. **4 duplicate registry INSERT blocks** in `create_ivm.rs` (set-op,
   DISTINCT ON, window, main aggregate/passthrough paths). Each block
   ladders 14× `DatumWithOid::new(...)` calls against
   `__reflex_ivm_reference`. Adding a column to the catalog is a 4-site
   edit. (Journal #1)
2. **200-line plpgsql trigger body** templated through 3-level
   `format!()` + `.replace()` in `schema_builder::build_trigger_ddls`
   (`schema_builder.rs:402–826`). Correctness-critical SQL is reviewable
   only as a Rust string literal. (Journal #5)
3. **4 token-level identifier rewriters**
   (`query_decomposer::replace_identifier`,
   `query_decomposer::strip_redundant_bare_alias`,
   `partition::substitute_identifier`,
   `trigger::replace_source_with_transition`) each with subtly different
   quote/dot handling. The `mixed_case_identifier` bug (`484f42e`) was
   in this family two commits ago. (Journal #8)

The user asked whether `sea-query` or `sql_query_builder` would help.
Evaluation (Phase 1 research): **no.** Both lack MERGE, DISTINCT ON,
FILTER, PARTITION OF, CREATE TRIGGER, plpgsql function bodies, and
neither composes with a `sqlparser` AST. Adopting one would replace
~20% of sites (the registry INSERTs and a few static DDLs) while adding
a heavyweight dependency that doesn't speak the dialect pg_reflex
actually emits.

**Decision**: build a small internal module (`src/sql_writer/`) that
covers exactly what we use, owns the catalog row shape, and consolidates
the identifier rewriters. No new external dependency. Zero change to
the `#[pg_extern]` surface — same SQL, same triggers, same registry, same
user behavior.

---

## Scope

### In scope (replaced by the new module)
- The 4 registry INSERT duplicates in `create_ivm.rs`
- DDL emitters in `schema_builder.rs`:
  `build_intermediate_table_ddl` (107), `build_target_table_ddl` (183),
  `build_delta_scratch_table_ddl` (165), `build_indexes_ddl` (305),
  `build_staging_table_ddl` (1034), `build_passthrough_scratch_ddls` (1052)
- The plpgsql body of `build_trigger_ddls` (402–826) and
  `build_deferred_trigger_ddls` (836–990) — relocated to `.sql` files
  via `include_str!`, with a tiny slot-substitution helper
- The 4 identifier rewriters listed above

### Explicitly out of scope (separate refactors)
- `reflex_build_delta_sql` (1003 LOC, `trigger.rs:1456`) — needs the
  `DeltaPlan` enum split (journal #3), not a SQL builder
- `create_reflex_ivm_impl` (1838 LOC, `create_ivm.rs:25`) — needs phase
  extraction (journal #4)
- `generate_base_query` / `generate_end_query`
  (`query_decomposer.rs:447, 644`) — these are sqlparser AST → SQL
  round-trips with light splicing; the builder buys nothing here
- All hot-path MERGE / UPDATE / DELETE / topk / min-max emission in
  `trigger.rs` — combinatorial branching on aggregate kind, not a
  composability problem

### Non-goals
- Hot-path performance change. The trigger fire path is not touched.
- User-visible behavior change. Every `#[pg_extern]` returns
  byte-identical results.
- Generic reusability. The module lives in-tree, knows about
  `AggregationPlan`, `IntermediateColumn`, and `__reflex_ivm_reference`.

---

## Module layout

```
src/sql_writer/
├── mod.rs            -- public API + re-exports; Ident newtype
├── ddl.rs            -- CreateTable, CreateIndex typed builders
├── registry.rs       -- RegistryRow builder + insert_registry_row()
├── identifier.rs     -- one canonical token-level rewriter
└── tests.rs          -- unit tests for the four submodules

sql/
├── trigger_body.plpgsql.in           -- relocated from schema_builder.rs:454+
└── deferred_trigger_body.plpgsql.in  -- relocated from schema_builder.rs:836+
```

### `mod.rs`
- Re-exports: `CreateTable`, `CreateIndex`, `RegistryRow`,
  `Ident`, `replace_identifier`.
- `Ident::quote(&str) -> String` — moves `quote_identifier`
  (`query_decomposer.rs:20`) here; the old function becomes a thin
  re-export until call sites migrate.

### `ddl.rs` — typed DDL builders
- `CreateTable::new(name).unlogged(bool).column(name, type_sql).partition_by(expr).build() -> String`
- `CreateIndex::new(name).on(table).columns(&[...]).gin(bool).where_clause(opt).build() -> String`
- Identifier quoting is baked in. Callers can no longer forget
  `quote_identifier` (the source of the `mixed_case_identifier` bug
  class).

### `registry.rs` — single source of truth for `__reflex_ivm_reference`
```rust
pub struct RegistryRow<'a> {
    pub view_name: &'a str,
    pub depth: i32,
    pub depends_on: &'a [String],
    pub depends_on_imv: &'a [String],
    pub aggregations_json: &'a str,
    pub graph_child: &'a [String],
    pub sql_query: &'a str,
    pub base_query: &'a str,
    pub end_query: &'a str,
    pub partition_by_json: &'a str,
    pub partition_join_paths_json: &'a str,
    pub not_null_columns_json: &'a str,
    pub storage: &'a str,
    pub mode: &'a str,
}

pub fn insert_registry_row(
    spi: &mut SpiClient,
    row: &RegistryRow,
) -> Result<(), spi::Error>;
```
- One column order, one OID mapping, one INSERT site. Replaces the 4
  duplicates at `create_ivm.rs:163, 246, 409, 1702` (approx).
- Adding a column to the catalog becomes a 1-line change to
  `RegistryRow` + 1 line to `insert_registry_row`.

### `identifier.rs` — one rewriter to rule them all
- `replace_identifier(sql, old, new) -> String` — canonical
  implementation handling: quoted identifiers, schema-qualified
  `"sch"."tbl"`, mixed case, identifiers adjacent to `(`, identifiers
  inside CAST / FILTER / WITHIN GROUP.
- `strip_redundant_bare_alias(sql, source) -> String` — kept as a
  thin pass that uses the same tokenizer.
- All four current implementations (`query_decomposer::replace_identifier`,
  `query_decomposer::strip_redundant_bare_alias`, `partition::substitute_identifier`,
  `trigger::replace_source_with_transition`) collapse to this module.
  The richest semantics (the post-`484f42e` mixed-case logic) wins; the
  others move to a single test fixture.

### Trigger-body relocation
- `sql/trigger_body.plpgsql.in` carries the literal plpgsql (advisory
  lock, Path B / C dispatch, filter-aware skip, dispatch loop).
- Slots use a single sentinel format `__REFLEX_SLOT_NAME__` substituted
  by a 20-line helper `slot_replace(template, &[(name, value)])`. No
  nested `format!()`.
- `build_trigger_ddls` shrinks from 424 lines (currently 402–826) to
  ~60 lines: `include_str!` the template, slot-replace, wrap in
  `CREATE OR REPLACE FUNCTION ... CREATE TRIGGER`.
- Same outcome for `build_deferred_trigger_ddls` (836–990) and the
  `__reflex_flush_deferred` body inside `build_deferred_flush_ddl`
  (996–1028).

---

## Plan: tests first, then migrate site-by-site

Follows the CLAUDE.md cycle: write tests → implement → check
correctness → benchmark → keep or revert.

### Phase 0 — Snapshot the current SQL output (before any change)
Write a test file `src/sql_writer/tests.rs` (initially under
`#[cfg(any(test, feature = "pg_test"))]`) that calls each of the 6
existing `build_*_ddl` functions with representative `AggregationPlan`
inputs and asserts on the **exact strings** they return today.

These golden snapshots are the contract the refactor must hold. If a
later phase's typed builder emits semantically equivalent but
whitespace-different SQL, the snapshot is updated **once** with a clear
journal note; subsequent phases must match the new snapshot exactly.

Also extend the existing `query_decomposer` tests to cover the union of
edge cases handled across all 4 identifier rewriters today (quoted,
schema-qualified, mixed case, adjacent-paren, inside CAST / FILTER).

**Bar**: `cargo pgrx test` green; snapshot tests record but pass.

### Phase 1 — Identifier rewriter consolidation
Smallest, lowest-risk extraction.
- Move the canonical `replace_identifier` to `sql_writer/identifier.rs`.
- Reroute `query_decomposer::replace_identifier`,
  `query_decomposer::strip_redundant_bare_alias`,
  `partition::substitute_identifier`, and
  `trigger::replace_source_with_transition` to call the canonical one.
- Delete the now-dead implementations once call sites compile.
- All existing tests for those four functions must pass unchanged
  (this is what proves the "richest semantics wins" claim).

**Bar**: `cargo pgrx test`, `cargo pgrx check`, `cargo clippy`,
`cargo fmt`. EXCEPT ALL oracle in `pg_test_correctness.rs` must pass.

### Phase 2 — `RegistryRow` builder
- Implement `sql_writer/registry.rs::RegistryRow` and
  `insert_registry_row`.
- Replace the 4 duplicate blocks at `create_ivm.rs:163, 246, 409, 1702`
  (set-op, DISTINCT ON, window, main) with single calls.
- Add a `view_kind` field to `RegistryRow` if and only if the journal
  #4.8 column is added in this phase — otherwise leave registry shape
  unchanged.
- The catalog DDL in `lib.rs:64–130` is untouched. Only the **insert
  site** is consolidated.

**Bar**: same as Phase 1, plus an explicit integration test that
creates a set-op IMV, a DISTINCT ON IMV, a window IMV, and a plain
aggregate IMV, and asserts `__reflex_ivm_reference` row contents are
unchanged vs. the pre-refactor snapshot.

### Phase 3 — Typed DDL builders
- Implement `CreateTable` and `CreateIndex` in `sql_writer/ddl.rs`.
- Migrate the 6 `schema_builder::build_*_ddl` functions to use them.
- Snapshot tests from Phase 0 verify byte-for-byte (or
  semantically-equivalent + acknowledged) output.
- Identifier quoting is now centralized; remove ad-hoc `format!("\"{}\"", ...)`
  and `quote_identifier(...)` at the migrated sites.

**Bar**: same as Phase 1. Snapshot diffs reviewed and acknowledged.

### Phase 4 — Plpgsql trigger body relocation
- Create `sql/trigger_body.plpgsql.in`, copy the literal SQL out of
  `build_trigger_ddls` (`schema_builder.rs:454–826`). Replace dynamic
  splices with `__REFLEX_SLOT_*__` sentinels.
- Implement `sql_writer::slot_replace`.
- `build_trigger_ddls` becomes: `include_str!` → `slot_replace` →
  wrap in `CREATE OR REPLACE FUNCTION` / `CREATE TRIGGER`.
- Repeat for `build_deferred_trigger_ddls` and the body inside
  `build_deferred_flush_ddl`.
- The relocated `.sql` files are now grep-able and reviewable as SQL.

**Bar**: same as Phase 1. Particular attention to `pg_test_e2e.rs` and
`pg_test_correctness.rs` — they exercise the trigger end-to-end, which
is what catches a malformed `EXECUTE format(...)` block.

---

## Critical files to modify

| File | Change |
|---|---|
| `src/lib.rs` | Declare `mod sql_writer;` |
| `src/sql_writer/mod.rs` (new) | Public re-exports + `Ident` |
| `src/sql_writer/ddl.rs` (new) | `CreateTable`, `CreateIndex` |
| `src/sql_writer/registry.rs` (new) | `RegistryRow` + insert helper |
| `src/sql_writer/identifier.rs` (new) | Canonical token rewriter |
| `src/sql_writer/tests.rs` (new) | Phase 0 snapshots + edge cases |
| `sql/trigger_body.plpgsql.in` (new) | Relocated plpgsql |
| `sql/deferred_trigger_body.plpgsql.in` (new) | Relocated plpgsql |
| `src/create_ivm.rs` | 4 INSERT sites → `insert_registry_row` |
| `src/schema_builder.rs` | DDL functions → typed builders; trigger DDL → `include_str!` + slot replace |
| `src/query_decomposer.rs` | Identifier rewriters become re-exports |
| `src/partition.rs` | `substitute_identifier` becomes a re-export |
| `src/trigger.rs` | `replace_source_with_transition` becomes a re-export |

No changes to: `Cargo.toml` (no new deps), `pg_reflex.control`, any
`sql/pg_reflex--*.sql` migration, `lib.rs:64–130` catalog DDL, any
`#[pg_extern]` signature, the runtime delta SQL builder.

---

## Reused existing utilities (do not reimplement)

- `query_decomposer::safe_identifier` (`:66`) — `[A-Za-z0-9_]` truncation
  to 63 chars. Move to `sql_writer/identifier.rs`, keep behavior.
- `query_decomposer::format_pg_text_array_literal` (`:40`) — used by
  every registry INSERT. Move under `sql_writer/registry.rs`'s private
  helpers; keep public re-export until other callers migrate.
- `query_decomposer::quote_identifier` (`:20`) — becomes
  `sql_writer::Ident::quote`; old name kept as a re-export.
- `DatumWithOid::new(...)` / `PgBuiltInOids::*OID` — pgrx-native; reused
  inside `insert_registry_row`.

---

## Verification

After each phase, run in this order. **The bar is "all of these pass";
any failure stops the phase and is investigated, not bypassed.**

1. `cargo fmt --check`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo pgrx check` — DDL parses, extension loads
4. `cargo pgrx test` — full unit + `#[pg_test]` suite, including the
   ~21 included test files (`pg_test_correctness.rs`, `pg_test_e2e.rs`,
   `unit_trigger.rs`, `unit_proptest.rs`, …)
5. **EXCEPT ALL oracle** (the project's gold-standard correctness gate):
   `pg_test_correctness.rs` calls `assert_imv_correct` (`lib.rs:847`)
   on every IMV it creates; every IMV's contents must match the
   freshly-evaluated source SQL in both directions. This is what
   catches a malformed trigger body or a swapped registry column.
6. End-to-end smoke against `db_clone` if local: pick 3 of the
   materialized views referenced in `/home/diviyan/fentech/algorithm/
   api/base-db-anchor-evm/base_db/sql`, recreate as IMVs, run a typical
   UPDATE batch, verify `assert_imv_correct` clean.
7. Benchmark: re-run `benchmarks/` against the previous tag; cold-path
   regression budget is ≤5% on `create_reflex_ivm`. Hot-path budget
   is 0% — `reflex_build_delta_sql` is not touched, so any regression
   indicates a mistake.

---

## Estimated cost

- Phase 0: 0.5 day (snapshot harness)
- Phase 1: 0.5 day (identifier consolidation)
- Phase 2: 0.5 day (registry builder)
- Phase 3: 1 day (typed DDL builders + migration)
- Phase 4: 1 day (plpgsql relocation, the riskiest phase)

**Total**: ~3.5 days, sequential. Each phase is its own commit / PR
and ships independently — if Phase 4 turns out to be more painful than
expected, Phases 0–3 still bank value.

## What this refactor explicitly does *not* claim

- It does not make the `reflex_build_delta_sql` combinatorics easier.
  That requires the `DeltaPlan` enum split (journal #3) — a separate,
  larger refactor.
- It does not shrink `create_reflex_ivm_impl`. That requires phase
  extraction (journal #4).
- It does not change correctness guarantees. The EXCEPT ALL oracle is
  what proves correctness; this refactor just changes where the SQL
  strings live.
- It does not open the door to a different storage backend, a
  different parser, or a public crate. If those become goals later,
  the `sql_writer` module is a reasonable jumping-off point — but
  promoting it to a crate is a separate decision (extraction criteria:
  a second pgrx extension in the same monorepo wants the same DDL
  emitters).
