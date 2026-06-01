# pg_reflex — Maintainer's Map

A navigation guide for working on this codebase, written for engineers who are
comfortable in Python/SQL but **new to Rust**. Read this once before your first
change.

> **Going deeper:** this page is the quick onboarding map. For the full code
> architecture (create + maintenance pipelines, dispatch strategy, invariants)
> see `docs/contributing/architecture-tour.md`; for PostgreSQL-level behaviour
> (HOT, vacuum, locks, partition swap) see `docs/concepts/internals.md`.

## The one mental model that matters

**Rust here is a code generator, not a compute engine.** pg_reflex builds
strings of SQL and hands them to PostgreSQL to execute. The heavy lifting —
joins, aggregates, `MERGE` — runs inside Postgres's C executor, completely
independent of this Rust code.

```
source table changes
   │  (trigger fires, once per statement, using transition tables)
   ▼
reflex_build_delta_sql()        ← Rust: pick a strategy, build SQL strings
   │
   ▼
EXECUTE '<generated SQL>'        ← Postgres: does the actual data work
   │
   ▼
IMV target table is up to date
```

Consequence: **95% of the code you will touch is `format!("... SQL ...")`** —
Rust's f-string equivalent. It reads like Python. The genuinely hard Rust
(FFI, memory, the `#[pg_extern]` plumbing) is tiny, stable, and lives in
`lib.rs` — you will rarely need to open it.

## Where do I change X?

| I want to change… | Look in | Notes |
|---|---|---|
| How a new IMV is **created / validated** | `create_ivm/mod.rs` | the `BuildContext` pipeline, top-to-bottom |
| Support a new **query shape** (UNION, DISTINCT ON, window, CTE) | `create_ivm/decompose.rs` | splits complex queries into sub-IMVs |
| **Unique-key / NOT-NULL inference** (correctness-critical) | `create_ivm/soundness.rs` | **bug hotspot** — most past correctness fixes live here |
| `reflex_compact_imv` / `reflex_rebuild_*` admin commands | `create_ivm/admin.rs` | maintenance commands, not creation |
| How a change is **applied** to an IMV (the strategy picker) | `trigger/mod.rs` | `reflex_build_delta_sql` — the dispatcher |
| The **`MERGE` / MIN/MAX recompute** SQL | `trigger/merge.rs` | |
| **INSERT vs full-rebuild** decision (selectivity, partitions) | `trigger/dispatch.rs` | Path B / Path C dispatch |
| Per-operation INSERT/DELETE/UPDATE codegen | `trigger/ops.rs` | incl. outer-join / passthrough arms |
| **Deferred (COMMIT-time) flush** | `trigger/deferred.rs` | the `reflex_flush_deferred` batch path |
| SQL **parsing / analysis** (what the query means) | `sql_analyzer.rs` | runs at create-time only |
| Aggregation planning (which agg, group keys) | `aggregation.rs` | |
| Table/column **name + identifier** helpers | `query_decomposer.rs`, `sql_writer/` | `quote_identifier`, transition-table names, etc. |
| Reconcile / drift repair | `reconcile.rs` | |
| Partitioned IMVs | `partition.rs` | |
| Dropping IMVs + cleanup | `drop_ivm.rs` | |
| Consistency / drift **audits** | `audit/` | |
| The **public SQL functions** (`#[pg_extern]`) | `lib.rs` | thin wrappers → `*_impl` functions |

Rule of thumb: **`create_ivm/` = "set up an IMV", `trigger/` = "keep it updated".**

## Walkthrough: one INSERT, end to end

1. A row is inserted into a source table. The trigger pg_reflex installed fires
   (statement-level, reading Postgres **transition tables** — so the row data
   never enters Rust).
2. The trigger body calls **`reflex_build_delta_sql`** (`trigger/mod.rs`). This
   is the dispatcher. It:
   - loads the IMV's plan from the catalog (`__reflex_ivm_reference`),
   - decides the operation arm (INSERT / DELETE / UPDATE, passthrough vs
     aggregate, self-join, outer-join-secondary),
   - calls into `ops.rs` / `merge.rs` / `dispatch.rs` to build the SQL,
   - returns a delimiter-separated string of SQL statements.
3. The plpgsql trigger body `EXECUTE`s those statements. Postgres applies the
   delta to the IMV's intermediate + target tables.
4. In **deferred** mode, steps 2–3 are batched and run once at COMMIT via
   `reflex_flush_deferred` (`trigger/deferred.rs`) instead of per statement.

Open those four files in order and you've seen the whole hot path.

## Module trees (after the 2026-06 split)

```
src/trigger/            "keep the IMV updated" (the maintenance hot path)
├── mod.rs        reflex_build_delta_sql dispatcher + delta-SQL cache + DeltaOp + name helpers
├── merge.rs      MERGE / MIN/MAX recompute SQL builders, null_safe_in
├── dispatch.rs   selectivity & partition dispatch (Path B/C), bulk insert/delete
├── ops.rs        per-operation INSERT/DELETE/UPDATE codegen, outer-join/passthrough
└── deferred.rs   reflex_flush_deferred (COMMIT-time batched flush)

src/create_ivm/         "set up a new IMV" (create-time, runs once per IMV)
├── mod.rs        input validation + the BuildContext creation pipeline + create_reflex_ivm_impl
├── decompose.rs  split UNION/INTERSECT/EXCEPT, DISTINCT ON, window, CTE queries into sub-IMVs
├── soundness.rs  unique-key + NOT-NULL inference  (correctness-critical)
└── admin.rs      reflex_compact_imv / reflex_rebuild_* maintenance commands
```

These two were split out of single 3.5k / 4.6k-line files. The split is a pure
move: each submodule starts with `use super::*;` and the parent `mod.rs`
re-exports via `pub(crate) use <submodule>::*;`, so every existing call path is
unchanged.

## "Rust for this codebase" — the ~15 things you'll actually meet

You do **not** need to learn Rust. You need to read these patterns:

- **`format!("... {x} ...", x = y)`** — string interpolation = Python f-string.
  This is most of the code.
- **`Spi::run(&sql)` / `Spi::get_one::<T>(&sql)`** — run SQL / read one value.
  This is how Rust talks to Postgres.
- **`match value { A => ..., B => ... }`** — like Python `match`/a big if-elif.
- **`Option<T>`** — a value that may be absent. `Some(x)` / `None`. `?` on an
  `Option`/`Result` means "bail out early if absent/error" (like an early
  `return None`).
- **`Result<T, E>`** — success `Ok(x)` or failure `Err(e)`.
- **`&str` vs `String`** — borrowed text vs owned text. If the compiler
  complains, `.to_string()` converts `&str → String`, `&s` converts the other
  way. This is the one borrow-checker friction you'll hit; it's mechanical.
- **`Vec<String>`** — a list of strings. `vec.push(x)` appends. `stmts: &mut
  Vec<String>` = "a list I append generated SQL onto" (the codegen idiom here).
- **`.iter().map(...).collect()`** — list comprehension.
- **`pub(crate) fn`** — visible across this crate (internal). `pub fn` +
  `#[pg_extern]` — exposed to SQL as a callable Postgres function.
- **`#[cfg(test)]` / `#[pg_test]`** — test-only code / a test that runs against
  a real Postgres.

If you can read those, you can read and edit the SQL-generation code.

## How to make a change safely (without deep Rust fluency)

The 1100+ tests are your safety net. The compiler is your second one. A typical
loop:

```sh
cargo check        # ~2s — does it compile? the compiler points at every problem
cargo pgrx test    # ~20s — did behavior change? 1123 tests against a real Postgres
cargo clippy       # lint
cargo fmt          # format
```

Workflow:
1. Make your edit to the relevant SQL-building function.
2. `cargo check`. Rust's errors are verbose but precise — they tell you the file,
   line, and usually the fix ("expected `String`, found `&str` → add
   `.to_string()`").
3. `cargo pgrx test`. If green, your change preserved correctness. If red, the
   failing test name tells you what broke.
4. `cargo clippy && cargo fmt` before committing.

Because correctness is verified mechanically, you can confidently change SQL
codegen even if the surrounding Rust still looks unfamiliar — if the tests pass,
the behavior is sound. When you fix a real bug, **add a test first** (see
`src/tests/`) so it can never regress.

## What to leave alone unless you know Rust FFI

`lib.rs` (the `#[pg_extern]` wrappers, extension wiring) and the trigger
installation plumbing rarely change and are where the only genuinely hard Rust
lives. If a task seems to require editing those, it's worth a second pair of
eyes.
