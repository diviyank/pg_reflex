# `cargo clippy -- -D warnings` fails on a current stable toolchain: `collapsible_else_if` in `src/trigger/ops.rs` — tooling (CI risk)

Pre-existing. Reported by every implementer and reviewer during the 1.12.0 hardening work on the
`release/1.12.0` branch (base commit 6946400), on pg15, pg16 and pg17 with rustc 1.93.1 locally.
CI installs the latest stable toolchain and runs `cargo clippy --features pg17 -- -D warnings`
(see `CLAUDE.md`, "Test suite"), so a newer lint the local toolchain lacks, or this one on a
toolchain that has it, fails the push. 1.11.4's CI already failed once on a `manual_filter` lint
that the local 1.95 did not report.

## Reproduction

```
cargo clippy --features pg17 -- -D warnings
```

The reported error is `collapsible_else_if`. On the base commit it was reported at
`src/trigger/ops.rs:1394` and `:1463` (Task 2 review). After the 1.12.0 Task 3 edits one
occurrence remained at `ops.rs:1395` (Task 3 review). On the current integration tree
(`release/1.12.0`, merge 866763e) the remaining construct is at lines 1393-1399:

```rust
            } else {
                if skip_target_delete {
                    stmts.push(format!("INSERT INTO {} {}", qv, end_query));
                } else {
                    stmts.push(rebuild);
                }
            }
```

An `else { if .. {..} else {..} }` with nothing else in the block, which clippy asks to be written
`else if skip_target_delete { .. } else { .. }`. The line numbers were found by reading the file;
clippy was not run for this report.

## Expected vs actual

Expected: clippy is clean under `-D warnings`. Actual: one `collapsible_else_if` error, so the
documented pre-push check fails.

## What is known about the mechanism

The Task 3 reviewer's reading: the `skip_target_delete` branch at this site is dead. The only
way to reach it with an empty `group_by_columns` would be an IMV with `skip_target_delete` set,
and `build_source_join_keys` (`src/create_ivm/soundness.rs`, about lines 209-271) only records a
source whose join equality maps to a GROUP BY column, so empty `group_by_columns` means no
`source_join_keys` entry and `skip_target_delete` is false. Deleting the dead branch would clear
the lint but was outside the 1.12.0 brief; this was a reading, and an attempt to reach the branch
with a sentinel `COUNT(DISTINCT ..)` or `SELECT DISTINCT ..` aliased `"x GROUP BY y"` stayed green
under mutation.

## Fix direction (hypothesis)

Either collapse to `else if skip_target_delete { .. } else { .. }` (behaviour-neutral) or delete
the dead branch and its tests' dependency on it. Before the next push run clippy exactly as CI
does, with CI's toolchain version (`rustup toolchain install <ci-version> --profile minimal -c
clippy`, then `cargo +<ci-version> clippy --features pg17 -- -D warnings`).
