# pg_reflex: A postgres incremental view maintenance package

The goal of this package is to create new tables, that replace Materialized views and enable users with fast and automatic updates.
This package is built in rust using pgrx.

Read more on the purpose of this package in the README.md file.


# Test suite

We can test and validate the package with:
- cargo pgrx test
- cargo clippy
- cargo fmt
- cargo pgrx schema (verify the generated SQL entities, e.g. after adding a `#[pg_extern]`)

`cargo pgrx check` does not exist in pgrx 0.18 — it was never a subcommand.

**`cargo pgrx test` installs the extension into the shared pgrx install
(`~/.pgrx/<version>/pgrx-install`) as part of running.** So two agents or shells testing
the same PG version concurrently overwrite each other's `.so`, and tests can execute
against the wrong build. A single failing run under concurrency is not evidence — re-run
before believing it. When benchmarking against a pgrx-managed instance, record which
commit the installed `.so` was built from, or the numbers are unattributable.

Migrations in `sql/*--*.sql` are **never executed by `cargo pgrx test`**; only the
bootstrap DDL in `src/lib.rs` is. New registry columns must exist in both to be visible
to tests and to upgrades.


# Benchmarks

There is a whole folder of benchmark scripts. Some of them use the local postgres, localhost and postgres user.
The db is called db_clone, and there's quite a lot of materialized views that could be replaced by IMVs
Check all the files in `/home/diviyan/fentech/algorithm/api/base-db-anchor-evm/base_db/sql`
to have all the view and index definitions.


# Development

You need to be really critic on the proposed modifications or approaches and fully comprehend the scope of the modifications ; what does it imply?
Is there a need for such modification? Is it worth the hassle/additional complexity in the code?

The development is always in these steps:

- Code tests (and do not modify them afterwards)
- Implement
- Test correctness
- Benchmark
- Evaluate the worth of the new development? it it worth it?
- If modification kept, then try to optimize it entering in a new development cycle.

There's a few development journals in `discussion*.md` files.

Use first LSP + ast-grep tools first to go through files. 


# Bug resolution methodology

Field-reported bugs are tracked as one Markdown report per issue in `untreated_bugs/`.
Each is resolved through the loop below. It is deliberately adversarial and
correctness-first: most reports are partly wrong, and a fast wrong fix to a correctness
package is worse than no fix.

## The loop, per bug

1. **Pre-spec.** Before dispatching work, write a short pre-spec that states the exact
   fix location, the tests required (with the property each must pin), what is explicitly
   out of scope, and — critically — a **Step 0 falsification**: the report's own root
   cause and fix direction are a hypothesis to disprove first, not a plan to execute.
   Reports routinely misdiagnose (a "sync doesn't touch X" that already does; a proposed
   predicate that PostgreSQL cannot actually use; a "perf-only" call that is a live
   correctness bug). Verify the mechanism in a real reproduction before writing a fix.

2. **Implement (TDD, isolated).** Work in a git worktree. Write tests first, watch them
   go RED **for the intended reason**, then fix; never weaken a test afterward. Fixtures
   must be **real** — real IMVs over real sources — never registry rows hand-inserted to
   simulate a shape (that has produced false-green tests twice). Use the bidirectional
   `EXCEPT ALL` / `assert_imv_correct` oracle for correctness, not string assertions on
   generated SQL. Commit each coherent piece as you go.

3. **Self-mutation.** Every assertion of the form "this must be absent / must refuse /
   must stay unchanged" has to be shown to go RED when the fix is reverted or the guard
   broken. A test that stays green under mutation is a false green and must be
   strengthened before the work is considered done. Report the mutation results.

4. **Adversarial review.** A separate reviewer is instructed to **refute**, not bless —
   to hunt for the one class of real defect (a residual silent-wrong-result, a
   false-green test, a new unclearable-finding retry loop, an over-broad predicate that
   silently breaks a legitimate case). The reviewer proves verdicts by mutation and
   construction, not assertion, and leaves the worktree pristine.

5. **Author fix round** on confirmed findings, then re-verify.

6. **Integration.** Fix branches are **code + tests only**. Release packaging — the
   version bump, `CHANGELOG.md`, and the `sql/*--*.sql` migration — is owned at
   integration, done once per batch. A new registry column goes in **both** the
   bootstrap DDL in `src/lib.rs` and the migration (they must be byte-identical, or
   fresh installs and upgrades diverge). The integrator re-runs the full suite on the
   merged tree (byte-identical to what merges into `main`) before merging.

## Principles that make it work

- **A negative result / no-fix is a valid, valuable outcome.** When investigation shows
  the report is already fixed elsewhere, or that no safe change exists (a skip that
  can't be proven sound, a fix that trades a plan-quality safeguard for a timing test),
  the deliverable is a rigorous write-up plus report update — not a manufactured fix.
  This is the `# Development` "is it worth it?" question with teeth.
- **Correctness bias is asymmetric.** A performance optimization whose failure mode is
  wrong data (a partition-pruning predicate that drops a needed partition, a
  skip-unchanged signal blind to one input) must fail toward doing the full work when in
  any doubt. Prove the safe direction; never ship the fast-but-sometimes-wrong one.
- **Refuse loudly, never no-op silently.** When a primitive can't handle an input, a
  clean error string (or a WARN naming the primitive that can) beats a silent no-op that
  corrupts or a raise that aborts the caller's transaction.
- **Don't print a remedy that can't clear its own finding.** An audit/doctor finding
  whose suggested fix structurally cannot resolve it sends operators into an infinite
  retry loop. Every prescribed remedy must be shown to converge.

## `untreated_bugs/` hygiene

- One report per issue; each states what was ruled out, the exact reproduction, severity,
  and a fix direction.
- On a fix, remove the report (the CHANGELOG and tests carry the record) or, when only
  part of its scope is closed, **narrow** it to the genuine residual with a pointer to
  where the rest is tracked.
- File newly-discovered adjacent bugs as their own reports rather than folding them
  silently into unrelated work.

## Concurrency discipline

Because `cargo pgrx test` installs into the shared `~/.pgrx/<ver>/pgrx-install` (see
`# Test suite`), parallel sessions clobber each other's `.so` and can even share cargo
fingerprints under a shared `CARGO_TARGET_DIR`. Give each session its own
`CARGO_TARGET_DIR`, warm the build before testing, treat a single failing/zero-test run
as noise and re-run, and take the integrator's serial full-suite run on the merged tree
as the authoritative result.


# Priorities

The main goal of this package is correctness. There is no use in such a package if the results are not correct. And trust in it would be broken.
The second goal is not over-complexifying the code of the package. Be simple and straightforward in the implementation.
And the third goal is performance

# Coding practices

We value code that explains itself through clear class, method, and variable names. Comments may be used when necessary to explain some tricky logic or for documentation, but should be avoided otherwise.
