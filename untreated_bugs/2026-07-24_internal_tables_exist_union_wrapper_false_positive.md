# 2026-07-24 — `internal-tables-exist` false-positives at Error severity on decomposed UNION-ALL wrapper IMVs, demanding aux tables the wrapper correctly does not own

**Status: untreated.** Found under PS-9 (B9) while choosing the "is an
intermediate expected" registry predicate. Confirmed by probe on `main` @
`eca3807` (1.11.0). Pre-existing, not a B9 regression.

`reflex_audit` / `reflex_doctor` report a **clean, freshly created, correct**
UNION-ALL IMV as having two missing internal tables, at `Severity::Error`, with a
`reflex_rebuild_imv` remedy that cannot change the outcome (the wrapper is not
supposed to own those tables, so nothing will ever create them). Every UNION /
UNION-ALL / set-op IMV in a database carries this finding permanently.

## Reproduction (probe output, pg17)

```sql
CREATE TABLE a (id BIGINT, v NUMERIC);
CREATE TABLE b (id BIGINT, v NUMERIC);
INSERT INTO a VALUES (1,10);
INSERT INTO b VALUES (2,20);
SELECT create_reflex_ivm('v', 'SELECT id, v FROM a UNION ALL SELECT id, v FROM b', 'id');
SELECT reflex_audit('v');
```

Registry after create — note the wrapper row's `aggregations`:

```
v            | end_query='' | aggregations={}                            | partition_columns=<NULL>
v__union_0   | end_query='' | aggregations={… "is_passthrough": true …}   | partition_columns=<NULL>
v__union_1   | end_query='' | aggregations={… "is_passthrough": true …}   | partition_columns=<NULL>
```

Audit output (verbatim, first finding):

```
[ERROR] v  internal-tables-exist
  Missing internal table(s) for IMV v:
    "__reflex_intermediate_v"
    "__reflex_affected_v"
  Suggested fix:
    SELECT reflex_rebuild_imv('v');
```

The IMV is correct and current: it contains both rows and maintains
incrementally through the two sub-IMVs.

## Root cause — predicate mismatch on the wrapper row

`InternalTablesExist` (`src/audit/checks_a_catastrophic.rs:267-288`) branches on
`imv.is_passthrough()`:

```rust
if imv.is_passthrough() {
    /* per-source scratch pair */
} else {
    required.push(intermediate_table_name(&imv.name));      // :284
    required.push(affected_groups_table_name(&imv.name));   // :285
}
```

`ImvRow::is_passthrough()` (`src/audit/mod.rs:82-90`) parses
`aggregations->>'is_passthrough'` and `unwrap_or(false)` on absence. Decomposed
wrapper rows are inserted by `RegistryRow::decomposed`
(`src/sql_writer/registry.rs:79-109`, used at five `insert_registry_row` sites in
`src/create_ivm/decompose.rs`: `:219, :280, :597, :736, :838`), which hardcodes

```rust
end_query: "",
aggregations_json: "{}",
```

So `{}` has no `is_passthrough` key → `unwrap_or(false)` → the check takes the
aggregate branch and demands an intermediate + affected-groups table for a node
that owns neither. The sibling predicate `end_query.is_empty()` is `true` on the
same row and classifies it correctly.

This is further evidence that PS-9's choice of `end_query.is_empty()` over
`aggregations->>'is_passthrough'` for `PartitionMirror` was right: it is the only
one of the two that is correct for decomposed rows. See the rationale comment in
`src/audit/checks_b_drift.rs` (`PartitionMirror::run`).

## What was ruled out

- **Not a missing-table problem.** The two relations genuinely do not exist and
  genuinely should not: a set-op wrapper's maintenance runs through its
  `__union_N` sub-IMVs, and the wrapper's own registry row records no
  aggregation. Nothing reads or writes `__reflex_intermediate_v`.
- **Not the passthrough-scratch branch failing.** The check never reaches it —
  the branch is chosen before any relation is probed.
- **Not a `search_path` artefact.** Bare names in `public` with the default test
  path; the same finding appears for schema-qualified wrappers.
- **Not fixable by the printed remedy.** `reflex_rebuild_imv` is an alias for
  `reflex_reconcile` (`src/lib.rs:823`), and neither emits intermediate DDL
  (that happens only at create time via `build_intermediate_table_ddl`,
  `src/create_ivm/mod.rs:1076`) — so the finding is unclearable, the B5/B9
  anti-pattern again.
- **Not limited to UNION ALL.** Any shape routed through
  `RegistryRow::decomposed` has `aggregations = '{}'`. CTE-decomposed *sub*-IMVs
  go through `create_reflex_ivm_impl` and get a real `aggregations` JSON, so they
  are unaffected; the *wrapper* rows are the ones at risk.

## Severity

S2. Report-only and non-destructive, but it is an **Error**-severity false
positive on a healthy object, permanent, one per set-op IMV — exactly the noise
that trains operators to stop reading `reflex_doctor` output, and it competes for
attention with the true Errors in the same report.

## Adjacent observation (not investigated)

The same probe emitted two further Errors on the same fixture:

```
[ERROR] v  trigger-attached
  Source v__union_0 is missing trigger(s): __reflex_trigger_ins_on_v__union_0, …
```

i.e. `trigger-attached` expects DML triggers on the *sub-IMV* relations listed in
the wrapper's `depends_on`. Whether that is also a false positive (sub-IMV → parent
propagation may be driven by the sub-IMV's own maintenance rather than by triggers
on its target) was **not** established here. Worth a separate probe; do not assume
it is the same bug.

## Fix direction

- Make the passthrough decision in `InternalTablesExist` use the same predicate
  the runtime does — `imv.end_query.is_empty()` — or, better, give `ImvRow` a
  single `owns_intermediate()` accessor implemented as `!end_query.is_empty()` and
  route **all** call sites through it (`checks_a_catastrophic.rs:267`,
  `checks_b_drift.rs` `PartitionMirror`, `checks_c_orphan.rs:170`'s `expected`
  set), so the two signals cannot drift apart again.
- Alternatively/additionally, stop `RegistryRow::decomposed` writing `'{}'`: emit
  a minimal `{"is_passthrough": true}` for wrapper rows so the JSON signal is
  truthful. This needs care — `'{}'` may be load-bearing elsewhere — and it does
  not fix rows already in the field, so the predicate change is the primary fix.
- Regression fixture: create a UNION-ALL IMV, assert `reflex_audit` on it returns
  **zero** `internal-tables-exist` findings.
