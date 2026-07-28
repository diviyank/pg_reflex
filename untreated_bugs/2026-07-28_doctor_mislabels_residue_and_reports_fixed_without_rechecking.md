# 2026-07-28 — `reflex_doctor` labels every empty-but-populatable partition "archive residue", and `fix => true` reports `fixed` without re-running its checks

**Status: untreated.** Split out of
`2026-07-28_partitioned_reconcile_destroys_dependent_imvs.md` §5.3 while fixing that
report on `fix/swap-ddl-destroys-dependents`. Two independent operability defects that
happened to be observed through the same damaged fixture. Both **survive** that fix — they
are properties of the doctor, not of the swap.

Severity: **medium.** No wrong data is produced by the doctor, but it misdirects the
operator diagnosing wrong data, and it can report a repair as complete while the IMV is
still wrong — which is the failure mode CLAUDE.md's "don't print a remedy that can't clear
its own finding" rule exists to prevent.

---

## Defect 1 — the "archive residue" diagnosis is unconditional

**Confirmed by code read** (`src/audit/checks_d_residue.rs:25-39`). The finding text is a
literal:

```rust
finding: format!(
    "Partition {} is empty but the IMV definition would populate it (archive residue)",
    src_child
),
```

The check tests one thing — *this partition is empty and the definition says it should not
be* — and then names one specific cause. `ignore_sources`-driven archive residue is only
one way to reach that state; the parent report reached it through partition-swap residue,
and any interrupted or partial rebuild reaches it too. An operator who follows the label
investigates `ignore_sources` and finds nothing.

**Fix direction.** Say what was observed, not what caused it: *"Partition X is empty but
the IMV definition would populate it"*, with the cause left to the operator or listed as
alternatives. `category: "archive_residue"` is a stable machine key and can stay; it is the
human-readable `finding` string that over-claims.

Low risk, but note `pg_test_audit.rs:920` builds a genuine `ignore_sources` fixture and may
assert on the text.

---

## Defect 2 — `fix => true` never re-runs its checks, so one pass can report `fixed` while findings remain

**Confirmed structurally by code read** (`src/doctor.rs:160-221`, and the same shape in the
per-IMV loop at `:475-509`): the finding set is enumerated **once**, each finding is
repaired in place, and the outcome string is recorded per finding. Nothing re-evaluates the
checks after the repairs. A finding that only becomes reportable *because* of an earlier
repair in the same pass is therefore absent from that pass's output, while the findings that
were repaired print `fixed` and the list visibly shrinks.

The doctor does verify individual repairs (`verify_pending_drained` at `:259`,
`verify_stale_cleared` at `:379`), so an individual repair cannot lie about *itself*. That
is not the same as the pass converging.

**Measured in the parent report** (§5.3), on the now-fixed swap-residue fixture:

| pass | outcomes | dependent after |
|---|---|---|
| 1 | 2 × residue `fixed`; 3 × F3 `reported` | **2 of 3 partitions, 477774 — silently wrong** |
| 2 | 1 × residue (`pa_p1_c`) `fixed` | 3 of 3, 716661 — correct |
| 3 | no findings | correct |

**Caveat, stated honestly:** that reproduction no longer arises from `reflex_reconcile`
after the swap fix, so **a fresh reproduction is needed before implementing**. The
structural cause above is confirmed on the current tree; the *observable* consequence has
not been re-measured on it. Step 0 for this report is therefore: construct a fixture where
one doctor repair unmasks a second finding, and confirm pass 1 still prints only `fixed`.

**Fix direction.** After applying repairs in a `fix => true` run, re-run the checks and
either (a) append the residual findings to the same report, or (b) mark the pass `partial`
when any check still fires. (a) is more useful and makes the report self-describing; (b) is
smaller. Either way the operator must not be able to read a shrinking finding list as
"done".

Do **not** implement this as an unbounded repair loop — a finding whose remedy cannot clear
it would then spin. Bounded re-check (one extra evaluation), reporting whatever survives.

## Acceptance test

1. A fixture where repairing finding A makes finding B reportable. A single
   `reflex_doctor(fix => true)` must not present a clean or shrinking list while B holds —
   it must either report B or mark the pass `partial`.
2. A fixture where every finding is genuinely cleared must still report a clean pass (no
   false `partial`).
3. Both shown RED against the current behaviour.
