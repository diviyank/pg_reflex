# SDD Progress — untreated-findings-remediation

Branch: feat/untreated-findings-remediation
Base commit: d6f1834

Order: T4(F9) → T1(F1) → T2(F4) → T3(F2) → T9(F8) → T7(F5) → T8(F6) → T5(F7) → T6(F3) → T10(F4b) → T11(F10)

## Task log
Task 4 (F9): complete (commits d6f1834..ae0804b, review clean — spec ✅, quality approved)
Task 1 (F1): complete (commits ae0804b..21b8029, review clean — spec ✅, quality approved)
Task 2 (F4): complete (commits 21b8029..99486b6, review clean — spec ✅, quality approved; row.4 index confirmed unaffected)
Task 3 (F2): complete (commits 99486b6..9e768b3, review clean — spec ✅, quality approved; stale E0277 diagnostic confirmed not in committed code)
Task 9 (F8): complete (commits 9e768b3..297f6f7, review clean — spec ✅, quality approved; fn id() is required Check trait method)
Task 7 (F5): complete (commits 297f6f7..a026372, review clean — spec ✅, quality approved; Branch A, F6 interaction empirically confirmed, doc updated)
Task 8 (F6): complete (commits a026372..e46fe7c incl fix e46fe7c, re-review clean — spec ✅, quality approved; fixed CRITICAL false-negative: source quoting + no silent error swallow)
Task 5 (F7): complete (commits e46fe7c..bd49763, review clean — spec ✅, quality approved)
  MINOR (defer to final review): unit_partition.rs f7 detector test omits "missing-from-live" branch (code handles it; integration test covers functionally)
Task 6 (F3): complete (commits bd49763..9d8dfa8 incl fix 9d8dfa8, review clean opus max-rigor — spec ✅, quality approved; drop-safety predicate airtight; fixed Important: propagate DROP error)
  MINOR (defer to final review): F3 orphan-check skips silently on registry/resolve failure (safe direction=no drop; lacks a notice)
Task 10 (F4b): complete (commits 9d8dfa8..1571d7f incl fix 1571d7f, re-review clean opus — spec ✅, quality approved; fixed 3 CRITICALs: atomicity via pgrx::error!, jsonb extraction, fidelity tests; full suite 1292/1292)
  MINOR (defer to final review): F4b atomicity test is happy-path only; does not force create-failure-after-drop (property is guaranteed in code via pgrx::error!, opus-verified)
