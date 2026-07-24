# 2026-07-24 — nothing checks (or can reinstall) the `__reflex_union_mirror_*` triggers a materialised UNION-ALL wrapper depends on

**Status: untreated.** Residual coverage gap left deliberately by PS-10, which
stopped `trigger-attached` from false-positiving on decomposed wrapper rows
(`src/audit/checks_a_catastrophic.rs`, `TriggerAttached::run`).

## What PS-10 fixed, and what it left

`trigger-attached` iterated a wrapper row's `depends_on` — which for a decomposed
wrapper is its own sub-IMVs — and demanded the consolidated
`__reflex_trigger_{ins,del,upd,trunc}_on_<sub-IMV>` set there. Probed on pg17:

- A **VIEW** wrapper (top-level `UNION ALL`, `UNION`/`INTERSECT`/`EXCEPT`,
  DISTINCT ON, window) has no triggers on its operands at all, by design.
- A **materialised** UNION-ALL wrapper (`install_union_all_intermediate_wrapper`,
  used when a CTE body is consumed by an aggregate) is maintained by
  `__reflex_union_mirror_{ins,del,upd}_<wrapper>_<operand_idx>` triggers ON each
  operand — never by the consolidated set.

so the finding was a false positive in both cases, and its remedy was actively
harmful (see below). PS-10 skips wrapper rows. Consequence: **the mirror triggers
are now checked by nothing.** If one is dropped, the materialised wrapper silently
stops receiving that operand's deltas, and every IMV reading the wrapper drifts.

## Why the check was not simply retargeted

Because there is no primitive that reinstalls a mirror trigger.
`install_union_mirror_triggers` (`src/create_ivm/decompose.rs:307`) runs only at
create time. A check that reported mirror-trigger absence today could only print
an unclearable remedy — the exact B5/B9/PS-10 anti-pattern being deleted. The
repair primitive has to come first.

Probe evidence that the *old* remedy was worse than silence — `reflex_rebuild_triggers`
on a sub-IMV target, which is what the check printed:

```
SELECT reflex_rebuild_triggers('tv__union_0')
=> pg_reflex: rebuilt 8 trigger DDL(s) for 'public.tv__union_0'

triggers on tv__union_0 afterwards:
  __reflex_trigger_del_on_public_tv__union_0
  __reflex_trigger_ins_on_public_tv__union_0
  __reflex_trigger_trunc_on_public_tv__union_0
  __reflex_trigger_upd_on_public_tv__union_0

reflex_audit('tv') afterwards: STILL reports the same trigger-attached Error
  (it expects `__reflex_trigger_ins_on_tv__union_0`, without the schema prefix)
```

i.e. it installed four consolidated triggers that do not belong on a sub-IMV
target, and did not clear the finding, so each retry added more.

## Fix direction

1. Add a repair primitive — e.g. extend `reflex_rebuild_triggers` (or a new
   `reflex_rebuild_union_mirror(<wrapper>)`) to re-emit
   `install_union_mirror_triggers` for a decomposed wrapper row, deriving the
   operand index from the position in `depends_on` (the same order
   `try_decompose_set_op` used to build `sub_imv_names`).
2. Then, and only then, extend `trigger-attached` (or add a `mirror-trigger`
   check) to expect `__reflex_union_mirror_{ins,del,upd}_<sanitized wrapper>_<i>`
   on operand `i` when the wrapper relation is a TABLE, and nothing when it is a
   VIEW, prescribing (1).

## Severity

S3. No known field occurrence; requires someone to drop a `__reflex_%` trigger by
hand or a partial restore. But the failure mode is silent divergence of every IMV
downstream of the wrapper, which is the family this project treats most seriously.
