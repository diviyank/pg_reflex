# 2026-07-28 — the alter-source staleness alarm is suppressed by NAME SHAPE, so a user IMV named `<root>__…` is silenced

**Status: untreated.** Split out of
`2026-07-28_partitioned_reconcile_destroys_dependent_imvs.md` §5.1 (where it was marked
*inferred*) while fixing that report on `fix/swap-ddl-destroys-dependents`. **Now confirmed
by code read**; not yet reproduced against a live fixture.

Severity: **low-medium.** Not wrong data on its own — it removes the only automatic signal
that a consumer went stale, in one specific naming case. It becomes serious only in
combination with something that actually leaves the consumer stale.

## The code (`src/lib.rs`, `__reflex_on_ddl_command_end`)

```sql
IF _reconcile_root IS NOT NULL
   AND ( _imv.name = _reconcile_root
         OR split_part(_imv.name, '.', 2)
            = split_part(_reconcile_root, '.', 2)
         OR split_part(_imv.name, '.', 2)
            LIKE split_part(_reconcile_root, '.', 2) || '\_\_%' )
THEN
    CONTINUE;
END IF;
```

`_reconcile_root` is set by `reflex_reconcile_with_orphans` around its
`DISABLE/ENABLE TRIGGER USER` on each generated sub-IMV of the chain it is rebuilding
(`src/reconcile.rs:850-856`), and by `reconcile_named_node` around a materialised
UNION-ALL operand rebuild (`src/reconcile.rs:1240-1242`). Its purpose is correct: those
ALTERs are pg_reflex's own, and the consumers they name are members of the chain already
being rebuilt.

The third disjunct implements that with a **name-shape test**. `create_reflex_ivm` names
generated sub-IMVs `<root>__<something>`, so the pattern matches them — but it matches any
IMV with that name, including a **user-declared** one. Nothing in the predicate consults
provenance.

The registry already carries the fact the predicate wants: `is_generated_sub_imv`
(`src/lib.rs` bootstrap DDL; rebuilt by `src/graph_repair.rs:29-66`). The suppression could
test it directly, plus the edge from the root, instead of inferring provenance from a
string.

## Why it is not merely cosmetic

The first two disjuncts (`_imv.name = _reconcile_root`, and the same after stripping the
schema) already cover the root itself. The third exists solely to cover generated children.
A generated child is identifiable exactly; a user IMV that happens to be called
`sales__daily` next to a root called `sales` is not a generated child and, when the chain
descent's ALTER passes through, is a legitimate consumer that *did* miss a refresh. It is
silenced.

## What was ruled out

* **"The suppression is scoped tightly enough by `_reconcile_root` being set."** No — the
  GUC being set says *a* chain is being rebuilt, not that this particular consumer belongs
  to it. That is what the three disjuncts are for, and the third one over-matches.
* **"The partition-swap fix removes this path."** No. The swap's ALTERs are now suppressed
  via `pg_reflex.internal_swap_root`, which is a different mechanism. The
  `_reconcile_root` branch and its `LIKE` remain on the chain-descent path, untouched.

## Fix direction

Replace the `LIKE '<root>\_\_%'` disjunct with a registry lookup:

```sql
OR EXISTS (SELECT 1 FROM public.__reflex_ivm_reference g
            WHERE g.name = _imv.name
              AND g.is_generated_sub_imv
              AND g.depends_on_imv @> ARRAY[_reconcile_root])
```

— or whatever exactly expresses "generated member of the chain rooted at
`_reconcile_root`". Verify against the decomposed-chain fixtures, which are the only place
generated children exist; `src/tests/pg_test_decomposed_chain.rs` and
`src/tests/pg_test_union_operand_direct_reconcile.rs:253` already exercise this alarm
deliberately.

Watch the cost: this predicate runs once per (altered source × dependent IMV) pair inside an
event trigger. A correlated `EXISTS` per pair is fine at realistic graph sizes but should
not become a scan of the whole registry per pair.

## Acceptance test

1. A **user-declared** IMV named `<root>__x` reading a generated sub-IMV of `<root>`'s
   chain must still receive its stale WARNING when `reflex_reconcile('<root>')` suppresses
   triggers on that sub-IMV. RED today.
2. A genuine **generated** sub-IMV of the same chain must still be silenced (no spurious
   warning, and no abort under `alter_source_policy = 'error'`). Must stay GREEN — this is
   the property the suppression exists for, and a naive fix that drops the disjunct breaks
   it.
