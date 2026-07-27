# 2026-07-27 — `reflex_reconcile_partition` commits its destructive pre-sync even when it reports ERROR

**Status: untreated.** Found while root-causing the `alp.sop_forecast_view` emptying
(field report, two occurrences). The mass-drop itself is fixed on
`fix/sync-orphan-drop-empty-guard`; this report is about the *failure atomicity* that
let the drop survive a reported failure, which the guard does not address.

## The mechanism

`reflex_reconcile_partition_impl` (`src/partition.rs`, around line 1478) runs, **before**
and **outside** the `Spi::connect_mut` that performs the actual reconcile:

```rust
if !skip_sync {
    cleanup_orphan_swap_tables(view_name);
    let _ = reflex_sync_partitions_impl(view_name, true);   // DDL: CREATE / DROP … CASCADE
}
```

The reconcile body's failures are then surfaced as a **returned value**, not a raised
error (`src/partition.rs`, around lines 1805-1807):

```rust
match outcome {
    Ok(s)  => s,
    Err(e) => format!("ERROR: {}", e),
}
```

Because the error is returned rather than raised, the statement commits. Every DDL the
pre-sync performed — including `DROP TABLE … CASCADE` on IMV partition children — is
durable, while the operator sees a result row reading `ERROR: …` and reasonably concludes
nothing happened.

## Observed instance

```
SELECT reflex_reconcile_partition('alp.sop_forecast_view','dem_plan_id','941');
                 reflex_reconcile_partition
 ------------------------------------------------------------------
  ERROR: reconcile_partition: missing target bound for child 'sop_forecast_view_941'
 (1 row)
```

Note it is a **row**, not an aborted statement. (The call itself is also a misuse — arg 2
is `partition_keys`, arg 3 is `source_partition`; the caller passed a column name and a key
value. That misuse is harmless on its own, but it routes through the destructive pre-sync
first, so a typo'd argument was sufficient to arm a partition drop.)

## Why this is worth fixing independently of the guard

The guard removes the *known* way the pre-sync destroys data. It does not change the
property that **an arbitrary DDL side effect of a failed operation is committed and
reported as a failure.** Any future destructive step added to the pre-sync inherits the
same hazard, and the operator's mental model ("it errored, so it did nothing") stays wrong.

Severity: high when it fires (silent data loss presented as a no-op), low frequency
(requires the pre-sync to mutate and the reconcile to then fail).

## Fix direction

Options, roughly in increasing order of invasiveness — the choice is a real design call,
not obvious:

1. **Report honestly.** When the pre-sync mutated anything (`SyncResult` is already
   non-default) and the reconcile then fails, include what the sync did in the returned
   error string, so "ERROR" never reads as "nothing happened". Cheapest; does not restore
   atomicity.
2. **Bring the pre-sync inside the reconcile's transaction scope**, so a failed reconcile
   rolls its DDL back. Needs care: the sync takes an advisory xact lock and is currently
   deliberately outside the `Spi::connect_mut`; moving it may change lock lifetime and
   interact with the `skip_sync` batch path.
3. **Raise instead of returning `ERROR: …`.** Most faithful to the "refuse loudly"
   principle and would abort the transaction, but it is a breaking behavioural change for
   every existing caller that inspects the returned text — including `reflex_doctor`'s
   repair path and the batch flush. Would need an audit of call sites first.

Whichever is chosen, pin it with a test that fails the reconcile *after* a mutating
pre-sync and asserts the partition set is either intact (2) or the failure text names the
DDL that happened (1) — and mutation-check that assertion, since "state unchanged" is
exactly the kind of claim that goes false-green.
