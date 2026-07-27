# 2026-07-27 — `resolve_anchor_source` single-owner branch skips the `source_partitioned_on` check

**Status: untreated.** Found while root-causing the `alp.sop_forecast_view` emptying
(field report, two occurrences). Adjacent to — but distinct from — the empty-enumeration
mass-drop, which is fixed on `fix/sync-orphan-drop-empty-guard`. That fix makes this
defect non-destructive; it does **not** make it correct.

## The mechanism

`resolve_anchor_source` (`src/partition.rs`, around lines 895-953) dispatches on how many
`depends_on` sources own the partition column:

```rust
match owners.len() {
    0 => Err("no source table owns partition column '{}'"),
    1 => Ok(owners.into_iter().next().unwrap()),   // <-- no partitioned check
    _ => { /* filters by source_partitioned_on(), prefers base over __cte_/__union_/__base,
             errors "…but none is partitioned on it — ambiguous" when the pool is empty */ }
}
```

The multi-owner branch requires the anchor to be **partitioned on that column** before
accepting it, and refuses loudly when no candidate qualifies. The single-owner branch
accepts the sole owner unconditionally — a table that merely *has* the column is returned
as the anchor even when it is not partitioned at all.

The anchor is the relation whose partition children are physically mirrored onto the IMV.
A non-partitioned anchor makes `list_partition_tree(&anchor)` return empty, so:

- **Before the empty-enumeration guard:** every IMV partition was dropped (the emptying).
- **After the guard:** sync refuses the drop and warns, but still creates nothing and
  silently mirrors an anchor that can never supply children. The IMV's partition set
  quietly stops tracking any source.

## Why the asymmetry is wrong

The two branches encode contradictory definitions of "anchor". With two owners, a
non-partitioned candidate is explicitly disqualified as unable to anchor child DDL
(the code comment says so: "A bare column on a non-partitioned source … cannot be the
anchor"). With one owner, that same relation is accepted. The count of *other* sources
has no bearing on whether a given relation can anchor partition mirroring.

## Reproduction

Not yet pinned in a test. Construct a partitioned IMV whose `partition_columns[0]` is
owned by exactly one `depends_on` entry that is itself **not** partitioned, then call
`reflex_sync_partitions(view, FALSE)`. Expected today: `Ok(anchor)` on a non-partitioned
relation, empty tree, no children created, no error. Expected after fix: the same
"not partitioned on it" refusal the multi-owner branch already emits.

Suspected live instance: `alp.sop_forecast_view` — worth confirming with
`SET pg_reflex.debug_resolve_anchor = on;` before `reflex_sync_partitions(view, FALSE)`,
which prints each source's `to_regclass` and column ownership.

## Fix direction

Apply `source_partitioned_on(client, s, &col)` in the single-owner branch too, and on
failure return the same class of error the multi-owner branch raises rather than a
silently unusable anchor. Guard against widening the refusal: an IMV whose anchor is
legitimately partitioned must be unaffected, and the existing single-owner tests must
stay green — so the check must be on *partitioned-on-the-column*, not merely
*partitioned*.

A second, independent question this raises: `resolve_anchor_source` returns the raw
`depends_on` string, and every downstream `to_regclass` on it is `search_path`-dependent.
Whether bare (unqualified) `depends_on` entries can resolve differently between the
event-trigger session and an operator session is not yet established, and is the other
candidate root cause for the field emptying. Tracked here as a note; split it out if
confirmed.
