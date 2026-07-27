# Adversarial review of `649b4d4` / `e38e38d` / `a592ac9` — VERDICT: DO NOT MERGE

Reviewed on pg16.11 (`--enable-cassert`), `CARGO_TARGET_DIR=/private/tmp/rfx-b1`.
Full suite on the committed tree: **1547 passed / 0 failed** — which is exactly why the
suite is not a safety net here: every crashing path needs a *raised* error inside the
reconcile, and no existing test produces one.

## CONFIRMED 1 — CRITICAL: `SubTransaction` has no `Drop`; a raised error leaks it and aborts the backend

`src/partition.rs:56-95` (no `impl Drop`), armed at `:1563`.

`Spi::connect_mut` does not catch query errors. pgrx 0.18 `spi.rs:401-429` is `SPI_connect`
+ closure, relying on transaction abort for cleanup. `SpiClient::update`'s `Result` carries
only SPI *status* codes; a real SQL error longjmps out of `SPI_execute`, becomes a Rust
panic, unwinds Rust frames **running no `Drop`**, and is re-`ereport`ed at the `#[pg_extern]`
boundary. The subtransaction — and the SPI connection opened inside it — are still live
when the error reaches the caller.

If the caller is plpgsql with an `EXCEPTION` handler, its `PG_CATCH` calls
`RollbackAndReleaseCurrentSubTransaction()` on *our* subtransaction, `AtEOSubXact_SPI`
force-pops the leaked SPI entry underneath it, and plpgsql's econtext stack no longer
matches:

```
TRAP: failed Assert("simple_econtext_stack->stack_econtext == estate->eval_econtext"),
      File: "pl_exec.c", Line: 8473, PID: 65419
LOG:  server process (PID 65419) was terminated by signal 6: Abort trap: 6
LOG:  all server processes terminated; reinitializing
```

The doc comment at `:49-51` ("a raised error ... aborts the whole transaction, which
discards this subtransaction along with it") is true **only when nothing catches**.

### Reproduction A — real in-tree doctor path, plain psql, one statement

`reflex_doctor(fix => true)` → `apply_doctor_repair` (`src/doctor.rs:879-890`) →
`__reflex_doctor_try_repair` (`src/lib.rs:337-350`, plpgsql with `EXCEPTION WHEN OTHERS`),
handed `SELECT reflex_reconcile_partition('<imv>','','<child>')` built at
`src/audit/checks_d_residue.rs:35` / `src/doctor.rs:754` — 3 args, `skip_sync = false`,
subtransaction armed. An ordinary DDL failure suffices: `drop_old_tgt` is `DROP TABLE`
with no CASCADE (`src/partition.rs:763`), so any dependent view blocks it.

```sql
CREATE VIEW advxa_pin AS SELECT * FROM advxa_v_advxa_src_1;
SELECT public.__reflex_doctor_try_repair(
  $q$SELECT reflex_reconcile_partition('advxa_v','','advxa_src_1')$q$);
-- server closed the connection unexpectedly   (SIGABRT, cluster reinitializes)
```

**Mutation proving it is this branch:** `git checkout f74fc56 -- src/partition.rs`, same
`.so` reinstalled → the identical statement returns cleanly
`failed:cannot drop table advxa_v_advxa_src_1 because other objects depend on it`.

### Reproduction B — narrowing probes

| probe | HEAD (`a592ac9`) | pre-fix (`f74fc56`) |
|---|---|---|
| R1: raise with no plpgsql catcher | survives | survives |
| R2: raise caught by `DO … EXCEPTION` | **SIGABRT** | `do_ok=true probe=caught` |
| R3: raise caught by `__reflex_doctor_try_repair` | **SIGABRT** | `out=failed:invalid input syntax…` |

R1 is why the author's "nothing catches → abort cleans up" reasoning looked sound.

### Reachability is wider than the doctor

`src/trigger/deferred.rs:822-853` wraps **every IMV's deferred flush statements** in
`DO $_reflex_imv_sp$ BEGIN … EXCEPTION WHEN OTHERS …`. Those include the partition dispatch
calling `PERFORM public.reflex_reconcile_partition('{view}', …)` (2 args, `skip_sync=false`)
at `src/trigger/dispatch.rs:346` and `:573`. So the ordinary write path for a partitioned
IMV with a hot partition has the crashing shape: any "would overlap partition", lock
timeout, deadlock, permission error or OOM inside the swap crashes the backend.
(Mechanism confirmed; end-to-end deferred-flush repro not built — the doctor path already
proves it with real in-tree code.)

**On cassert:** in a release build the Assert compiles out and
`plpgsql_destroy_econtext` pops a mismatched `ExprContext` while the caller's own
subtransaction is never released — silently wrong state instead of a clean stop. Same
defect, alarm removed. (Reasoned, not measured — no non-cassert PG available.)

**Minimum fix direction:** `SubTransaction` needs a `Drop` that rolls back if neither
`release` nor `rollback` ran — and even then, unwinding out of a `Spi::connect_mut` whose
SPI connection was opened *inside* the subtransaction needs care: either open the
subtransaction so it does not enclose an SPI connection it can force-pop, or catch with
`PgTryBuilder` rather than letting the panic escape.

## CONFIRMED 2 — HIGH: the batch path is fully unprotected, and its justification is inverted

Gate at `src/partition.rs:1563`; false justification at `:1547-1549`, which claims the
caller's plpgsql `EXCEPTION` block "is one". A plpgsql `EXCEPTION` block *is* a
subtransaction, but it only rolls back when an error is **RAISED**.
`reflex_reconcile_partition` signals failure by **returning** `ERROR: …`, so the block
completes normally and **releases** — committing everything. The premise is exactly
inverted.

The batch path is the one that matters: `src/partition.rs:2963-2966` issues
`PERFORM public.reflex_reconcile_partition(imv, '', <comma-joined nodes>, true)` — one
call, many children, `skip_sync = true`.

Constructed A/B, 8 real leaves + 1 bogus, same call shape as the flush:

```
ADV-B: skip_sync[ERROR: … missing intermediate bound for child '…ghost_child']  repaired=8/8
    || protected[same ERROR]                                                    repaired=0/8
```

All eight children DETACH/ATTACH-swapped and **committed** while the call reported `ERROR`.
This is precisely claim 2 ("children 1..N-1's swaps previously committed"), unfixed, on the
automatic path. Reached via returned `Err` per child from the pre-checks at `:2043-2054`,
so it is not made moot by Finding 1's "real DDL errors raise".

**T4 (`pg_part_skip_sync_reconcile_opens_no_subtransaction`) pins this defect in place.**
It must be deleted, not adjusted, when this is fixed.

## CONFIRMED 3 — MEDIUM-HIGH: new report accurate but understates residual; old report deleted prematurely

`src/partition.rs:3108-3128`. The new report's claim is confirmed by construction
(`ADV-C: probe=post-reconcile-statements-ran`): the `EXCEPTION` branch never fires, so
`__reflex_refresh_partition_snapshot(root)` and
`DELETE FROM __reflex_partition_pending WHERE source_root = root` both run — `failures`
stays 0, `known_stale` never set, no WARNING, pending row gone, evidence destroyed.

What it omits: `root_stmts` puts `PERFORM public.reflex_sync_partitions(imv, true)` (`:2936`)
and `DROP TABLE IF EXISTS "<schema>"."<child>" CASCADE` (`:2944`, `:2948`) **before** the
reconcile in the same block. Since the block commits, the deleted report's stated property
— "an arbitrary DDL side effect of a failed operation is committed and reported as a
failure" — is **not closed**. It moved from the Rust pre-sync to the plpgsql pre-sync, plus
the partial swaps of Finding 2. The report should have been **narrowed, not deleted**.

## CONFIRMED 4 — MEDIUM: failure path drops the advisory lock while the caller keeps working

Lock at `src/partition.rs:1176`; rollback at `:1918`. Measured in one transaction:

```
locks_after_failed = 0     -- reconcile returned ERROR (subxact rolled back)
locks_after_ok     = 1     -- reconcile succeeded
```

Claim 6's premise is right; its conclusion ("safe because the function does no further
DDL") is true of the *function*, false of the *transaction*. At
`src/trigger/dispatch.rs:346`, the `PERFORM` discards the `ERROR:` string and the dispatch
block proceeds to `{merge_execs}`, `ANALYZE`, `{dead_cleanup}`, `{tdel}`, `{tins}` — real
DML on the same IMV, now without the two-key advisory lock the pre-fix code held for the
rest of the transaction. (Lost invariant CONFIRMED by code + lock measurement; a two-session
interleaving producing wrong data is PLAUSIBLE, not built.)

## CONFIRMED 5 — LOW: T5 does not pin the property its docstring claims

`pg_part_reconcile_keeps_two_key_advisory_lock_after_subtransaction` stays **green** under
M1 (savepoint removed). It pins only the two-key form, never the subtransaction
interaction, and never exercises the failure path — where the lock is in fact gone.

## Mutation matrix — re-run independently

| mutation | T1 | T2 | T3 | T4 | T5 |
|---|---|---|---|---|---|
| M1 `subxact = None` | **RED** | **RED** | **RED** | green | green |
| M2 unconditional `Some(SubTransaction::begin())` | — | — | — | **RED** | — |
| M3 one-key `pg_advisory_xact_lock(hashtext($1)::bigint)` | — | — | — | — | **RED** |

Each RED for the intended reason. **The author's reported matrix is accurate.** The T2/T3
`ALTER EVENT TRIGGER … DISABLE` trick is legitimate — scoped to the two
`CREATE TABLE … PARTITION OF` statements, re-enabled before the reconcile. The false green
is T4's *premise* (Finding 2), not its mechanics.

## Attacks that found nothing

- **Claim 1** (`Spi::connect_mut` is not a subtransaction): verified against pgrx 0.18
  `spi.rs:401-429`. Corrected doc comments are right; the old ones were wrong.
- **Claim 5's error predicate** (`sync.starts_with("ERROR")`, `:1584`): no misclassification
  constructible. Every failure return begins `"ERROR: "`; `SyncResult::into_message`
  (`:1032-1052`) always begins `"sync: +…"` and appends the refusal mid-string. Fragile in
  style, not currently wrong.
- **Nested reconcile / double-`Release`**: cascade at `:1886` opens a nested `SubTransaction`
  inside the parent's while the parent's `SpiClient` is live. Success path unbroken;
  `release`/`rollback` both taking `self` by value makes double-run impossible. Failure path
  subsumed by Finding 1.
- **Memory-context / resource-owner bookkeeping**: `begin`/`restore` faithfully mirror
  plpgsql's `exec_stmt_block`. What is missing is the *unwind* case, not the ordered case.
- **Unclearable-finding retry loop**: none introduced. Findings 2/3 are the opposite
  pathology — a queue that clears itself when it should retry.

## Housekeeping on the committed tree

- `cargo fmt --check` clean.
- `cargo clippy --all-targets --features pg16 --no-default-features` — 4 `needless_borrow`
  warnings, all `src/tests/pg_test_audit.rs:1251,1339,1340,1341`, untouched by this branch.
- `cargo pgrx test pg16` full serial run: `1547 passed; 0 failed` in 57.83s.

## Severity ranking

1. **Finding 1** — backend SIGABRT / cluster restart on doctor-repair and deferred-flush paths. Blocking.
2. **Finding 2** — batch path commits partial swaps on failure; T4 locks it in.
3. **Finding 3** — flush block commits its own destructive DDL and self-drains the pending queue; old report deleted while its core property is open.
4. **Finding 4** — advisory lock dropped early, dispatch continues unserialized.
5. **Finding 5** — T5 does not test what it documents.
