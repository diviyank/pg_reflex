# Test harness: ScChain::commit reports a failed transaction as committed

**Severity:** false-green risk in the test suite (no product impact).
**Found:** 2026-10-14 by the scoped re-review of the 1.12.2 push-scoped reader
(ledger `2026-10-14-1.12.2-bulk-leaf-rebuild/Afix1-review.txt`, H1).

`ScChain::commit` (src/tests/pg_test_v112_swap_cascade.rs) uses `dblink(..., false)`, which keeps the
rows of the last row-returning statement and downgrades an error to a NOTICE. Any error after a
statement that returned a row (`set_config`, `SC_FLUSH`, `reflex_flush_deferred`), or at COMMIT,
reads as `Ok(())`. Proven: a deferred FK violation at COMMIT, and `SELECT 1; SELECT 1/0; SELECT 2`,
both return `Ok(())`. Tests asserting `Ok` + `refreshes == []` + oracles would stay green on an
aborted transaction (masked today because push errors are caught upstream and committed as stale).

Fix direction: `commit()` returns Err when `sc_xact` holds no row for the transaction. Planned at
1.12.2 integration, on the merged tree, then re-run every suite (it may surface hidden failures).
