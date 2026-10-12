# The DEFERRED flush of a partitioned passthrough that joins an ignored partitioned source with an observed aggregate IMV fails with `column "plan" does not exist` — S2 (loud, the IMV is flagged stale)

Pre-existing: seen on pg16 on `d0a3e06` (1.12.0 integration branch) while building the fixture of
`src/tests/pg_test_v112_swap_cascade.rs`; the shape was dropped from that fixture for this reason.
Not reduced to a minimal case, not tried in IMMEDIATE.

## Reproduction (pg16, every statement committed)

Sources and `sc_sfv` as built by `sc_open` in `src/tests/pg_test_v112_swap_cascade.rs`
(`sc_ss` is LIST(`plan`) x RANGE(`d`); `sc_sfv` is a keyed DEFERRED passthrough over it, partitioned
by `[plan, d]`). Then:

```sql
SELECT create_reflex_ivm('sc_total',
  'SELECT sf.product, SUM(sf.quantity) AS total FROM sc_sfv AS sf GROUP BY sf.product',
  'product', NULL, 'DEFERRED');
SELECT create_reflex_ivm('sc_share',
  'SELECT sf.plan, sf.product, sf.loc, sf.d, sf.quantity, pt.total
     FROM sc_sfv AS sf JOIN sc_total AS pt ON pt.product = sf.product',
  'plan, product, loc, d', NULL, 'DEFERRED', '!sc_sfv', ARRAY['plan']);
INSERT INTO sc_ss VALUES (3, '2027-02-20', 2027, 1, 1, 5, 5);
```

## Expected vs actual

Expected: `sc_total` receives the insert, and its changed total reaches `sc_share` (which observes
`sc_total` and ignores `sc_sfv`). Actual: the COMMIT succeeds, `sc_total` is right, and `sc_share`
is left with `known_stale = TRUE` and

```
deferred flush failed: column "plan" does not exist (SQLSTATE 42703). The staged delta was
discarded; run reflex_reconcile('sc_share') to repair.
```

Every later write repeats it (evidence:
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/logs/b-sc-probe4.out`, step
`plain insert` and the steps after it).

## What was ruled out

- Not the partition swap: a plain `INSERT` is enough.
- Not silent: the IMV is flagged and the reason names the failed flush.

## Fix direction (hypothesis)

`sc_share` is partitioned by a column its observed source (`sc_total`) does not have; the flush of
`sc_total`'s delta seems to look for the partition column on the delta's own rows. Either derive
the affected partitions through the join (none can be named here: every plan reads every total, so
the delta must be applied to all partitions), or refuse the shape at create.
