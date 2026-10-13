# IMVs created before 1.12.0 over two sources that share trigger names (`s.t` and `s_t`) stay silently stale after the upgrade — S1 (SILENT wrong result, existing deployments only)

1.12.0 refuses at create a source whose trigger-name suffix equals that of another relation
(`67f27a3`; the report
`2026-10-10_sources_schema_dot_table_and_schema_underscore_table_share_trigger_functions.md` was
removed with that fix). The refusal does nothing for a pair created earlier. Source:
`p3-a-report.md` (item 2 Step 0, concerns of rounds 1 and 3) and the probe output
`logs/a-probe0-partition-suffix.txt`, in the ledger
`.superpowers/sdd/2026-10-07-1.12.0-create-contract-hardening/`; pg17, tree `d0a3e06` before the
guard. Not checked on 1.11.5.

## Reproduction (pg17, before the guard; one transaction)

```sql
CREATE SCHEMA zzs;
CREATE TABLE zzs.t(id INT PRIMARY KEY, v INT);
CREATE TABLE zzs_t(id INT PRIMARY KEY, v INT);
SELECT create_reflex_ivm('a', 'SELECT id, v FROM zzs.t');
SELECT create_reflex_ivm('b', 'SELECT id, v FROM zzs_t');
INSERT INTO zzs.t VALUES (1, 1);
SELECT count(*) FROM a;      -- 0; zzs.t holds 1 row
```

No error. After the second create, `pg_trigger` listed the four reflex triggers on `zzs_t` only
(from the removed report).

## What Step 0 of the fix established (both creation orders, IMMEDIATE and DEFERRED)

- The reverse order reproduces too: whichever IMV is created first goes stale.
- IMMEDIATE: after the second create **both** IMVs are stale after writes, in both orders.
- DEFERRED: the first IMV is stale, the second maintained.
- Dropping the second IMV removes the shared triggers: the first stays unmaintained for good, no
  error.

## Expected vs actual

Expected on an upgraded database that holds such a pair: the IMVs are reported
(`reflex_audit`, `reflex_doctor`) and a remedy that works is printed. Actual: they stay stale
until recreated, and the second recreate is then refused by the new guard.

## What is established and what is not

Established: the four points above, before the guard. Not established:
- what `reflex_audit` and `reflex_doctor` say about such a pair ("I did not check what
  `reflex_audit` says about such a pair", `p3-a-report.md`);
- the remedy for this very case. The guard prints one of three remedies depending on what the
  colliding relation is; three are performed and followed by the oracle in tests
  (`v112_cr_trigger_names_of_an_imv_target_against_a_read_table`,
  `…_of_a_free_table_against_an_imv_target`, `…_of_a_generated_table_against_a_read_table`). The
  branch "both relations are base tables already read by IMVs", reachable only for IMVs created
  before the guard, is **not proven**. Its text is "drop the IMVs that read <t>, rename that
  table, and create them again";
- the upgrade itself: nobody ran this pair through `ALTER EXTENSION … UPDATE`.

## Fix direction (hypothesis to falsify)

Add an audit check: two registered sources (entries of `depends_on`, across IMVs) with the same
`source_trigger_suffix` and different `to_regclass` are an ERROR finding naming both and the
remedy above; prove the remedy on a pair created with the guard disabled (drop the IMVs of one
table, rename it, recreate, reconcile the IMVs of the other, oracle on both). The durable cure is
an injective suffix (a hash of the registered spelling for a schema-qualified source), which
needs a dual-name lookup in drop, rebuild and audit because the names are stored in deployed
databases.
