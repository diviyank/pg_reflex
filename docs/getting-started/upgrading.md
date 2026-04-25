# Upgrading

Existing IMVs, triggers, and registry data are preserved across upgrades. There is **no need to recreate views**.

## From the prebuilt `.deb` package

```bash
sudo dpkg -i pg-reflex-NEW_VERSION-pg17-amd64.deb
psql -d mydb -c "ALTER EXTENSION pg_reflex UPDATE TO 'NEW_VERSION';"
```

## From source

```bash
cd pg_reflex
git pull
./install.sh --release --pg-config $(which pg_config)
psql -d mydb -c "ALTER EXTENSION pg_reflex UPDATE;"
```

## Migration chain

| From → to | What changes | Operator action |
|---|---|---|
| 1.0.0 → 1.0.1 | bug fixes, no schema changes | none |
| 1.0.1 → 1.0.2 | UNLOGGED target tables, hash index, MERGE RETURNING | none |
| 1.0.2 → 1.0.3 | `storage_mode`, `refresh_mode` columns; deferred infra | none |
| 1.0.3 → 1.0.4 | persistent affected-groups table, predicate-filter skip | none |
| 1.0.4 → 1.1.0 → 1.1.1 | DROP CASCADE, FILTER + DISTINCT ON | none |
| 1.1.1 → 1.1.2 → 1.1.3 | algebraic BOOL_OR, perf | none |
| 1.1.3 → 1.2.0 | observability columns, event triggers, scoped MIN/MAX recompute | none |
| 1.2.0 → 1.2.1 | `pg_reflex.alter_source_policy` GUC, scheduled reconcile, PK inference UX | none |
| 1.2.1 → 1.3.0 | top-K MIN/MAX (opt-in), flush histogram, pg_stat_statements tagging | none for existing IMVs; opt-in to top-K per IMV |

`ALTER EXTENSION pg_reflex UPDATE` walks the chain automatically.

## After upgrade

Run the smoke check:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'pg_reflex';
SELECT count(*) FROM public.__reflex_ivm_reference;
SELECT * FROM reflex_ivm_status();
```

If any IMV shows `last_error` after upgrade, run `SELECT reflex_rebuild_imv('<name>')`.

[Operations runbook :material-arrow-right-bold:](../operations/runbook.md){ .md-button }
