# Upgrading

Existing IMVs, triggers, and registry data are preserved across upgrades. Unless the notes of a release say otherwise, there is no need to recreate views. **1.12.0 says otherwise**: see [Upgrading to 1.12.0](#upgrading-to-1120).

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
| 1.3.0 → 1.4.0 | top-K auto-enabled (K=16) on freshly created IMVs, N1 heap-shrinkage gate, non-NUMERIC top-K element types, UPDATE staleness fix | none for existing IMVs; the migration provisions `__reflex_shrunk_<view>` for IMVs already on top-K. Existing top-K IMVs over `TEXT` / `DATE` / `TIMESTAMP` should run `reflex_rebuild_imv('<name>')` to pick up the corrected trigger codegen. |
| 1.4.0 → 1.4.1 | bug fix: internal reflex tables and trigger-body SPI calls are now schema-qualified so DML under `SET search_path = '<schema>'` (excluding `public`) no longer fails with `relation "__reflex_delta_<…>" does not exist` | **breaking for existing IMVs**: the upgrade cannot rewrite already-installed trigger function bodies or move legacy bare-name internal tables. Drop and recreate every existing IMV (`SELECT drop_reflex_ivm('<name>'); SELECT create_reflex_ivm('<name>', '<SELECT …>', …);`). The migration script emits a per-IMV `NOTICE` listing what to rebuild. |
| 1.5.1 → 1.6.0 | partitioning Phase 1 + Phase 2 (`partition_by`, `reflex_sync_partitions`, `reflex_reconcile_partition`, atomic DETACH/ATTACH swap, per-partition trigger dispatch); new catalog columns (`partition_columns`, `partition_strategy`, `wipe_floor_rows`, `partition_dispatch_cost_cap`); event-trigger widened to `CREATE TABLE PARTITION OF`; mixed-case quoted column-name fix carried over from unreleased 1.5.2 | none for non-partitioned IMVs; the migration re-emits trigger function bodies so the mixed-case codegen takes effect. Operators with existing mixed-case quoted source columns should DROP + recreate the affected IMVs to get case-preserved target column names. |
| 1.6.0 → 1.6.1 | PG 18 compatibility (partitioned parents now LOGGED, children UNLOGGED — see [`CHANGELOG.md`](../changelog.md)); CI hygiene; internal pipeline refactor.  No catalog or trigger changes. | none for existing IMVs.  Advisory: if you `pg_upgrade` to PG 18 a cluster that has partitioned IMVs created on 1.6.0, drop and recreate those IMVs first so they are recreated with LOGGED parents. |
| 1.6.1 → 1.6.2 | deferred trigger body uses named-column INSERT (fixes `column "X" is of type Y but expression is of type Z` after source DDL drift); `reflex_rebuild_triggers` is now deferred-aware; create_ivm guards staging shape against drift. The migration repairs every drifted `__reflex_delta_<src>` and re-emits trigger bodies. | none for existing IMVs whose source schemas have not drifted. **If a previously-existing IMV's source was dropped+recreated (e.g. to add partitioning on PG ≤ 17), the migration will drop the stale staging delta — including any pending deferred rows.** Run `SELECT reflex_flush_deferred('<src>')` on each affected source BEFORE upgrading if those rows matter. |
| 1.6.2 → 1.6.3 | CTE / window-function decomposition correctness: CTE decomposition runs before window/DISTINCT-ON decomposition (window in the top-level SELECT over CTEs no longer fails with `relation "<cte>" does not exist`); a window nested in a subquery/derived table is rejected instead of crashing (SIGSEGV); a window / `DISTINCT ON` inside a CTE referenced by an outer query is rejected with guidance (`kind: mv`); `partition_by` propagates to CTE sub-IMVs; `MAX`/`MIN` over a table-qualified non-numeric column (e.g. `MAX(t.ts)` on `timestamptz`/`date`/`text`) no longer fails with `… is of type numeric …`. No catalog or trigger changes (no-op migration marker). | none for existing IMVs. Views kept as `kind: mv` because they nest a window / `DISTINCT ON` inside a referenced CTE stay `kind: mv` — that shape is still not an IMV, but now fails fast with guidance rather than crashing. |
| 1.6.3 → 1.6.4 | Correctness fixes hardened by a new differential fuzz harness. **Runtime** (reach existing IMVs on recompile): LEFT/RIGHT JOIN secondary-side maintenance no longer drops/duplicates rows; DEFERRED flush no longer raises `duplicate key value violates unique constraint` on insert-then-update of the same key in one batch; filtered-IMV `WHERE` is alias-stripped. **Create-time** (newly-created IMVs only): structural NOT-NULL inference replaces the unsound runtime data-probe that could silently drop rows; long generated identifiers truncated to 63 bytes; an aggregate IMV with a `GROUP BY` key not projected bare is rejected. No catalog or trigger changes (no-op migration marker). | none required for the runtime fixes. **The create-time NOT-NULL fix does not reach aggregate IMVs created under ≤1.6.3** — the over-promotion is baked into stored metadata and the intermediate-table schema, and cannot be corrected in place. Drop and recreate any aggregate IMV created before 1.6.4 to clear a latent over-promotion: `SELECT drop_reflex_ivm('<name>'); SELECT create_reflex_ivm('<name>', '<SELECT …>', …);` |
| 1.6.4 → 1.6.5 | Three create-time correctness fixes for view shapes hit during real migrations. A `DEFERRED` IMV over a CTE no longer fails at creation with `zero-length delimited identifier at or near ""` (a CTE sub-IMV's already-quoted schema was being re-quoted into `""schema""`). Explicit `unique_columns` are now threaded into the outer passthrough IMV of a CTE query (previously dropped, so a JOIN passthrough over CTEs silently fell back to full refresh on DELETE/UPDATE). `MIN`/`MAX` over a **materialized-view** column no longer fails with `… is of type numeric …` — column types now come from `pg_catalog` (which covers matviews) instead of `information_schema.columns` (which omits them), completing the 1.6.3 type-resolution fix. No catalog or trigger changes (no-op migration marker). | none required for fixes 1 and 3 (they unblock shapes that could not be created before). **For the `unique_columns` fix, an IMV built from a CTE query under ≤1.6.4 keeps its empty key (full refresh on DELETE/UPDATE).** Drop and recreate it with an explicit key to gain incremental DELETE/UPDATE: `SELECT drop_reflex_ivm('<name>'); SELECT create_reflex_ivm('<name>', '<SELECT …>', '<key cols>');` |
| 1.6.5 → 1.7.0 | Refactor + correctness for intermediate UNION-ALL CTE-body wrappers (wrapper construction centralised; three defects fixed). No catalog, trigger-body, or signature changes. | none for existing IMVs. |
| 1.7.0 → 1.7.1 | Correctness fix for Path C (the `INSERT_PROMOTED` bulk-INSERT dispatch fired by UPDATE triggers): relation names now go through `safe_identifier` (63-char truncation+hash) instead of hand-concatenated `split_part`. The migration re-emits trigger bodies. | none for existing IMVs; the migration rebuilds trigger function bodies. |
| 1.7.1 → 1.7.2 | Correctness fix for `drop_reflex_ivm` orphaning the target/aux tables of an IMV created with a **bare** name under a non-public `search_path` (new `target_schema` column records the creation schema). | none for IMVs created under `public`. IMVs created bare under a custom `search_path` are now dropped cleanly; the migration `NOTICE` lists any pre-existing orphans to clean up manually. |
| 1.7.2 → 1.7.3 | Create-time correctness: failed decomposed-IMV creation no longer orphans already-created sub-IMVs (soft-error rejections roll the partial sub-IMVs back); partition-anchor resolution refined. Compiled-only — no catalog or signature change. | none for existing IMVs. |
| 1.7.3 → 1.7.4 | Partitioned-IMV creation correctness: partition-anchor resolution accepts sources co-partitioned on the join key and ignores sources partitioned on a different column. Compiled-only — no catalog or signature change. | none for existing IMVs. |
| 1.7.4 → 1.7.5 | Widened CTE/JOIN passthrough unique-key inference (chained-CTE cascades auto-resolve sound keys). One additive catalog column (`max_one_row`, default FALSE); no backfill. | none for existing IMVs; inference re-runs at create time and existing IMVs keep their stored keys. |
| 1.7.5 → 1.7.6 | `ignore_sources` is now honored on the DEFERRED trigger path (the three deferred trigger bodies + `reflex_flush_deferred`), closing a gap where only IMMEDIATE honored it. No catalog change; the migration rebuilds source triggers via `reflex_rebuild_triggers`. | none for existing IMVs — the migration re-emits trigger bodies so the fix applies without recreating IMVs. A source whose rebuild fails (dropped table, ambiguous bare name) is logged as a `NOTICE` and skipped. |
| … 1.7.6 → 1.10.5 | See the [changelog](../changelog.md) for each intermediate release. `ALTER EXTENSION pg_reflex UPDATE` applies every step automatically. | see changelog per version. |
| 1.10.5 → 1.10.6 | The `sql_drop` event trigger now also cleans up an IMV whose own **target** table is dropped (directly, via `DROP SCHEMA … CASCADE`, or by dropping a view the IMV is built on), not only IMVs whose *source* was dropped. The migration re-creates the `reflex_on_sql_drop` plpgsql function; no catalog or signature change. | none for existing IMVs. Pre-1.10.6 orphaned registry rows are **not** retroactively cleaned — drop them with `drop_reflex_ivm(name, true)`, or `DELETE FROM public.__reflex_ivm_reference` for rows whose target is already gone. |
| … 1.10.6 → 1.11.3 | See the [changelog](../changelog.md) for each intermediate release. | see changelog per version. |
| 1.11.3 → 1.11.4 | Silent-wipe observability and prevention. A failed deferred flush now marks the IMV `known_stale` (it previously discarded the staged delta silently); `reflex_ivm_status()` gains `is_estimate`, counts anomalous IMVs exactly, and reports IMVs whose partition source is capped; new `__reflex_event_log` table and `reflex_prune_event_log`; new `pg_reflex.flush_failure_policy` GUC (`warn` default, opt-in `error`); **`create_reflex_ivm` refuses an unsound `ignore_sources` entry** unless acknowledged with a `'!'` prefix or `reflex_ack_ignore_source`; `reflex_audit` / `reflex_doctor` (F13) report existing ones; a change to an ignored source that joins onto the partition key queues the affected partitions for heal (`reflex_heal_ignored_sources`, run by `reflex_scheduled_reconcile` and doctor F14). The migration adds `ignore_ack`, the event log, the heal queue, recreates `reflex_ivm_status()` (return-shape change), and installs heal triggers on existing partitioned IMVs. | See [Upgrading to 1.11.4](#upgrading-to-1114) below: upgrade in a quiet window with `lock_timeout` set, treat `HEALED` from `reflex_scheduled_reconcile` as success, and add the `'!'` marker to accepted unsound ignores before recreating any IMV or running `reflex_rebuild_chain`. |
| 1.11.4 → 1.11.5 | Rebuilds of an IMV with dependents (source TRUNCATE, `reflex_reconcile`, wipe dispatch, partition rebuilds) hand dependents a row diff instead of clearing or fully rebuilding them; a DEFERRED IMV whose source is truncated is rebuilt once at COMMIT; volume dispatch sizes two-level partitions by their leaves and uses a 0.9 threshold for IMVs with dependents; a multi-source guard rebuild failure marks the IMV stale instead of aborting COMMIT. IMMEDIATE IMVs no longer rebuild on a large statement (the DEFERRED flush still may), and a passthrough IMMEDIATE IMV without a key for a source is fully refreshed by every write to it (`create_reflex_ivm` warns). The migration adds five functions, replaces `__reflex_deferred_flush_fn` and `__reflex_partition_child_for_key`, rewrites the installed deferred TRUNCATE trigger bodies and cuts the rebuild out of the IMMEDIATE trigger bodies, in place. | **Writes to IMV sources fail until the update runs.** See [Upgrading to 1.11.5](#upgrading-to-1115): in a quiet window, install the library, immediately run `ALTER EXTENSION pg_reflex UPDATE TO '1.11.5'` in every database, then recycle connection pools. The update takes no lock on sources. |
| 1.11.5 → 1.12.0 | Correctness of the create-time contract and of maintenance: SQL built from the query text, aggregates and HAVING, joins, subqueries, CTEs, column types, identifier spelling, partitioned sources. Also: sources a partition swap cannot follow are refused, one schema's partition maintenance no longer waits on another's locks, a swap refreshes each dependent once, a transient lock error no longer suspends a root. Several definitions are now refused at create and several shapes are recomputed in full on write. The migration replaces `__reflex_on_sql_drop`, `__reflex_on_ddl_command_end` and `__reflex_partition_flush_fn`, and creates `__reflex_named_relation` and `__reflex_reads_relation`; it does not rewrite per-source trigger functions. | **Partition DDL on a source fails until the update runs**: run `ALTER EXTENSION pg_reflex UPDATE TO '1.12.0'` right after installing the library. Then **recreate every IMV.** See [Upgrading to 1.12.0](#upgrading-to-1120) for the steps, the refused definitions with their remedies, the slower shapes, and what stays stale if IMVs are not recreated. |

`ALTER EXTENSION pg_reflex UPDATE` walks the chain automatically.

## Upgrading to 1.11.4

1.11.4 changes three things a client can see. Check each before upgrading a
production database.

1. **The upgrade blocks writes to ignored sources.** For every enabled
   partitioned IMV with `ignore_sources`, the migration installs heal
   triggers on each ignored source it can map to a partition. `CREATE
   TRIGGER` takes a `SHARE ROW EXCLUSIVE` lock on that source, which blocks
   writes to it and waits behind any open transaction on it. Upgrade in a
   quiet window:

    ```sql
    SET lock_timeout = '10s';
    ALTER EXTENSION pg_reflex UPDATE TO '1.11.4';
    ```

    On a lock timeout the whole upgrade rolls back; run it again.

2. **Unsound ignores are refused at create time.** `create_reflex_ivm`
   refuses an `ignore_sources` entry the query filters or joins on, unless
   the entry carries the `'!'` marker (`'!demand_planning'`) or was
   acknowledged with `reflex_ack_ignore_source`. Existing IMVs keep working,
   `reflex_rebuild_imv` (a reconcile) is not affected, and
   `create_reflex_ivm_if_not_exists` of an existing IMV is a no-op. What is
   refused is a *new* create of such a definition, including a drop and
   recreate, and `reflex_rebuild_chain`, which replays the create. After
   upgrading:

    ```sql
    SELECT * FROM reflex_audit() WHERE category = 'ignore-soundness';
    SELECT reflex_ack_ignore_source('<imv>', '<source>');  -- per accepted risk
    ```

    A client that recreates IMVs from stored definitions must add the `'!'`
    marker to each accepted entry, and only once the database runs 1.11.4:
    earlier versions read `'!demand_planning'` as a relation name that
    matches nothing, so that source would no longer be ignored. The refusal
    is returned as an `ERROR: …` string, not raised; check for it.

3. **New status values.** `reflex_scheduled_reconcile` returns a `HEALED`
   row for each IMV it healed before its reconcile rows; treat it as
   success. `reflex_ivm_status()` has a trailing `is_estimate` column, and
   reports more IMVs as `known_stale` (capped partition sources, queued
   heals, unhealable ignored sources), which monitoring may alert on.

## Upgrading to 1.11.5

The 1.11.5 library generates SQL that calls five functions the update
creates: `reflex_rebuild_target_rows`, `__reflex_target_propagates`,
`__reflex_rebuild_cost_rows`, `__reflex_xid_is_current` and
`__reflex_xid_precedes`. Between installing the library and running the
update in a database, maintenance that needs one of them fails with
`function … does not exist`:

| Operation on an IMV source | Missing function | Effect |
|---|---|---|
| the flush of a DEFERRED grouped aggregate or partitioned passthrough IMV (volume dispatch) | `__reflex_target_propagates`, `__reflex_rebuild_cost_rows` | the flush fails; under the default `flush_failure_policy` the IMV is marked `known_stale` |
| `TRUNCATE` of a source of an IMMEDIATE IMV; a trigger-side full refresh (self-join, set-op, FULL JOIN fallbacks; any `INSERT` / `UPDATE` / `DELETE` of a source a passthrough IMMEDIATE IMV has no key mapping for) | `reflex_rebuild_target_rows` | the statement fails |
| `COMMIT` of a transaction that leaves a DEFERRED IMV to rebuild (e.g. two of its sources written) | `__reflex_xid_is_current`, `__reflex_xid_precedes` | the `COMMIT` aborts and its writes roll back |
| `TRUNCATE` of a source of a DEFERRED IMV | `reflex_rebuild_target_rows` | the `COMMIT` succeeds; the IMV is marked `known_stale` |

These failures are loud: the write does not happen, so no IMV silently
diverges from its sources. Other writes to IMMEDIATE IMVs (aggregate and
keyed passthrough `INSERT` / `UPDATE` / `DELETE`, partitioned or not) keep
working: a statement trigger no longer calls the volume-dispatch functions. Keep the
window short:

1. Pick a quiet window.
2. Install the 1.11.5 library (`.deb` or `install.sh`).
3. Immediately, in **every** database that has the extension:

    ```sql
    ALTER EXTENSION pg_reflex UPDATE TO '1.11.5';
    ```

    It reports `INFO: pg_reflex 1.11.5: rewrote N of M deferred TRUNCATE
    trigger bodies found (…)` and `INFO: pg_reflex 1.11.5: removed the
    rebuild (Path B) from N of M statement-trigger bodies found (…)`. A
    `WARNING` lists any body it could not rewrite, with the remedy.
4. Recycle connection pools, so that no session keeps the 1.11.4 library it
    loaded before the install.
5. Run `SELECT reflex_reconcile('<imv>')` for every IMV that
    `reflex_ivm_status()` reports `known_stale` (a DEFERRED IMV whose source
    was truncated, or whose flush failed, during the window).

### Behaviour changes in 1.11.5

- **IMMEDIATE IMVs stay incremental on large statements.** The statement
  triggers no longer switch to a rebuild (Path B and the IMMEDIATE volume
  dispatch are removed): a statement sweeping most of a source writes more
  WAL and leaves more bloat than a rebuild would. Bulk jobs should write
  through DEFERRED IMVs, whose flush still rebuilds hot partitions.
- **Keyless IMMEDIATE passthroughs refresh on every write.** A passthrough
  IMMEDIATE IMV with no key mapping for a source (no unique key passed, PK
  not projected) is fully refreshed by every INSERT, UPDATE and DELETE on
  it — INSERT too, since 1.11.5. `create_reflex_ivm` raises a WARNING for
  such an IMV. On an existing one, recreate it with a unique key (3rd
  argument) or as DEFERRED.

### Rebuilds at COMMIT in 1.11.5

These operations now do a full rebuild of a DEFERRED IMV inside `COMMIT`:

- a `TRUNCATE` of one of its sources;
- a transaction writing two sources of a multi-source IMV (the cross-source
  guard);
- a reconcile of the IMV reached from inside a trigger, e.g. the
  high-selectivity or hot-partition dispatch of an upstream DEFERRED IMV's
  flush refreshing a DEFERRED dependent (the trigger's statement may not have
  staged its own delta yet);
- a partition- or key-scoped reconcile (`reflex_reconcile_partition`, a
  partition swap, the scoped cascade) of an IMV that has deltas staged in the
  transaction, when a staged row's partition cannot be told from the row
  itself: the IMV joins another table (or reads its source twice), so its
  partition column, or another source's change, reaches partitions other
  than the row's own. An IMV reading one source table skips only the staged
  deltas of the partitions it rebuilt and needs no full rebuild.

The rebuild reruns the base query and, when the IMV has dependents, diffs it
into them, so on a large IMV the `COMMIT` can take minutes.

- `statement_timeout`, and a connection pooler's query timeout,
  apply to that `COMMIT`. A cancel is not caught by the flush's failure
  handling (`pg_reflex.flush_failure_policy` does not apply): the `COMMIT`
  aborts and the transaction's writes roll back. Jobs that TRUNCATE and
  reload sources of large IMVs should disable the timeout for their
  transaction:

    ```sql
    BEGIN;
    SET LOCAL statement_timeout = 0;
    TRUNCATE source_table;
    INSERT INTO source_table ...;
    COMMIT;
    ```

- The diff stages rows in a temporary table, so a transaction that
  rebuilds an IMV with dependents (also via `reflex_reconcile`) cannot be
  prepared with `PREPARE TRANSACTION`.

## Upgrading to 1.12.0

**Recreate every IMV after the update.** Three things an IMV is built from
are generated when it is created and are not touched by the update:

- its **stored plan and query**: the aggregation plan, the column types,
  the unique key and its mappings, the columns an UPDATE must touch to
  matter, and the query text itself;
- the **spellings its sources are registered under** (`depends_on`), by
  which the triggers of a source find the IMVs to maintain;
- the **per-source trigger functions**, and for a partitioned source the
  triggers on each table of its partition tree.

Most 1.12.0 fixes are in one of the three. An IMV created by 1.11.5 keeps
being maintained after the update as 1.11.5 planned it, including the
cases 1.11.5 got wrong.

**The 1.12.0 library requires the update.** Its partition flush calls
`public.__reflex_reads_relation`, which `ALTER EXTENSION pg_reflex UPDATE TO
'1.12.0'` creates. Between installing the library and running the update:

- a transaction that creates, attaches or detaches a partition of a source
  (a partition swap included) fails at COMMIT with `function
  public.__reflex_reads_relation(text[], text) does not exist`, and is
  rolled back;
- `reflex_flush_partition_source()` raises the same error, and so does
  `reflex_flush_partitions()` when a root is pending;
- ordinary writes to sources, partitioned or not, keep being maintained.

So run the update in every database right after installing the library,
before any partition DDL on a source.

### Steps

1. **Collect the definitions** you will recreate from: your own
   `create_reflex_ivm` calls (deployment scripts). The registry is not a
   substitute for an IMV built from a CTE query: its `sql_query` holds the
   body pg_reflex rewrote to read the generated sub-IMVs, not your text.
   To list what exists, dependents first:

    ```sql
    SELECT name, graph_depth, refresh_mode, storage_mode, create_args
    FROM public.__reflex_ivm_reference
    WHERE NOT is_generated_sub_imv
    ORDER BY graph_depth DESC, name;
    ```

2. **Check the definitions against 1.12.0 before dropping anything.** A
   refused create returns `ERROR: [reflex-unsupported] …` as text and
   builds nothing. Run your create script against a schema-only copy of
   the database with 1.12.0 installed, and rewrite what is refused
   ([the list](#definitions-that-are-now-refused) below).

3. In a quiet window, with partition swaps paused, install the library,
   then immediately in every database:

    ```sql
    ALTER EXTENSION pg_reflex UPDATE TO '1.12.0';
    SELECT extversion FROM pg_extension WHERE extname = 'pg_reflex';  -- 1.12.0
    ```

    Recycle connection pools so that no session keeps the 1.11.5 library.

4. **Drop every IMV**, in the order of the listing above:

    ```sql
    SELECT drop_reflex_ivm('<imv>', true);
    ```

    Dropping the last IMV of a source removes that source's triggers and
    trigger functions, which is what lets the creates below generate the
    1.12.0 ones. Recreating IMVs one at a time does not: a source still
    read by an IMV that was not dropped keeps its 1.11.5 functions. If you
    recreate that way, finish with the `reflex_rebuild_triggers` statements
    of [If IMVs are not recreated](#if-imvs-are-not-recreated).

5. **Create every IMV**, sources before dependents, from the definitions of
   step 1.

6. Check:

    ```sql
    SELECT reflex_audit();
    SELECT name, known_stale, last_error FROM reflex_ivm_status();
    ```

`reflex_rebuild_chain('<imv>')` drops and recreates one IMV from its stored
arguments in one transaction. When 1.12.0 refuses the definition it raises
`reflex_rebuild_chain: failed to recreate IMV '<imv>': ERROR:
[reflex-unsupported] …` and leaves the IMV as it was. It is not a way to
recreate everything: it refuses an IMV that pg_reflex decomposed into
sub-IMVs (a CTE query), and it does not regenerate the trigger functions of
a source another IMV still reads. `reflex_rebuild_imv('<imv>')` and
`reflex_reconcile('<imv>')` do not recreate: they rebuild the rows from the
stored query, keep working on an IMV whose definition is now refused, and
change nothing in its plan.

### Definitions that are now refused

`create_reflex_ivm` returns the message as text (`ERROR:
[reflex-unsupported] …`); nothing is created. An existing IMV is not
affected until it is recreated, and `create_reflex_ivm_if_not_exists` on an
existing IMV still returns its no-op.

Definitions 1.11.5 created and, in the cases tried, maintained correctly.
These are the ones to rewrite:

| Definition | Message begins | Remedy |
|---|---|---|
| A function PostgreSQL labels `VOLATILE`, anywhere in the query. `CREATE FUNCTION` labels a function `VOLATILE` unless told otherwise, so this covers every helper created without a label. An unqualified call is matched by name in every schema and for every overload. | `Volatile function(s) <name> in IMV '<imv>': <signature>. Fix: if deterministic, run ALTER FUNCTION <signature> IMMUTABLE (or STABLE); otherwise remove it from the IMV query, or compute the value into a source column.` | If the function is deterministic, run the `ALTER FUNCTION` statement the message prints, then create. A volatile built-in (`nextval`, …) gets no `ALTER` remedy: remove it. |
| A clock or session function, in any spelling: one-argument `age(x)`, `current_setting()`, `CURRENT_USER`, `SESSION_USER`, `USER`, `CURRENT_ROLE`, `CURRENT_SCHEMA`, `CURRENT_CATALOG`, `SYSTEM_USER`, `LOCALTIME`, `LOCALTIMESTAMP`, `transaction_timestamp()`, `current_database()`, `current_schemas()`, `pg_backend_pid()`, `inet_client_addr()` / `inet_client_port()` / `inet_server_addr()` / `inet_server_port()`, `txid_current()`, `txid_current_if_assigned()`, `txid_current_snapshot()`, `pg_current_xact_id()`, `pg_current_xact_id_if_assigned()`, `pg_current_snapshot()`, `pg_postmaster_start_time()`, `pg_conf_load_time()`, `pg_my_temp_schema()`, `pg_trigger_depth()`. (1.11.5 already refused `NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, `RANDOM()`, `CLOCK_TIMESTAMP()`, `STATEMENT_TIMESTAMP()`, `TIMEOFDAY()`, `GEN_RANDOM_UUID()`.) | `Non-deterministic functions (NOW, CURRENT_TIMESTAMP, RANDOM, etc.) are not supported anywhere in the query … Fix: remove <name> from the IMV query, or compute the value into a source column` | Compute the value into a source column, or apply it in the query that reads the IMV. `age(TIMESTAMPTZ '2025-01-01', created_at)`, with a fixed reference, is accepted. |
| A string literal holding `now`, `today`, `tomorrow` or `yesterday` given a date/time type: `'now'::timestamptz`, `DATE 'today'` | same message | Write the date itself, or keep the bound in a table (see [Clauses](../sql-reference/clauses.md#deterministic-functions-only)). |
| `SUM` / `AVG` over a `REAL` column or expression | `SUM / AVG over the REAL (single precision) expression '<arg>' is not supported. … Fix: sum the exact values, SUM((<arg>)::numeric) or AVG((<arg>)::numeric); cast the aggregate back, SUM((<arg>)::numeric)::real, to keep a REAL result column.` | As printed. `MIN` / `MAX` over `REAL`, and `DOUBLE PRECISION` sums, stay accepted. |
| `HAVING COUNT(DISTINCT x)` when the SELECT list does not hold the same `COUNT(DISTINCT x)` | `HAVING holds COUNT(DISTINCT x), which the SELECT list does not: … Fix: compute it in a CTE, the other aggregates in a second one, and join them on the group columns, with IS NOT DISTINCT FROM …` | The two-CTE form the message prints (example below). |
| A column alias list that renames a source's columns: `FROM customers AS c(cid, nm)` | `the column alias list of 'c' renames its column 'id' to 'cid', and pg_reflex follows the columns of a source by their own names. Fix: drop the alias list and rename in the select list, c.id AS cid.` | As printed. When the renamed column is the primary key of a passthrough, also pass the new name as the key (3rd argument, `'cid'`): no key is inferred from a renamed primary key, and an IMV without a key is refreshed in full by every write. A list that repeats the columns' own names is accepted. |
| A **partitioned** source whose name, or whose schema's name, PostgreSQL writes in double quotes: upper case, a space, a reserved word, or a column-name keyword (`position`, `time`, `values`, `overlay`, …) even when the query writes it without quotes | `the source <name> is a partitioned table whose name PostgreSQL writes in double quotes, and pg_reflex follows the partitions of a source by its unquoted name. Fix: give the table and its schema lower-case names that are not keywords (ALTER TABLE ... RENAME TO ...), then create the IMV.` | Rename, then create. |
| A source that is a **partition** of a partitioned table, at any depth, read in FROM, a JOIN, a CTE or a subquery. 1.11.5 maintained it for statements addressed to the partition itself only: a write through the partitioned table fires the triggers of that table, not the partition's, and was missed. | `a source is a partition, and a write addressed to its partitioned table would not reach the IMV. Fix: <remedy>. The partition is <partition>, of <partitioned table>.` | The remedy printed depends on the tree. In general: `read the partitioned table and keep the rows of the partition with a WHERE on the partition key: FROM <partitioned table> WHERE <bound>`, where `<bound>` is the partition's constraint (`((plan IS NOT NULL) AND (plan = 'p1'::text))`); copy it into your query. Below a table partitioned on several columns: `read the partitioned table itself, FROM <partitioned table>, and pass partition_by => ARRAY[]::text[] (7th argument of create_reflex_ivm)`. Under a partitioned table whose name needs quotes: rename it first. For a HASH partition the printed bound names the partitioned table by OID (`satisfies_hash_partition('<oid>'::oid, …)`), which a dump and restore changes. Not refused when the partition is listed in `ignore_sources` with `!`. |
| A table of an **inheritance hierarchy** (`INHERITS`): a child, or a parent that has children. A write to another table of the hierarchy was missed. | `a source belongs to an inheritance hierarchy (INHERITS), and a write addressed to another table of the hierarchy would not reach the IMV. Fix: take the source out of the hierarchy, ALTER TABLE <source> NO INHERIT <parent>, for each table it inherits from. The source is <source>.` (for a parent: `take its children out of the hierarchy, ALTER TABLE <child> NO INHERIT <source>, …`) | As printed. A parent whose children were all detached, and a partitioned table, are accepted. Not refused when ignored with `!`. |
| Inside an aggregate's `ORDER BY`, a form of the next table: `SUM(v ORDER BY - -v)`, `array_agg(v ORDER BY 0x1F)`. pg_reflex drops the order of an order-insensitive aggregate, so these were correct; the refusal applies to every clause pg_reflex can write back. | as in the next table | Write `-(-v)`, or the integer in decimal. |

```sql
-- was: SELECT region, SUM(amount) AS total FROM orders
--      GROUP BY region HAVING COUNT(DISTINCT customer_id) > 3
SELECT create_reflex_ivm('big_regions',
    'WITH cd AS (SELECT region, COUNT(DISTINCT customer_id) AS n
                 FROM orders GROUP BY region),
          agg AS (SELECT region, SUM(amount) AS total
                  FROM orders GROUP BY region)
     SELECT agg.region, agg.total
     FROM agg JOIN cd ON cd.region IS NOT DISTINCT FROM agg.region
     WHERE cd.n > 3');
```

Each CTE is maintained incrementally; the outer SELECT that joins them is
refreshed from the two CTE tables, which hold one row per group.

Definitions 1.11.5 created but maintained wrongly, or that raised on a
later write. Recreating them is refused, and the IMV you have is not right
either:

| Definition | Message begins | Remedy |
|---|---|---|
| `SUM` / `AVG` / `MIN` / `MAX` / `BOOL_OR` with `DISTINCT`, under any spacing or comment | `DISTINCT modifier on SUM/AVG/MIN/MAX/BOOL_OR is not supported. Only COUNT(DISTINCT col) is supported.` | `WITH d AS (SELECT DISTINCT grp, val FROM t) SELECT grp, SUM(val) FROM d GROUP BY grp` |
| `COUNT(DISTINCT x)` inside an expression: `COUNT(DISTINCT x) + 1`, `COALESCE(COUNT(DISTINCT x), 0)` | `COUNT(DISTINCT) nested inside an expression is not supported (the DISTINCT would be lost).` | Compute it in a CTE and apply the expression in the outer SELECT. A bare cast, `COUNT(DISTINCT x)::int`, is accepted. |
| More than one `COUNT(DISTINCT …)` in a SELECT list | `More than one COUNT(DISTINCT ...) in the same SELECT list is not supported` | One CTE per distinct count, joined on the group columns with `IS NOT DISTINCT FROM`; the message prints the query. |
| A set-returning function in the SELECT list (`unnest(tags)`) with `unique_columns` | `A set-returning function in the SELECT list (<name>) yields several rows per source row, so the IMV cannot be maintained by a unique key` | Do not pass `unique_columns`. The IMV has no key and is refreshed by every write. |
| `COUNT(DISTINCT v)`, or an aggregate under `IS NULL` / `BETWEEN` / `IN`, without GROUP BY and with `unique_columns` | `<expr> without GROUP BY is recomputed on every write and cannot be maintained by a unique key` | Do not pass `unique_columns`. |
| An aggregate other than SUM, COUNT, AVG, MIN, MAX, BOOL_OR in an ungrouped query that ends up with a unique key | `Unsupported aggregate(s): <NAME>. Supported: SUM, COUNT, AVG, MIN, MAX, BOOL_OR. Another aggregate is accepted only without GROUP BY in an IMV with no unique key …` | Do not pass `unique_columns`, and do not name an output column like a primary key column of the source. |
| A function called in attribute notation on a source row: `c.display_name` for `display_name(customers)` | `'c.display_name' calls a function in attribute notation …` | Store the value in a column of the source (for an immutable function, a generated column) and select it. |
| `SUM` over `money` (it raised on UPDATE) | `SUM / AVG over '<arg>' is not supported: PostgreSQL sums it as money …` | `SUM((x)::numeric)::money` |
| A WHERE clause reading the source of a one-source query as a whole row: `FROM orders o WHERE o IS NOT NULL` | `the WHERE clause reads 'o' as a whole row …` | Test the columns themselves. |
| A function in the top-level FROM clause: `FROM t, unnest(arr) AS e`, `generate_series(…)` | `A function in FROM (<name>) is not supported` | Store the function's rows in a table and join it; a function of the row's own columns can be called in the SELECT list (`SELECT g, unnest(arr) AS e FROM t`). |
| Two tables whose pg_reflex trigger names would be equal: a `.` and a `_` are written alike in those names, so `s.t` and `s_t`, or `a.b_c` and `a_b.c`. The second create replaced the triggers of the first: in IMMEDIATE mode the IMVs of both stopped being maintained, in DEFERRED those of the first; dropping the second removed the shared triggers. Refused when the second is created, or when one definition reads both. | `two relations would get the same pg_reflex trigger names, and the IMVs of one would go unmaintained. Fix: <remedy>. The relations are <a> and <b>: a '.' and a '_' are written alike in the names of the triggers and of their functions.` | The printed remedy depends on what each relation is: `rename the table <t> (ALTER TABLE ... RENAME TO ...), which no IMV reads yet, then create the IMV over the new name`; for the target of another IMV, `drop the IMV <imv> (SELECT drop_reflex_ivm('<imv>')) and create it again under another name, then create this IMV over that name`; for a table this IMV builds for itself, `give this IMV another name`; when both tables are already read by IMVs (created before 1.12.0), `drop the IMVs that read <t>, rename that table, and create them again over the new name`. |
| Two prefix operators in a row (`- -v`, `- - -v`, `@ -v`, `~ -v`, `- @v`, `- ~v`) anywhere pg_reflex writes the expression back: a group key, an aggregate argument, FILTER, HAVING, a CTE body, a set-operation operand, `DISTINCT` / `DISTINCT ON`, a window, a join condition, a WHERE conjunct of an aggregate or of a decomposed query. They are written back without a space: `--v` starts a comment (silently wrong in a group key and in `SELECT DISTINCT`), `@-` is read as one operator (an error at create or on every write). | `pg_reflex writes two operators in a row back as '--', which PostgreSQL reads as the start of a comment. Fix: put the operand in parentheses, -(-v) in place of - -v, or write v alone.` (`'@-'` … `one operator`) | As printed. `- +v` and `+ -v` are accepted. |
| An integer written `0x1F` (written back as the bit string `X'1F'`), or `0X1F`, `0o17`, `0b101` ending an unaliased item (read as the number 0 aliased `X1F`, `o17`, `b101`: silently wrong), in the same places | `pg_reflex writes the hexadecimal integer 0x1F back as the bit string X'1F', which is not a number. Fix: write the integer in decimal: 31.` / `pg_reflex reads the integer 0b101 as the number 0 followed by the name b101, where PostgreSQL reads one integer. Fix: write the integer in decimal: 5.` | Write the integer in decimal. A bit string written `X'1F'` is accepted. |

Also refused: an identifier written `U&"…"` with an escape sequence or a
`UESCAPE` clause (`pg_reflex does not read the identifier <written>, written
with Unicode escapes.`). Write the name itself between double quotes.

In a plain SELECT (no aggregate, CTE, `DISTINCT ON`, window or HAVING),
PostgreSQL runs the select list, the FROM clause and the WHERE conjuncts
pg_reflex keeps as written from your text, so the operator and integer
forms above are accepted there.

A **DEFERRED create in a transaction that has already written to one of
its sources** (a write staged for a DEFERRED IMV of that source) is
refused: `DEFERRED IMV '<imv>' cannot be created in a transaction that
already wrote to its source '<source>': the staged write would reach the
new IMV a second time at COMMIT. Fix: COMMIT the writes, then create the IMV
in its own transaction.` On 1.11.5 such a create failed on the re-creation
of the shared flush trigger, which a create no longer runs.

Definitions that already failed at create on 1.11.5 with a raw PostgreSQL
error now return a tagged message with a remedy. No IMV of these shapes
exists, so there is nothing to do: an unsupported aggregate with GROUP BY;
a column that is neither an aggregate nor a group key beside an ungrouped
aggregate; an aggregate under `IS NULL` / `BETWEEN` / `IN` / `LIKE` beside
GROUP BY; a bare constant beside GROUP BY or DISTINCT (cast it:
`'EUR'::text`); two grouping keys or two output columns of one name; `SUM`
over an interval or a time (`SUM(EXTRACT(EPOCH FROM x))`); a
window function reading a column the SELECT list does not project bare, or
reading it with a qualifier.

Candidates among existing IMVs. These are text searches and over-report (a
two-argument `age`, a `CAST(x AS numeric(10,2))`):

```sql
SELECT name
FROM public.__reflex_ivm_reference
WHERE sql_query ~* '\m(age|current_setting|current_user|session_user|current_role|current_schema|current_catalog|current_database|localtime|localtimestamp|transaction_timestamp|pg_backend_pid|txid_current|nextval)\M'
   OR sql_query ~* '\m(sum|avg|min|max|bool_or)\s*\(\s*distinct\M'
   OR sql_query ~* '''\s*(now|today|tomorrow|yesterday)\s*'''
   OR sql_query ~* '\mas\s+\w+\s*\(';

-- functions left VOLATILE, outside extensions
SELECT p.oid::regprocedure
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE p.prokind = 'f' AND p.provolatile = 'v'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND NOT EXISTS (SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_proc'::regclass
                    AND d.objid = p.oid AND d.deptype = 'e');

-- REAL columns
SELECT c.relname, a.attname
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE a.atttypid = 'real'::regtype AND a.attnum > 0 AND NOT a.attisdropped
  AND c.relkind IN ('r', 'p');

-- sources that are a partition or a table of an inheritance hierarchy
-- (not refused when listed in ignore_sources with '!')
SELECT DISTINCT r.name, s.src,
       CASE WHEN c.relispartition THEN 'partition' ELSE 'inheritance' END AS kind
FROM public.__reflex_ivm_reference r
CROSS JOIN LATERAL unnest(r.depends_on) AS s(src)
JOIN pg_class c ON c.oid = to_regclass(s.src)
WHERE c.relispartition
   OR (c.relkind = 'r' AND EXISTS (SELECT 1 FROM pg_inherits i
                                   WHERE i.inhrelid = c.oid OR i.inhparent = c.oid));

-- registered sources whose trigger names would be equal
SELECT replace(replace(src, '.', '_'), '"', '') AS trigger_suffix,
       array_agg(DISTINCT src) AS sources
FROM public.__reflex_ivm_reference, unnest(depends_on) AS src
GROUP BY 1
HAVING count(DISTINCT to_regclass(src)) > 1;

-- operators in a row and non-decimal integers (text search, over-reports
-- a binary minus before a negative number, and matches inside strings)
SELECT name FROM public.__reflex_ivm_reference
WHERE sql_query ~ '[-@~]\s+[-@~]'
   OR sql_query ~* '\m0[xob][0-9a-f_]';
```

The trigger-name query also finds pairs whose IMVs 1.11.5 already left
unmaintained: those IMVs are wrong now, and recreating them is refused
until one table is renamed.

### Shapes that become slower

Each of these is now recomputed from the IMV's query on the writes named,
because no incremental path is right for it. Before, they were wrong on
some writes.

| Shape | Writes that recompute the IMV in full | Alternative |
|---|---|---|
| The query holds a `RIGHT JOIN` | every write to any source. On 1.11.5 writes to the table after the keyword were incremental. | Write the join as a `LEFT JOIN` with the tables swapped. |
| A table read through a subquery in WHERE, HAVING, the SELECT list, FILTER or a join condition (`IN`, `NOT IN`, `= ANY`, `EXISTS`, scalar) | writes to that table. Writes to the tables of the outer FROM clause keep their delta when the IMV is an aggregate or a passthrough with a key; a passthrough reading a second table through a subquery infers no key, so pass it as the 3rd argument. | An inner join, when the subquery column is unique. |
| A table read inside a LEFT JOINed derived table or parenthesised join | writes to that table | LEFT JOIN the tables one by one. |
| A source read more than once: self-join, two aliases of one dimension, a CTE referenced twice | every write to it, as before, and now also an UPDATE that changes no column the IMV reads (1.11.5 skipped it) | none |
| A source read as a whole row, or a value named like the source: `COUNT(t)`, `to_jsonb(t)`, `t.*` in an expression, a column named like its table's alias | writes to that source | `COUNT(*)`; name the columns. |
| An aggregate grouped by another column than the join key, over a LEFT JOIN whose WHERE or later inner join reads the LEFT JOINed table | writes to that table | none |
| A passthrough reading a derived table (`FROM (SELECT …) x`), or with a set-returning function in the SELECT list | every write, with or without a key | For a derived table, move its filter and columns into the outer query. |
| A keyed passthrough join where a joined table's key mapping does not identify its rows, or whose declared key takes columns from two tables | writes to that table (to either table for a two-table key) | none |
| A partitioned source: a transaction that creates, attaches or detaches a partition **and** writes to the source or to one of its partitions | at COMMIT, a full `reflex_reconcile` of every IMV of that source that does not mirror its partitions (ungrouped, grouped by another column, joins) | Change partitions in an earlier transaction, or load the new partition directly: both stay incremental. |
| `TRUNCATE` of a partition | a rebuild of every IMV of the source (it was not maintained at all before) | none |
| An IMV built on a partitioned IMV whose partitions are rebuilt (a swap, `reflex_reconcile_partition`), when its query does not prove that its rows of one partition key come only from rows of that key, or when the rebuilt keys cannot be named (a RANGE or DEFAULT partition created or altered in the transaction, a NULL key) | a whole refresh of that IMV, once per swap, with the NOTICE `pg_reflex: '<imv>' is refreshed whole, not by partition, after the partition rebuild of '<source>': <reason>`. Some of these were refreshed by partition on 1.11.5, and some of those were wrong. | Mirror the partitions of the IMV it reads (`partition_by` on the same column), read that IMV once, as a top-level relation, and take the partition column from it. |

To see, for every IMV and source, whether a statement recomputes the IMV
instead of reading the changed rows:

```sql
SELECT r.name, src AS source, op,
       reflex_build_delta_sql(r.name, src, op, r.base_query, r.end_query,
                              r.aggregations::text, r.base_query)
           !~ '__reflex_(new|old)_' AS recomputed_in_full
FROM public.__reflex_ivm_reference r,
     unnest(r.depends_on) AS src,
     unnest(ARRAY['INSERT', 'UPDATE', 'DELETE']) AS op
WHERE r.enabled AND src NOT LIKE '<%'
  AND r.aggregations ? 'group_by_columns'
ORDER BY 1, 2, 3;
```

It reads the registry and runs nothing. It does not show the UPDATE that is
no longer skipped (fourth row) nor the partition rows, and it leaves out the
view layer of a window IMV, which holds no rows. The outer SELECT of a CTE
query that joins its CTEs shows as recomputed for its CTE tables: it has no
key and is refreshed from them.

### Other visible changes

- **`depends_on` spellings.** A recreated IMV registers one spelling per
  relation: lower case for an unquoted name, no quotes around a name that
  does not need them, a table named by a column-name keyword quoted
  (`"position"`), a table of another schema with the quotes its parts need
  (`"S"."T"`). The stored `sql_query` is your text with those words edited
  in place. A client that reads `depends_on` must expect them.
- **Column types.** A recreated IMV has PostgreSQL's type for every column
  of its query: a `varchar(n)` / `char(n)` group key (was `text`),
  `COUNT(*)` / `SUM(int)` over a partitioned or quoted source `bigint` (was
  `integer`), `MIN` / `MAX` over an integer expression `integer` (was
  `numeric`), enum, array, interval, money and time keys and extrema their
  own type (were `text`).
- **Triggers on partitions.** Every partition of a source carries six
  triggers (two for TRUNCATE and
  `reflex_partition_{ins,del,upd,guard}_on_<source>`), and the source two
  more than before. Creating an IMV takes `SHARE ROW EXCLUSIVE` on each
  partition, dropping the last IMV of the source `ACCESS EXCLUSIVE`. Two
  statements are refused: a write addressed to a partition whose columns
  are in another order than the source's (write through the source), and a
  write to a table in the transaction that detached it.
- **Return strings.** `reflex_flush_deferred` that reaches no enabled
  DEFERRED IMV returns `NO DEFERRED IMVS: no enabled DEFERRED IMV depends on
  '<source>'` and raises a WARNING. `reflex_sync_partitions` appends `;
  refilled recreated partition(s) …` when it refills a leaf. A client
  comparing either with a fixed string must be updated.
- **Audit.** `trigger-attached` also reports disabled triggers and the
  triggers of every table of a partition tree, and its remedy is two
  statements. `trigger-mode-matches` no longer reports an IMMEDIATE IMV
  that shares a source with a DEFERRED one. See
  [`reflex_audit`](../api/reflex_audit.md).
- **A WARNING at create.** Two-argument `date_trunc`, `date_part`,
  `EXTRACT`, `to_char`, `to_date`, two-argument `to_timestamp` and
  one-argument `date` are accepted with a WARNING: on `timestamptz` their
  result depends on the writer's `TimeZone`. `date_trunc('day', ts, 'UTC')`
  clears it.
- **Several schemas in one database.** The partition maintenance of one
  schema no longer waits on another schema's locks: the COMMIT-time
  partition flush, `reflex_flush_partition_source`,
  `reflex_sync_partitions`, `reflex_reconcile_partition`,
  `reflex_reconcile`, `reflex_rebuild_imv`, `refresh_reflex_imv` and
  DEFERRED creates and writes. A source registered without a schema
  (`orders`) is a table of `public` only: partition DDL and `ALTER TABLE`
  on `tb.orders` no longer sync, lock, flag stale or (under
  `pg_reflex.alter_source_policy = 'error'`) refuse on account of the IMVs
  of `public.orders`. `reflex_ivm_status()`, `reflex_audit()` and
  `reflex_flush_partitions()` called without an argument read every schema
  and can still wait behind another schema's partition DDL; pass an IMV to
  `reflex_audit` or a root to `reflex_flush_partition_source` to keep to
  one.
- **Transient lock errors no longer suspend a partitioned source.** A
  deadlock, serialization failure or lock timeout in the partition flush
  is not counted toward the 5-failure cap; the root stays in
  `__reflex_partition_pending` with the error in `last_error`, its IMVs are
  flagged `known_stale`, and the next partition DDL COMMIT on the source or
  a manual flush retries it, without a cap. After the retry succeeds, some
  correct IMVs can stay flagged `known_stale` until `reflex_reconcile`.
  Other errors are counted once per COMMIT and suspend at 5, as before. A
  root is flushed once per COMMIT, however many partition DDL commands the
  transaction ran.
- **Partition swaps** refresh each IMV built on the rebuilt IMV once, by
  partition or by key when its query proves it, otherwise whole with the
  NOTICE above.
  `reflex_reconcile_partition('<imv>', '', '<p>')` with a partition `<p>` of
  a source the IMV does not mirror returns `ERROR: reconcile_partition:
  '<p>' is not a partition of '<source>', the source whose partitions
  '<imv>' mirrors`.
- **`DOUBLE PRECISION` sums are unchanged**, and can differ from the query
  in the last digits as before: see
  [Unsupported shapes](../limitations/unsupported-shapes.md#sum-avg-over-double-precision).

### If IMVs are not recreated

The IMVs keep working as on 1.11.5. What you can get without recreating:

1. **The fixes applied when a write is processed.** The maintenance SQL is
   built from the stored query by the library, so the fixes to source-name
   rewriting (non-ASCII text, string literals, mixed quoting), to predicate
   splicing, and the routing of a write to a full recompute (RIGHT JOIN,
   subquery tables, LEFT JOINed derived tables, a source read twice) apply
   to existing IMVs as soon as sessions run the new library. Rows already
   wrong stay wrong until `SELECT reflex_reconcile('<imv>');`.

2. **The 1.12.0 trigger functions and partition triggers**, per source.
   Generate the statements, then run them:

    ```sql
    SELECT DISTINCT 'SELECT reflex_rebuild_triggers(' || quote_literal(src) || ');'
    FROM public.__reflex_ivm_reference, unnest(depends_on) AS src
    WHERE EXISTS (SELECT 1 FROM pg_trigger t
                  WHERE t.tgrelid = to_regclass(src)
                    AND t.tgname LIKE '\_\_reflex\_trigger\_%');
    ```

    The `EXISTS` keeps to the sources that carry triggers today. Do not run
    `reflex_rebuild_triggers` on the `<imv>__base` table of a window IMV: it
    carries none by design, and once given triggers every write to its
    source raises `aggregations for '<imv>' must be valid JSON`.

    This regenerates the source's trigger functions (the TRUNCATE ones
    changed) and, for a partitioned source, installs the triggers on every
    table of its partition tree, after which statements and TRUNCATEs
    addressed to a partition are maintained. Then run
    `SELECT reflex_reconcile('<imv>');` for the IMVs of a partitioned source:
    writes addressed to its partitions before that point were not applied.

What stays as 1.11.5 built it until the IMV is recreated. `reflex_reconcile`
and `reflex_rebuild_imv` rebuild the data from the stored queries; they do
not replan the IMV:

- an aggregate nested in an expression (`ROUND(AVG(v), 2)`, `SUM(v) /
  COUNT(*)`, a nested `FILTER`), HAVING on a `FILTER` or an all-NULL `SUM`,
  two aggregates whose arguments collide (`MAX(v)` beside `MAX(-v)`): the
  plan is stored;
- column types and collations of the IMV and of its intermediate table;
- unquoted upper-case names, two spellings of one table across IMVs (`t`
  and `"t"`), a table in a mixed-case schema: the registered spellings and
  the stored query;
- which UPDATEs are skipped as irrelevant (self-joins under two spellings,
  NATURAL joins, a filter on the null-extended table of an outer join,
  columns compared across tables in WHERE): the relevant columns and
  per-source filters are stored;
- the key and key mappings of a passthrough, and the group-key mappings of
  an aggregate over a join;
- a CTE query holding a literal or a column named like the CTE: the
  rewritten body is stored;
- a definition 1.12.0 refuses: it keeps running as before, which for the
  second table above means wrong data. An IMV over a partition keeps
  missing writes addressed to the partitioned table; IMVs over two tables
  whose trigger names are equal stay unmaintained.

## After upgrade

Run the smoke check:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'pg_reflex';
SELECT count(*) FROM public.__reflex_ivm_reference;
SELECT * FROM reflex_ivm_status();
```

If any IMV shows `last_error` after upgrade, run `SELECT reflex_rebuild_imv('<name>')`.

[Operations runbook :material-arrow-right-bold:](../operations/runbook.md){ .md-button }
