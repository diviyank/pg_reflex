# Changelog

## [Unreleased]

## [1.12.2] - 2026-10-14

A performance release for DEFERRED chains, with one correctness fix. A
partition push refreshes the DEFERRED aggregates that read the rebuilt IMV
by group, instead of leaving them to an expensive COMMIT delta flush. The
MIN / MAX recompute of shrunk groups prunes partitions, and a large DEFERRED
top-K MIN / MAX delta replaces its groups from the base query. An UPDATE of
a DEFERRED source that leaves every column its IMVs read unchanged costs
nothing at COMMIT. The fix: an empty `reflex.wipe_threshold`,
`reflex.wipe_floor_rows` or `pg_reflex.debug_resolve_anchor` setting, which a
session gets after `SET LOCAL` or `SET` then `RESET`, discarded DEFERRED
flushes or aborted partitioned creates.
`ALTER EXTENSION pg_reflex UPDATE TO '1.12.2';`

---

### Changed

- **A push refreshes the DEFERRED aggregates that observe the rebuilt IMV
  by group.** When every rebuilt leaf of a partition push is row-diffed, a
  DEFERRED aggregate that reads the rebuilt IMV and is grouped by its
  partition column is refreshed for the pushed groups in the push, and the
  watermark makes its COMMIT flush skip the deltas the refresh already
  reflects. It used to apply the whole staged delta at COMMIT, ending in a
  MIN / MAX recompute that scanned every partition. The refresh is chosen
  only when the push's own diff stages at least half as many rows as the
  rebuilt IMV holds in those groups; a low-churn push keeps the COMMIT
  flush. Errors while choosing are contained and fall back to the COMMIT
  flush. On a 370 k-row-per-leaf fixture (three leaves, 70 % churn) the
  COMMIT drops from 5.8–7.5 s to 1.6 s, and the push takes 1.1–3.4 s longer.
  Extrapolated, not measured, for the field push whose
  `forecast_dp_year_agg` flush took 73.8 s: about 55–65 s saved per push.
- **The MIN / MAX recompute prunes partitions.** The recompute of shrunk
  or emptied groups now also filters each group column with
  `= ANY($n)`, binding the affected keys as typed arrays rather than
  printing them, so the planner prunes the source's partitions. It applies
  when no affected key is NULL and every key column is `int2`, `int4`,
  `int8`, `numeric`, `text`, `varchar`, `bpchar`, `date`, `timestamp`,
  `bool` or `uuid`; a column with more than 100 distinct affected values
  is left unfiltered. Otherwise the recompute is the one 1.12.1 ran. IMMEDIATE
  and DEFERRED. A 100-row change that shrinks a group: flush −39 to −44 %;
  a whole-plan change: −0 to −4 %.
- **A large DEFERRED top-K MIN / MAX delta replaces its groups.** When the
  rows staged for the affected groups reach half of the rows those groups
  hold, the flush deletes the groups' intermediate rows and re-inserts them
  from the base query, scoped and pruned as above, instead of subtracting
  and adding both deltas and recomputing the shrunk groups. It applies to
  every DEFERRED top-K MIN / MAX IMV with a GROUP BY, an unpartitioned
  target, no DISTINCT aggregate and no `ignored_sources`, with no opt-in.
  Flush −33 to −52 % at a staged/held ratio of 0.5 to 2; below the ratio
  the extra decision pass costs about +5 %.
- **An UPDATE that leaves every read column unchanged costs nothing at
  COMMIT.** The UPDATE trigger of a source with a DEFERRED IMV now compares
  OLD and NEW on the columns its IMVs read, and on their `WHERE`
  predicates, before capturing anything. When they are equal, the row
  counts match and nothing is staged yet for the source in the
  transaction, the statement stages nothing, queues nothing and maintains
  none of the source's IMMEDIATE IMVs. A 2 M-row `UPDATE` of an unread
  column: COMMIT 13.5 s → 11 ms; a 1-row one: 5.4 → 0.95 ms per
  transaction. The same holds for the second hop: a mirror's refresh UPDATE
  that changes only columns its DEFERRED reader does not read stages
  nothing, so the reader's COMMIT flush went from ~9 s to under 0.2 s.
  The price: an UPDATE whose first 100 rows leave the read columns unchanged
  but whose later rows change them pays the full comparison (~+3 %).
  Sources whose IMVs are all IMMEDIATE are unchanged: their trigger already
  skips each IMV by value. Trigger bodies are rendered when IMVs are
  created, so existing sources need `reflex_rebuild_triggers` (see
  Migration).

### Fixed

- **(SILENT) An empty `reflex.*` setting discarded DEFERRED flushes.** A
  custom setting reads back as `''` for the rest of the session after
  `SET LOCAL` or `SET` then `RESET`. The DEFERRED dispatch cast
  `reflex.wipe_threshold` straight to `numeric`, raised 22P02, and the
  flush failure policy discarded the IMV's delta; its readers were not
  flagged. Since 1.4.5. An empty `reflex.wipe_threshold` or
  `reflex.wipe_floor_rows` now reads as the default everywhere. The same
  defect skipped Path B in `reflex_apply_partition_delta` and Path C in
  IMMEDIATE trigger bodies (with a WARNING), and an empty
  `pg_reflex.debug_resolve_anchor` made every partitioned create, flush and
  rebuild fail with 22P02. `reflex_apply_partition_delta` reads its setting
  through the new `__reflex_setting()`, which also turns an unparsable
  value into the default with a WARNING; SQL the library generates reads
  the settings inline, so it does not depend on the migration. An
  unparsable value there (`'abc'`) still fails the flush loudly: the IMV
  is flagged `known_stale` with the `reflex_reconcile` remedy.

### Behaviour notes

- The unread-column check, like the COMMIT-time spurious-change and
  filter-skip checks before it, compares with each type's `=` (`json` and
  `xml` as text). A change that only alters the representation is no
  change: numeric scale (`5` → `5.00`), float `0` → `-0`, a case-only
  change under `citext` or a nondeterministic collation, `bpchar` trailing
  blanks and `jsonb` number scale. The IMVs keep the earlier rendering.
  This is by design, and now also holds for the IMMEDIATE IMVs of a source
  that has a DEFERRED IMV, which 1.12.1 maintained on every UPDATE.
- The top-K bulk replace applies to every installed DEFERRED top-K MIN / MAX
  IMV that qualifies, without opt-in; below its ratio a flush costs about
  5 % more.

### Known issues

Filed in `untreated_bugs/`, all present before 1.12.2:

- An ERROR inside a push's rebuild region under a user `SAVEPOINT` aborts
  the backend on an assertion build; a release build is not yet checked.
- IMMEDIATE top-K MIN / MAX: a writable CTE that inserts into and updates
  the same groups of a source leaves a phantom heap entry (silent wrong
  MIN / MAX after a later DELETE).
- An IMV joining a source with a DEFERRED IMV over that source counts the
  joint delta twice (silent).
- The readers of an IMV whose DEFERRED flush failed are not reported stale.
- UPDATE trigger bodies return early on an empty NEW transition table and
  miss rows removed by a row movement that a BEFORE INSERT trigger
  cancels (silent).

### Tests

- `pg_test_v1122_push_scoped_reader.rs`,
  `pg_test_v1122_minmax_pruned_recompute.rs`,
  `pg_test_v1122_minmax_bulk_replace.rs`, `pg_test_v1122_guc_empty_cast.rs`,
  `pg_test_v1122_unread_column_check.rs`. The swap-cascade tests expect the
  extra group refresh of the observing aggregate.
- The dblink test harnesses (`ScChain::commit`, `v112_sh_remote`,
  `v112_pl_call_on`) reported a transaction that failed at COMMIT as
  committed; they now check the outcome in the database.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.12.2';` creates
  `public.__reflex_setting(text, anyelement)` and replaces
  `public.reflex_apply_partition_delta()`, both byte-identical to the
  bootstrap text. No registry column, table, index, trigger, event trigger
  or function signature changes; no IMV needs to be recreated. Run it
  promptly; the new library does not depend on it for correctness.
- To get the unread-column skip on existing sources, and to replace the
  setting reads of trigger bodies rendered before 1.12.2, run
  `SELECT reflex_rebuild_triggers('<source>');` for each source with a
  DEFERRED IMV (for base-db: `alp.sales_simulation`,
  `alp.sop_forecast_view` and the other DEFERRED sources). It takes a
  `SHARE ROW EXCLUSIVE` lock on each source; the migration does not do it.

---

## [1.12.1] - 2026-10-13

A performance release for partition swaps of a source read by a chain of
IMVs. A push that swaps leaves of a partitioned source no longer refreshes,
at COMMIT, the IMVs that list the rebuilt partition-mirroring IMV in their
`ignored_sources`: their affected partitions are queued as stale and are
rebuilt by `reflex_heal_ignored_sources`, `reflex_scheduled_reconcile` or
`reflex_doctor(fix => TRUE)`, as `ignore_sources` documents. A FULL JOIN
passthrough refreshes only the rows of the join keys that changed, and an
aggregate IMV without a unique index is diffed by its groups.
`ALTER EXTENSION pg_reflex UPDATE TO '1.12.1';`

---

### Changed

- **A partition swap honours `ignored_sources`.** When the partition flush
  rebuilds a partition-mirroring IMV because partitions of its source were
  attached, detached, dropped or swapped, the IMVs that ignore the rebuilt
  IMV are no longer refreshed in that COMMIT (1.11.5 and 1.12.0 refreshed
  them, which put the whole chain's cost on the COMMIT). Their affected
  partitions, the keys before and after the rebuild and those of dropped
  partitions, are queued in `__reflex_heal_pending`; an IMV with no heal
  mapping onto the rebuilt IMV's partition column is queued whole.
  `reflex_ivm_status` reports every IMV made wrong by the queue as
  `known_stale`, including the parent and sibling sub-IMVs of a generated
  CTE sub-IMV and every IMV reading one, with the remedy
  `SELECT reflex_heal_ignored_sources('<imv>');`. A named heal also heals
  the queued IMVs the named IMV reads and its generated sub-IMVs, deepest
  first. Explicit `reflex_reconcile`, `reflex_reconcile_partition` and
  `reflex_rebuild_imv` calls still refresh the ignoring IMVs. In DEFERRED
  mode, a user write to the source in the same transaction as a swap
  rebuilds the IMV whole, which still refreshes them.
  On a field-shaped chain with 100 k-row leaves (PostgreSQL 16, assertion
  build): one leaf swapped commits in 2.9 s instead of 25.2 s, ten leaves
  in 26.6 s instead of 133.9 s (12.1 → 2.6 s per leaf). The work moves to
  the heal rather than going away: the heal then takes 18.5 s and 58.2 s,
  so COMMIT plus heal is about what 1.12.0 spent in the COMMIT alone.
- **A FULL JOIN passthrough refreshes only the changed join keys.** A write
  to either side used to rebuild the whole IMV. The rebuild is now limited
  to rows whose `COALESCE`d output columns hold the changed values, old and
  new, when the alignment proof accepts the pair: both columns of one type,
  type modifier and collation, every source read once, later joins LEFT.
  Otherwise the whole rebuild stays.
- **An aggregate IMV without a unique index is diffed by its groups.** A
  rebuild compared whole rows; it now matches rows on the GROUP BY columns
  when neither the new rows nor the target repeat a group, and falls back to
  the whole-row comparison otherwise.
- The ignored-source heal trigger no longer compares whole transition
  tables for the readers the partition flush queued directly.

### Fixed

- A dependent whose refresh after a rebuild returns an error is flagged
  `known_stale` with the remedy `SELECT reflex_reconcile('<imv>');`; it was
  left with a WARNING only.

### Tests

- `pg_test_v112_full_join_scope.rs`, `pg_test_v112_partition_perf.rs`,
  `pg_test_v1121_heal_own_rebuild.rs`, and new cases in
  `pg_test_rebuild_diff.rs`. The swap-cascade tests that required the
  ignoring IMVs to be right at COMMIT now require them to be reported stale,
  and right after the heal. `pg_rbd_invalid_unique_index_is_not_a_key` runs
  on a passthrough (an aggregate is now diffed by its groups); its
  assertions are unchanged.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.12.1';` replaces
  `public.__reflex_heal_on_ignored_change()`, byte-identical to the
  bootstrap text. No registry column, table, index, trigger, event trigger
  or function signature changes.
- Applications that need the IMVs ignoring a rebuilt IMV to be fresh after a
  push call `reflex_heal_ignored_sources('<imv>')` for them (base-db 6.7.7
  `refresh-views` does).

---

## [1.12.0] - 2026-10-13

A correctness release for what `create_reflex_ivm` accepts and for how an
accepted IMV is maintained. It started as a hardening of the create-time
contract (non-ASCII text or a string literal could corrupt the generated
delta SQL, an aggregate nested in an expression stored the wrong value,
DISTINCT and volatile-function checks could be bypassed by spelling) and
then took in the defects of the same kind met along the way: joins,
HAVING, subqueries, CTEs, column types, identifier spelling and
partitioned sources. The rule applied throughout: a shape that cannot be maintained
incrementally is recomputed in full on the writes concerned, and a shape
that cannot be maintained at all is refused at create with a message that
carries the rewrite. Both cost something, listed under Changed: several
definitions 1.11.5 created are now refused, and several shapes are slower.

It also refuses at create the sources pg_reflex cannot follow (a partition,
a table of an inheritance hierarchy, two tables whose triggers would share
names) and the expressions it writes back with another meaning; keeps the
partition maintenance of one schema from waiting on the locks of another;
refreshes each IMV built on a rebuilt partitioned IMV once per partition
swap, within the rebuilt partitions when its query proves it; and no longer
suspends a partitioned source for a transient lock error.

Stored plans, registered source spellings and per-source trigger functions
are generated when an IMV is created, so most fixes reach an IMV only when
it is recreated. **Recreate every IMV after the update**; see "Upgrading
to 1.12.0" in `docs/getting-started/upgrading.md`.
`ALTER EXTENSION pg_reflex UPDATE TO '1.12.0';` **is required by the new
library**: until it runs, every transaction that changes the partitions of
a source fails at COMMIT.

---

### Fixed

Maintenance SQL assembled from the query text:

- **(SILENT) Non-ASCII text in an IMV query corrupted the maintenance SQL.**
  The source-name rewriter copied the query byte by byte, so `'Été'` reached
  PostgreSQL as mojibake and the IMV stopped following its source. Text is
  now copied as UTF-8. The same defect is fixed in the alias stripper, the
  partition-predicate substitution, JOIN `ON` splitting (which could also
  panic) and long-name truncation.
- **(SILENT) A string literal containing the source's name was rewritten.**
  `WHERE season = 'a-prod-b'` over table `prod` had the literal replaced by
  the transition-table name. Single-quoted, `E''` and dollar-quoted strings
  are skipped; a multi-byte character next to a match counts as part of the
  identifier.
- **(SILENT) A source referenced with mixed quoting (`"s".t`, `s."t"`) was
  not recognised**, so each write added every existing row again. Both
  spellings are rewritten, and the self-join and outer-join guards recognise
  every spelling the rewriter does.
- **(SILENT) Predicates spliced into maintenance SQL by keyword search.** A
  `WHERE`, `GROUP BY` or `OR` inside a string literal, a comment or a
  subquery moved the splice point: duplicated rows, a MIN / MAX recompute
  that ignored half of a `WHERE a OR b`, or a syntax error on every write. A
  literal- and depth-aware scanner finds the clause; a predicate with a
  top-level `OR` is parenthesised; a fallback that cannot scope its work
  rebuilds instead of inserting unscoped.
- **(SILENT) A string literal equal to a CTE alias was rewritten**, and so
  was a column named like the CTE: `WITH season AS (…) SELECT season,
  SUM(v) … GROUP BY season` grouped by the whole row of the sub-IMV. Only
  relation references of a decomposed CTE are pointed at its sub-IMV now.
- **(SILENT) `reflex_reconcile_partition` left a leaf unrepaired when a
  partition bound held the partition column's name** (column `k`, bound
  `'a k b'`): the constraint was rewritten inside the literal, matched
  nothing, and the call returned `RECONCILED`. Other bounds raised a syntax
  error, as did a partition column named like a type (`date`, `text`,
  `zone`). Literals and cast type names are left alone.

Aggregates:

- **(SILENT) An aggregate nested in an expression lost its meaning.**
  `ROUND(AVG(v), 2)` stored the SUM; a nested `SUM` over an all-NULL group
  read 0; `FILTER` on any nested aggregate was ignored; nested `MIN` / `MAX`
  under integer division were wrong. Nested aggregates now use the mapping
  top-level ones use.
- **(SILENT) HAVING read its aggregates through a mapping of its own.**
  `HAVING COUNT(*) FILTER (WHERE v > 1) > 1` ignored the FILTER, `HAVING
  SUM(v) >= 0` kept an all-NULL group, `HAVING SUM(v) / 2 > 2` divided
  exactly where PostgreSQL divides integers. HAVING now reads through the
  SELECT-list mapping. `HAVING ROUND(AVG(v), 2) > 1`, `COALESCE(SUM(v), 0)`
  and `BOOL_OR(b)` in HAVING, which failed at create, are created.
- **(SILENT) `CAST(SUM(v) / 2 AS numeric)` was emitted as `SUM(v) /
  2::numeric`**, in the SELECT list and in HAVING. The whole operand is
  cast.
- **(SILENT) Two aggregates whose arguments differ only in characters a
  column name cannot hold shared one stored column**: `MAX(v)` beside
  `MAX(-v)`, `SUM(v + 1)` beside `SUM(v - 1)`, `SUM(v)` beside `SUM("V")`,
  `COUNT(*)` beside a column named `star`. Each (aggregate, argument) pair
  has its own column.
- **(SILENT) Result and extremum types were looked up by column name.** An
  output alias equal to a source column took that column's type (`AVG(n) AS
  v` was rounded to the integer type of `v`); `MIN` / `MAX` over a float
  expression went through NUMERIC; `MAX(interval)` and `MAX(time)` were
  stored as text and compared as strings. Types are now read from
  PostgreSQL's description of the query, by position. This also creates
  what failed at create before: `MAX(upper(g))`, `MIN` / `MAX … FILTER`
  over text, a group key aliased to another column's name.
- **(SILENT) An ungrouped `BOOL_OR` over a NOT NULL column read `false` on
  an emptied source**, where the query gives NULL.
- **(SILENT) `SUM` / `AVG` over a `REAL` argument drifted from the query**
  on ordinary data. Refused at create (see Changed).
- **(SILENT) More than one `COUNT(DISTINCT …)` in a SELECT list** was
  created with wrong counts. Refused at create.
- **(SILENT) `HAVING COUNT(DISTINCT x)` without the same `COUNT(DISTINCT
  x)` in the SELECT list** was evaluated as `COUNT(x)`. Refused at create.
- **(SILENT) `COUNT(DISTINCT v)` inside an expression** (`COUNT(DISTINCT v)
  + 1`) was accepted and wrong. Refused at create.
- **(SILENT) DISTINCT aggregates were detected by searching the query
  text**, so `SUM( DISTINCT v)` or a comment between the tokens was accepted
  and maintained as a plain `SUM`. Detection reads the parsed query.
- **An ungrouped aggregate with HAVING** (`SELECT SUM(v) FROM t HAVING
  COUNT(*) > 1`) and **`COUNT(DISTINCT x)` with a HAVING on that same
  count** failed at create with a raw PostgreSQL error; both are created.
- **A built-in aggregate written qualified or quoted** (`pg_catalog.sum(v)`,
  `"sum"(v)`) failed at create; it is the built-in and is maintained.
- **`MIN` / `MAX` over text with a non-default collation** keep that
  collation in their stored columns; a user type is stored under its
  schema, so writes under another `search_path` no longer raise `type …
  does not exist`.

Joins:

- **(SILENT) An aggregate over a `RIGHT JOIN` was wrong after writes** to
  the table before the keyword, and after a join-key UPDATE of the other
  one. A query holding a RIGHT JOIN is now recomputed in full on every
  write.
- **(SILENT) `LEFT JOIN` onto a derived table or a parenthesised join**:
  writes to a table inside it left the aggregate wrong. Those writes
  recompute the IMV.
- **(SILENT) A WHERE clause filtering the null-extended table of an outer
  join** (`d LEFT JOIN f … WHERE f.v > 0 OR f.v IS NULL`, the anti-join
  `WHERE f.kind IS NULL`, the RIGHT and FULL forms, a filter on a table
  reached through the LEFT JOINed one): a row failing the filter was
  skipped although it turns a null-extended row into a matched one. The
  per-source filter is kept only when the WHERE provably rejects
  null-extended rows.
- **(SILENT) An UPDATE was skipped as irrelevant when it was not**: a
  self-join written with two spellings of the table (`"t" a JOIN t b`, `t a
  JOIN public.t b`) or filtered on one alias; the join column of a `NATURAL
  JOIN`; a column compared with another table's column in WHERE (`m.v >
  j.v`); a column read only by a correlated subquery; a table alias
  declared twice in the query; a row passed whole to a function
  (`to_jsonb(t)`).
- **(SILENT) A join keyword holding a comment or a line break** (`LEFT /* c
  */ JOIN`, `LEFT⏎JOIN`) was not seen as an outer join: duplicate key on a
  keyed passthrough, silently wrong in DEFERRED. Join kinds are read from
  the parsed query.
- **(SILENT) An aggregate over a join mapped the joined table to a group
  column that only shared its name** (`f JOIN d ON d.id = f.k GROUP BY
  d.k`, also with the group column aliased): a DELETE on `d` left the IMV
  wrong. A group column is matched to a join column only when it is a side
  of the equality, by case when quoted.
- **(SILENT) A keyless inner-join passthrough inferred a key mapping that
  did not identify the changed rows**: an UPDATE of one row deleted every
  result row sharing its join value. Such a table has no mapping and is
  fully refreshed.
- **A passthrough over `d LEFT JOIN f … JOIN o ON o.kind = f.kind`** raised
  `column reference "kind" is ambiguous` on every write to `f`, and was
  silently wrong in DEFERRED. A key column is mapped through its own column
  only.

Subqueries, derived tables and CTEs:

- **(SILENT) A write to a table read only through a subquery in WHERE,
  HAVING or the SELECT list** (`IN`, `NOT IN`, `= ANY`, `EXISTS`, a scalar
  subquery) left an aggregate wrong and raised `duplicate key value violates
  unique constraint "__reflex_uk_…"` on a keyed passthrough. Such a write
  recomputes the IMV; writes to the tables of the outer FROM clause keep
  their delta.
- **A DEFERRED IMV over a derived table** (`FROM (SELECT g, v FROM s WHERE
  v > 0) x`) raised `relation "__reflex_delta_<subquery:x>" does not exist`
  at COMMIT.
- **(SILENT) A DEFERRED IMV over a CTE reading two tables, both written in
  one transaction**: the CTE's sub-IMV was rebuilt at COMMIT and its
  readers were not.

Identifiers and source names:

- **(SILENT) An unquoted name written in upper case was not folded.**
  `SELECT id, G FROM t` went stale after an UPDATE of `g`; `FROM T` in a
  DEFERRED IMV was never flushed by `reflex_flush_deferred('t')`. Unquoted
  names are folded to lower case at create, ASCII letters only, as
  PostgreSQL does in a UTF-8 database.
- **(SILENT) Two IMVs reading one table as `t` and `"t"`**: the triggers
  select IMVs by the spelling of the first, so the second was never
  maintained, and dropping the first removed the triggers under the second.
  Quotes PostgreSQL does not need are dropped from the query at create;
  same for a table named by a keyword (`zone` and `"zone"`).
- **(SILENT) A table in a mixed-case schema** (`"S"."T"`) was registered as
  `S.T`: triggers went on `s.t` when it existed, and the create raised
  otherwise. Each part is registered with the quotes it needs.
- **(SILENT) A quoted source beside a lower-case namesake** (`"NnT"` and
  `nnt`): the NOT NULL probe of a key column read the namesake's
  constraints, and a NULL-key group was lost.
- **(SILENT in DEFERRED) Two tables whose names differ only by case** (`"T"`
  and `t`) shared one trigger function; the first write raised, and a
  DEFERRED flush over any mixed-case source failed.
- **`DROP TABLE other.t` dropped the IMVs of `public.t`.** The `sql_drop`
  hook matched the bare name whatever the schema; it now does so for a
  table of `public` only.
- **`drop_reflex_ivm` left the triggers and functions of a source whose
  trigger names exceed 63 bytes**, and left CTE sub-IMVs that nothing read.

Partitioned sources:

- **(SILENT) A statement addressed to a partition was not maintained.**
  INSERT, UPDATE, DELETE, COPY and MERGE on a partition (leaf or branch)
  fire the statement triggers of that table only, and pg_reflex had
  triggers on the source alone. Every table of the partition tree now
  carries the source's write triggers.
- **(SILENT) `TRUNCATE` of a partition was not propagated.** Every table of
  the tree carries the source's truncate triggers; one TRUNCATE statement
  rebuilds each IMV of the source once.
- **(SILENT) A partition created or attached and written through the
  parent in one transaction** was counted twice by an ungrouped aggregate
  or one grouped by another column than the partition key. The COMMIT-time
  partition flush reconciles such IMVs instead of applying the partition's
  rows as a delta.
- **(SILENT) A leaf recreated by `reflex_sync_partitions` stayed empty**
  with a clean audit. The sync refills a leaf it recreates beside a filled
  sibling, and the audit remedy for a dropped leaf now includes
  `reflex_reconcile_partition`.
- **A partition flush failed in a session that had run a DEFERRED flush**
  (`"__reflex_new_<source>" is not a table`), leaving the source's IMVs
  flagged stale.

IMVs built on a partition-mirroring IMV, after a partition swap or
`reflex_reconcile_partition`:

- **(SILENT) A dependent was refreshed within the rebuilt partitions
  because it was partitioned or grouped by a column of the same name**,
  although its rows of one partition key depended on rows of other keys (a
  self-join on the rebuilt IMV, a partition column taken from a mapping
  table): 360 wrong rows after a one-leaf swap in the test fixture. A
  dependent is now refreshed by partition only when it mirrors the
  partitions of the rebuilt IMV, by key only when its query proves that a
  change of key `k` changes only its rows of key `k` (one read of the
  rebuilt IMV, as a top-level relation; the column taken from it, from a
  table equated to it in an inner join, or `COALESCE` over the two columns
  of the first join's equality; level by level for a partition tree), and
  whole otherwise.
- **(SILENT) A dependent refreshed by key missed the keys that left the
  rebuilt partitions**: the keys were read after the rebuild only. They
  are now the values a `FOR VALUES IN (…)` bound lists, else the keys read
  before and after the rebuild when the partition was not created or
  altered in the transaction; otherwise, and for a NULL key, the
  dependent is refreshed whole.
- **(SILENT) A dependent refreshed by key did not pass the refresh on** to
  the IMVs that read it while ignoring it; and **a scope proven for the
  first partition level was applied at the leaf level**, so rows derived
  into another leaf of a two-level dependent were not refreshed.
- **(SILENT) A partition change of `tb.t` was not queued when an IMV of
  `public` was named `t`**: the event trigger took the root for an IMV of
  pg_reflex by its bare name, and the IMVs of `tb.t` stayed stale.

- **(SILENT) A dependent took the key path over a key column of another
  type** (`numeric` beside `integer`, `timestamp` beside `timestamptz`):
  the keys were re-parsed as text. Keys are handed to a dependent only
  when its column has the rebuilt IMV's type, type modifier and collation;
  otherwise it is refreshed whole.
- **(SILENT) Rows of a dropped mirror partition never reached the IMV's
  readers.** A source leaf detached, dropped, or swapped for an empty
  table (listed or DEFAULT) dropped the matching partition of a mirroring
  IMV with its rows, and no DML reached the IMVs reading it. Every drop of
  a mirror partition now first deletes its rows through the IMV's root,
  and the readers that ignore the IMV are caught up on the keys those rows
  held (by key when their query proves the keys are theirs, else whole,
  and recursively for their own ignoring readers).
- **(SILENT) A mirror partition that the IMV's query still fills was
  dropped** when its source partition went: an IMV partitioned by a
  column taken from a mapping table, a source partition feeding other
  partitions of the IMV, a plan re-attached under another name. A mirror
  partition is dropped only when the query yields no row inside its
  bounds; the whole reconcile also rebuilds the leaves that mirror no
  source leaf, and `reflex_audit` / `reflex_doctor` no longer report such
  a kept partition (their remedy could not clear it).
- **(SILENT) An IMV was flushed per partition when its partition column
  was not the source's.** The per-partition path is now taken only when
  every partition level of the IMV is proven to follow a source level
  partitioned on that column; otherwise the IMV is rebuilt whole.
- **A leaf swap's orphan heal could drop a whole top-level branch** of a
  LIST-over-LIST IMV with repeated values; it now compares the swapped
  child's siblings only. A failing flush flags every IMV reading the root
  and, recursively, their readers `known_stale`, not only the direct ones.

Several schemas in one database:

- **The partition flush of one schema waited on the locks of another.**
  The query that finds the DEFAULT partitions of a source evaluated
  `pg_get_expr(relpartbound, …)` on every partition of the database, which
  opens each one; behind a transaction holding another schema's partitions
  (a swap in progress) the COMMIT-time flush, the manual flushes,
  `reflex_sync_partitions`, `reflex_reconcile_partition`, `reflex_reconcile`,
  `reflex_rebuild_imv` and `refresh_reflex_imv` waited, or failed on
  `lock_timeout`, and two schemas changing partitions at once could each
  wait on the other. The
  query now reads `pg_partitioned_table.partdefid`, a catalog column.
- **Partition DDL on `tb.t` reached the IMVs of `public` that read a table
  `t`.** The event trigger and the flush matched a source registered
  without a schema against the bare name of any schema's table: they
  synced and locked the IMVs of `public.t`, flagged them `known_stale` on a
  lock timeout, counted the failures against `tb.t`, and under
  `pg_reflex.alter_source_policy = 'error'` refused `ALTER TABLE tb.t`. A
  source registered without a schema is now a table of `public` only
  (`__reflex_named_relation`, `__reflex_reads_relation`, created by the
  migration).
- **Every DEFERRED create re-created the shared flush trigger** on
  `public.__reflex_deferred_pending`, so an open DEFERRED create blocked
  every DEFERRED write of every schema until it committed. The trigger is
  created only when it is missing or differs.

Partition swap cost:

- **One partition swap refreshed the IMVs built on the rebuilt IMV in
  full, several times.** A dependent's partition is named
  `<dependent>_<partition>`; past 63 bytes PostgreSQL cuts the name and
  appends a hash, the source partition read back from it was wrong, the
  partition refresh failed silently and fell to a full `reflex_reconcile`;
  and a dependent reached by several paths was refreshed once per path.
  Each dependent is now queued once per cascade with the union of the
  scopes that reach it, and refreshed after every IMV it reads. On a
  2.16 M-row source (30 plans × 24 months × 3000 rows, the field's names,
  DEFERRED, PostgreSQL 16): one leaf swapped and committed 152.5 s →
  22.1 s, the same by explicit flush 155.3 s → 22.1 s, 20 leaves of one
  plan 208.8 s → 29.5 s.
- **Each partition DDL command of a transaction flushed its root again at
  COMMIT**; the root is now flushed once per COMMIT.
- **Every partition DDL statement paid for the registry size, and an
  internal trigger toggle re-synced the whole reader chain.** The name
  predicates the DDL event trigger evaluates per registry row are PL/pgSQL
  (an SQL function with a FROM was parsed and planned at every call), and
  pg_reflex's own `DISABLE` / `ENABLE TRIGGER USER` around a relocation no
  longer syncs the readers of the toggled IMV, which in turn toggled their
  own. With a forecast-shaped chain beside 50 other IMVs (PostgreSQL 16
  release build): one leaf swapped 1285 ms → 125 ms, a plan attached
  2804 ms → 162 ms, a plan detached 1861 ms → 92 ms, an unrelated
  `ALTER TABLE` 22.5 ms → 6.6 ms.

Partition flush failures:

- **A transient lock error suspended a partitioned source.** A deadlock
  (`40P01`), serialization failure (`40001`) or lock timeout (`55P03`) in
  the partition flush counted toward the 5-failure cap like a logic error,
  and with one flush per DDL command a single COMMIT of a three-leaf swap
  that met a lock reached the cap: the root was skipped by every later
  flush and its IMVs stayed stale until `reflex_reset_partition_failures`.
  These three SQLSTATEs are no longer counted; the root stays queued and
  is retried at the next partition DDL COMMIT or manual flush. Its IMVs
  are flagged `known_stale` with the lock error in the meantime, as
  before. Other errors count once per COMMIT and suspend at 5, as before.

Audit and maintenance entry points:

- **`reflex_audit` reported errors that no remedy could clear**:
  `internal-tables-exist` demanded an affected-groups table of an ungrouped
  aggregate, `trigger-attached` rebuilt by hand the trigger names of a
  source longer than about 37 bytes, `trigger-mode-matches` flagged an
  IMMEDIATE IMV sharing a source with a DEFERRED one.
- **`reflex_audit` did not report a missing `__reflex_scratch_<imv>`
  table**; it does, and `reflex_reconcile` recreates it.
- **`reflex_rebuild_triggers`** called with a bare registered name
  installed a second, inert trigger set; called with the quoted name of a
  CTE sub-IMV it raised. It rebuilds under the registered spelling.
- **A `SUM(DISTINCT …) OVER ()` definition** returns the tagged rejection
  instead of a raw PostgreSQL error.

### Changed

**Definitions `create_reflex_ivm` now refuses.** Each returns `ERROR:
[reflex-unsupported] …` with the rewrite; nothing is created. An existing
IMV of such a definition keeps being maintained until it is recreated.
`docs/getting-started/upgrading.md` gives the message and remedy of each.

Created on 1.11.5 and correct there in at least the cases tried; these
are the definitions to rewrite:

- **A function PostgreSQL labels `VOLATILE`**, read from `pg_proc`.
  `CREATE FUNCTION` labels a function `VOLATILE` unless told otherwise, so
  an unlabelled deterministic function is refused. The message prints
  `ALTER FUNCTION … IMMUTABLE (or STABLE)`. An unqualified call is matched
  by name in every schema and overload; a schema-qualified call in that
  schema only.
- **Clock and session functions, however spelled**: besides `NOW()`,
  `CURRENT_TIMESTAMP` / `DATE` / `TIME`, `RANDOM()`, `CLOCK_TIMESTAMP()`,
  `STATEMENT_TIMESTAMP()`, `TIMEOFDAY()` and `GEN_RANDOM_UUID()`, now also
  one-argument `age(x)`, `current_setting()`, `CURRENT_USER`,
  `SESSION_USER`, `USER`, `CURRENT_ROLE`, `CURRENT_SCHEMA`,
  `CURRENT_CATALOG`, `SYSTEM_USER`, `LOCALTIME`, `LOCALTIMESTAMP`,
  `transaction_timestamp()`, `current_database()`, `current_schemas()`,
  `pg_backend_pid()`, `inet_client_addr()` / `_port()`, `inet_server_addr()`
  / `_port()`, `txid_current*()`, `pg_current_xact_id*()`,
  `pg_current_snapshot()`, `pg_postmaster_start_time()`,
  `pg_conf_load_time()`, `pg_my_temp_schema()`, `pg_trigger_depth()`. The
  list applies to unqualified and `pg_catalog`-qualified calls; a function
  of the same name in another schema is judged by its volatility.
- **A string literal holding `now`, `today`, `tomorrow` or `yesterday`
  given a date/time type**: `'now'::timestamptz`, `DATE 'today'`,
  `date('yesterday')`.
- **`SUM` / `AVG` over a `REAL` argument.** Remedy: `SUM((x)::numeric)`,
  and `SUM((x)::numeric)::real` to keep a REAL column.
- **`HAVING COUNT(DISTINCT x)` when the SELECT list does not hold the same
  `COUNT(DISTINCT x)`.** Remedy: two CTEs joined with `IS NOT DISTINCT
  FROM`.
- **A column alias list that renames a source's columns** (`FROM customers
  AS c(cid, nm)`). Remedy: rename in the SELECT list, and for a
  passthrough whose primary key is renamed pass the new name as the key
  (no key is inferred from a renamed primary key). A list that repeats the
  columns' own names is accepted.
- **A partitioned source registered under a name that needs its quotes**:
  upper case, a space, a reserved word, or a column-name keyword
  (`position`, `time`, `values`, `overlay`, …) even written bare. Remedy:
  rename the table and its schema to lower-case names that are not
  keywords. Unreserved keywords, non-ASCII letters and `$` are accepted.
- **A source that is a partition**, at any depth, in FROM, a JOIN, a CTE
  or a subquery. It was maintained only for statements addressed to the
  partition itself: PostgreSQL fires the triggers of the table a statement
  names, so a write through the partitioned table was missed. Remedy, as
  printed: `FROM <partitioned table> WHERE <the partition's bound>`; under a
  table partitioned on several columns, read the partitioned table itself
  and pass `partition_by => ARRAY[]::text[]`; under a partitioned table
  whose name needs quotes, rename it. A partition listed in
  `ignore_sources` with `!` is not refused.
- **A table of an inheritance hierarchy** (`INHERITS`): a child, or a
  parent that has children. A write to another table of the hierarchy was
  missed. Remedy: `ALTER TABLE <child> NO INHERIT <parent>`. Not refused
  when ignored with `!`.
- **Inside an aggregate's `ORDER BY`**, the forms of the next list
  (`SUM(v ORDER BY - -v)`, `array_agg(v ORDER BY 0x1F)`): the order of an
  order-insensitive aggregate was dropped, so these were correct; the
  refusal covers every clause pg_reflex can write back. Remedy: as below.

Created on 1.11.5 but wrong or raising on a later write:

- **Two tables whose pg_reflex trigger names would be equal**: a `.` and a
  `_` are written alike in those names, so `s.t` and `s_t`, or `a.b_c` and
  `a_b.c`, got one set of triggers and functions, and the IMVs of the
  first went unmaintained. Refused when the second is created, or when
  both are read by one definition. Remedy, as printed: rename the table no
  IMV reads yet; or drop the IMV whose target collides and create it under
  another name; or give the IMV being created another name when its own
  generated table collides.
- **An expression pg_reflex writes back with another meaning**, wherever
  it is written back (group key, aggregate argument, FILTER, HAVING, a CTE
  body, a set-operation operand, DISTINCT and DISTINCT ON, a window, a
  join condition, a WHERE conjunct of an aggregate or of a decomposed
  query): two prefix operators in a row are printed without a space, so
  `- -v` becomes `--v`, the start of a comment (silently wrong in a group
  key and in `SELECT DISTINCT`), and `@ -v`, `~ -v`, `- @v`, `- ~v` become
  one operator (an error at create, or on every write); an integer written
  `0x1F` is printed as the bit string `X'1F'`; `0X1F`, `0o17`, `0b101`
  ending an unaliased item are read as the number `0` aliased `X1F`
  (silently wrong). Remedy, as printed: `-(-v)`, `@(-v)`, or the integer in
  decimal. `- +v` and `+ -v` are not refused. In a plain SELECT (no
  aggregate, CTE, DISTINCT ON, window or HAVING), the select list, the FROM
  clause and the WHERE conjuncts pg_reflex keeps as written run as written
  and are not refused.

- `SUM` / `AVG` / `MIN` / `MAX` / `BOOL_OR` with `DISTINCT` under any
  spelling; `COUNT(DISTINCT x)` inside an expression; more than one
  `COUNT(DISTINCT …)` in a SELECT list.
- A set-returning function in the SELECT list (`unnest(tags)`) together
  with `unique_columns`; an aggregate without GROUP BY that pg_reflex
  leaves to PostgreSQL (`COUNT(DISTINCT v)`, `SUM(v) IS NULL`) together
  with `unique_columns`. Remedy for both: do not pass `unique_columns`.
- A function called in attribute notation on a source row (`c.display_name`
  for `display_name(customers)`).
- `SUM` over `money`, which raised on UPDATE (over `interval` or `time`
  it was a raw error at create). Remedy: `SUM((x)::numeric)::money`,
  `SUM(EXTRACT(EPOCH FROM x))`.
- A WHERE clause reading the source of a one-source query as a whole row
  (`FROM orders o WHERE o IS NOT NULL`).
- A function in the top-level FROM clause (`FROM t, unnest(arr) AS e`),
  silently wrong after an UPDATE of `arr`.
- An aggregate other than SUM, COUNT, AVG, MIN, MAX, BOOL_OR in an
  ungrouped query that ends up with a unique key.

Also refused: an identifier written `U&"…"` with an escape sequence or a
`UESCAPE` clause (write the name itself between double quotes); and a
DEFERRED create in a transaction that has already written to one of its
sources through a DEFERRED IMV (`DEFERRED IMV '<imv>' cannot be created in
a transaction that already wrote to its source '<source>': the staged write
would reach the new IMV a second time at COMMIT. Fix: COMMIT the writes,
then create the IMV in its own transaction.`). Such a create used to fail
on the re-creation of the shared flush trigger, which a create no longer
runs.

A raw PostgreSQL error at create on 1.11.5, now a tagged message with a
remedy: an aggregate pg_reflex does not decompose (still accepted in a
single-level query with no GROUP BY and no unique key, recomputed on every
write); a select item that is neither an aggregate nor a group key in an
ungrouped aggregate query; an aggregate under IS NULL / BETWEEN / IN /
LIKE / an array beside GROUP BY or DISTINCT; a bare constant select item
beside GROUP BY or DISTINCT (`'x'`, `NULL`, `TRUE`, `3.5`, `-1`; cast it);
two grouping keys sharing a column name; two output columns of one name;
`COUNT(DISTINCT x)` beside another aggregate inside an expression or in
HAVING, or with a HAVING subquery; a window function reading a column the
SELECT list does not project bare, a qualified column or an unaliased
aggregate.

**Shapes that are now recomputed in full** (the IMV is rebuilt from its
query by the writing statement, or by the flush in DEFERRED mode):

- any query holding a `RIGHT JOIN`: every write to any of its sources.
  Writes to the table after the keyword were incremental on 1.11.5;
- a table read inside a LEFT JOINed derived table or parenthesised join:
  writes to that table;
- a table read through a subquery in an expression (WHERE, HAVING, SELECT
  list, FILTER, JOIN ON): writes to that table. An UPDATE of a column the
  subquery does not read recomputes too. A passthrough of this shape
  infers no key, and without one writes to its outer tables recompute as
  well: pass the key as the third argument;
- a source read more than once (self-join, two aliases of one dimension, a
  CTE referenced twice): such writes already recomputed; an UPDATE that
  changes no column the IMV reads is no longer skipped and recomputes too;
- a query that reads a source as a whole row or names a value like the
  source (`COUNT(t)`, `to_jsonb(t)`, `t.*` in an expression, a column named
  like its table's alias): writes to that source;
- an aggregate grouped by a column other than the join key, over a LEFT
  JOIN whose WHERE or later inner join reads a column of the LEFT JOINed
  table: writes to that table;
- a passthrough over a derived table or with a set-returning function in
  the SELECT list infers no key and is refreshed by every write;
- a keyed passthrough join: a joined table whose key mapping does not
  identify its rows is refreshed by every write to it; with a declared key
  whose columns come from two tables, by every write to either;
- a query holding a quoted identifier equal to a source's bare name that
  belongs to another relation (`sa.dim` beside `sb."dim"`): writes to that
  source;
- a DEFERRED IMV over a CTE whose two tables are written in one
  transaction: the readers of the CTE's sub-IMV are rebuilt at COMMIT;
- a partitioned source: a transaction that creates, attaches or detaches
  a partition **and** writes to the source or one of its partitions
  reconciles in full, at COMMIT, every IMV of that source that does not
  mirror its partitions. Loading the new partition directly, or changing
  partitions in an earlier transaction, stays incremental. A TRUNCATE of
  a partition rebuilds every IMV of the source;
- an IMV built on a partitioned IMV whose partitions are rebuilt (a swap,
  `reflex_reconcile_partition`), when its query does not prove that its
  rows of one partition key come from rows of that key only (see Fixed),
  or when the rebuilt keys cannot be named: refreshed whole, once per
  cascade, with the NOTICE `pg_reflex: '<imv>' is refreshed whole, not by
  partition, after the partition rebuild of '<source>': <reason>`. Some of
  these were refreshed by partition before, and some of those were wrong.

Skips that are off, without a full recompute: no UPDATE skip for a query
holding a NATURAL join or declaring a table alias twice; no UPDATE skip,
promotion or row filtering for a table an outer join null-extends unless
the WHERE provably rejects its null-extended rows; no bulk-insert shortcut
under a FULL JOIN.

**Registry spellings.** `depends_on` and the stored `sql_query` hold one
spelling per relation: unquoted names in lower case; no quotes around a
name that does not need them (`"orders"` is registered `orders`); a table
named by a column-name keyword quoted (`"position"`), by an unreserved
keyword bare; a table of another schema with the quotes each part needs
(`"S"."T"`); a name with a non-ASCII letter or a `$` bare. `t` and
`public.t` remain two registered spellings. The stored `sql_query` is the
caller's text with those words edited in place. An unaliased reference to
a CTE is stored as `FROM "<imv>__cte_<alias>" AS <alias>`. A client that
reads `depends_on` must expect these spellings on recreated IMVs.

**Column types follow PostgreSQL.** A `varchar(n)` / `char(n)` group key
keeps its type (was `text`); `COUNT(*)` / `SUM(int)` over a partitioned or
quoted source is `bigint` (was `integer`); `MIN` / `MAX` over an integer
expression is `integer` (was `numeric`); enum, array, interval, money and
time keys and extrema keep their type (were `text`). Equal intervals that
spell differently (`1 day`, `24 hours`) are one group. Intermediate column
names change for aggregates whose arguments collide and for arguments
longer than 39 bytes once sanitised.

**Triggers of a partitioned source.** The source carries six triggers
instead of four: `__reflex_trigger_btrunc_on_<source>` (BEFORE TRUNCATE)
and `reflex_partitioned_source_<source>` (BEFORE INSERT / UPDATE / DELETE)
are new. Every partition, leaf and branch, carries six:
`__reflex_trigger_btrunc_on_<source>`, `__reflex_trigger_trunc_on_<source>`
and `reflex_partition_{ins,del,upd,guard}_on_<source>`; a partition whose
columns are in another order than the source's carries the two truncate
triggers and the guard. The `reflex_partition…` names have no `__reflex_`
prefix. They are installed at create and by `reflex_rebuild_triggers`, for
a new or attached partition at the COMMIT of its DDL, and removed at the
COMMIT of a DETACH and with the last IMV of the source. Create takes
`SHARE ROW EXCLUSIVE` on every partition, drop `ACCESS EXCLUSIVE`.

**New refusals on a partitioned source**, raised by the guard trigger:

- INSERT / UPDATE / DELETE addressed to a partition whose columns are in
  another order than the source's (a table created on its own, then
  attached): `pg_reflex: … cannot maintain the incremental views of
  <source>: the columns of this partition are in a different order …`,
  with the hint to address the statement to the source. A WARNING says so
  when the trigger is installed. Refused also when no row is touched.
- a write to a table in the transaction that detached it: `… the table is
  no longer a partition of <source> and carries its triggers until the
  transaction that detached it commits`.

**Return strings and audit texts.**

- `reflex_flush_deferred(source)` that reaches no enabled DEFERRED IMV
  returns `NO DEFERRED IMVS: no enabled DEFERRED IMV depends on
  '<source>'` (was `NO DEFERRED IMVS`) and, called directly, raises a
  WARNING. It accepts any spelling of the source.
- `reflex_sync_partitions` appends `; refilled recreated partition(s) <p>:
  <result>` when it refills a leaf.
- `reflex_reconcile_partition(<imv>, '', '<p>')` with a partition `<p>`
  outside the tree of the source the IMV mirrors returns `ERROR:
  reconcile_partition: '<p>' is not a partition of '<source>', the source
  whose partitions '<imv>' mirrors` and rebuilds nothing.
- A partition rebuild that refreshes a dependent whole says so in a NOTICE
  (`pg_reflex: '<imv>' is refreshed whole, not by partition, after the
  partition rebuild of '<source>': <reason>`); the reason names the
  condition the dependent's query does not meet, or the error of a
  partition refresh that failed.
- `reflex_rebuild_chain` on an IMV whose definition 1.12.0 refuses raises
  `reflex_rebuild_chain: failed to recreate IMV '<imv>': ERROR:
  [reflex-unsupported] …` and keeps the IMV. `reflex_rebuild_imv` and
  `reflex_reconcile` rebuild the data from the stored query without
  validating it again and keep working on such an IMV.
- `reflex_audit`, `trigger-attached`: new findings `Source <s> has
  disabled trigger(s): …` and `Table <t> of the partition tree of source
  <s> is missing trigger(s): … and has disabled trigger(s): …`. The remedy
  of every `trigger-attached` finding is two statements: `SELECT
  reflex_rebuild_triggers('<source>');` then `SELECT
  reflex_reconcile('<imv>');`.
- `reflex_audit`, `trigger-mode-matches`: a source read by a DEFERRED IMV
  is expected to carry DEFERRED functions whatever the mode of the IMV
  audited; the finding reads `Trigger function <f> is in <mode> mode but
  <reason>.`
- `reflex_audit`, `internal-tables-exist`: requires `__reflex_scratch_<imv>`
  and the `__reflex_shrunk_<imv>` table of a top-K MIN / MAX IMV, and no
  `__reflex_affected_<imv>` for an ungrouped aggregate.
- `reflex_audit`, `partition-mirror`: the remedy for a dropped leaf is
  `SELECT reflex_sync_partitions('<imv>'); SELECT
  reflex_reconcile_partition('<imv>', '', '<source partition>');`.
- `reflex_rebuild_triggers` drops and recreates disabled triggers of the
  source and of its partition tree. Create and the COMMIT-time sync only
  add missing triggers, so a `DISABLE TRIGGER` is undone by
  `reflex_rebuild_triggers` alone.
- `create_reflex_ivm_if_not_exists` on an existing IMV returns its no-op
  before validating the query, so a deployment script that re-runs a
  definition 1.12.0 would refuse keeps working.
- Time-zone-sensitive functions are accepted with one WARNING (two-argument
  `date_trunc`, `date_part`, `EXTRACT`, `to_char`, `to_date`, two-argument
  `to_timestamp`, one-argument `date`): on `timestamptz` their result
  depends on the writer's `TimeZone`.
- A HAVING clause that does not parse is kept as written and fails at
  create; it used to be dropped from the end query.

**Tests.** `v112_sq_commit_path` (the DEFERRED subquery cells replayed
with real COMMITs) is `#[ignore]`d: a committed DEFERRED create fails the
creates of tests running beside it. CI runs it alone, after the suite, on
each PostgreSQL version.

**Known limitations.**

- `SUM` / `AVG` over `DOUBLE PRECISION` stay accepted and are maintained by
  adding and subtracting, which is not the order PostgreSQL adds rows in.
  Measured on 20 000 rows of two-decimal values in 5 groups (sums about 4.5
  million) through 90 writing statements: largest error of a SUM 1.2e-8.
  After one outlier is inserted and deleted the error stays while the group
  has rows: 5.5e-5 for a value of 1e12, 0.055 for 1e15, 10.07 for 1e18. It
  disappears when the group empties and refills. Sum `(x)::numeric` when
  cancellation matters.
- Unquoted identifiers are folded as in a UTF-8 database (ASCII letters
  only). In a single-byte encoding PostgreSQL also folds non-ASCII letters
  through the locale; write such names in lower case.
- Session-dependent functions are found by a name list, and a moving
  date/time value that is not a typed literal is not seen (`WHERE ts <
  'now'`, `daterange('2024-01-01', 'today')`).
- `reflex_ivm_status()`, `reflex_audit()` and `reflex_flush_partitions()`
  called without an argument read the IMVs or pending roots of every
  schema, so they can still wait behind another schema's partition DDL;
  `reflex_audit('<imv>')` and `reflex_flush_partition_source('<root>')` do
  not. The COMMIT-time flush uses the per-root path.
- A partitioned source whose flush keeps meeting a transient lock error is
  retried without a cap, each attempt waiting up to `lock_timeout`; its
  IMVs stay flagged `known_stale` with the lock error meanwhile. After the
  retry succeeds, IMVs that are correct can stay flagged `known_stale`
  until their next `reflex_reconcile`.
- On PostgreSQL builds compiled with `--enable-cassert` and older than the
  minor releases carrying upstream commit `e371cb7b89` (after 15.19, 16.15,
  17.11, 18.6), an error caught by a COMMIT-time flush (partition or
  DEFERRED) at an explicit `COMMIT` aborts the backend on a PostgreSQL
  assertion. 1.11.5 does the same. Production builds have no assertions:
  the flush logs its WARNING, flags the IMVs `known_stale` and the COMMIT
  succeeds.

**Silent defects that remain open**, filed in `untreated_bugs/`:

- `DROP TABLE` of a source partition that holds rows leaves every IMV of
  the source stale (`DETACH` then `DROP` is followed).
- A partition attached with rows whose name does not read back unquoted
  (`"C2_D"`, `"c4 d"`) is lost.
- A table that is already a source and is then attached as a partition:
  its IMVs miss the writes addressed to the new partitioned table. (A
  source that is a partition when the IMV is created is refused.)
- Swapping a partition of a partitioned source that an IMV reads but does
  not mirror (a second partitioned source of a join) leaves that IMV stale,
  unflagged.
- A partition change and a write to another source of a join IMV in one
  transaction count the joined rows twice.
- `reflex_reconcile` called by hand in the transaction that attached a
  filled partition: the flush adds the partition's rows again.
- `SET CONSTRAINTS ALL IMMEDIATE` on a partitioned source read by several
  DEFERRED IMVs applies a write twice.
- A CTE over a partitioned source does not follow a new partition (silent
  in DEFERRED, raises in IMMEDIATE).
- IMVs created before 1.12.0 over two sources that share trigger names
  (`s.t` and `s_t`) stay unmaintained until recreated; 1.12.0 refuses the
  second create.
- `COUNT(DISTINCT e.id)` over a LEFT JOIN is silently wrong in DEFERRED
  (raises in IMMEDIATE).
- An ungrouped `MAX(d) - MIN(d)` over dates reads 0 on an emptied source
  (IMMEDIATE).
- An aggregate whose WHERE is two AND-ed conjuncts that each hold an OR
  (`(a OR (b AND c)) AND (d OR (e AND f))`) goes stale on an UPDATE that
  moves a row out of it (the same on 1.11.5).
- A type named like the source table is rewritten at trigger time (silent
  in DEFERRED); a three-part column reference with a bare FROM entry
  (`SELECT public.t.id FROM t`) is silently stale in DEFERRED.
- One statement writing two sources of an IMMEDIATE join IMV counts
  ΔA ⋈ ΔB twice (open since 1.11.5).

### Tests

- 22 new `pg_test` files (`pg_test_v112_*`) and 6 new unit-test files.
  The suite on the release tree: 3689 passed, 0 failed, 1 ignored on
  PostgreSQL 16, 17 and 18; 3685 / 0 / 1 on PostgreSQL 15.
- The partition-swap, schema-isolation and suspension tests commit each
  step through `dblink` in private databases; the schema-isolation tests
  hold every table of another schema `ACCESS EXCLUSIVE` while one schema
  is maintained under a `lock_timeout`.
- Fixtures are real tables and real IMVs; each cell compares the IMV with
  a fresh run of its query (bidirectional `EXCEPT ALL`) after create and
  after every write, in IMMEDIATE and DEFERRED. DEFERRED and partition
  scenarios commit each step through a second session, so the flush is the
  one COMMIT runs.
- Each fix was reverted to check that its tests fail. Cells that pass
  either way are kept as pins of the surrounding behaviour.
- No test present on 1.11.5 was deleted, renamed, ignored or edited.
- Outside the suite, 650 definitions were run in both modes on 1.11.5 and
  on the tree before the last fix rounds, every step committed: no
  definition correct on 1.11.5 was wrong or raising.

### Internal (no behaviour change)

- Code the compiler reports unused is removed; test-only helpers are gated.
- Comments state invariants and reasons; release numbers, report names and
  finding tags are removed. Each file is proven token-identical.
- Defects met and not fixed are filed one per report in `untreated_bugs/`.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.12.0';` replaces
  `public.__reflex_on_sql_drop()`, creates
  `public.__reflex_named_relation(TEXT)` and
  `public.__reflex_reads_relation(TEXT[], TEXT)`, and replaces
  `public.__reflex_on_ddl_command_end()` and
  `public.__reflex_partition_flush_fn()`, each byte-identical to the
  bootstrap text. No registry column, table, index, trigger, event trigger
  or function signature changes.
- **The new library requires the migration.** Its partition flush calls
  `__reflex_reads_relation`: between installing the library and running
  `ALTER EXTENSION`, a transaction that creates, attaches or detaches a
  partition of a source fails at COMMIT with `function
  public.__reflex_reads_relation(text[], text) does not exist` and is
  rolled back, and `reflex_flush_partition_source` raises the same. Run the
  update right after installing the library.
- The migration does not rewrite per-source trigger functions. A source
  keeps its 1.11.5 functions and trigger set until
  `reflex_rebuild_triggers('<source>')` runs or its IMVs are recreated.
  Run it only on sources that carry triggers: on the `<imv>__base` table
  of a window IMV it installs a set that makes every later write raise.
- Recreate every IMV after the update: see "Upgrading to 1.12.0" in
  `docs/getting-started/upgrading.md` for the steps, the refused
  definitions and what stays stale otherwise.

---

## [1.11.5] - 2026-10-01

Rebuilds no longer wipe the IMVs built on the rebuilt one. In the field
(2026-09-30) a rebuild of `current_assortment_activity_view` — a TRUNCATE +
INSERT — reached `sop_forecast_view`, a 123 M-row passthrough IMV that LEFT
JOINs it, as "every row deleted": every plan partition was emptied, and the
discarded DEFERRED flush that should have refilled it made the loss
permanent. TRUNCATE / reconcile / partition rebuilds of an IMV with
dependents now hand dependents a row diff instead of clearing or fully
rebuilding them; DEFERRED truncates rebuild once at COMMIT; volume dispatch
sizes two-level partitions by their leaves and uses a higher threshold for
IMVs with dependents; multi-source guard rebuild failures mark the IMV stale
instead of aborting COMMIT. `ALTER EXTENSION pg_reflex UPDATE TO '1.11.5';`

---

### Fixed

- **(DATA LOSS) A source TRUNCATE cleared the IMV.** Clearing is only right
  when the truncated source empties the result; on the nullable side of an
  outer join, under `NOT EXISTS` or for an ungrouped aggregate the result
  keeps rows, and they were deleted — in DEFERRED mode outside the flush's
  savepoint, so a failed flush made the loss permanent. The IMV is now
  rebuilt from its base query. An IMMEDIATE IMV is rebuilt at TRUNCATE time;
  a DEFERRED one at COMMIT, skipping the deltas staged for it before that
  rebuild and applying the later ones, after every upstream DEFERRED IMV with work staged in the
  transaction has settled (decided exactly by the new
  `__reflex_xid_is_current`; bounded — on exhaustion it rebuilds anyway and
  marks the IMV `known_stale`). A rebuild that cannot run marks the IMV
  `known_stale` instead of clearing it.
- **(DATA LOSS) A rebuild of an IMV with dependents reached them as
  "everything deleted, everything inserted".** `reflex_reconcile`, the
  wipe-threshold dispatch and the trigger-side full-refresh fallbacks
  rewrote the target with DELETE / TRUNCATE + INSERT; an outer-join or
  aggregate dependent then dropped or recomputed every row. The target is
  now rewritten with `reflex_rebuild_target_rows`: a keyed (or whole-row)
  DELETE / UPDATE / INSERT diff, so a dependent receives exactly the rows
  that changed and its work is proportional to them. An IMV without
  dependents keeps the cheaper DELETE + INSERT. Key columns holding NULLs
  match NULL-safely (a no-op rebuild rewrites no NULL-key row); a target
  with a second unique or exclusion index, where a keyed UPDATE swapping two
  rows' values would raise 23505, uses the whole-row diff. The diff is
  computed into a temporary table first and applied by row identity, so the
  dependents' triggers plan their own delta work under the caller's planner
  settings.
- **(DATA LOSS) Partition rebuilds of an IMV with dependents.** A reconcile
  of a partitioned IMV and a partition swap diff each populated leaf through
  the root instead of swapping it, and cascade a full refresh only to
  dependents that ignore the IMV (they see none of the diff). Partitioned
  rebuilds always diff, also when a generated child failed: the dependent
  stays consistent with its parent, the stale flags are kept, and the call
  still returns `ERROR`.
- **(SILENT) The multi-source cross-source guard rebuilt inside the flush.**
  It could read an upstream DEFERRED IMV before that IMV's own COMMIT-time
  flush, and a failure aborted the COMMIT. It now runs through the same
  COMMIT-time path: it waits for upstream DEFERRED IMVs to settle, runs
  isolated in a subtransaction, and a failure marks the IMV `known_stale`
  instead of aborting the COMMIT (`flush_failure_policy = error` still
  aborts).
- **(SILENT) DML after a mid-transaction `SET CONSTRAINTS ALL IMMEDIATE`
  was skipped for a DEFERRED IMV already rebuilt in the transaction**
  (source TRUNCATE or guard). Each rebuild now records its command id and
  the next xid; deltas staged after it are applied, and a second TRUNCATE,
  two sources changed again, or a staged row whose aborted flush leaves its
  position unknown rebuild it again.
- **Volume dispatch misjudged two-level partitioned IMVs.** A dirty
  partition was sized by the partitioned child's own `reltuples` (0 / -1,
  so always the 1000-row floor), and any ≥ 500-row change to a plan rebuilt
  the whole plan. It is now sized by the sum of its leaves
  (`__reflex_rebuild_cost_rows`). An IMV with dependents uses a wipe
  threshold of 0.9 instead of 0.5 (`__reflex_target_propagates`): its
  rebuild also pays for the diff and for its dependents' maintenance, so
  the incremental path stays cheaper for longer.
- **A DEFERRED partitioned passthrough IMV's flush never went hot.** The
  flush routes every operation through the UPDATE dispatch, which counted the
  distinct partition values touched instead of the changed rows, so a flush
  changing 80% of a plan was maintained by keyed DELETE + INSERT instead of
  one partition rebuild. A partition now counts its changed rows — the larger
  of its old-image and new-image counts, so N rows updated in place count N
  and a row moved across partitions counts once on each side. This covers
  every operation the flush nets (bulk UPDATE, upsert, MERGE, and also bulk
  INSERT and DELETE, which can now go hot) and passthrough IMVs that join
  other tables (`sop_forecast_view`). The numerator counts source rows, not
  IMV rows: a WHERE filter leans it hot, a one-to-many join cold. A hot flush
  runs at COMMIT and holds its locks until the COMMIT ends: the leaf diff of a
  target with observing dependents takes `EXCLUSIVE` on the IMV root (other
  writers wait, readers do not), the swap's `DETACH` takes
  `ACCESS EXCLUSIVE` on the plan-level child and the leaf (readers not pruned
  away from that plan wait). IMMEDIATE IMVs never go hot (next entry).
- **(SILENT) IMMEDIATE mixed upsert / MERGE / writable CTE double count;
  IMMEDIATE dispatch now stays incremental** (pre-existing: since 1.4.6 for
  unpartitioned IMVs, 1.6.0 for partitioned ones). One statement that both
  changes and inserts or deletes source rows fires several statement
  triggers (an upsert: AFTER UPDATE, then AFTER INSERT), each seeing the
  source as the whole statement left it. When one of them rebuilt the IMV —
  the trigger's pre-scratch "Path B" (`|statement rows| / |source| >=
  wipe_threshold`), the unpartitioned high-selectivity dispatch, the
  partitioned aggregate or passthrough hot-partition dispatch — the rebuild
  already held the later triggers' rows, which they applied again: an
  aggregate silently double counted them (a partitioned `GROUP BY plan, m,
  id` IMV upserted with 1600 conflicting + 400 new rows: 800 wrong rows), a
  passthrough failed with 23505. A statement trigger now never rebuilds:
  Path B is removed from the trigger bodies and the dispatch runs only in the
  DEFERRED flush, which nets the transaction into one delta first.
- **(SILENT) A volume-dispatch rebuild that failed softly lost the
  statement's change.** The dispatch hands a bulk change to
  `reflex_reconcile` / `reflex_reconcile_partition`, which report some
  failures (e.g. a mirror child that cannot be swapped) as a returned
  `ERROR: …` string, and the dispatch discarded it with `PERFORM`: the
  unpartitioned high-selectivity path then skipped the statement's delta
  too, so the source change committed and the IMV stayed behind it with
  `known_stale = FALSE`. Every dispatch call (hot partition, trip-cap,
  high-selectivity) now raises on an `ERROR` result: an IMMEDIATE IMV's
  writing statement fails, and a DEFERRED IMV is marked `known_stale` by its
  flush savepoint (`pg_reflex.flush_failure_policy` applies). The IMMEDIATE
  trigger body's pre-scratch Path B, which also discarded it, is removed
  (next entries); several partition-flush callers still discard it (filed,
  see Known limits).
- **(SILENT) A keyless passthrough IMMEDIATE IMV double inserted on a
  mixed statement** (pre-existing). With no key mapping for the source, the
  DELETE / UPDATE triggers fully refresh the IMV, and the INSERT trigger
  then appended its transition rows again: an upsert, MERGE or writable CTE
  that both updated and inserted left the inserted rows twice. Every
  statement on such a source, INSERT included, now fully refreshes the IMV
  (idempotent, as a diff when it has dependents): a keyless passthrough
  query (outer-join nullable side, `DISTINCT`) has no per-row delta that is
  right in every shape, so INSERTs on such a source now cost a refresh.
- **(SILENT) A partition flush applied a delta on top of the reconcile that
  already held it** (pre-existing). `reflex_flush_partitions` runs, per IMV,
  the reconcile of the swapped partition and then the staged partition
  deltas; when the reconcile rebuilt from base tables that already held the
  delta's rows, the delta was applied again. The IMV's calls now stop at the
  first `RECONCILED` result.
- **(SILENT) The DEFERRED flush dispatch of a LIST-partitioned IMV skipped
  or double-applied rows** (pre-existing). Rows whose partition value is
  NULL were left out of the cold body's partition pruning, so they were
  never maintained; and a hot child holding several LIST values was excluded
  from the cold body by one representative value only, so its other values'
  rows were maintained twice (cold delta + partition rebuild). The cold body
  now excludes every value the batch touched in a hot child, and prunes by
  every non-NULL cold value. A NULL partition value resolves to the child
  holding it (`__reflex_partition_child_for_key` now maps a NULL key to the
  DEFAULT child, or a LIST child listing NULL), so it is rebuilt with that
  child when the child goes hot and kept in the cold body otherwise (LIST
  and RANGE, one- and two-level). A child touched only by NULL values has no
  representative key for `reflex_reconcile_partition` and stays cold.
- IMMEDIATE partitioned passthrough statements emit the plain cold body (no
  volume-dispatch block), so they no longer call
  `__reflex_target_propagates` / `__reflex_rebuild_cost_rows`; the IMMEDIATE
  keyed DELETE / UPDATE prune LIST partitions by the touched values (NULL
  values unpruned).
- **A user unique index with NULLs distinct was used as the rebuild diff
  key.** Such an index admits several rows whose key is NULL, so a rebuild
  of an IMV holding two of them aborted with "yields duplicate keys". A
  unique index is now a diff key only when it is `NULLS NOT DISTINCT` or
  all its key columns are `NOT NULL` (pg_reflex's own `__reflex_uk_*`
  indexes are); otherwise the next candidate or the whole-row diff is used.
- **A reconcile of a DEFERRED IMV mid-transaction double-applied the deltas
  already staged for it.** `reflex_reconcile` / `reflex_rebuild_imv`, and the
  refresh an upstream rebuild gives its IGNORING dependents (high-selectivity
  dispatch, partitioned cascade), rebuilt from base tables that already held
  the staged writes, and the COMMIT flush applied them again (aggregates
  silently wrong, keyed passthroughs flagged stale). The rebuild now records a
  watermark so the flush skips the deltas staged before it and applies the
  later ones; reached from inside a trigger, the IMV is rebuilt by the
  COMMIT-time pass instead, after the statement has staged its own delta.
- **The multi-source guard's COMMIT-time rebuild could drop orphan IMV
  partitions.** It now reconciles with `drop_orphans => FALSE`.
- **A postponed COMMIT-time rebuild could deadlock with a session flushing the
  same IMV.** Its stale flag, and the other registry writes of the COMMIT-time
  pass, now take the IMV's advisory lock before the registry row.
- **(SILENT) A partition- or key-scoped reconcile of a DEFERRED IMV
  double-applied the deltas staged for the rebuilt partitions** (pre-existing).
  `reflex_reconcile_partition`, a partition swap's COMMIT-time flush, the
  hot-partition cascade and the key-scoped cascade rebuilt slices from base
  tables that already held the transaction's writes, and the COMMIT flush
  applied them again. The rebuild now records its watermark with the slice it
  rebuilt (the source partitions' constraints, or the keys); the flush skips
  only the earlier staged rows that fall in that slice and applies the rest,
  each image of a row moved across partitions on its own side. When a staged
  row's slice cannot be told from the row — the IMV joins another table or
  reads its source twice — the IMV is listed for a full rebuild at COMMIT
  instead, as is a scoped reconcile reached from inside a trigger. That full
  rebuild is the cost of the fallback on a large IMV (see Known limits).
- **An in-trigger reconcile of a disabled DEFERRED IMV was queued for COMMIT**,
  where `reflex_reconcile` refused it and the IMV was flagged `known_stale`. It
  is refused at once, and an IMV disabled after being queued is dropped from
  the COMMIT-time pass without a flag.
- **The flush and COMMIT-time rebuild failure handlers wrote the registry
  without the IMV's advisory lock**: the failed block's rollback released the
  lock taken inside it. Each handler takes it again first.

### Added

- `reflex_rebuild_target_rows(view, rebuild_sql)` — the dependent-safe
  target rebuild used by every rebuild path.
- Internal: `__reflex_target_propagates(view)`,
  `__reflex_rebuild_cost_rows(view, child oid)`,
  `__reflex_xid_is_current(xid)` and `__reflex_xid_precedes(xid, xid)`.

### Changed

- A source TRUNCATE on a DEFERRED IMV enqueues a `'TRUNCATE'` request row
  in `__reflex_deferred_pending`; the COMMIT-time event that fires for it
  deletes exactly that row, and bulk pending deletes keep such rows.
- Default wipe threshold for an IMV with dependents: 0.9
  (`reflex.wipe_threshold` and the per-IMV `wipe_threshold` still win).
- `reflex_reconcile` (and `reflex_reconcile_partition`) of a DEFERRED IMV
  called from inside a trigger returns `RECONCILE QUEUED FOR COMMIT`: the IMV
  is rebuilt by the COMMIT-time pass, after the statement stages its delta.
- IMMEDIATE IMVs no longer switch to a rebuild on a large statement: the
  statement triggers' Path B is removed, and the high-selectivity and
  hot-partition dispatch run only in the DEFERRED flush.
- A passthrough IMMEDIATE IMV with no key mapping for a source is fully
  refreshed by every INSERT on it too (it already was by UPDATE / DELETE).
  On a large IMV this is orders of magnitude slower than a keyed delta
  (~225 ms per single-row INSERT on a 300 k-row IMV), so `create_reflex_ivm`
  now raises a WARNING naming the unkeyed sources: pass a unique key (3rd
  argument) or use DEFERRED. The INFO for a source whose PK is not projected
  no longer claims a row-matching fallback.

### Known limits

**Large IMMEDIATE statements stay incremental.** A bulk UPDATE / INSERT /
DELETE on the source of an IMMEDIATE IMV is maintained by the set-based
incremental delta however large, where 1.11.4 could rebuild the IMV or the
touched partitions. On a statement sweeping most of a source this writes
more WAL and leaves more dead tuples than a rebuild would; bulk jobs should
write through DEFERRED IMVs, whose flush still switches to a rebuild. A
passthrough IMMEDIATE IMV with no key mapping for the source is the
exception: every statement, INSERT included, fully refreshes it
(`create_reflex_ivm` warns).

Filed in `untreated_bugs/`: a passthrough dependent with no key mapping for
a source still fully refreshes (now as a diff) on every statement on it;
hot/cold dispatch still classifies a two-level mirror per plan, not per leaf;
`create_reflex_ivm` rejects three mixed-case name shapes; the partitioned
passthrough 23505 flush failure of the incident is not reproduced yet;
trigger regeneration through `reflex_rebuild_triggers` inside past
`ALTER EXTENSION` updates (and for bare-name public sources, always) was a
silent no-op; several
`reflex_flush_partitions` / partition-delta callers still discard a soft
`ERROR` from `reflex_reconcile`; under `SET CONSTRAINTS ALL IMMEDIATE` one
statement writing two sources of a DEFERRED join IMV counts their cross
product twice; under `SET CONSTRAINTS ALL IMMEDIATE` a DEFERRED IMV queued
for the COMMIT-time reconcile from inside a statement's trigger (the dispatch
refreshing an ignoring dependent that also reads the written table) is
rebuilt before that statement stages its delta, which is then applied twice;
a data-modifying CTE and a `reflex_reconcile` of a DEFERRED IMV over the same
source in one statement apply the CTE's write twice; two sessions
flushing different sources of one DEFERRED join IMV can deadlock at COMMIT
(40P01, retryable). All pre-existing. New in 1.11.5: a partition- or
key-scoped reconcile of a DEFERRED IMV reached from inside a trigger becomes a
full rebuild of the IMV at COMMIT; the aggregate partition dispatch now sizes
a partition by its source rows but still counts dirty groups, so an aggregate
partition with many source rows per group rarely goes hot. Pre-existing and
(SILENT): an IMMEDIATE MIN / MAX aggregate whose source gets one statement
that both updates and inserts (upsert, MERGE) recomputes the updated groups
from a source that already holds the inserted rows, which the INSERT trigger
then merges again; a later DELETE of those rows can leave a wrong MIN / MAX;
one statement writing two sources of an IMMEDIATE join IMV (a data-modifying
CTE, or a user trigger on one source writing the other) applies ΔA ⋈ ΔB
twice. PostgreSQL 15 only: a MERGE with UPDATE and INSERT actions into a
multi-level partitioned source hands the AFTER UPDATE trigger a NEW
transition table holding inserted rows, so every IMV over it, IMMEDIATE or
DEFERRED, gets wrong deltas (a PostgreSQL 15 defect, reproduced without
pg_reflex; pg16+ correct). On PostgreSQL 15 avoid such a MERGE into a
multi-level partitioned source (split it into UPDATE and INSERT), or upgrade.

Cost of a DEFERRED TRUNCATE rebuild of a large partitioned IMV with
dependents: each populated leaf is diffed by staging
`SELECT * FROM (<base query>) WHERE <leaf constraint>` into a temporary
table and comparing it with the leaf (src/partition.rs leaf diff →
`rebuild_target_rows`), so the rebuild after a TRUNCATE pushes the whole
base query through temporary storage at COMMIT, leaf by leaf — about 123 M
rows for `alp.sop_forecast_view` on db_clone. Temporary space
(`temp_file_limit` for the diff's sorts and hashes, and the temp
tablespace) is the failure mode: the rebuild fails, the IMV is marked
`known_stale` and keeps its rows (it is not wiped). Size them for the
largest IMV with dependents, or reconcile it outside peak hours.

Full rebuilds now run inside `COMMIT`: a DEFERRED IMV whose source was
truncated, a DEFERRED multi-source IMV whose cross-source guard fires
(two of its sources written in one transaction), a DEFERRED IMV reconciled
from inside a trigger (the high-selectivity dispatch refreshing a DEFERRED
ignoring dependent, a hot-partition cascade into a DEFERRED dependent), and a
DEFERRED IMV joining another table that a partition- or key-scoped reconcile
reached after the transaction staged deltas for it, are rebuilt from their
base query — and diffed into their dependents — when the transaction
commits. `statement_timeout` (and a pooler's query timeout) covers that
`COMMIT`; a cancel (`57014`) is not caught by the flush's failure handling,
so it aborts the `COMMIT` and rolls back the job's writes instead of marking
the IMV `known_stale`. A job that TRUNCATEs and reloads a source of a large
IMV should run `SET LOCAL statement_timeout = 0` in that transaction. The
rebuild diff of an IMV with dependents stages rows in a temporary table, so
a transaction that runs one cannot `PREPARE TRANSACTION` (two-phase commit);
filed with the keyless whole-row diff's two full sorts of the target.

### Tests

- New `pg_test_truncate_outer_join.rs`, `pg_test_rebuild_diff.rs`,
  `pg_test_reconcile_diff_apply.rs`, `pg_test_rebuild_cost.rs`,
  `pg_test_rebuild_commit.rs`, `pg_test_deferred_marker.rs` and
  `pg_test_statement_mixed_dml.rs` (upsert / MERGE / writable CTE in both
  modes, and the trigger-body migration replayed on 1.11.4 bodies); every must-be-absent / must-stay-unchanged
  assertion mutation-checked. Cost assertions count the rows written after a
  boundary command id (`rows_rewritten_since`: target rows whose `cmin` is
  past the boundary), which also sees leaves refilled or swapped in; a few
  keyed-diff tests count rows inserted + updated + deleted in the current
  transaction over the partition tree (`tree_xact_changes`), and COMMIT-time
  tests read `pg_stat_user_tables`.

### Migration

- `sql/pg_reflex--1.11.4--1.11.5.sql` creates the five functions
  (`reflex_rebuild_target_rows`, `__reflex_target_propagates`,
  `__reflex_rebuild_cost_rows`, `__reflex_xid_is_current`,
  `__reflex_xid_precedes`), replaces `__reflex_deferred_flush_fn`, rewrites
  the pending-row DELETE in every installed deferred TRUNCATE trigger body
  (no lock on the sources; an INFO line reports how many it found and
  rewrote, a WARNING lists any it skipped with the remedy), cuts the Path B
  rebuild out of every installed IMMEDIATE INSERT / DELETE / UPDATE trigger
  body (in place, no lock; INFO / WARNING likewise), replaces
  `__reflex_partition_child_for_key` (a NULL key resolves to its DEFAULT
  child), and deletes leftover `'TRUNCATE'` request rows.
- **Writes and COMMITs on IMV sources fail until the update runs.** The
  1.11.5 library's generated SQL calls the five functions, so between the
  library install and the update: the flush of a DEFERRED grouped
  aggregate or partitioned passthrough IMV (the IMV is marked `known_stale`
  under the default `flush_failure_policy`), a `TRUNCATE` of an IMMEDIATE
  IMV's source and the trigger-side full refreshes (including every write
  to a source a passthrough IMMEDIATE IMV has no key mapping for) fail with
  `function … does not exist`; a `COMMIT` that leaves a
  DEFERRED IMV to rebuild aborts; a `TRUNCATE` of a DEFERRED IMV's source
  commits and marks the IMV `known_stale`. The failures are loud — the
  write is rolled back, no IMV silently diverges. In a quiet window: install
  the library, immediately run `ALTER EXTENSION pg_reflex UPDATE TO
  '1.11.5'` in every database that has the extension, recycle connection
  pools, then `reflex_reconcile` any IMV left `known_stale`
  (docs/getting-started/upgrading.md, "Upgrading to 1.11.5").

## [1.11.4] - 2026-09-18

Silent-wipe observability and prevention. In the field a 1.86 M-row
forecast IMV was emptied to 2 161 rows by a partition swap evaluated while
its own status filter excluded the slice, and `reflex_ivm_status()` kept
reporting `known_stale = false`, `last_error = NULL` and the pre-wipe row
count. On a dev cluster, four partition source roots had been capped at the
flush failure cap for up to six weeks — every later flush skipped them —
while every dependent IMV reported healthy, because a full reconcile clears
`known_stale` but never re-arms the root. This release makes those states
visible, records them durably, lets an operator make a flush failure abort
the caller, refuses at create time the `ignore_sources` shape that caused
the wipe, and heals the incident shape: a change to an ignored source that
joins onto the partition key queues exactly the affected partitions for
rebuild. `ALTER EXTENSION pg_reflex UPDATE TO '1.11.4';`

---

### Fixed

- **(SILENT) A caught deferred-flush failure left the IMV looking healthy.**
  The per-IMV handler discarded the staged delta and recorded `last_error`,
  but never set `known_stale`, and the next successful flush cleared
  `last_error` — erasing the only evidence of a missed change. The handler
  now sets `known_stale` with a `stale_reason` that names the failure and
  prescribes `reflex_reconcile('<imv>')`, and a successful flush keeps
  `last_error` while the IMV is still stale.
- **(SILENT) `reflex_ivm_status()` reported the pre-wipe row count.**
  `row_count` is the planner estimate `reltuples`, which a partition-scoped
  rebuild never refreshes on the parent. An IMV carrying an anomaly —
  `known_stale`, a retained `last_error`, or a slice-changing rebuild newer
  than the target's last `ANALYZE` — is now counted exactly, and the new
  `is_estimate` column says which one the caller got.
- **(SILENT) An IMV whose partition source is capped reported healthy.**
  A source root that fails five consecutive partition flushes is skipped by
  every later flush with only a `WARNING`. `reflex_ivm_status()` now reports
  every IMV depending on a capped root — directly or through other IMVs — as
  `known_stale`, with the root, its failure count, its last error and the
  `reflex_reset_partition_failures` + `reflex_flush_partition_source` remedy
  in `stale_reason`. Derived live from the queue, so a reconcile cannot hide
  it and the report clears exactly when the root drains.
- **(SILENT, CREATE-TIME) `create_reflex_ivm` accepted an unsound
  `ignore_sources` entry.** Ignoring a source the query filters or
  inner-joins on means a change to it never refreshes the IMV, and a later
  partition rebuild from the base query makes the divergence permanent for
  the slices it rewrites — the mechanism of the field wipe. Such an entry is
  now refused unless acknowledged; see *Changed*.
- **(SILENT) A slice emptied while an ignored source excluded it never
  healed.** A partition swap evaluated while `demand_planning.status` was
  outside the IMV's filter correctly wrote zero rows, and when the status
  returned nothing refreshed the IMV, because `demand_planning` is ignored.
  When an ignored source joins by equality onto the expression the IMV's
  first partition column projects, `create_reflex_ivm` now installs
  statement triggers on it that queue the changed rows' partition keys into
  `__reflex_heal_pending` (an `UPDATE` only when a column the query reads
  changed). The write itself never rebuilds. Until a sweep heals it, the
  IMV and every IMV reading it report `known_stale` with the
  `reflex_heal_ignored_sources('<imv>')` remedy. Ignored sources that
  cannot be mapped to partitions keep the previous contract: a source read
  more than once, a join other than `INNER` or a `LEFT JOIN` of the source,
  an `OR`, or a key column that is not of a built-in type matching the
  partition column's (same type, two integer types, or `text` and
  `varchar`). The trigger function is `SECURITY DEFINER`, so a writer
  needs no pg_reflex grant nor `USAGE` on `public`, and it cannot be called
  directly. It evaluates no user-defined function: watched columns are
  compared through a NULL flag and their types' output functions, never a
  cast, and a key is rendered only while its column is of a built-in type.
  It opens no subtransaction and never fails the write: a `TRUNCATE`
  (also `TRUNCATE ONLY` of an inheritance parent) marks the IMV
  `known_stale`, keeping any earlier `stale_reason` and `stale_since`, and
  a renamed or dropped mapped column, or a key retyped to a user type,
  queues nothing and is reported stale from the catalog by
  `reflex_ivm_status` and `reflex_doctor` (F14).
- **`reflex_reconcile` of a partitioned IMV with a dependent raised a false
  alter alarm, and under `alter_source_policy = 'error'` aborted.** The
  partition sync's own `DISABLE` / `ENABLE TRIGGER USER` around its default
  relocation is an `ALTER TABLE` on the IMV root, which the alter-source
  alarm reported to every dependent ("was altered; … may be stale — run
  `reflex_rebuild_imv`") on every call. The toggle is now bracketed by
  `pg_reflex.internal_trigger_toggle_root` for that statement only; a
  user's `ALTER` of the same relation, even in the same transaction, is
  still reported.
- **`reflex_reconcile` printed one `already exists, skipping` NOTICE per
  partition child and sync pass.** The sync re-issued `CREATE TABLE IF NOT
  EXISTS` for every existing child (24 NOTICEs for a 3-leaf IMV); it now
  creates only missing children, re-issuing every create after it drops a
  bound-collision orphan whose `CASCADE` may have removed one. The
  deferred cross-source guard no longer re-creates its batch marker on
  every later flush of the batch.
- **`reflex_ivm_status()` counted an actively swapped partitioned IMV
  exactly on every call.** A slice-changing swap logged a `rebuild` event
  that only an `ANALYZE` of the IMV root retired, and autovacuum never
  analyzes a partitioned parent, so an IMV swapped at every push never
  returned to the O(1) estimate. The event is now stamped before the swap
  ANALYZEs the rebuilt leaf, which retires it, and a partitioned IMV's
  estimate is the sum of its leaves' `reltuples`.
- An invalid `pg_reflex.flush_failure_policy` value falls back to `warn`
  and now raises a `WARNING` naming it, matching
  `pg_reflex.alter_source_policy`.

### Added

- `public.__reflex_event_log`: a durable row for every caught flush failure
  (`event = 'error'`, with `sqlstate`) and every rebuild that changed a
  slice's row count (`event = 'rebuild'`, with `slice`, `rows_before`,
  `rows_after`). Never written on an ordinary flush. Pruned with the new
  `reflex_prune_event_log(INTERVAL)`.
- `pg_reflex.flush_failure_policy` GUC: `warn` (default) keeps the
  per-IMV catch; `error` lets a failed deferred flush abort the caller's
  transaction so the offending write is rolled back.
- `reflex_ack_ignore_source(imv, source)` and the create-time `'!source'`
  marker, backed by a new `ignore_ack` registry column. The function
  records the acknowledgement in the registry and in the stored
  `create_args` in one statement, so `reflex_rebuild_chain` replays it.
- `ignore-soundness` audit check, forwarded by `reflex_doctor` as **F13**
  (report-only).
- `is_estimate` column on `reflex_ivm_status()`.
- `reflex_heal_ignored_sources(imv DEFAULT NULL)`: rebuilds the queued
  partitions, shallowest IMV first. A heal removes only the queue rows it
  read, so a key re-queued mid-heal survives; a failed heal keeps its rows
  with `last_error`, while a query cancel or shutdown is re-raised. An
  integer key the partition column is too narrow to hold is drained without
  a rebuild. `reflex_scheduled_reconcile` runs it first (one
  `HEALED` row per IMV), and `reflex_doctor` reports queued heals as
  **F14**, running the heal under `fix => TRUE` and claiming `fixed` only
  once the queue is empty. `drop_reflex_ivm` deletes the IMV's queue rows.

### Changed

- **`create_reflex_ivm` refuses an `ignore_sources` entry the query depends
  on** — referenced in `WHERE`, `HAVING`, `JOIN … ON` or `JOIN … USING`,
  inner- or cross-joined, or unattributable (CTEs, set operations, wildcard
  projections, qualifiers from nested scopes). The refusal message leads
  with the `reflex_ack_ignore_source` call that clears it. Because
  `reflex_rebuild_chain` replays the create, **audit before rebuilding a
  chain or recreating an IMV**:
  `SELECT * FROM reflex_audit() WHERE category = 'ignore-soundness';`.
  `reflex_rebuild_imv` is a reconcile and is not affected. A client that
  drops and recreates IMVs from stored definitions must add the `'!'`
  marker to the entries it accepts, or its creates are refused; a
  `create_reflex_ivm_if_not_exists` of an existing IMV is a no-op and is
  not refused.
  Columns referenced only in the projection or `GROUP BY` are not yet
  checked (filed in `untreated_bugs/`).
- `reflex_ivm_status()` returns one extra trailing column, `is_estimate`.
- `reflex_ivm_status()` estimates a partitioned IMV's `row_count` as the sum
  of its leaves' `reltuples`. A never-analyzed leaf counts as 0 when it has
  no storage (a mirrored empty partition); a never-analyzed leaf holding
  data forces an exact count.
- `reflex_scheduled_reconcile` returns one `HEALED` (or error) row per
  healed IMV before its reconcile rows.

### Tests

- New `pg_test_status_window_wipe.rs` (the field incident reproduced, plus
  the flush-observability and policy defects), `pg_test_ignore_soundness.rs`
  (create-time refusal, ack replay, nested-scope and `USING` attribution),
  `pg_test_capped_source_status.rs` (direct and transitive capped-root
  reporting, and convergence of the printed remedy) and
  `pg_test_ignored_source_heal.rs` (queue on status return, no queue for an
  unreferenced column, sweep, doctor and explicit heal each restore the
  slice exactly, no trigger on an unmappable source, drop clears the
  queue, no user-defined code in the trigger, no directly callable heal
  function, `NULL`/`''` watched changes, `TRUNCATE ONLY` of an inheritance
  parent, a writer without `USAGE` on `public`, a key retyped to a user
  type, key type mapping, dependents of edge-whitespace keys, cancels,
  truncate bookkeeping) and `pg_test_reconcile_log_noise.rs` (no
  re-issued child creates, marker created once, children dropped with an
  orphan recreated, no alter alarm on the relocation toggle under the
  `error` policy while a user `ALTER` still raises).
  `swi_rebuild_anomaly_clears_after_analyze` is replaced by
  `swi_rebuild_anomaly_retires_on_the_swap_itself`. Every must-refuse /
  must-report assertion was mutation-checked: reverting its guard turns it
  RED.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.11.4';` adds the `ignore_ack`
  column, the `__reflex_event_log` table and index,
  `reflex_prune_event_log`, the `__reflex_heal_pending` table, the
  `__reflex_heal_on_ignored_change` trigger function, recreates
  `reflex_ivm_status()` with the `is_estimate` column (return-shape change,
  `DROP` + `CREATE`), creates `reflex_ack_ignore_source` and
  `reflex_heal_ignored_sources`, replaces `__reflex_on_ddl_command_end`,
  and finally runs
  `reflex_rebuild_imv_metadata` on every enabled partitioned IMV with
  ignored sources to install its heal triggers. Installing a trigger takes
  a `SHARE ROW EXCLUSIVE` lock on each ignored source, which blocks writes
  to it and waits behind any open transaction on it: upgrade in a quiet
  window, with `lock_timeout` set. Run the `ignore-soundness` audit before
  any IMV recreate or `reflex_rebuild_chain`.
- Clients of `reflex_scheduled_reconcile` must treat a `HEALED` status as
  success, alongside `RECONCILED`.

---


## [1.11.3] - 2026-07-28

Four correctness fixes on the partitioned-IMV maintenance paths. Three of
them were silent — they produced wrong or absent data with no error, so an
affected cluster shows no symptom until someone reads the result. Two
destroyed data outright: a partition swap that flattened a sub-partitioned
mirror child so the next sync emptied the IMV (2.8 M rows → 0 in the
reproduction), and a `reflex_reconcile` that destroyed every dependent IMV
of a partitioned one. The remaining two are a failure-atomicity fix that
also closes a backend-abort class, and the removal of an `AccessExclusive`
lock that froze all readers of an IMV whenever a source partition was
added. This migration **must** be applied: the dependent-cascade fix is
split between the library and the `__reflex_on_ddl_command_end` trigger
body, and the new library against the old trigger is worse than either
half alone. `ALTER EXTENSION pg_reflex UPDATE TO '1.11.3';`

---

### Fixed

- **(DATA LOSS) The partition swap flattened a sub-partitioned mirror
  child, and the next partition sync then emptied the whole IMV.**
  `build_swap_partition_ddl` built the replacement with
  `CREATE TABLE ... (LIKE old INCLUDING ALL)`, and `LIKE` never copies
  partitioning — PostgreSQL has no `INCLUDING PARTITIONING` — so a
  depth-≥2 mirror child was replaced by a plain table. Data stayed correct
  at that instant, which is why it went unnoticed; the next sync's
  shape-drift heal then dropped those children and recreated them EMPTY,
  with NOTICE-level output only and `known_stale` never set. `reflex_reconcile`
  now resolves mirror *leaves* rather than immediate source children, and
  `execute_partition_swap_for_child` refuses outright on a partitioned child.
  An IMV already flattened in the field still holds correct data but is
  armed — repair it with `reflex_reconcile`, and do **not** run
  `reflex_sync_partitions` on it first.
- **(DATA LOSS) `reflex_reconcile` on a partitioned IMV destroyed its
  dependent IMVs.** Each swap's `ALTER TABLE` fired the `ddl_command_end`
  trigger, whose dependent auto-sync re-mirrored the parent's *transient*
  mid-swap child set, adopted a `..._swap_tgt_*` child and dropped the real
  one as a bound-collision orphan; the closing RENAME never revisited the
  dependent, leaving it empty with `known_stale` false. The swap now
  publishes `pg_reflex.internal_swap_root` for the duration of its
  DETACH/ATTACH and the trigger returns immediately while it is set, with
  dependents refreshed explicitly once the swap completes. Removing the
  unguarded mid-swap re-sync also deleted an O(N²·D) term: reconcile is
  ~3.9× faster at 50 partitions.
- **(SILENT PARTIAL COMMIT / BACKEND ABORT) `reflex_reconcile_partition`
  committed the destructive DDL of its pre-sync even when it reported
  `ERROR`.** The pre-sync ran outside the reconcile's scope and failures
  were returned as a string rather than raised, so `DROP TABLE ... CASCADE`
  survived a reported failure while the operator read "ERROR" as "nothing
  happened". The pre-sync and the reconcile body now share one
  subtransaction. A multi-child reconcile that failed on child N also
  committed children 1..N-1's swaps; that is closed too, including on the
  batch flush path.
- **(AVAILABILITY) Adding a source partition froze every reader of the
  IMV.** `reflex_sync_partitions` created the mirror child in place with
  `CREATE TABLE ... PARTITION OF <live IMV root>`, taking `AccessExclusive`
  on the root and holding it — through the COMMIT-time reconcile — for the
  whole transaction. Readers pruning to entirely unrelated partitions
  blocked for the full window. New nodes are now built detached, attached
  empty and filled in place, so the root stays at `ShareUpdateExclusive` at
  any mirror depth. Measured at depth 1: root `AccessExclusive` window
  0.48 s → none, concurrent reader latency 184 ms → 13 ms.

### Tests

- New suites `pg_test_subpartition_dataloss.rs` and
  `pg_test_reconcile_dependent_dataloss.rs`, plus
  `pg_test_partition_attach_locks.rs` asserting lock shape from a second
  session. Every assertion is mutation-checked: reverting the guard it
  pins turns it RED.
- `benchmarks/bench_partition_scaling.sh` — a reusable partition-count
  scaling harness (N = 10…200, log-log slope and successive-N ratios) that
  reproduces the 1.11.1 flush regression it was built to guard against
  (53.2 ms vs 4.9 ms median at N=200).

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.11.3';` replays the
  `__reflex_on_ddl_command_end` function body, which gained an early return
  while `pg_reflex.internal_swap_root` is set. **This is not optional** —
  the new library paired with the old trigger body still corrupts dependent
  IMVs mid-swap. No registry columns were added or changed.

---

## [1.11.2] - 2026-07-27

Two module-only fixes: a performance cliff that made every flush of a
partitioned passthrough IMV scan the whole IMV instead of the changed
partition (a 96-minute `COMMIT` in the field; 461.5 ms → 0.207 ms on a
one-day affected set), and a catastrophic data-loss guard — a partition
sync that read an unreadable anchor as "every child is an orphan" and
dropped every partition of the IMV. No DDL, no new function, no registry
column. Replace the `.so`, then
`ALTER EXTENSION pg_reflex UPDATE TO '1.11.2';`.

---

### Fixed

- **(PERFORMANCE CLIFF) The cold `DELETE` of a partitioned passthrough IMV
  scanned every leaf of the IMV on every flush.** The partition-dispatch
  branch spliced an ungated `IS NOT DISTINCT FROM` membership match against
  the `__reflex_pt_old_<view>_<source>` scratch table. No operator family
  covers that operator, so PostgreSQL could neither prune partitions nor use
  the IMV's unique index — the cost was a function of the IMV's total size,
  not of the change. Measured on a 49-leaf / 930k-row / 132 MB field IMV with
  a **one-day** affected set: **461.5 ms** executing all 49 leaves, against
  **0.207 ms** executing one and pruning 48 with the sargable form. With a
  one-month affected set the ungated form exceeded a 90 s `statement_timeout`
  while the sargable form ran in ~106 ms. Because DEFERRED maintenance runs
  inside the caller's `COMMIT`, this surfaced as a 96-minute `COMMIT` on a
  bulk push and blocked every writer queued behind it.

  This was the one branch the 1.11.1 PS-5 gate deliberately skipped, on the
  grounds that emitting the dispatch `DO` block per variant would run the
  hot-leaf swap twice. That does not follow: the aggregate sibling
  (`build_partition_aware_dispatch_sql_strategy`) already splits a gated pair
  into separate `EXECUTE`s *inside one* block, and the cold `DELETE` is its
  own `EXECUTE`. The variants now expand there while the swap stays single.
  The cold `INSERT` needs no gate — it is scoped by `pt_new`, not by a
  membership predicate. The 1.11.1 nullable-key correctness fix is fully
  preserved: the gate picks the NULL-safe form from the data whenever the
  scratch table holds a NULL key.

  Affects every partitioned passthrough IMV whose key columns are not all
  catalog `NOT NULL`. Because passthrough IMVs never populate
  `not_null_columns` at all — `initial_aggregate_materialization` and
  `reflex_probe_not_null_columns_impl` both return early on `is_passthrough`
  — that is *every* partitioned passthrough IMV, and no operator-side
  workaround existed. That metadata half is filed but **not** fixed here, in
  `untreated_bugs/2026-07-27_partitioned_passthrough_membership_ungated.md`.

- **(CATASTROPHIC, DATA LOSS) `reflex_sync_partitions` could drop every
  partition of an IMV, emptying it.** `list_partition_tree` returns an empty
  `Vec` both when the anchor genuinely has no children and when it could not
  be read at all — an unqualified anchor name `to_regclass` cannot resolve
  under the caller's `search_path`, a non-partitioned anchor, or a failed
  catalog query. An empty enumeration therefore carries no information about
  orphanhood, but the sync read it as "every child is an orphan". Reachable
  from the SQL default (`drop_orphans` defaults to `TRUE`) and from
  `reflex_reconcile_partition`, which runs the sync with `drop_orphans=true`
  before its own work. It now refuses loudly — a `WARNING` naming the anchor
  plus a marker in the returned message — mirroring the F3 fail-safe already
  present in `execute_partition_swap_for_child`.

### Tests

- `partitioned_passthrough_cold_delete_is_gated_and_swaps_once` — asserts both
  gated variants reach the cold `DELETE` at each of its two emission sites
  (2 variants × 2 sites) **and** that `reflex_reconcile_partition` is emitted
  exactly once, pinning the swap-single invariant the old carve-out existed to
  protect. Counting bare occurrences without the site factor passes on the
  ungated form, so the factor is load-bearing.
- `pg_part_sync_refuses_mass_drop_on_empty_source_tree` — covers the
  empty-enumeration refusal, asserting the IMV's partitions survive.
- `passthrough_keyed_delete_predicate` (the ungated builder) had no callers
  left and is removed, so the pre-fix form cannot be reintroduced by accident.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.11.2';` — no-op marker only
  (`SELECT 1 WHERE FALSE;`). Both fixes are module-only. The maintenance SQL is
  regenerated from the registry on every flush (`reflex_flush_deferred` calls
  `reflex_build_delta_sql` at flush time rather than executing a stored trigger
  body), so **no trigger rebuild and no reconcile are required** — the next
  flush of an affected IMV already uses the fixed SQL.

## [1.11.1] - 2026-07-24

Audit truthfulness, eleven silent-wrong-result fixes (nullable-key MIN/MAX
maintenance; a materialised UNION-ALL wrapper's mirror trigger functions
colliding under long wrapper names; a nullable explicit unique key on a
passthrough IMV; a LEFT JOIN aggregate grouped by the secondary's join
column; the DEFERRED cross-source guard double-reconciling a UNION-ALL
operand; a FULL OUTER JOIN aggregate's scoped recompute dropping a
newly-surfaced group; a synchronous partition-sync orphan collision going
permanently `known_stale`; an outer-join-blind catalog NOT-NULL set
silently staling a LEFT/RIGHT JOIN aggregate's NULL group; a qualified
GROUP BY column misclassified as stable by its first `.`-segment; and
directly-named UNION-ALL operand reconciles — including every unattended
scheduled-sweep pass — silently doubling their wrapper), an
unclearable-finding fix (a group-capture table dropped independently of its
intermediate), two performance-cliff fixes (MIN/MAX affected-group scoping,
and the keyed outer-join-secondary passthrough's membership predicate —
579x on the latter), wrapper-reconcile safety, targeted-recovery
observability, a recovery path for a dropped internal table, plus a repair
primitive and real audit coverage for materialised UNION-ALL wrapper mirror
triggers. Replace the `.so`, then
`ALTER EXTENSION pg_reflex UPDATE TO '1.11.1';`. The update adds two registry
columns (fast-default `ADD COLUMN`, no table rewrite) and one corrective metadata
backfill; no reconcile is needed.

**Added**

- `reflex_rebuild_imv` / directly-targeted `reflex_reconcile` retries are now
  observable: `__reflex_ivm_reference` gains `rebuild_count` and `last_rebuild_at`,
  surfaced by `reflex_ivm_status` (an **additive** change — two columns appended to
  its result; `SELECT *`-into-fixed-rowtype callers should name columns). The counter
  increments only on direct operator recovery — not trigger-fired maintenance, the
  scheduled sweep, or the recursive cascade — and also on `reflex_doctor(fix => true)`
  recoveries. When a rebuild targets an IMV it cannot converge (a matview source, or
  an `ignore_sources`-fed / anchor-empty partition), it now WARNs and names the
  primitive that can (`refresh_imv_depending_on` / `reflex_reconcile_partition`)
  instead of returning a bare success — so a non-converging retry loop is visible
  in-database rather than only in `pg_stat_statements`.

- `reflex_rebuild_union_mirror(wrapper TEXT)`: reinstalls a materialised
  UNION-ALL wrapper's `__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>`
  triggers on every operand recorded in its `depends_on`. Previously nothing
  could repair a dropped mirror trigger — `reflex_rebuild_triggers` installs
  the wrong (consolidated) trigger set on a sub-IMV target and never clears
  the finding. Refuses cleanly on a VIEW wrapper (no operand triggers by
  design) or a non-wrapper IMV. Restores future maintenance only; it does not
  backfill deltas missed while the trigger was absent.

**Fixed**

- **(silent wrong result)** A MIN/MAX aggregate IMV over a **nullable group key**
  returned a stale extremum forever after a retraction. The recompute scoped its
  source re-aggregation with `(group cols) IN (SELECT … FROM affected)`, and
  `(NULL) IN (…)` is NULL — never true — so a NULL group whose current MIN/MAX was
  retracted was excluded from the rescan and never re-derived. Fixed with a NULL-safe
  correlated `EXISTS (… IS NOT DISTINCT FROM …)` scope.

- **(silent wrong result)** The same NULL-unsafe `IN` in the **top-K** scalar-refresh
  path (`build_topk_scalar_refresh_sql`) left a NULL group's MIN/MAX wrong after an
  UPDATE that touched a middle-ranked row (one that shrank neither the min- nor the
  max-heap), because the forced recompute is scoped to shrunk heaps only and the
  scalar refresh skipped the NULL group. Fixed NULL-safe, gated on whether the
  affected set actually holds a NULL key: the common path keeps the sargable `=`
  (index scan) and only a batch touching a NULL group pays the NULL-safe scan.

- **(transaction abort / silent column-shift)** `reflex_reconcile` on a decomposed
  wrapper IMV (top-level `UNION`/`UNION ALL`/`INTERSECT`/`EXCEPT`, `DISTINCT ON`,
  window) either raised `"<view>" is not a table` — aborting the caller's whole
  transaction, since the wrapper is a VIEW and `reconcile_one` tried to `TRUNCATE` it
  — or, for a materialised UNION-ALL wrapper (extra leading `__reflex_src_idx`
  column), column-shifted its payload on the `INSERT`. `reconcile_one` now refuses a
  wrapper node with a clean error string before any DDL, so direct calls,
  `refresh_imv_depending_on`, and `reflex_doctor(fix => true)` all fail safe and
  transaction-preserving. Wrappers are maintained through their operands; reconcile
  the parent or the operands, not the wrapper.

- `reflex_scheduled_reconcile` raised `"<view>" is not a table` and returned nothing
  — the entire scheduled sweep died — in any database containing a decomposed
  set-op / `DISTINCT ON` / window IMV. Such a wrapper is a view maintained through
  its generated sub-IMVs, so `reflex_reconcile` cannot operate on it; nothing ever
  advances the wrapper's `last_update_date` either (only the tail of `reconcile_one`
  writes it, which a wrapper never reaches), so it became a sweep candidate
  `max_age_minutes` after creation and stayed one forever. The candidate query now
  skips planless rows, using the predicate already applied two CTEs down.

- `reflex_reconcile` (and therefore `reflex_rebuild_imv`) now **recreates a dropped
  `__reflex_intermediate_<view>`** instead of failing on it. Previously the
  intermediate's DDL was emitted only at create time, so an intermediate lost to a
  `DROP … CASCADE` or a dropped schema left the IMV unrepairable by any exposed
  primitive: the partitioned path returned `ERROR: partition reconcile failed` from
  the swap's ATTACH, the unpartitioned path raised 42P01 on `TRUNCATE`, and
  `internal-tables-exist` reported the condition at Error severity while prescribing
  the very call that could not fix it. The heal rebuilds the table, its indexes and
  the group-capture tables from the registry row, then lets the existing partition
  sync mirror the children. It runs under the IMV's advisory lock and re-probes
  after acquiring it, so concurrent callers cannot race the DDL, and it refuses to
  build a shape whose group-key types it cannot resolve rather than committing a
  wrongly-typed table.

- `internal-tables-exist` and `trigger-attached` false-positived at Error severity on
  every decomposed set-op / `DISTINCT ON` / window **wrapper** IMV, demanding
  internal tables and consolidated triggers the wrapper correctly does not own (it is
  a view; its maintenance runs through the generated sub-IMVs). `trigger-attached`'s
  remedy was actively harmful: `reflex_rebuild_triggers` on a sub-IMV installed four
  junk consolidated triggers on its target and left the finding standing, so every
  retry added four more. Wrappers are now classified by their `aggregations` key
  count and excluded from both checks.

- `trigger-attached`'s wrapper exclusion above was unconditional, which also
  silenced the real check for a **materialised** UNION-ALL wrapper's mirror
  triggers (a VIEW wrapper genuinely has none; a materialised one is
  maintained by `__reflex_union_mirror_*` triggers this check didn't know
  about). It now checks a materialised (TABLE) wrapper's operands and reports
  any missing mirror trigger, naming `reflex_rebuild_union_mirror`; a VIEW
  wrapper stays silent, unchanged. The expected trigger name is compared
  against PostgreSQL's own `NAMEDATALEN`-truncated form (not the untruncated
  one), so a wrapper whose generated trigger name exceeds 63 bytes is not
  permanently misreported as broken.

- **(silent wrong result)** A materialised UNION-ALL wrapper's per-operand mirror
  trigger *functions* (`__reflex_union_mirror_{ins,del,upd}_<wrapper>_<i>`, installed
  by `install_union_mirror_triggers`) shared one unbounded, non-length-capped name.
  PostgreSQL silently truncates any identifier over `NAMEDATALEN-1` (63 bytes) at a
  char boundary, so a wrapper name ≥ 38 bytes truncated away the one character
  distinguishing `ins`/`del`/`upd`, collapsing two or three of the `CREATE OR REPLACE
  FUNCTION`s onto the same `proname` — the last one issued silently overwrote the
  others' bodies while the already-bound triggers kept their captured `tgfoid`, so an
  `INSERT` on one operand could execute another DML kind's body outright (erroring
  immediately) or, at a second threshold, mis-tag the row with the wrong
  `__reflex_src_idx` instead of erroring at all. Fixed by hashing the full raw name
  (DML tag, wrapper, and operand index together) through the same `safe_identifier`
  truncation scheme used elsewhere, so the operand index and DML-kind tag both
  survive regardless of wrapper length. `drop_reflex_ivm` now also probes the legacy
  (pre-1.11.1) unhashed name when cleaning up a wrapper's functions, so dropping a
  wrapper created before this fix does not leak them. This fix only changes what a
  *new* `create_reflex_ivm` call produces — a wrapper already collided under the old
  scheme is not automatically repaired or flagged; run `reflex_rebuild_union_mirror`
  on it by hand (see `untreated_bugs/2026-07-25_union_mirror_function_name_collision.md`
  for the residual detection gap).

- The `partition-mirror` audit check (surfaced by `reflex_doctor` as F3) reported
  phantom intermediate-partition drift on partitioned **passthrough** IMVs. A
  passthrough IMV owns no `__reflex_intermediate_<view>` table — the runtime never
  creates one — so the check diffed the anchor source's child set against an absent
  parent's necessarily-empty child list and reported every child as missing. The
  remedy it printed, `reflex_sync_partitions`, gates its intermediate-child DDL on
  that same relation's existence, so it could never create any of them: the finding
  survived its own remedy indefinitely. Field impact: 42 IMVs across 5 tenants,
  including one reporting 17 phantom missing children while sync returned
  `sync: +0 intermediate, +675 target`.

  The intermediate half of the comparison now runs only when an intermediate is
  both expected (non-empty `end_query`, the same signal the maintenance runtime
  uses) and present. The target half always runs, so target-tree drift is still
  reported when the intermediate parent is absent — previously an early return
  suppressed it. A genuinely missing intermediate parent stays reported by
  `internal-tables-exist`, which already covers relation absence at Error severity.

- (metadata backfill) The 1.11.0 `requires_explicit_refresh` backfill matched
  `ignore_sources` by exact string only, so an IMV that ignored a real table by *bare*
  name while `depends_on` stored it *qualified* was under-flagged and stayed invisible
  to the matview-source checks. The 1.11.1 migration re-backfills with a bare-or-
  qualified match mirroring create-time; it only ever flips the flag on, never off, so
  it cannot suppress a maintainable IMV.

- **(silent wrong result)** A passthrough IMV built with an *explicit* unique key over
  a column that can be NULL in the target silently diverged from its source: the keyed
  `DELETE`/`UPDATE` maintenance scoped row removal with a plain
  `(key) IN (SELECT key FROM …)`, and `(NULL) IN (…)` is never TRUE. A source `DELETE`
  of a NULL-key row left a permanent phantom target row; a source `UPDATE` of one left
  the stale row in place *and* inserted the new one, colliding on the target's unique
  index and aborting the caller's own transaction. Fixed with a NULL-safe gated
  predicate mirroring the 1.11.0 nullable-key aggregate pattern: a sargable `IN`
  (byte-identical SQL) when every key column is provably `NOT NULL`, a correlated
  `EXISTS … IS NOT DISTINCT FROM` otherwise. Covers both the primary keyed passthrough
  path and the LEFT/RIGHT-JOIN secondary keyed path.

- **(silent wrong result)** An aggregate IMV over a LEFT JOIN, grouped by the
  *secondary* (nullable) side's join column, silently diverged whenever the join's
  `ON` clause equated two columns sharing the same bare name (e.g.
  `fa LEFT JOIN fb ON fa.k = fb.k GROUP BY fb.k`): the join-condition mapping matched
  by bare column name only, so the 1.4.6 incremental fast path treated the group
  column as stable when it actually migrates NULL↔value as the secondary changes.
  `INSERT` into the secondary silently double-counted the migrated rows; `DELETE`
  silently lost them. Fixed by verifying no mapped `GROUP BY` column is
  secondary-derived before taking the fast path; falls through to the existing
  (already correct) stable-column recompute otherwise.

- **(silent wrong result)** The DEFERRED cross-source consistency guard (fires when
  one transaction mutates ≥2 sources of an IMV) reconciled a machine-generated
  UNION-ALL operand sub-IMV by calling the public `reflex_reconcile` on it directly.
  Running inside the deferred-flush trigger, that call skipped the
  trigger-suppressed rebuild path and ran with the operand's own mirror trigger
  live, silently doubling its materialised wrapper — and anything reading it — on an
  ordinary transaction touching both of the operand's sources together. Fixed: the
  guard now rebuilds a generated operand with propagation suppressed and explicitly
  resyncs the wrapper's operand slice, instead of calling `reflex_reconcile` on it
  directly. Also now flags `known_stale` (with a reason) instead of silently
  discarding a staged delta if that repair itself fails.

- **(unclearable finding)** `reflex_reconcile`'s heal for a dropped
  `__reflex_intermediate_<view>` (added earlier in this release) gated on the
  intermediate's own presence, so a `__reflex_affected_<view>` /
  `__reflex_shrunk_<view>` group-capture table dropped **without** its
  intermediate — physically independent, since it's `CREATE TABLE ... AS SELECT
  ... FROM <intermediate> WHERE FALSE`, a CTAS that records no catalog
  dependency — was never recreated. `internal-tables-exist` reports this shape
  at Error and prints `reflex_rebuild_imv` (an alias for the same reconcile) as
  the remedy, so the finding could not be cleared by the fix it names. The heal
  now probes the intermediate and its group-capture companions independently,
  in build order, recreating only what's missing under the same advisory lock.

- **(hardening, currently dead code)** The in-place partitioned passthrough UPDATE
  body's `pt_old` membership predicate (its delete-gone and its
  `assert_inplace_update` guard) was a plain `(key) IN (SELECT ... FROM pt_old)`
  with no NOT-NULL gate — the third instance of this release's nullable-explicit-
  key defect, after the primary keyed passthrough path and the LEFT/RIGHT-JOIN
  secondary path. Gated on `plan.not_null_columns`, byte-identical when every key
  column is proven NOT NULL. Investigation found this body is not currently
  reachable (a `pg_attribute.attname`/`name`-type read silently returns no
  columns, so every partitioned keyed UPDATE falls back to the standard,
  already-safe cold body) and that naively enabling it raises on an ordinary
  partition-key change, unrelated to NULLs — recorded in
  `untreated_bugs/2026-07-25_inplace_partitioned_upsert_path_dead_and_broken.md`
  as separate, still-open follow-up work; this fix only ensures a future revival
  does not also inherit the NULL-unsafe predicate.

- **(performance)** A MIN/MAX aggregate IMV's affected-group recompute scoped its
  membership `EXISTS` filter with the non-sargable `IS NOT DISTINCT FROM`
  unconditionally — even on the `eq = true` fast branch, where the sibling
  `update_join`/`exists_join` predicates in the same statement were already
  correctly gated to the sargable `=`. `IS NOT DISTINCT FROM` is neither hashable
  nor mergejoinable, degrading the semi-join to the affected table from a
  Hash/Merge Semi Join to a Nested Loop Semi Join for every MIN/MAX IMV — measured
  700-3700x slower at realistic source sizes, not just ones holding a NULL group
  key. Gated on `eq` like its siblings.

- **(silent wrong result)** An aggregate outer-join-secondary IMV with a `FULL
  JOIN`/`FULL OUTER JOIN` base query could take a scoped recompute path (either
  the join-key-scoped fast path or the STABLE-column fallback) that assumed a
  group-by column qualified by a table other than the one just mutated is stable.
  A FULL JOIN's unmatched-row semantics make that false for either side: a write
  to one side can surface or drop a brand-new group driven by the other side's
  join value, without that other table's own row changing. Both scoped strategies
  now bail out to the same unconditional full intermediate+target rebuild the
  passthrough branch already used for this join type.

- **(silent wrong result)** The synchronous DDL-event partition auto-sync
  (`__reflex_on_ddl_command_end`) left a source-side repartition's old, orphaned
  mirror child behind when a `DETACH`+`ATTACH` replaced a leaf at the same bound;
  the next `CREATE` for the replacement's mirror then collided on identical
  bounds (`would overlap partition`), and the IMV went `known_stale` on every
  subsequent swap regardless of manual reconcile. The sync now drops a *confirmed*
  orphan sibling (bounds byte-identical to the incoming child, mapped to no live
  source leaf) before each `CREATE`, scoped to the exact immediate parent
  receiving the incoming child — never the whole multi-level subtree, which an
  adversarial review pass found could otherwise CASCADE-drop a same-bound leaf
  under a completely unrelated, still-live branch.

- **(silent wrong result)** An aggregate IMV's catalog-derived NOT-NULL set
  (`plan.not_null_columns`) was a bare-column-name union across every source
  table, blind to both outer-join nullability and bare-name collisions across
  sources: a column on the nullable side of a LEFT/RIGHT JOIN is NULL for every
  unmatched row regardless of its own table's `NOT NULL` declaration, and one
  source's `NOT NULL` column silently promoted every other source's
  same-named nullable column even with no outer join involved at all. Once a
  name was in the set it was never withdrawn — `optimize_not_null_sums` stored
  it verbatim and `infer_not_null_columns` only ever adds. Downstream, this
  made group-key matching use a plain `=` (never TRUE for the genuinely NULL
  group) and suppressed the NULL-safe alternative entirely: the NULL group's
  target row was neither deleted nor re-inserted on a primary-side mutation
  (silently stale), and the intermediate's `MERGE ... ON` inserted a duplicate
  NULL-key row. Fixed by reducing the catalog union to the join's actual
  output nullability at `materialize_aggregate` time — the one place holding
  both the catalog facts and the join structure — before it reaches
  `plan.not_null_columns`; nothing survives a RIGHT/FULL join, matching the
  bail-out `infer_not_null_columns`/`provably_not_null_key_columns` already
  take elsewhere. A dropped name only ever costs the sargable `=` form, never
  correctness; a genuinely non-NULL GROUP BY key is added back afterward by
  the existing (correctly outer-join-aware) `infer_not_null_columns`.

- **(performance)** The keyed outer-join-secondary passthrough path matched
  the full base relation against its small `(OLD ∪ NEW)` transition delta with
  `IS NOT DISTINCT FROM` whenever a key column was not statically proven NOT
  NULL — and since those target columns come from the nullable side of the
  outer join, `provably_not_null_key_columns` can never prove them: this was
  the routine cost for every such IMV, not an edge case. `IS NOT DISTINCT
  FROM` belongs to no PostgreSQL operator family, so the semi-join degrades to
  a Nested Loop Semi Join over the entire base relation — measured 579x
  slower (23.7s vs 41ms at 500k base rows / 2k changed keys) than the sargable
  form. Now emits the same runtime-gated fast/safe pair this release's
  MIN/MAX fix established (an uncorrelated NULL-freeness probe decides at
  execution time, via a `One-Time Filter`-skipped `InitPlan`, whether the
  batch actually contains a NULL key), so the common case pays ~6ms instead.
  Applied to the outer-join-secondary DELETE/INSERT and the unpartitioned
  keyed passthrough DELETE; the partition-dispatch branch keeps its single
  always-safe form to avoid double-running the hot-leaf swap. Output stays
  byte-identical when every key column is proven NOT NULL.

- **(silent wrong result)** An outer-join-secondary aggregate's GROUP BY
  stability check (deciding whether a scoped/targeted recompute is safe, or
  whether a group might migrate on a secondary-side mutation) took only the
  FIRST `.`-delimited segment of a qualified column reference as its
  qualifier. A genuinely 3+-part reference (e.g. `public.fb.k`, or an
  expression like `DATE_TRUNC('month', fb.ts)` whose stringified qualifier
  segment doesn't match any real table) never matched the mutated secondary
  table's name, so it was always classified stable regardless of which table
  it actually belonged to — silently skipping the rebuild a migrating group
  needed on a secondary INSERT/UPDATE/DELETE. Fixed by checking every
  identifier chain's immediate qualifier segment against the secondary,
  applied consistently to both the join-key-scoped fast path and the
  STABLE-column fallback.

- **(silent wrong result)** Directly naming a machine-generated UNION-ALL
  operand sub-IMV in `reflex_reconcile`/`reflex_rebuild_imv` — or via
  `reflex_doctor`'s F4 repair, or (unattended) every `reflex_scheduled_reconcile`
  drift sweep, since an operand is `REBUILDABLE_NODE` and reconciled standalone
  by design — rebuilt it with `TRUNCATE` + `INSERT`. The operand's
  `__reflex_union_mirror_*` trigger mirrors its rows into its materialised
  wrapper's `__reflex_src_idx` slice but has no `TRUNCATE` handler, so the
  `TRUNCATE` removed nothing from the wrapper while the `INSERT` re-appended the
  operand's full row set: every such repair, including every unattended
  scheduled-sweep pass over a materialised UNION-ALL chain, silently DOUBLED
  the wrapper's slice and everything reading it. Now routes a directly-named
  operand through the same trigger-suppressed rebuild + explicit wrapper-slice
  resync this release's DEFERRED cross-source guard fix already established,
  bracketed with the wrapper as the internal reconcile root so
  `alter_source_policy='error'` cannot abort the caller. A failed rebuild
  leaves the wrapper's existing slice alone rather than publish undefined
  operand contents. Any other target is unaffected (one added registry probe,
  then the identical prior rebuild).

**Known limitations**

- Directly reconciling a machine-generated *operand* sub-IMV of a **materialised**
  UNION-ALL wrapper (`reflex_reconcile('<wrapper>__cte_u__union_0')`) still doubles the
  wrapper: the operand rebuild fires its live `__reflex_union_mirror_ins_*` trigger.
  Reachable only by naming an internal `…__union_N` node directly — reconciling the
  wrapper (now refused) or its parent (trigger-suppressed descent) is safe. Filed in
  `untreated_bugs/2026-07-24_reconcile_raises_on_decomposed_wrapper.md`.
- No sound package-level "skip-unchanged reconcile" exists: a blanket reconcile is a
  trustless output-correctness certificate, and no signal cheaper than recomputing the
  output can license skipping it without downgrading the guarantee. For the matview-
  driven cost case (`current_assortment_activity_view`, ~2.5 h), use push-based
  invalidation instead — call `refresh_imv_depending_on('<mv>')` after
  `REFRESH MATERIALIZED VIEW`. Investigated in
  `untreated_bugs/2026-07-24_current_assortment_reconcile_cost.md`.
- The heal repairs the aggregate branch only: a missing target table or a missing
  per-source passthrough scratch table is still `reflex_rebuild_triggers`' job, so
  `internal-tables-exist` converges for aggregate IMVs, not universally.
- `reflex_doctor` never mentions a missing intermediate at all —
  `internal-tables-exist` findings are `reflex_audit`-only and are not forwarded to
  the doctor summarizer.
- A stray childless `__reflex_intermediate_<view>` beside a passthrough IMV is
  reported by no check. The relation is inert; the alternative would arm a
  `DROP TABLE … CASCADE` remedy keyed on a passthrough classification.

## [1.11.0] - 2026-07-24

Correctness and truthfulness across decomposed IMV chains, the doctor, and
nullable-key maintenance cost. Replace the `.so`, then
`ALTER EXTENSION pg_reflex UPDATE TO '1.11.0';`, then run
`SELECT reflex_repair_dependency_graph();` in a fresh session to backfill the
dependency graph on existing rows.

**Fixed**

- A CTE-decomposed IMV never recorded the edge to the sub-IMV pg_reflex generated
  for it: `resolve_existing_imv_deps` matched `depends_on` against the registry
  `name` by exact string equality, but the decomposer persists the sub-IMV source
  double-quoted to preserve identifier case, so the match never fired. Every
  decomposed IMV had an empty `depends_on_imv`, an empty `graph_child` on its
  generated child, and a collapsed `graph_depth`. Consequences, all fixed:
  `reflex_reconcile` / `reflex_rebuild_imv` on the parent re-aggregated a stale
  child and returned `RECONCILED` (silent stale data when the child reads a
  materialized view); and `reflex_rebuild_chain` / non-cascade `drop_reflex_ivm`
  on a generated child saw no dependents and silently ended the parent's
  maintenance. `reflex_reconcile` now rebuilds a decomposed IMV's generated
  sub-IMVs bottom-up (triggers suppressed, so nothing double-counts) before the
  IMV itself, and refuses the two destructive operations on a generated child.
- `reflex_reconcile` on a decomposed chain no longer skips a generated child
  under a mixed-case name, and no longer descends into set-op / CTE wrapper nodes
  it cannot rebuild (which silently column-shifted or aborted).
- `reflex_doctor` classified the pending queue on `attempts` (an enqueue counter),
  reported an arbitrarily old `last_error` next to a freshly reset age, and left a
  capped root's prescribed flush a guaranteed no-op. It now classifies on
  `failures`, dates findings from a new `last_attempt_at`, distinguishes capped
  roots as `F2b`, and re-arms them (one attempt per `fix` invocation, via the new
  `reflex_reset_partition_failures`) before flushing. A repair is reported `fixed`
  only when the pending row actually drained.
- `reflex_doctor`'s orphan-object check reported every intermediate partition of
  every partitioned IMV, and every aux table of a bare-name IMV, as an orphan with
  a `DROP … CASCADE` fix; ownership is now decided on catalog OIDs
  (`pg_partition_root`) rather than name strings, with a `COALESCE(target_schema,
  'public')` fallback for pre-1.7.2 rows.
- The aggregate target-sync and intermediate MERGE emitted a non-sargable
  `IS NOT DISTINCT FROM` join on nullable group keys, forcing a full scan of the
  target/intermediate on every delta. Both now emit a runtime-gated pair — a
  sargable `=` form when the affected set has no NULL key, the null-safe form only
  when it does — restoring index use and partition pruning (200k-group,
  1-row-delta target sync 52 ms → 0.09 ms; intermediate MERGE 14 ms → 0.04 ms).
  NOT-NULL-keyed IMVs are byte-for-byte unchanged.
- `reflex_doctor(fix => TRUE, drop_orphans => FALSE)` no longer drops orphan
  partitions through its F4 / F4b reconcile after refusing to at the F3 step: the
  reconcile repairs now call the new two-argument `reflex_reconcile(view,
  drop_orphans)` overload and honour the caller's choice.
- `reflex_rebuild_chain` silently downgraded an IMV rebuilt from an absent
  `create_args` (pre-1.10.8 rows) to `IMMEDIATE` / unpartitioned / no-unique-key,
  and hard-aborted on a CTE/set-op-decomposed parent (its stored query names a
  generated sibling the CASCADE drop removes first, so the recreate referenced a
  vanished relation — D22). It now refuses up front, before any drop, on a
  decomposed parent (recover via `reflex_reconcile`) and on any IMV lacking a
  faithful `create_args`; the SQL migration backfills reconstructible `create_args`
  for legacy rows (marked `"backfilled": true`; a rebuild from such a row warns
  that `topk_k` / `explicit_unpartitioned` took create-time defaults).
- An IMV whose only real sources are materialized views is a snapshot frozen at
  create time (PG fires no trigger on a matview) but was recorded
  indistinguishably from a maintainable IMV, so every health surface read clean.
  It is now flagged `requires_explicit_refresh` and surfaced by `reflex_ivm_status`
  and as `reflex_doctor` finding `F12`, whose remedy is
  `refresh_imv_depending_on('<matview>')`. The flag is structural and permanent —
  kept strictly distinct from `known_stale` so it never becomes a false repair
  failure.
- A passthrough IMV missing its per-source scratch pair
  (`__reflex_pt_new/old_<imv>_<source>`) failed every flush with 42P01, was caught
  as a WARNING, and silently lost the staged deltas on each commit. `reflex_audit`
  now prescribes `reflex_rebuild_triggers` + `reflex_reconcile` (not the no-op
  `reflex_rebuild_imv`) for that condition, and the upgrade recreates missing
  scratch pairs idempotently and marks any still-wedged IMV `known_stale` so the
  doctor prescribes a reconcile rather than the audit reporting a false green.
- `reflex_scheduled_reconcile` ran the whole registry in one transaction, so a
  `statement_timeout` discarded every rebuild already done and it could not finish
  on a large multi-tenant registry. It is now a resumable batched driver (see
  Changed), and per-IMV `last_update_date` is stamped with `clock_timestamp()`
  rather than one shared transaction-start `NOW()`, so a batch's timestamps are
  distinct and can date an individual rebuild.
- `reflex_sync_partitions` took a one-key advisory lock on the IMV name while all
  maintenance paths use the two-key form — a different advisory-lock space, so a
  sync and a concurrent maintenance of the same IMV did not mutually exclude at the
  advisory level. Aligned to the two-key form (removes a lock-ordering surface).

**Changed**

- `reflex_doctor` classifies a decomposed parent as `F4b` from the registry graph
  (`is_generated_sub_imv` + `depends_on_imv`) rather than a `name LIKE` heuristic
  that missed every schema-qualified chain, and prescribes `reflex_reconcile`
  (which now handles the chain) rather than `reflex_rebuild_chain`.
- `graph_depth` changes value for every decomposed IMV (up for CTE parents, down
  for set-op wrappers). Every consumer orders by it ascending and wants the
  corrected order.
- **`reflex_scheduled_reconcile` signature (public API change).** New optional
  arguments `batch_size` (default `0` = unlimited, the previous one-call-sweeps-all
  behaviour) and `target_schema` (default `''` = all schemas), and a new output
  column `remaining`. For a large registry, bound each pg_cron tick with a
  `batch_size` and call until `remaining = 0`; candidates rotate oldest-first
  within a `graph_depth` level so a bounded sweep advances through the whole
  registry. `remaining = 0` means the batch deferred nothing this call, not that
  the registry is healthy — monitoring must still check per-row `status`. The
  upgrade drops the old single-argument function and recreates the new form
  (the return type changed), so default-argument pg_cron jobs keep working.

**Added**

- Columns `is_generated_sub_imv`, `requires_explicit_refresh`, and
  `__reflex_partition_pending.last_attempt_at`.
- Functions `reflex_repair_dependency_graph()`, `reflex_reset_partition_failures()`,
  and the two-argument `reflex_reconcile(view, drop_orphans)` overload.
- `reflex_doctor` finding `F12` (matview-only / `requires_explicit_refresh`) and
  `F2b` (a partition-flush root at the failure cap, distinct from a still-retrying
  `F2`).

**Known limitations**

- MIN / MAX over a **nullable** group key can stay `NULL` after the NULL group's
  extremum is retracted: the recompute scopes its re-aggregation with a
  null-unsafe `IN (...)`, which excludes the NULL group. Pre-existing (not changed
  in 1.11.0), narrow, and tracked in
  `untreated_bugs/2026-07-23_min_max_recompute_scope_null_unsafe_in.md`. The
  differential fuzz harness does not yet emit MIN/MAX, so it does not cover this.
- The `requires_explicit_refresh` upgrade backfill matches `ignore_sources` by
  exact stored spelling, so an IMV that ignores a real source by *bare* name while
  `depends_on` stores it qualified can be under-flagged on upgrade (new IMVs are
  unaffected — create-time matches both forms). Tracked in
  `untreated_bugs/2026-07-24_gap2_analyze_intermediate_residual.md`.

**Migration**

- `ALTER EXTENSION pg_reflex UPDATE TO '1.11.0';` —
  [`sql/pg_reflex--1.10.11--1.11.0.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.11--1.11.0.sql).
  After the upgrade, in a fresh session, run `SELECT reflex_repair_dependency_graph();`
  to backfill `depends_on_imv` / `graph_child` / `graph_depth` /
  `is_generated_sub_imv` on rows created before 1.11.0, then re-run `reflex_doctor()`.

## [1.10.11] - 2026-07-22

Non-superuser reconcile, and real archive-residue verification for joins.
`reflex_reconcile` on a partitioned IMV no longer requires superuser, and
the `archive_residue` check now confirms or clears residue on multi-source
and aggregate IMVs instead of reporting "cannot confirm" (the placeholder
1.10.10 left in place). Replace the `.so`, then
`ALTER EXTENSION pg_reflex UPDATE TO '1.10.11';`.

### Fixed

- **`reflex_reconcile` failed for non-superuser roles with "permission
  denied to set parameter session_replication_role".** The partition-sync
  relocation path (new in 1.10.10) suppressed downstream chained-IMV
  triggers with the session-wide, superuser-only `session_replication_role`
  GUC. It now uses ownership-scoped `ALTER TABLE ... DISABLE/ENABLE TRIGGER
  USER` on the relocation roots, which a non-superuser table owner may run.
  The suppression is strictly narrower than the old replica role, and every
  root that was disabled is re-enabled on all exit paths — including a
  partial-failure bailout, so the target table's downstream-feeding trigger
  is never left disabled in the committing transaction.
- **`archive_residue` could not verify residue on multi-source or aggregate
  IMVs.** 1.10.10 replaced a false-positive with a "cannot confirm" advisory
  for any join. The check now verifies directly: for each empty IMV
  partition it probes the IMV's own definition (`base_query`) scoped to that
  partition's constraint — the intermediate constraint for aggregates, the
  target constraint for passthrough, mirroring how reconcile fills a
  partition. It reports confirmed residue (with a runnable
  `reflex_reconcile_partition` fix) only when the definition would have
  produced rows, clears correctly-empty partitions (filtered out, or no join
  match), and degrades a failed or timed-out probe to a prose advisory. Each
  probe is bounded by a best-effort, non-superuser `statement_timeout`.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.10.11';` after swapping the module:
  [`sql/pg_reflex--1.10.10--1.10.11.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.10--1.10.11.sql)
  is a no-op delta — both fixes ship entirely in the module, with no SQL
  signature, table, or trigger changes. Re-run `reflex_doctor()` to re-check
  any partitions previously reported as "cannot confirm".

## [1.10.10] - 2026-07-21

The partition-flush deadlock release. It fixes an unbounded retry loop that
could hang a committing backend indefinitely, closes the circular
default-partition state that made an IMV partition leaf unrecoverable, adds a
safety refusal to `reflex_rebuild_chain` before it CASCADE-drops dependents,
and removes a false-positive `archive_residue` warning. Replace the `.so`,
then `ALTER EXTENSION pg_reflex UPDATE TO '1.10.10';`.

### Fixed

- **Unbounded partition-flush retry loop hung committing backends.** The
  `__reflex_partition_flush_trigger` constraint trigger fired on any `INSERT
  OR UPDATE` of `__reflex_partition_pending`; the flush's own `EXCEPTION`
  handler wrote `last_error` back onto the same row, which re-armed the
  trigger at COMMIT and retried forever. The trigger is now scoped to
  `UPDATE OF enqueued_at` — only the DDL event trigger's re-enqueue touches
  that column — and a per-root `failures` counter caps retries at 5, honored on
  both the queue-drain and the commit-time single-root flush paths (the latter
  canonicalizing the root key so a bare-name call matches the stored pending
  row). A capped root keeps its pending row and `last_error` for `reflex_doctor`
  to surface, and its dependent IMVs stay `known_stale` instead of silently
  succeeding.
- **A new IMV partition leaf could be unrecoverable (SQLSTATE 23514).**
  Rows sat in the parent's DEFAULT partition because the leaf did not exist
  yet, and the leaf could not be created because those same rows violated its
  bounds while still attached to DEFAULT. Syncing an IMV now drains every
  DEFAULT partition in the tree into a holding table, builds the missing
  leaves, then re-inserts the rows so PostgreSQL routing places each in its
  correct leaf (moving them, never deleting) — recovering stranded rows at any
  partition depth, including multi-level (sub-partitioned) trees. The
  relocation preserves IMV content, so downstream chained-IMV maintenance is
  suppressed during it to avoid re-counting merely-relocated rows.
- **`archive_residue` false-positived on multi-source IMVs.** The check
  compares a source partition's row count against the IMV partition's — sound
  only for a roughly 1:1 single-source IMV. For any multi-source IMV (a join)
  those counts are unrelated, filtered or not, so it now reports "cannot
  verify" instead of a false warning.

### Changed

- **`reflex_rebuild_chain(view_name, cascade => FALSE)` gained a `cascade`
  argument.** Without it, the function now refuses when dependent IMVs would
  be CASCADE-dropped by the rebuild. Pass `cascade => TRUE` to drop and
  recreate every transitive dependent in dependency order (shallowest first);
  this still refuses if any dependent predates the `create_args` column
  (1.10.8) with no stored creation arguments to replay.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.10.10';` after swapping the module:
  [`sql/pg_reflex--1.10.9--1.10.10.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.9--1.10.10.sql)
  adds the `failures` column, redefines the flush trigger, and drops the old
  one-argument `reflex_rebuild_chain(TEXT)` so the new two-argument version
  installs without an ambiguous overload. Roots stuck in
  `__reflex_partition_pending` should be re-checked with `reflex_doctor()`
  after upgrading.

## [1.10.9] - 2026-07-05

`reflex_doctor` / `reflex_audit` hardening. The audit engine can no longer be
aborted by a single failing check, `reflex_doctor` now repairs archive residue
correctly, and neither crashes on IMVs registered in a non-`public` schema.
This is a module-only release — no catalog or SQL surface changed. Replace the
`.so`, then `ALTER EXTENSION pg_reflex UPDATE TO '1.10.9';`.

### Fixed

- **Crash on non-`public`-schema partitioned IMVs.** The `archive_residue` audit
  check queried partition children by bare relname; for an IMV registered in a
  non-`public` schema, running `reflex_audit()` / `reflex_doctor()` under a
  `search_path` that excluded that schema raised `relation "…" does not exist`
  and aborted the whole call. The target-child count and the source-child
  partition-constraint lookup are now schema-qualified.
- **A single failing check aborted the entire audit.** Audit checks now run with
  engine-level isolation: a check that raises a Postgres error yields a
  `check-errored` finding and the remaining checks still run, so one bad IMV can
  no longer take down `reflex_audit()` / `reflex_doctor()`.
- **`archive_residue` no longer forfeits sibling partitions.** A malformed or
  unresolvable stored `where_predicate` (or partition constraint) on one
  partition now degrades to a "could not verify" finding for that partition
  instead of aborting the whole check.
- **`reflex_doctor` archive-residue rows were unusable.** They now carry the real
  IMV name and an executable `reflex_reconcile_partition(...)` command (from the
  audit's structured findings) instead of a blank object and a `...` placeholder,
  and `fix => TRUE` actually reconciles residue (it previously reported but never
  repaired). The audit's own suggested fix passed the source child as
  `partition_keys` instead of `source_partition`; corrected.
- **Fix mode no longer executes prose as SQL.** Only `SELECT`-prefixed suggested
  fixes are auto-run; advisory ("could not verify" and bare-name) findings are
  report-only.

### Added

- `reflex_doctor` now surfaces the **orphan aux tables** (F9: orphan intermediate
  / staging / scratch) and **duplicate trigger functions** (F11) that
  `reflex_audit` detects — report-only, with the `DROP` / consolidation command
  in the action column.

### Changed

- When an IMV has many (> 3) residual partitions, `reflex_doctor` collapses the
  repair to a single `reflex_reconcile(<imv>)` instead of one
  `reflex_reconcile_partition` per partition (one tree sync instead of N).

## [1.10.8] - 2026-07-03

The "untreated findings" remediation release. It closes the silent IMV-staleness
failure modes surfaced by the 2026-07-02 multi-tenant production incident and adds
`reflex_doctor()` — a single operator entrypoint that diagnoses every inconsistency
class and, on request, applies only non-breaking repairs. Replace the `.so`, then
`ALTER EXTENSION pg_reflex UPDATE TO '1.10.8';`.

### Added

- **`reflex_doctor(target, fix, drop_orphans, max_attempts)`** — one entrypoint that
  detects wedged pending roots, `known_stale` IMVs, archive residue, snapshot
  divergence, and orphan-overlap, and (only under `fix => TRUE`) applies **non-breaking**
  repairs. **Dry-run by default** (`fix => FALSE`): it prints the diagnosis and the exact
  remediation SQL and mutates nothing. The one destructive-ish repair (dropping a
  confirmed orphan, F3) is gated behind `drop_orphans => TRUE`; the one non-additive
  repair (chain rebuild, F4b) is **never auto-performed** — it is reported with the
  `reflex_rebuild_chain(...)` call to run. Each repair runs in its own subtransaction,
  so one failure records `failed:<err>` for that row without aborting the report.
- **`reflex_rebuild_chain(view)`** — in-extension recovery for a corrupted *decomposed*
  (CTE / set-op) IMV chain: an **atomic** CASCADE drop + recreate from the create-time
  arguments now captured in the new `__reflex_ivm_reference.create_args` column. A
  recreate failure aborts the whole operation (the drop rolls back) — never a
  half-dropped chain.
- **`reflex_partition_pending_status()`** — read-only reporter of the partition
  pending-queue backlog (source root, age, `attempts`, `last_error`).
- **`reflex_refresh_partition_snapshot_if_diverged(source_root)`** — on-demand snapshot
  self-heal: oid-diffs `__reflex_source_partition_snapshot` against the live leaf set and
  rebuilds only on divergence.
- **`known_stale` / `stale_reason`** columns in `reflex_ivm_status()` (backed by new
  `__reflex_ivm_reference.known_stale` / `stale_reason` / `stale_since`): a durable health
  flag set on any caught cascade/flush failure and cleared on a successful reconcile, so
  `SELECT * FROM reflex_ivm_status() WHERE known_stale` answers "is anything wrong now?".
- Two new `reflex_audit` checks: **`archive_residue`** (a partitioned IMV partition that is
  empty while its authoritative — `ignore_sources` — source has rows for that key; suggests
  `reflex_reconcile_partition`) and **`bare_name_ambiguity`** (an unqualified `depends_on`
  entry that resolves to a relation in more than one schema, a shared-multi-tenant footgun).
- GUC **`pg_reflex.debug_resolve_anchor`** (default `off`) — gates the `REFLEX-DBG
  resolve_anchor` NOTICE spam that previously buried real WARNINGs on multi-IMV cascades.

### Fixed

- **A failed partition flush permanently wedged the pending queue (silent data-staleness).**
  The DDL enqueue used `ON CONFLICT (source_root) DO NOTHING`; once a root's row was stuck
  (a flush had failed), every later `ATTACH`/`DETACH`/`CREATE … PARTITION OF` was a no-op
  insert that never re-fired the `AFTER INSERT` drain trigger — so the IMV silently stopped
  updating (observed: `nvg.sales_simulation` wedged 6 days). The enqueue is now
  `ON CONFLICT DO UPDATE` (bumping `attempts`/`enqueued_at`) **and** the drain constraint
  trigger fires `AFTER INSERT OR UPDATE`, so a stuck root is retried on the next partition
  DDL. Failed drains record `last_error` on the pending row.
- **`drop_orphans=false` orphan partitions caused later `would overlap partition` swap
  aborts** that left an IMV (and its dependents) stale. A partition swap now detects a
  *confirmed* orphan target child (bounds match the incoming swap target and **no** live
  source partition backs it) and drops it inside the same swap transaction before `ATTACH`;
  the reconcile immediately repopulates, so no committed data is lost. The drop is
  fail-safe (skipped if the live-source set cannot be enumerated) and error-propagating,
  and can never remove a live-backed child.

### Changed

- **`reflex_rebuild_imv` documented as an anchor-scoped alias of `reflex_reconcile`.** A
  repro confirmed the incident's "rebuild left the partition empty" was not a skip bug: the
  function re-derives every child of the *anchor* source and does **not** fill partition
  keys fed only by a source listed in `ignore_sources`. Use `reflex_reconcile_partition`
  (or `reflex_doctor` / the new `archive_residue` check) for that case.

### Performance

- **`reflex_ivm_status()` no longer full-scans every IMV target.** Its `row_count`
  column ran an exact `COUNT(*)` per IMV — O(rows) per target, so a mature registry
  with large IMVs took seconds-to-minutes. It now reports the planner estimate
  `pg_class.reltuples` when the target is analyzed (`reltuples > 0`) and falls back to
  an exact `count(*)` only when the estimate is unavailable (empty or never-analyzed
  target, where the count is cheap). `row_count` is therefore an estimate for large
  tables; exact for small/empty ones. No signature or migration change.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.10.8';` after swapping the module:
  [`sql/pg_reflex--1.10.7--1.10.8.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.7--1.10.8.sql)
  adds the six new columns and the new SQL-callable functions. Like 1.10.0 it does **not**
  drain the pending queue (ALTER EXTENSION UPDATE runs in `creating_extension` mode); drain
  any backlog afterward with `SELECT public.reflex_flush_partitions();` as its own statement.

## [1.10.7] - 2026-06-23

Partition-flush reconcile of a partitioned IMV no longer full-rebuilds its
non-co-partitioned aggregate dependents on every swapped leaf — the cascade is
now key-scoped and fires once per flush. This is the root cause of the forecast
`push_baseline` COMMIT `TimeoutError` (`forecast_dp_year_agg` was rescanning the
whole parent, per leaf, past the asyncpg 200 s ceiling). Pure Rust: replace the
`.so`, then `ALTER EXTENSION pg_reflex UPDATE TO '1.10.7';` (no-op migration).

### Fixed

- **Cascade from a partitioned parent IMV into a NON-partitioned aggregate
  dependent whose `GROUP BY` includes the parent's partition key was a full
  `reflex_reconcile`.** It `TRUNCATE`d and rebuilt the dependent by rescanning
  EVERY source partition, even when a single parent key changed. It is now a
  key-scoped rebuild: a literal-pruned `DELETE` + `INSERT` of only the affected
  `partition_col IN (<keys>)` slices of the dependent's intermediate and target
  (the literal `IN` list lets PostgreSQL prune the source partitions; a
  subquery / array form does not — verified by `EXPLAIN`). A `DO`-block
  `EXCEPTION` branch falls back to a full `reflex_reconcile`, so the
  optimization can never leave a dependent incorrect. Affects the
  `*_dp_year_agg` / `*_sp_year_agg` family.
- **The flush fired that cascade once per swapped leaf.** A 12-month
  single-plan push reconciled the parent's 12 monthly leaves and rebuilt the
  dependent's same key slice 12×. The batch flush now reconciles all of one
  IMV's changed leaves in a single `reflex_reconcile_partition` call (its
  `source_partition` argument accepts a comma-separated list), so the dependent
  cascade fires ONCE over the union of affected keys. The saving scales with the
  dependent's per-rebuild cost.

### Tests

- `pg_part_cascade_scoped_to_reconciled_key` and
  `pg_part_flush_cascade_scoped_to_attached_key` — reconciling (by key) or
  attaching (derived-key flush path) one partition must not rebuild unrelated
  keys' dependent groups; a corrupted unrelated key's row must survive.
- `pg_part_flush_cascade_fires_once_for_multileaf` — a statement-level trigger
  counts cascade `INSERT`s into the dependent's target; a two-leaf flush must
  cascade exactly once while both keys still land correctly.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.10.7';` after replacing the module.
  No DDL — the change is entirely in the recompiled Rust; no IMV rebuild
  required (the EXCEPTION fallback preserves correctness):
  [`sql/pg_reflex--1.10.6--1.10.7.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.6--1.10.7.sql)
  is a no-op marker.

## [1.10.6] - 2026-06-18

Dropping an IMV's target table (or the schema/view it lives on) now cleans the
IMV up instead of orphaning its registry row. Replace the `.so`, then
`ALTER EXTENSION pg_reflex UPDATE TO '1.10.6';` (the migration re-creates the
`__reflex_on_sql_drop` event-trigger function — see Migration below).

### Fixed

- **The `sql_drop` event trigger only reacted to a *source* table being dropped,
  so dropping an IMV's own *target* table left an orphaned `__reflex_ivm_reference`
  row pointing at a relation that no longer exists.** Two paths hit this: a target
  removed via `DROP SCHEMA … CASCADE` while its sources lived elsewhere, and IMVs
  built on a **view** (a dropped view is `object_type = 'view'`, which the source
  branch's `object_type = 'table'` filter skips entirely). Observed as 8 surviving
  `yse.*` registry rows after `DROP SCHEMA yse CASCADE`. `__reflex_on_sql_drop` now
  also tears down the IMV whose registered target table *is* the dropped table,
  matched on the exact target identity (`target_schema` + bare name) — never a
  prefix, so a partition-swap maintenance cycle dropping child / `__reflex_swap_*`
  tables can never be mistaken for the registered target.
- **Identifier quoting did not escape an embedded double-quote.** `quote` now
  doubles embedded `"`, so IMV / source names containing a `"` no longer generate
  malformed teardown and maintenance DDL.

### Changed

- **Internal: registry reads go through a single typed seam.** `read_imv` returns
  a typed `ImvRecord` DTO and the `reflex_set_*` setters delegate to typed registry
  writers, replacing hand-rolled `SELECT … FROM __reflex_ivm_reference` across the
  drop, reconcile, and audit paths. Behavior-neutral — no user-facing change.

### Migration

- The orphan fix re-creates the `__reflex_on_sql_drop` plpgsql function (no
  signature or schema change): [`sql/pg_reflex--1.10.5--1.10.6.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.5--1.10.6.sql).
  The quoting fix and the registry-read-seam refactor are pure Rust — no migration
  DDL. No IMV rebuild required. Pre-1.10.6 orphaned rows are not retroactively
  cleaned: drop them with `drop_reflex_ivm(name, true)`, or a bare
  `DELETE FROM public.__reflex_ivm_reference WHERE …` for rows whose target is
  already gone.

## [1.10.5] - 2026-06-14

Attaching a new top-level partition with many empty sub-partitions is now far
cheaper. Replace the `.so`, then `ALTER EXTENSION pg_reflex UPDATE TO '1.10.5';`
(the migration drops and recreates `reflex_reconcile_partition` to add a 4th
argument — see below).

### Changed

- **`reflex_reconcile_partition` gained a 4th `skip_sync boolean DEFAULT false`
  argument.** When true, the per-partition reconcile skips its orphan-swap
  cleanup and its internal whole-tree `reflex_sync_partitions` — used by the
  batch flush, which now syncs the tree once up front instead of once per leaf.
  The `DEFAULT false` keeps every existing 2-/3-arg call site working unchanged.

### Fixed

- **Attaching a partition whose sub-partitions are mostly empty reconciled every
  leaf, one by one.** A demand-plan macro-partition attaches a fixed window of
  monthly sub-partitions (e.g. 48 months), but typically only a handful hold
  data. `reflex_flush_partitions_impl` emitted one `reflex_reconcile_partition`
  per attached leaf, and each call (a) re-ran `reflex_sync_partitions` over the
  whole tree and (b) paid the fixed per-leaf swap DDL (`CREATE … LIKE` + fill +
  `ANALYZE` + `DETACH`/`ATTACH`/`DROP`) — a cost independent of row count, so the
  empty months cost the same as the populated ones. On `base-db-anchor-evm`,
  attaching one `sales_simulation` demand plan to `sop_forecast_view` took ~46s
  at COMMIT regardless of how few months actually held forecast rows.

  The flush now syncs each IMV's partition tree **once** up front, then drives the
  per-leaf reconciles with `skip_sync => true`. A brand-new (`AttachNew`) leaf
  whose **source is empty** is a provable empty→empty no-op — the up-front sync
  has already created its empty mirror partition and there is no prior target
  data to clear — so its per-leaf swap (and its downstream cascade) is skipped
  entirely. `SwapFill` (the partition's OID changed) and surviving-ancestor
  refills always fill, since they may hold stale target rows; any source-probe
  failure falls back to filling, so the skip never trades correctness for speed.
  A/B on a 48-leaf attach with 44 empty months: the isolated flush dropped from
  898s to 51s (~17.6×, debug build); the IMV remains exactly equal to the source
  with all 48 months mirrored (empty months present but empty).

## [1.10.4] - 2026-06-11

Two follow-up fixes to 1.10.x: the partition attach/detach no-op skip now also
covers DETACH-then-DROP, and `drop_reflex_ivm` no longer leaks the per-source
DEFERRED staging delta table. Replace the `.so`, then `ALTER EXTENSION pg_reflex
UPDATE TO '1.10.4';`.

### Fixed

- **DETACH-then-DROP of an irrelevant partition force-reconciled dependent
  unpartitioned IMVs.** The 1.10.3 incremental partition-delta path skips
  updating a dependent unpartitioned IMV when an attached/detached partition
  holds no rows the IMV's `WHERE` filter keeps — by probing the partition
  child's rows against `where_predicate`. The partition flush is a DEFERRED
  constraint trigger firing at COMMIT, so ATTACH and DETACH-without-drop both
  leave the child alive to probe. But when a partition is DETACHed **and**
  DROPped in the *same transaction* (what migration tools that wrap in a txn
  do), the child is gone by flush time: `reflex_flush_partitions_impl`'s `Drop`
  branch can no longer resolve its OID and fell back to a full `reflex_reconcile`
  (TRUNCATE + rebuild + the whole downstream cascade — the exact cost the
  optimization removes). On `base-db-anchor-evm`, detaching+dropping a
  non-`sop_current_view` assortment partition of `assortment_activity_relation`
  rebuilt the entire `current_assortment_activity_view` ~20-IMV subtree. This was
  a performance regression only; the reconcile produced correct data. The fix
  proves the dropped partition was irrelevant from its captured `LIST` bound
  rather than its (now-gone) rows: `refresh_source_snapshot` now records each
  leaf's `FOR VALUES …` bound in `__reflex_source_partition_snapshot.bound`, and
  the new `reflex_partition_drop_maybe_skip` SPI probes the IMV filter against
  that bound. It is sound by construction — the probe relation exposes only the
  partition key column, so a predicate touching a non-key column errors → trap →
  reconcile, and `RANGE`/`HASH`/multi-key/no-filter/inconclusive branches all
  fall back to `reflex_reconcile`. Only a clean "no partition value passes the
  filter" proves the no-op and skips.
- **`drop_reflex_ivm` leaked the per-source DEFERRED staging delta table.**
  Dropping an IMV tore down every per-IMV artifact (target, intermediate,
  affected, shrunk, scratch, partition transition tables) but never dropped the
  per-source `__reflex_delta_<source>` staging delta shared by all DEFERRED IMVs
  on a source — leaving an orphan relation that `reflex_audit`'s `OrphanStaging`
  check flagged. `drop_reflex_ivm` now drops it when the last IMV on the source
  goes away (`other_count == 0`); `IF EXISTS` makes it a no-op for IMMEDIATE
  IMVs that never created one. Pre-1.10.4 orphans are also healed at create time
  by `ensure_staging_matches_source`.

### Migration

- Both fixes recompile into the module. The DETACH-then-DROP fix adds one new SQL
  function: [`sql/pg_reflex--1.10.3--1.10.4.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.3--1.10.4.sql)
  installs `reflex_partition_drop_maybe_skip` on existing databases. The
  `drop_reflex_ivm` staging fix is pure Rust (the C-language function's `CREATE`
  is unchanged) — no migration DDL. No IMV rebuild or refresh required. Existing
  partitioned-source snapshots gain bounds on their next flush, so the *first*
  detach-drop after upgrade still reconciles, then self-heals.

### Tests

- New `src/tests/pg_test_partition.rs` regressions: `detach_irrelevant_partition_not_dropped_is_noop`
  (working baseline), `detach_then_drop_irrelevant_partition_is_noop` (the
  regression — RED before the fix), `detach_then_drop_in_filter_partition_removes_rows`
  (relevant drop must reconcile), and `detach_then_drop_nonkey_predicate_reconciles`
  (soundness guard — a non-key-column filter must never skip). New
  `pg_test_drop.rs::drop_deferred_imv_wipes_every_nonmaintenance_table` count
  baseline for the staging-delta cleanup. 1256 tests pass; clippy + fmt clean.

## [1.10.3] - 2026-06-11

Two independent changes: a correctness + performance fix for incremental
maintenance of IMVs whose FROM clause contains a `UNION ALL` subquery, and a new
incremental partition-delta path that stops partition attach/detach from
full-rebuilding dependent unpartitioned IMVs.

The `UNION ALL` fix targets IMVs like `SELECT g, SUM(x) FROM (… FROM a UNION ALL
… FROM b) st GROUP BY g`. A mutation to a table in one operand was maintained as
if the whole subquery were the delta, re-scanning and re-counting the *unchanged*
sibling operands — a silent wrong answer (previously logged as
"correctness-neutral") plus an O(base) full scan. That fix is in the Rust trigger
codegen, recompiled into the module — no catalog/schema or function-signature
change. The partition-delta optimization adds one new SQL function (installed by
the migration below). Upgrade with `ALTER EXTENSION pg_reflex UPDATE TO '1.10.3';`
after replacing the `.so`.

### Added

- **Incremental partition delta for unpartitioned IMVs.** Attaching or detaching
  a partition on a `LIST`/`RANGE`-partitioned source previously forced a full
  `reflex_reconcile` (TRUNCATE + rebuild) of every dependent *unpartitioned* IMV,
  detonating the entire downstream cascade even when the partition change kept no
  rows the IMV actually retains. A partition change is semantically a bulk DML on
  the source (ATTACH-with-data ≡ bulk `INSERT`, DETACH ≡ bulk `DELETE`), so the
  new `reflex_apply_partition_delta(imv, source, op, child, trans)` SPI feeds the
  partition child through the *same* incremental maintenance pipeline the
  INSERT/DELETE triggers use (`reflex_build_delta_sql`): a `where_predicate`
  pred-check skip short-circuits a filtered-out partition in O(1), a Path B ratio
  check falls back to reconcile for large bulk changes, and every uncertain
  branch (no incremental delta available, e.g. FULL-JOIN secondary) falls back to
  `reflex_reconcile`. Propagation is now write-driven: a net-zero delta writes
  nothing, so the cascade dies at whatever depth it nets to zero. Attaching a
  non-current LIST partition to a filtered IMV (the `base-db-anchor-evm`
  `current_assortment_activity_view` case) now skips the full rebuild of its
  ~20-IMV downstream subtree entirely. The trigger hot path is unchanged; the
  flush wiring (`src/partition.rs`) dispatches per changed partition, with
  `SwapFill` and inaccessible-detached-child still taking the conservative
  reconcile. See `plans/2026-06-11-incremental-partition-delta-unpartitioned-imv.md`.

### Fixed

- **`UNION ALL`-subquery aggregates double-counted unchanged operands.** The
  trigger-time delta rewriter swapped only the mutated operand's table for its
  transition table and left the sibling operands referencing their full base
  tables, then MERGE-**added** the result as if it were a delta. So every
  sibling row was re-counted on every mutation: a silent wrong `SUM` in
  overlapping groups (masked in the field only because the production
  `sop_incoming_stock_baseline_view` projects `0 AS purch` for its second
  operand, so the double-count summed to zero) and a full-base re-scan
  (O(base) — a 1-row delta took ~18 min on the production view). `UNION ALL`
  is a multiset sum, so `SUM`/`COUNT` distribute over it; the delta query now
  prunes the subquery to only the operand(s) referencing the changed source
  before the transition swap, so maintenance is both correct **and** O(delta).
  DELETE/UPDATE shared the bug via the same rewriter and are fixed the same way.
- **Passthrough over a `UNION ALL` subquery duplicated sibling rows.** The same
  root cause surfaced in the non-aggregate path as an INSERT re-appending the
  full unchanged operand. Fixed by the same operand scoping (INSERT + UPDATE).
- **Non-distributive set-op subqueries now recompute correctly.** A source
  inside a `UNION` (distinct) / `INTERSECT` / `EXCEPT` subquery has no valid
  incremental delta; such mutations now fall back to a correct full recompute
  (both aggregate and passthrough) instead of the invalid operand swap.

### Tests

- New `src/tests/pg_test_union_subquery_delta.rs` (8 oracle regressions):
  aggregate INSERT/DELETE/UPDATE with a non-zero sibling operand, the
  zero-sibling production shape, passthrough INSERT + keyed UPDATE/DELETE, the
  no-key cross-operand collision (full-refresh) case, and the `UNION`-distinct
  recompute fallback. `replay_sop_baseline_secondary_is_sublinear` (the field
  replay that took 18 min) is un-ignored and now an active sublinear guard.
  Full `pg_test` + unit suites stay green; `clippy`/`fmt` clean.
- New partition-delta oracle regressions in `src/tests/pg_test_partition.rs`:
  no-op attach of a non-current LIST partition (content unchanged + downstream
  child `last_update_date` unchanged), relevant attach, empty partition
  (`SKIPPED`), DETACH no-op vs. current-partition removal, Path B fallback,
  join-IMV incremental match, and the `''`→reconcile fallback shape.

### Migration

- The `UNION ALL` fix needs no catalog/schema or function-signature change. The
  corrected delta SQL is generated at trigger time from the unchanged stored
  `base_query`, so existing IMVs are fixed automatically once the recompiled
  module is loaded — no rebuild, refresh, or DROP/recreate needed.
- The partition-delta optimization adds one new SQL function. Replace the `.so`
  with the 1.10.3 build, then run `ALTER EXTENSION pg_reflex UPDATE TO '1.10.3';`
  — [`sql/pg_reflex--1.10.2--1.10.3.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.10.2--1.10.3.sql)
  installs `reflex_apply_partition_delta` on existing databases. No IMV rebuild
  or refresh is required.

## [1.10.2] - 2026-06-10

Correctness + efficiency fix for IMVs filtered by an uncorrelated scalar
subquery, e.g. `WHERE assortment_id = (SELECT assortment_id FROM
sop_current_view)`. The filter was dropped from the IMV's per-source metadata,
so the relevance-skip never fired: updates to rows *outside* the filter were
maintained anyway, and — because the keyed passthrough DELETE matches only the
unique key — an out-of-filter update sharing those key values silently deleted
in-filter rows. No catalog/schema or function-signature changes; the fix is in
the Rust query analyzer, recompiled into the module: `ALTER EXTENSION pg_reflex
UPDATE TO '1.10.2';`.

### Fixed

- **Scalar-subquery WHERE filters were dropped from per-source metadata.**
  `collect_imv_relevant_where` attributed each WHERE conjunct against *every*
  relation in the query — including one that appears only inside the subquery
  (and is therefore also registered as a source). So `col = (SELECT … FROM
  other)` looked "cross-source" and was discarded, leaving the source's
  `imv_relevant_where` empty and the filter-aware relevance-skip permanently
  off. Result: needless maintenance (and downstream cascade) for irrelevant
  updates, plus silent loss of correct rows when an out-of-filter update
  collided on the unique key. The analyzer now attributes conjuncts against the
  **outer FROM**'s sources only and treats scalar subqueries as opaque values
  (a subquery-depth-aware ref collector), so single-outer-source IMVs with a
  subquery filter capture the predicate and the skip fires. Multi-source IMVs
  are unchanged.

### Tests

- `test_passthrough_subquery_filter_skips_noncurrent_group_deferred` — oracle
  reproducing the wrong-delete (RED before the fix); and
  `cov_rebuild_metadata_restores_subquery_filter_skip` — verifies the in-place
  migration path below. Full unit + `pg_test` oracle suites stay green.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.10.2';` — no-op marker
  (`sql/pg_reflex--1.10.1--1.10.2.sql`). The per-source metadata is persisted at
  CREATE time, so **existing** IMVs with a scalar-subquery filter must be
  refreshed in place — no DROP/recreate needed — by running, after the upgrade,
  `SELECT reflex_rebuild_imv_metadata('<schema.imv>');` for each. New IMVs get
  the correct metadata automatically.

## [1.10.1] - 2026-06-10

Performance fix for the incremental maintenance of aggregate IMVs that
`LEFT JOIN` a secondary table. A tiny change to the secondary re-aggregated
the ENTIRE base instead of the few affected groups, so propagating a
single-row source update through such an IMV took minutes — observed as 18
minutes for a 2-row delta on a 9-source view. No catalog/schema or
function-signature changes; the fix is in the Rust trigger-time SQL generator,
recompiled into the module: `ALTER EXTENSION pg_reflex UPDATE TO '1.10.1';`.

### Fixed

- **Aggregate `LEFT JOIN`-secondary updates re-aggregated the whole base.**
  `outer_join_secondary_stmts` built its "affected groups" set by re-running
  the full base aggregation with the secondary swapped for its transition
  table. Because the secondary is outer-joined, the join preserves every
  primary row, so the affected set was *every* group and the recompute then
  re-scanned the entire base a second time. When the secondary carries a
  `source_join_keys` mapping (its join keys map to GROUP BY columns and cover a
  unique key), the maintenance is now scoped by `(group_cols) IN (changed keys
  from OLD∪NEW transition)` — a predicate on GROUP BY columns that the planner
  pushes below the aggregation into the indexed base scan. Falls back to the
  prior broad recompute when no mapping is available, so no shape regresses.
  (18 min → ~50 ms on the `sop_incoming_stock_baseline_view` shape.)

### Tests

- New `test_outer_join_secondary_aggregate_scopes_recompute_by_join_keys`
  pins the scoped maintenance SQL. The full `pg_test` oracle suite — including
  the both-sources-mutated double-count guard and the secondary keyed
  LEFT-JOIN all-ops oracles — stays green.

### Internal (no behaviour change)

- The weak-stub archive that lets the test binaries link against postgres
  backend globals is now also built on macOS and force-loaded
  (`+whole-archive`) into the `cfg(test)` binary, so `cargo test` and
  `cargo pgrx test` run locally on macOS (previously aborted at load with
  `dyld: symbol not found in flat namespace '_CacheMemoryContext'`). The
  cdylib is unaffected — the stubs stay `cfg(test)`-scoped and never reach the
  module postgres dlopens.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.10.1';` — no-op marker
  (`sql/pg_reflex--1.10.0--1.10.1.sql`). The fix is pure Rust codegen in the
  recompiled module; replace the `.so` first. Existing IMVs continue operating
  without intervention.

## [1.10.0] - 2026-06-10

Fixes the long-standing hole where `ALTER TABLE source ATTACH PARTITION
child_with_data` created the IMV partition but never synced its data, leaving
it empty (row count 0). ATTACH re-parents existing rows without firing row
triggers, so the reconcile path is the only thing that can capture them — and
nothing drained the partition pending queue automatically. Apply after
replacing the module: `ALTER EXTENSION pg_reflex UPDATE TO '1.10.0';` (the
migration installs the new auto-drain trigger and drains any pre-existing
wedged queue rows).

### Fixed

- **ATTACH PARTITION with data left the IMV partition empty.** The
  `ddl_command_end` event trigger enqueued the source root into
  `__reflex_partition_pending` and created the empty IMV partition structure,
  but no code path ever drained the queue, so the data-filling reconcile never
  ran. A new commit-time auto-drain (below) closes this.
- **One broken root wedged the entire partition flush.**
  `reflex_flush_partitions` drained all pending roots in a single transaction
  with `?`-error propagation and a trailing cross-root `DELETE`, so the first
  failing root aborted and rolled back the whole batch — draining nothing and
  re-wedging on every retry. Each root is now reconciled, snapshot-refreshed,
  and drained atomically inside its own PL/pgSQL subtransaction; a failing root
  is left pending with a `WARNING` and the next root proceeds.
- **Shape drift made reconcile throw `"… is not partitioned"`.** When a source
  child was rebuilt partitioned under the same name (gaining a sub-level), the
  mirrored IMV child — left a plain table by `CREATE … IF NOT EXISTS`, which
  silently skips wrong-shape children — became fatal to every reconcile of that
  node. `reflex_sync_partitions` now detects the `relkind` mismatch, drops the
  stale child top-down, and rebuilds it with the correct shape.

### Added

- **Commit-time auto-drain of the partition pending queue.** A `DEFERRABLE
  INITIALLY DEFERRED` constraint trigger on `__reflex_partition_pending` fires
  a scoped `reflex_flush_partition_source` per enqueued root at COMMIT, so
  ATTACH-with-data syncs the IMV automatically with no manual
  `reflex_flush_partitions()` call. Mirrors the deferred-DML flush; recursion is
  bounded by the existing `NOT LIKE '%__reflex_%'` enqueue guard.
- **`__reflex_refresh_partition_snapshot(text)`** — internal SQL-callable
  wrapper so the per-root flush subtransaction refreshes the source snapshot
  atomically with its reconciles. Not part of the public API.

### Tests

- `#[pg_test] flush_isolates_failing_root_from_healthy_root`: a corrupted root
  no longer prevents a healthy root from reconciling and draining.
- `#[pg_test] sync_heals_leaf_child_into_partitioned`: a depth-2 IMV whose
  same-named source child is rebuilt partitioned is healed from `relkind='r'`
  to `'p'` with data preserved (reproduces the production poison).
- `#[pg_test] attach_with_data_auto_syncs_at_commit`: ATTACH of a pre-populated
  partition auto-syncs the IMV data at COMMIT with no manual flush.
- Unit `shape_mismatch_detects_leaf_where_partitioned_expected` for the pure
  `partition_shape_mismatch` helper.

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.10.0';` after replacing the module. The
migration registers `__reflex_refresh_partition_snapshot` and installs the
`__reflex_partition_flush_trigger` auto-drain constraint trigger (pure DDL). The
two Rust fixes (per-root isolation, shape heal) ship in the recompiled module
and need no DDL. To clear any pre-1.10.0 backlog, run
`SELECT reflex_flush_partitions();` **after** the upgrade as its own statement —
it must not run inside the `ALTER EXTENSION` script, which runs in
extension-creation mode and rejects DDL on the runtime partition tables
(SQLSTATE 55000 "is not a member of extension").

## [1.9.2] - 2026-06-08

A correctness fix for the commit-time cascade flush of CTE-decomposed views.
No catalog/schema or function-signature changes — the fix is in the Rust
trigger-time SQL rewriter, recompiled into the module, so existing IMVs keep
working: `ALTER EXTENSION pg_reflex UPDATE TO '1.9.2';`.

### Fixed

- **`zero-length delimited identifier` (SQLSTATE 42601) at COMMIT for
  DEFERRED CTE-decomposed views.** A CTE whose inner body is a passthrough
  feeding an outer aggregate decomposes into a passthrough sub-IMV
  (`s.v__cte_base`) plus an aggregate parent (`s.v`). On a base mutation, the
  sub-IMV's maintenance cascades to the parent, which is flushed with its
  source passed **unquoted** (`s.v__cte_base`, as enqueued by the sub-IMV's
  deferred triggers) while the parent's stored `base_query` references that
  source **quoted** (`"s"."v__cte_base"`). The trigger-time delta rewriter
  `replace_source_with_transition` could not match across the `"."`, so its
  bare-token pass replaced the bare name *inside* the existing quotes and
  injected the transition name, emitting `"s".""__reflex_old_…""` — a `.""`
  zero-length delimited identifier the scanner rejects, aborting the user's
  transaction at commit. (Where the per-IMV flush swallowed it as a warning,
  the parent IMV silently went stale — a correctness violation.) The fix
  rewrites the source's **quoted spellings first**, so the bare-token pass can
  never inject quotes inside an existing delimited identifier. It generalizes
  to any schema-qualified source whose `base_query` references it quoted, not
  just CTE sub-IMVs. The earlier `canonical_source` quoting fix hardened the
  name-builders but had not reached this rewriter.

### Tests

- Unit regressions in `replace_source_with_transition` for a schema-qualified
  source referenced quoted in `base_query`, and for an unqualified source the
  `base_query` happens to quote (both assert no `""` is emitted and the
  transition reference is correct).
- End-to-end `#[pg_test]` for the schema-qualified passthrough→aggregate
  DEFERRED decomposition: drives the real decomposed `base_query` through the
  aggregate parent's delta builder with the unquoted cascade source (the call
  that previously emitted the `""`) and asserts none is produced, guarded by a
  precondition that the parent really does reference the source quoted.

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.9.2';` after replacing the module. No
DDL runs — the migration file is a no-op marker. Nothing to recreate; the
corrected rewriter applies to the next flush of every affected IMV.

## [1.9.1] - 2026-06-05

A performance release for partitioned passthrough maintenance. No catalog/schema
or function-signature changes — everything ships in the recompiled module, so
existing IMVs keep working: `ALTER EXTENSION pg_reflex UPDATE TO '1.9.1';`. (The
new `reflex.assert_inplace_update` GUC is registered in `_PG_init`, not via
catalog DDL.)

### Added

- **In-place upsert for partitioned passthrough UPDATE.** The cold dispatch
  branch now maintains a pure-data UPDATE with `INSERT … ON CONFLICT (<key>) DO
  UPDATE` (on the existing `__reflex_uk_` index) plus a keyed delete-gone for
  rows that disappeared or left the query's WHERE filter, instead of a full
  DELETE + recompute INSERT. Measured ~3–4.5× faster flush on a 33.7M-row,
  837-leaf passthrough IMV (100–110k pure-data rows: ~11.2 s → ~2.5–3.9 s).
  Falls back to the prior DELETE+INSERT for non-partitioned, keyless, or
  non-UPDATE cases.
- **`reflex.assert_inplace_update` GUC** (boolean, default `off`). When on, the
  in-place path re-derives the affected key set after maintenance and raises on
  any divergence from a fresh recompute — a runtime correctness self-check for
  CI/fuzz (always on in the test suite) and first-rollout canary use.

### Changed

- **LIST partition-key pruning of the cold passthrough DELETE/INSERT.** The cold
  body now constrains the partition key to the touched cold partitions, so the
  planner prunes to the affected leaves (cold keyed-delete planning ~110 ms →
  ~7 ms and execution ~3.7× on the 837-leaf benchmark). Semantic no-op; LIST
  only (RANGE unchanged).

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.9.1';` after replacing the module. No
DDL runs — the migration file is a no-op marker. Nothing to recreate; the new
cold-UPDATE codegen applies to the next flush of every existing partitioned
passthrough IMV.

## [1.9.0] - 2026-06-04

A performance + correctness release for passthrough maintenance. Partitioned
passthrough/aggregate IMVs now dispatch DML to only the affected child
partitions; passthrough LEFT-JOIN secondaries are maintained with a keyed
delete + delta insert instead of a full rebuild; and inner CTE sub-IMVs (and
single-source passthrough IMVs generally) now detect a source PRIMARY KEY and
maintain incrementally instead of full-rebuilding on every flush. There are no
catalog/schema changes and no function-signature changes — every change ships in
the recompiled module, so existing IMVs keep working: `ALTER EXTENSION pg_reflex
UPDATE TO '1.9.0';`.

---

### Added

- **Keyed incremental maintenance for passthrough LEFT-JOIN secondaries**
  (audit #3). A change on a to-one secondary source is applied via a keyed
  delete + delta insert rather than a full rebuild. FULL OUTER passthrough
  secondaries retain the (correct) full-rebuild fallback.
- **Auto-indexing of passthrough secondary join keys** (coverage-checked), so
  the keyed secondary path has an index to probe.

### Performance

- **Partition-aware trigger dispatch** for partitioned passthrough and aggregate
  IMVs (LIST and RANGE). A DML batch is routed to only the affected ("hot")
  child partitions instead of re-scanning the mirrored tree; RANGE binds the hot
  child OIDs and excludes the cold ones.
- Child resolution during dispatch is now **O(partitions), not O(rows)** —
  per-value materialization replaces a per-row child lookup.

### Fixed

- **Inner CTE sub-IMVs now maintain incrementally.** A single-source passthrough
  (including the inner `__cte_<alias>` IMV of a CTE chain) never detected its
  source PRIMARY KEY because the catalog lookup read `pg_attribute.attname`
  (type `name`) into a `text[]` binding and silently swallowed the resulting
  type mismatch, leaving the IMV keyless and forcing a full rebuild on every
  flush (and, in a CTE chain, an oversized inner delta that re-scanned the outer
  level too). The lookup now casts `attname::text` (matching the three sibling
  catalog queries). A CTE whose source has no provable unique key keeps the
  (correct) keyless full-rebuild fallback — no unsound key is ever assigned.
- Case is preserved in the secondary-key auto-index for mixed-case columns.

### Tests

- Oracle coverage for keyed passthrough secondaries (IMMEDIATE + DEFERRED),
  partitioned-passthrough hot/cold dispatch (LIST + RANGE), and keyed inner CTE
  maintenance: catalog key detection, IMMEDIATE correctness under all DML
  including rows entering/leaving a CTE's WHERE filter, O(K) inner delta in
  DEFERRED mode, and the retained keyless fallback.

### Migration

- No-op marker (`sql/pg_reflex--1.8.2--1.9.0.sql`): no DDL — all changes ship in
  the module. Run `ALTER EXTENSION pg_reflex UPDATE TO '1.9.0';` after replacing
  the `.so`.

---

## [1.8.2] - 2026-06-03

The IMV's partition depth is now decoupled from the source's: it can mirror the
source at a **shallower depth**, all the way down to **unpartitioned**.

**Shallow mirroring.** Declaring `partition_by => ARRAY['dem_plan_id']` on a
`LIST (dem_plan_id) → RANGE (order_date)` source now mirrors **only** the
`dem_plan_id` level (a single-level IMV) instead of being rejected when the
deeper level's column isn't a bare projected output column. This unblocks IMVs
whose deeper partition key only exists as a computed projection — e.g. a
`FULL JOIN`-coalesced `COALESCE(a.order_date, b.order_date) AS order_date`,
which cannot be a partition level. `partition_by` is now **authoritative** for
the IMV's partition depth: it mirrors exactly the declared levels. With
`partition_by` omitted, auto-mirror keeps as many leading levels as have a bare
projected column and **prunes** the rest (instead of rejecting). Capture stays
correct at the coarser granularity: a source sub-partition (`order_date`)
swap/attach/detach collapses to an atomic refill of the whole matching
top-level (`dem_plan_id`) IMV partition.

**Unpartitioned target (depth 0).** An **empty** `partition_by`
(`ARRAY[]::text[]`) forces an **unpartitioned** target IMV on a partitioned
source, instead of auto-mirroring. (Omitting `partition_by` still auto-mirrors —
only an explicit empty array opts out.) Such an IMV captures ordinary DML
through the source-root trigger; source **partition swaps** (which fire no DML
trigger) are captured by a **full reconcile** — the `ddl_command_end` event
trigger enqueues the source root for **any** enabled dependent IMV, and
`reflex_flush_partitions` full-reconciles each unpartitioned dependent of a
dirty root. This also closes a prior silent-staleness gap for any unpartitioned
IMV on a swap-driven source.

Run `ALTER EXTENSION pg_reflex UPDATE TO '1.8.2';` and replace the `.so`. The
migration is **additive and non-breaking**: it adds a nullable
`__reflex_ivm_reference.partition_depth` column (`NULL` = mirror the full source
depth, so existing partitioned IMVs are unaffected — no recreate needed), an
`ancestors TEXT[]` column on `__reflex_source_partition_snapshot`, and redefines
the `ddl_command_end` event-trigger function (relaxed enqueue).

### Added

- **Shallow partition mirroring.** Explicit `partition_by` declares the IMV's
  partition depth; auto-mirror prunes to the deepest level whose column is a
  bare projected output column. New nullable catalog column
  `partition_depth INT` (`NULL` = full source depth). See
  `plans/2026-06-03-imv-partition-depth.md`.
- **Unpartitioned IMV on a partitioned source** via `partition_by => ARRAY[]::text[]`.

### Fixed

- `reflex_sync_partitions`, `reflex_reconcile_partition`, `reflex_flush_*`, and
  the audit partition-tree-drift check are all depth-aware: a shallow IMV is
  never re-deepened by a sync, and a source leaf change reconciles up to the
  IMV's mirror-depth partition.
- Unpartitioned IMVs on a partitioned source no longer go silently stale on a
  source partition swap — the flush full-reconciles them. (Tradeoff: a swap
  triggers a whole-IMV reconcile, since there are no partitions to scope it.)

## [1.8.1] - 2026-06-02

Multi-level (sub-partition) source support: an IMV whose source is partitioned
more than one level deep (e.g. `LIST (dem_plan_id) → RANGE (order_date)`) now
mirrors the **entire** source partition hierarchy and can be reconciled at any
level. Partition `DETACH`/`ATTACH` swaps — which fire no DML trigger — are
captured by the DDL event trigger and applied by a new flush.

Run `ALTER EXTENSION pg_reflex UPDATE TO '1.8.1';` and replace the `.so`. The
migration adds the two capture catalog tables, the flush functions, the
`source_partition` argument on `reflex_reconcile_partition`, the enqueue branch
on the `ddl_command_end` event trigger, and **seeds the partition snapshot for
existing partitioned IMVs** so the first post-upgrade swap is incremental.

### Added

- **Full-hierarchy partition mirroring.** `create_reflex_ivm(..., partition_by)`
  and `reflex_sync_partitions` now walk the source's whole partition tree
  recursively and build a matching multi-level IMV tree (internal nodes carry a
  sub-`PARTITION BY`). All partition-key columns at every level must be bare
  projected columns in the IMV's unique key / GROUP BY.
- **`reflex_reconcile_partition(view, partition_keys, source_partition DEFAULT '')`.**
  The new third argument reconciles a named source partition at any level by
  expanding it to its leaves and atomic-swapping each. The legacy 2-arg form is
  unchanged and now also correct on sub-partitioned sources.
- **`reflex_flush_partitions()` / `reflex_flush_partition_source(root)`.** Apply
  pending source partition swaps. The `ddl_command_end` event trigger enqueues
  the affected source root (resolved via `pg_partition_root`, pg_reflex-owned
  tables excluded) into `__reflex_partition_pending`; the flush oid-diffs the
  live leaf set against `__reflex_source_partition_snapshot` to classify each
  change as attach (new) / swap (oid changed) / detach (dropped) and reconciles
  or drops the matching IMV leaf. New catalog tables
  `__reflex_source_partition_snapshot` and `__reflex_partition_pending`.
- **Audit drift-check.** `reflex_audit(view)` now flags any divergence between a
  partitioned source's recursive leaf set and the IMV's mirrored leaves — a
  correctness backstop for a forgotten flush or an uncaptured write vector.

### Notes

- No triggers are placed on sub-partitions: swaps are DDL (captured by the event
  trigger + flush) and root-routed DML is covered by the existing root trigger,
  so newly-attached sub-partitions need no trigger management.
- Known limitation: `detach → modify the same table in place → re-attach the
  same table` (unchanged oid) is not auto-detected; attach a freshly-built table
  (the supported pattern) or call `reflex_reconcile_partition(view, '', leaf)`
  explicitly. The audit drift-check surfaces it either way.

---

## [1.7.6] - 2026-06-01

Correctness release: **`ignore_sources` is now honored on the DEFERRED trigger
path**, closing a gap where it only worked for IMMEDIATE IMVs. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.6';` and replace the `.so`. The
migration rebuilds existing source triggers so the fix takes effect without
re-creating IMVs.

---

### Fixed

- **`ignore_sources` was silently ignored on the DEFERRED path.** The guard
  that skips an IMV when DML hits a source it listed in `ignore_sources` existed
  only in the IMMEDIATE trigger body. The three deferred trigger bodies
  (INSERT/DELETE, UPDATE, TRUNCATE) and the commit-time `reflex_flush_deferred`
  never consulted `ignored_sources`. So whenever a source's trigger was the
  *deferred flavour* (installed because some sibling IMV on that source is
  DEFERRED), an IMV that had ignored that source was maintained anyway — both
  inline (for IMMEDIATE IMVs processed within the deferred trigger) and at flush
  (for DEFERRED IMVs). The deferred bodies now emit the same `ignored_sources`
  skip guard as the immediate body (via a new `__REFLEX_SLOT_BARE_SOURCE__`
  slot), and `reflex_flush_deferred` excludes IMVs whose `ignored_sources`
  overlaps the (qualified, bare) source name. No catalog schema change; the
  migration rebuilds trigger bodies via `reflex_rebuild_triggers`.

### Testing

- `pg_test_deferred.rs`: `pg_test_deferred_ignore_sources_skips_imv` — a
  non-ignoring DEFERRED sibling installs the deferred trigger; an ignoring
  DEFERRED IMV (flush path) and an ignoring IMMEDIATE IMV (inline path) must
  both stay stale after the source mutates.
- Full suite: 1120 tests pass; `cargo clippy` and `cargo fmt` clean.

## [1.7.5] - 2026-05-31

Feature release: **widened CTE/JOIN passthrough unique-key inference**, so
chained-CTE cascades (e.g. the `forecast_analysis_view` shape) auto-resolve
sound unique keys and get incremental DELETE/UPDATE instead of full refresh. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.5';` and replace the `.so`. One
additive catalog column (`max_one_row`), no data backfill.

---

### Added

- **Sound unique-key inference across JOINs and chained CTEs.** Equi-join
  equivalence in projected-key matching (a key projected through
  `f.k = dl.k` is recognized on either side), aggregate-IMV GROUP BY keys are
  registered as sound unique keys, CROSS JOIN to an ungrouped aggregate is
  classified to-one, and the anchor probe now detects `__reflex_uk_*` indexes.
  A new `__reflex_ivm_reference.max_one_row` flag (default FALSE) records when a
  sub-IMV yields at most one row. Existing IMVs keep their stored keys;
  inference re-runs at create time.

### Fixed

- Dropped an unsound `LIKE` wildcard in registry lookups.

### Testing

- `pg_test_cte.rs`: forecast-shape unique-key cascade integration test, plus
  cross-join and chained-CTE coverage.

## [1.7.4] - 2026-05-31

Correctness release for **partitioned IMV creation**. The fix is entirely in
the compiled extension — no catalog schema change and no SQL function signature
change — so the migration only bumps the installed version. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.4';` and replace the `.so`.

---

### Fixed

- **Partition-anchor resolution now accepts sources co-partitioned on the join
  key, and ignores sources partitioned on a *different* column.** This extends
  the 1.7.3 anchor fix along two axes:
  - A candidate anchor must be partitioned **on the partition column itself**,
    not merely partitioned on something. The looser "partitioned at all" check
    is replaced by the new `source_partitioned_on(source, col)` helper, so a
    source partitioned on an unrelated column is no longer treated as a
    candidate.
  - When several sources are **co-partitioned on the same column** (a JOIN whose
    key *is* the partition column), their partition layouts align, so any of
    them is a sound anchor for the child DDL — this is no longer reported as
    `multiple sources own partition column '<col>' — ambiguous`. This covers the
    case where *every* owner is a reflex intermediate and there is no base
    table at all — the `forecast_analysis_view` shape, where
    `…__cte_forecast_sales FULL JOIN …__cte_history_sales ON dem_plan_id`
    produces two partitioned `__cte_` owners and zero base owners. Base owners
    are still preferred when present; otherwise the anchor is chosen
    deterministically (lexicographically) for stability across rebuilds, and
    non-anchor co-owners (which own the column natively) fall through to Path B.
    The error now fires only when **no** source is partitioned on the column.

### Testing

- `pg_test_partition.rs`: `pg_part_copartitioned_full_join_of_cte_intermediates`
  (two partitioned `__cte_` owners, zero base — the `forecast_analysis_view`
  branch) plus the co-partitioned base-table cases.
- Full suite: 1111 tests pass; `cargo clippy` and `cargo fmt` clean.

## [1.7.3] - 2026-05-31

Correctness release for IMV **creation**. Both fixes are entirely in the
compiled extension — no catalog schema change and no SQL function signature
change — so the migration only bumps the installed version. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.3';` and replace the `.so`.

---

### Fixed

- **Failed creation of a decomposed IMV no longer orphans its sub-IMVs.**
  Creation rejections are returned as `"ERROR…"` strings, so the function
  returns normally and the surrounding transaction is *not* aborted. A query
  that decomposes into several sub-IMVs (a CTE `WITH` chain, or a `UNION ALL`
  set-op) materialises them one at a time; when a *later* operand/CTE — or the
  final outer body — was soft-rejected, the sub-IMVs already created were
  committed and left behind, polluting the IMV space. Every soft-reject path in
  `try_decompose_ctes` (reserved-prefix conflict, a sub-IMV create failing,
  non-SELECT body, body create failing) and `try_decompose_set_op` (an operand
  create failing, the non-`ALL` "cannot be intermediate" rejection) now rolls
  back the sub-IMVs it had already created — `cascade`, in reverse creation
  order, so nested descendants go too. Hard failures (a raised PostgreSQL error)
  were already rolled back by the transaction abort and are unaffected.

- **Partition-anchor resolution now prefers the base source over derived
  intermediates.** A decomposed query can produce two *partitioned* owners of
  the partition column: a base partitioned table AND a partition-inheriting
  reflex sub-IMV (e.g. a CTE that joins `sop_forecast_view` to a
  `…__cte_date_limits` sub-IMV that inherited partitioning). `resolve_anchor_source`
  treated that as `multiple sources own partition column '<col>' — ambiguous`
  and blocked the whole IMV. It now prefers the sole *base* (non
  `__cte_`/`__union_`/`__base`) partitioned owner — the table whose partition
  children are physically mirrored — falling back to the sole partitioned owner,
  and erroring only when the choice is still genuinely ambiguous. All four
  anchor call sites benefit.

### Testing

- `pg_test_error.rs`: `test_cte_decomposition_failure_rolls_back_sub_imvs`,
  `test_set_op_decomposition_failure_rolls_back_sub_imvs`.
- `pg_test_partition.rs`: `pg_part_anchor_prefers_base_over_cte_intermediate`.
- Full suite: 1108 tests pass; `cargo clippy` and `cargo fmt` clean.

## [1.7.2] - 2026-05-31

Correctness release fixing `drop_reflex_ivm`, which silently orphaned the
target + auxiliary tables of any IMV created with a bare (unqualified) name
under a non-`public` `search_path`. Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.7.2';` — the migration adds one
nullable catalog column (`__reflex_ivm_reference.target_schema`) so teardown
is independent of the session `search_path`. No data backfill; pre-existing
rows keep NULL and use the legacy `search_path` fallback.

---

### Fixed

- **`drop_reflex_ivm` silently orphaned the target + aux tables of any IMV
  created with a bare name under a non-`public` `search_path`.** All
  teardown DDL derived its relation names from the stored (bare) `name` and
  resolved the target via `to_regclass(name)`, both honouring the session
  `search_path` *at drop time*. An IMV created while `search_path = alp`
  landed its objects in `alp`, but a later `drop_reflex_ivm` run under a
  different `search_path` issued unqualified `DROP TABLE IF EXISTS …` that
  resolved against the wrong schema, skipped every real object, deleted only
  the catalog row, and left the table + `__reflex_intermediate_*` /
  `__reflex_affected_*` / `__reflex_uk_*` artifacts behind. A same-named
  decoy relation of a different kind in the `search_path` (e.g. a
  materialized view) could also be hit instead, surfacing as
  `ERROR: "<name>" is not a table`. Creation now records the object schema
  in `target_schema` (`current_schema()` for bare names), and
  `drop_reflex_ivm` re-qualifies all teardown DDL with it. Legacy rows with
  a NULL `target_schema` fall back to the prior `search_path` behaviour.

## [1.7.1] - 2026-05-31

Correctness release fixing Path C — the INSERT_PROMOTED smart bulk-INSERT
dispatch fired only by UPDATE triggers. Two compounding defects: derived
relation names were re-built by raw `split_part` + string concat that
bypassed the canonical `safe_identifier` hash, and the bulk-INSERT entry
gate did not match the Rust-side `aggregate_insert_stmts` safety check.
Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.1';` — the migration script
registers three new SQL-callable name helpers **and** automatically calls
`reflex_rebuild_triggers` for every distinct source in
`__reflex_ivm_reference.depends_on`, so existing IMVs pick up the new
trigger body without operator intervention. No catalog schema changes.

---

### Fixed

- **`ERROR: relation "<…>" does not exist` during UPDATE, surfaced as a
  `WARNING: pg_reflex Path C smart bulk-INSERT failed for <imv>` log
  line followed by silent fallback to MERGE.** The Path C plpgsql block
  derived the intermediate / scratch / target relation names by parsing
  the IMV name with `split_part(name, '.', 1|2)` and concatenating
  `'"<schema>"."__reflex_intermediate_<view>"'` by hand. Two manifestations:

    * **Bare-name IMVs** (default-schema, no `schema.` prefix): the second
      `split_part` returned the empty string and the constructed
      `"foo"."__reflex_intermediate_"` was not a real relation. The outer
      EXCEPTION caught it and Path C was silently disabled for every
      default-schema IMV.

    * **Long IMV names:** when `__reflex_intermediate_<bare>` crossed PG's
      63-char NAMEDATALEN, the real relation got the 8-hex `safe()` hash
      suffix while the Path C concat did not, so the constructed name
      pointed at a relation that did not exist. The outer EXCEPTION
      degraded Path C to a fall-through, but the WARNING surfaced.

  Three new SQL-callable wrappers — `reflex_intermediate_table_name`,
  `reflex_delta_scratch_table_name`, `reflex_quote_identifier` — expose
  the same Rust helpers every other call site uses (`split_qualified_name`
  + `safe_identifier`). The Path C body calls them instead of
  `split_part` + concat. The unique-index lookup is rewritten to join
  via `to_regclass(...)::regclass` + `pg_index.indisunique` (the previous
  `pg_indexes.indexdef ILIKE '%UNIQUE%'` form false-positived on
  comments / column names).

- **Silent double-counting in Path C for single-source aggregates.** The
  bulk-INSERT path skips MERGE on the assumption that the affected slice
  of intermediate group keys cannot already be populated. That holds
  only when the source's identity uniquely determines its slice of keys
  — i.e., when the analyser captured a `source_join_keys` entry for the
  trigger source. For single-source aggregates one row can feed many
  group keys, and other rows (filter-passed before this UPDATE) may
  already be contributing; bulk-INSERT then duplicated the affected
  groups in the intermediate / target. Pre-1.7.1 the concat bug masked
  this for bare-name IMVs (Path C errored out before any DML), but
  schema-qualified single-source aggregates with the right shape hit
  the silent duplicate-rows path. `reflex_build_path_c_explain_sql`
  now returns the empty string when the plan has no `source_join_keys`
  entry for the trigger source — matching the Rust-side
  `aggregate_insert_stmts` gate — and the trigger body falls through
  to the standard MERGE path.

### Testing

- Two regression locks added in `src/tests/unit_trigger.rs`:
  `test_path_c_block_does_not_split_part_imv_name` and
  `test_path_c_block_does_not_concat_raw_reflex_names`. Both fail
  against the 1.7.0 Path C template and pass with the 1.7.1 body.
- Full suite: **1104 tests pass** (was 1102 in 1.7.0, +2 from above).

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.7.1';` runs
[`sql/pg_reflex--1.7.0--1.7.1.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.7.0--1.7.1.sql)
which registers the three new SQL-callable name-helper functions. No
data is migrated; no IMV needs to be dropped or recreated.

After the `ALTER EXTENSION` completes, run the rebuild loop below in a
normal SQL session (outside the `creating_extension` flag PG sets for
ALTER EXTENSION) to pick up the new Path C body on every existing
trigger. The per-source trigger functions (`__reflex_*_trigger_on_*`)
were created from `create_reflex_ivm` outside any extension-creation
context, so they belong to the database — PG's
`creating_extension`-mode safety check refuses to `CREATE OR REPLACE`
them mid-upgrade with `"function … is not a member of extension
\"pg_reflex\""`. Running the loop separately sidesteps the check:

```sql
DO $$
DECLARE _src TEXT; _msg TEXT; _ok INT := 0; _err INT := 0;
BEGIN
  FOR _src IN
    SELECT DISTINCT s
    FROM public.__reflex_ivm_reference, unnest(depends_on) AS s
    WHERE enabled = TRUE AND s NOT LIKE '<%'
    ORDER BY 1
  LOOP
    BEGIN
      _msg := public.reflex_rebuild_triggers(_src);
      IF _msg LIKE 'ERROR:%' THEN
        RAISE NOTICE 'skipped %: %', _src, _msg;
        _err := _err + 1;
      ELSE
        _ok := _ok + 1;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'skipped %: %', _src, SQLERRM;
      _err := _err + 1;
    END;
  END LOOP;
  RAISE NOTICE 'rebuilt % source(s); % skipped', _ok, _err;
END $$;
```

View / matview sources (PG raises `relation … cannot have triggers`)
are expected to be skipped — pg_reflex doesn't put triggers on views,
those sources are maintained via cascade from upstream IMVs. Bare
`depends_on` entries that resolve to multiple schemas raise
`source name 'X' is ambiguous` — qualify those rows in
`public.__reflex_ivm_reference.depends_on` and re-run the loop. The
unreached source keeps its 1.7.0 trigger body (still correct — Path C
just falls through to MERGE for the affected IMVs); no IMV is wrong as
a result of skipping a source.

## [1.7.0] - 2026-05-28

Refactor + correctness release for intermediate `UNION ALL` CTE-body wrappers.
The inline wrapper-construction code in `try_decompose_set_op` is centralised
into one helper; the wrapper table gains a `__reflex_src_idx` discriminator
column that fixes a cross-operand `DELETE` over-delete; non-`ALL` set ops
used as CTE bodies are now rejected at create time with an actionable error;
and `drop_reflex_ivm` cascade no longer leaks `__reflex_union_mirror_*` trigger
functions in `pg_proc`. No catalog schema changes, no trigger body changes,
no API changes. Run `ALTER EXTENSION pg_reflex UPDATE TO '1.7.0';` to register
the new version; the migration file is a no-op marker. **Existing UNION-ALL
CTE IMVs created under ≤1.6.5 must be dropped and recreated** to pick up the
cross-operand `DELETE` fix (see Migration).

---

### Fixed

- **Cross-operand `DELETE` over-delete in intermediate `UNION ALL` CTE
  wrappers.**  A `DELETE` from operand A would over-delete a wrapper row
  contributed by operand B when both operands projected the same column
  values for the deleted row.  The per-operand mirror trigger matched by
  all-column `IS NOT DISTINCT FROM` with no operand-identity filter, so it
  also removed B's row.  The wrapper table now carries a leading
  `__reflex_src_idx SMALLINT NOT NULL` discriminator column populated with
  the operand index; the mirror `DELETE` predicate now scopes to
  `__reflex_src_idx = <operand_idx> AND <cols> IS NOT DISTINCT FROM <old>`.
  *Create-time fix — recreate any UNION-ALL CTE IMV built under ≤1.6.5
  (see Migration).*
- **`__reflex_union_mirror_*` trigger functions orphaned in `pg_proc`
  after `drop_reflex_ivm` cascade.**  Operand-sub-IMV cascade dropped the
  mirror triggers themselves but not their plpgsql functions, leaving
  three orphans per operand in `public` until a same-named overwrite.
  `drop_reflex_ivm_impl_inner` now detects UNION-ALL wrappers by the
  `__union_<i>` suffix on `depends_on_imv` entries and issues
  `DROP FUNCTION IF EXISTS … CASCADE` per operand index per op.
  *Drop-time fix — applies to any UNION-ALL IMV dropped under 1.7.0,
  regardless of when it was created.*

### Changed

- **`UNION` / `INTERSECT` / `EXCEPT` (without `ALL`) used as a CTE body
  consumed by an outer IMV are now rejected at create time** with an
  actionable error pointing to three workarounds: hoist to the outermost
  SELECT (stays a VIEW), use `kind: mv`, or rewrite as `UNION ALL` if
  operands are guaranteed disjoint.  Previously these shapes silently
  emitted a VIEW that failed deep in the consumer's trigger install with
  `Triggers on views cannot have transition tables`.  Outer-level (top of
  SELECT) `UNION` / `INTERSECT` / `EXCEPT` continue to work as VIEW
  wrappers; only the intermediate-position (CTE body) case is rejected.
  *Create-time validation — strictly better error UX, no shape that
  worked before stops working.*
- **Intermediate `UNION ALL` wrapper construction is centralised** into a
  new private helper `install_union_all_intermediate_wrapper` in
  `src/create_ivm.rs`.  `try_decompose_set_op` no longer carries inline
  `CREATE UNLOGGED TABLE` + per-operand trigger-install + registry-insert
  code.  The dropped helper `query_table_column_names` was its only
  remaining caller and is removed.  No user-visible change beyond what
  Fixed and Changed entries describe.

### Testing

- **Six regression tests added.**  In `src/tests/pg_test_drop.rs`: wrapper
  table carries `__reflex_src_idx SMALLINT NOT NULL` (column-presence
  check via `pg_attribute`); cross-operand DELETE isolation (operand A
  delete preserves operand B's same-valued row); `__reflex_union_mirror_*`
  functions in `pg_proc` are zero after `drop_reflex_ivm(…, TRUE)`.  In
  `src/tests/pg_test_error.rs`: explicit-reject tests for `UNION`,
  `INTERSECT`, and `EXCEPT` (no `ALL`) used as CTE body.  Full suite:
  **1102 tests pass**.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.7.0';` runs
  [`sql/pg_reflex--1.6.5--1.7.0.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.5--1.7.0.sql),
  a **no-op marker** — there are no catalog or function-body changes.
- **UNION-ALL CTE-body IMVs created under ≤1.6.5** were built without
  `__reflex_src_idx` on their wrapper table, and the corresponding mirror
  trigger function bodies do not reference it.  Cross-operand `DELETE`
  over-delete remains until the IMV is recreated.  Recipe:

  ```sql
  SELECT drop_reflex_ivm('<top-level-imv>', TRUE);
  SELECT create_reflex_ivm('<top-level-imv>', '<SELECT …>', …);
  ```

  The cascade drop in ≤1.6.5 also leaks `__reflex_union_mirror_*`
  functions in `pg_proc`; under 1.7.0 the cascade cleans them.  Operators
  who upgraded a database with pre-1.7.0 wrappers in place can clear the
  pre-existing orphans manually after the upgrade:

  ```sql
  DO $do$ DECLARE r RECORD;
  BEGIN
    FOR r IN
      SELECT 'public.' || p.proname || '()' AS sig
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = 'public'
        AND p.proname LIKE '__reflex_union_mirror_%'
    LOOP
      EXECUTE 'DROP FUNCTION IF EXISTS ' || r.sig || ' CASCADE';
    END LOOP;
  END $do$;
  ```

- **Non-UNION-ALL IMVs** are unaffected.
- **Outer-level (top of SELECT) `UNION ALL` IMVs** continue to be
  maintained as zero-overhead VIEW wrappers over operand sub-IMVs
  (unchanged).

## [1.6.5] - 2026-05-26

Correctness release fixing three independent create-time defects hit while
migrating real views (CTE-decomposed, `DEFERRED`, materialized-view-sourced) to
IMVs.  No catalog schema changes, no trigger body changes, no API changes.  Run
`ALTER EXTENSION pg_reflex UPDATE TO '1.6.5';` to register the new version; the
migration file is a no-op marker.  All three fixes are create-time: each
previously caused `create_reflex_ivm` to error outright or bake the wrong value
into the new IMV, so no IMV that was already created successfully is affected.

---

### Fixed

- **`DEFERRED` IMV over a CTE failed at creation with `zero-length delimited
  identifier at or near ""`.**  A CTE-decomposed sub-IMV is referenced by the
  rewritten outer query in already-quoted form (`"schema"."view__cte_x"`, needed
  to preserve identifier case).  In `DEFERRED` mode the staging-delta table-name
  builder re-quoted the already-quoted schema, emitting `""schema""` — which
  PostgreSQL rejects.  The schema component is now unquoted before being
  re-quoted.  `IMMEDIATE` mode was unaffected (its trigger names strip quotes).
  *Create-time fix — unblocks a shape that could not be created before.*
- **Explicit `unique_columns` were silently dropped for any query containing
  CTEs.**  The CTE-decomposition path did not thread the caller's
  `unique_columns` into the outer passthrough IMV (the set-op and `DISTINCT ON`
  paths already did), so a JOIN passthrough over CTEs reported "no unique key"
  and fell back to **full refresh** on `DELETE`/`UPDATE` even when a key was
  supplied.  The key now reaches the outer IMV's stored metadata.  *Create-time
  fix — recreate CTE IMVs built under ≤1.6.4 to gain incremental `DELETE`/
  `UPDATE` (see Migration).*
- **`MIN`/`MAX` over a materialized-view column failed at creation with `column
  "…" is of type numeric but expression is of type timestamp with time zone`**
  (or any non-numeric type).  Source column types were collected from
  `information_schema.columns`, which **omits materialized views**, so the
  column type was never found and the `MIN`/`MAX` intermediate column defaulted
  to `NUMERIC`.  Types are now read from `pg_catalog`, which covers every
  relkind.  This completes the 1.6.3 `MIN`/`MAX` type-resolution fix, which
  handled table-qualified columns but not matview-sourced ones.  *Create-time
  fix — unblocks a shape that could not be created before.*

### Testing

- Five regression tests added: two unit tests for the staging-delta name builder
  (quoted vs bare source), and `pg_test`s for deferred-CTE passthrough creation +
  flush, for threading explicit `unique_columns` through CTE decomposition, and
  for `MIN`/`MAX` over a `TIMESTAMPTZ` materialized-view column.  Full suite:
  1083 tests pass.

### Migration

No DDL is required.  Fixes 1 and 3 unblock creation of shapes that could not be
created before, so they have no existing-IMV impact.  Fix 2 is baked into stored
metadata at create time: an IMV built from a CTE query under ≤1.6.4 keeps its
empty unique key (full refresh on `DELETE`/`UPDATE`).  To pick up incremental
`DELETE`/`UPDATE`, drop and recreate it with an explicit key:

```sql
SELECT drop_reflex_ivm('<name>');
SELECT create_reflex_ivm('<name>', '<SELECT …>', '<key cols>');
```

## [1.6.4] - 2026-05-24

Correctness release hardened by a new differential fuzz harness.  No catalog
schema changes, no trigger body changes, no API changes.  Run `ALTER EXTENSION
pg_reflex UPDATE TO '1.6.4';` to register the new version; the migration file is
a no-op marker.  The runtime fixes below reach existing IMVs automatically once
the new module is loaded; the create-time fixes only affect newly-created IMVs.

---

### Fixed

- **LEFT / RIGHT JOIN secondary-side maintenance dropped or duplicated rows.**
  Inserting, updating, or deleting a row on the **secondary** side of an outer
  join could drop or duplicate the joined rows, and a primary row that gained or
  lost its match could be deleted outright instead of reverting to a NULL-filled
  row.  Secondary-side `INSERT` routing, affected-group scoping (by stable key),
  and quoted-source detection are corrected inside `reflex_build_delta_sql`.
  *Runtime fix — applies to existing IMVs on recompile.*
- **DEFERRED-mode duplicate-key flush.**  A deferred batch that `INSERT`ed a new
  key and then `UPDATE`d that **same** key before flush emitted both the
  new-side and the old-side delta for the key, so `reflex_flush_deferred` failed
  with `duplicate key value violates unique constraint`.  The two delta sides
  are now netted per unique key before the `MERGE`.  *Runtime fix.*
- **Silent row loss from unsound NOT-NULL inference.**  The former runtime
  data-probe marked a column `NOT NULL` whenever the **create-time** data
  happened to be NULL-free — using transient data as a proxy for a query
  guarantee.  Maintenance then matched that key with `=` instead of
  `IS NOT DISTINCT FROM` and **silently dropped rows** when a NULL appeared
  later: an unmatched primary-side `LEFT JOIN` insert, or a `GROUP BY` key that
  became NULL.  `NOT NULL` is now promoted only when the query **structurally**
  guarantees it (an INNER-join equi-key, or a catalog-`NOT NULL` base column on a
  non-nullable join side); quoted / qualified column references are rejected from
  the inference.  *Create-time fix — recreate existing aggregate IMVs to clear a
  stale over-promotion (see Migration).*
- **Filtered-IMV maintenance emitted invalid SQL from a qualified WHERE.**  A
  query-level `WHERE` carried into maintenance kept its table-qualified column
  references and failed against the transition table; the predicate is now
  alias-stripped.  *Runtime fix.*
- **Long generated column identifiers exceeded the 63-byte limit.**  A carried
  expression with a long derived name (e.g. a `EXISTS` projection) produced an
  identifier over Postgres's `NAMEDATALEN` and failed at creation; generated
  identifiers are now truncated to 63 bytes on a char boundary.  *Create-time
  fix.*

### Changed

- **An aggregate IMV whose `GROUP BY` key is not projected bare in the `SELECT`
  is now rejected up front** with a clear error, instead of failing later in
  codegen with a confusing message.  *Create-time validation.*

### Testing

- **Differential fuzz harness** (`src/tests/pg_test_fuzz.rs`, proptest under the
  `pg_test` feature).  For each generated query it builds a real
  `MATERIALIZED VIEW` and a pg_reflex IMV, applies the same DML, and asserts the
  two agree row-for-row (exact for non-float columns; NULL-safe relative epsilon
  for float columns).  Covers single-table aggregates, 2-source `LEFT JOIN`
  aggregates, carried scalars, CTE decomposition, and basic `WHERE` filters in
  both `IMMEDIATE` and `DEFERRED` modes.  The three NOT-NULL / deferred fixes
  above were found by this harness and are frozen as regression tests.  See
  [`docs/contributing/testing.md`](docs/contributing/testing.md) and
  [`docs/fuzz-findings.md`](docs/fuzz-findings.md).

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.6.4';` runs
  [`sql/pg_reflex--1.6.3--1.6.4.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.3--1.6.4.sql),
  a **no-op marker** — all fixes are in the recompiled module.  The runtime
  fixes (JOIN secondary-side, deferred netting, filtered WHERE) reach every
  existing IMV at its next trigger fire.  The create-time NOT-NULL fix does
  **not** reach an aggregate IMV created under an earlier version — its
  over-promotion is baked into the stored `aggregations.not_null_columns` and the
  intermediate-table schema, and neither the migration nor
  `reflex_rebuild_triggers` can undo it.  **Drop and recreate any aggregate IMV
  created before 1.6.4** to clear a latent over-promotion:
  `SELECT drop_reflex_ivm('<name>'); SELECT create_reflex_ivm('<name>', '<SELECT …>', …);`

## [1.6.3] - 2026-05-20

Correctness release for CTE / window-function decomposition and MIN/MAX type
resolution.  No catalog schema changes, no trigger body changes, no API
changes — existing IMVs operate without intervention.  Run `ALTER EXTENSION
pg_reflex UPDATE TO '1.6.3';` to register the new version; the migration file
is a no-op marker.

---

### Fixed

- **Window function over CTEs dropped sibling CTEs / crashed the backend.**
  A query with a window function and `WITH` CTEs hit the window-decomposition
  path on the query-wide "has a window anywhere" flag.  Two failure modes:
  - A window in the **top-level SELECT** over CTEs (e.g. `WITH a AS (…),
    b AS (…) SELECT a.x, b.y, ROW_NUMBER() OVER (…) FROM a JOIN b …`) built a
    `__base` sub-IMV that omitted the `WITH` list, so it referenced a CTE that
    no longer existed and failed with `relation "<sibling_cte>" does not exist`.
  - A window nested in a derived-table subquery (e.g. the classic
    `… FROM (SELECT …, ROW_NUMBER() OVER (…) AS rn FROM t) s WHERE s.rn = 1`,
    including when wrapped in a CTE) had no top-level window to split off, so
    decomposition re-fed an identical base query into the pipeline and recursed
    until the backend **crashed (SIGSEGV)**.
  - Fixes: CTE decomposition now runs **before** distinct-on / window
    decomposition (so sibling CTEs are preserved and the top-level-window-over-
    CTEs case works); and window decomposition is gated on an actual
    **top-level-SELECT** window — a window that exists only in a subquery /
    derived table now returns a clean error instead of recursing.
- **`MAX` / `MIN` over a table-qualified non-numeric column failed at
  creation** with `column "…" is of type numeric but expression is of type
  timestamp with time zone` (also for `date` / `text`).  The intermediate
  column resolved its type correctly from the aggregate's `source_arg`
  (`e.ts` → `timestamptz`), but the **target** table column type was resolved
  by stripping the `__max_`/`__min_` prefix off the *sanitized* column name
  (`__max_e_ts` → `e_ts`, no qualifier), which could not be resolved and
  defaulted to `NUMERIC` — so the two tables disagreed.  The target column
  type is now derived from the matching intermediate column's source argument,
  guaranteeing it equals the intermediate column type.  Bare args
  (`MAX(ts)`) were unaffected; only qualified args (`MAX(e.ts)`) over a
  non-numeric column triggered the mismatch.

### Changed

- **Window functions / `DISTINCT ON` inside a CTE referenced by an outer query
  are now rejected up front** with an actionable error instead of failing
  obscurely or crashing.  Such a CTE decomposes into a read-time VIEW (windows
  and `DISTINCT ON` cannot be incrementally maintained), and a parent IMV
  cannot install row-level triggers with transition tables on a VIEW.  The
  error directs the operator to move the window / `DISTINCT ON` to the
  outermost SELECT, or define the view with `kind: mv`.
- **Partitioning propagates to CTE sub-IMVs.**  When a partitioned IMV is built
  from a `WITH … SELECT …` query, each CTE sub-IMV now inherits the parent's
  `partition_by` columns that appear in that CTE's output projection.  The
  parent view remains partitioned as before.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.6.3';` runs
  [`sql/pg_reflex--1.6.2--1.6.3.sql`](https://github.com/diviyank/pg_reflex/blob/main/sql/pg_reflex--1.6.2--1.6.3.sql),
  a **no-op marker** — all changes are in the recompiled module
  (decomposition / type-resolution / DDL codegen), with no catalog, trigger,
  or API changes.  Views previously kept as `kind: mv` because they nest a
  window / `DISTINCT ON` inside a CTE referenced by an outer query stay
  `kind: mv`; that shape is still not an IMV, but now fails fast with guidance.

## [1.6.2] - 2026-05-19

Patch release fixing a catastrophic deferred-trigger failure on sources
whose `__reflex_delta_<src>` staging table outlived a source DDL change
(IMV drop+recreate, source DROP/CREATE — the latter unavoidable on PG ≤ 17
when adding partitioning to an existing table).  Run `ALTER EXTENSION
pg_reflex UPDATE TO '1.6.2';` to apply the staging-shape repair and pick
up the new trigger codegen.

---

### Fixed

- **Deferred trigger fails after the source's column order drifts** —
  e.g. `column "creation_date" is of type timestamp with time zone but
  expression is of type integer` on any INSERT/UPDATE/DELETE that fires
  the deferred trigger.  Root cause: the deferred trigger body did
  `INSERT INTO __reflex_delta_<src> SELECT '<op>', * FROM <transition>`
  — a **positional** bind.  The per-source staging delta is created
  with `IF NOT EXISTS` and is not dropped by `drop_reflex_ivm`, so it
  outlives the IMV and the source.  When the source's column ORDER
  changed (typically because a partitioned table replaced its
  unpartitioned predecessor with a different layout), the staging's
  column positions no longer matched the transition table's, and the
  trigger died on every DML.
  - `sql/deferred_trigger_body.plpgsql.in` and
    `sql/deferred_trigger_update_body.plpgsql.in` now emit
    `INSERT INTO staging (__reflex_op, "col_a", "col_b", …) SELECT
    '<op>', "col_a", "col_b", … FROM transition`.  Column names are
    resolved at trigger DDL build time from the live source catalog.
- **`reflex_rebuild_triggers` silently broke DEFERRED IMVs.**  The
  function always emitted the immediate-mode trigger body, so calling
  it on a source that fed at least one DEFERRED IMV replaced the
  deferred body with the immediate one — staging stopped accumulating
  and `reflex_flush_deferred` had nothing to flush.  It now inspects
  `__reflex_ivm_reference.refresh_mode` and picks the correct builder.

### Added

- **Staging shape guard in `create_reflex_ivm` (DEFERRED).** Before
  installing the deferred trigger on a source, the create path
  compares the staging table's column NAMES against the source's live
  shape.  Three outcomes:
  - identical sets (any order) → reuse the staging,
  - sets differ + staging empty → drop+recreate the staging from the
    current source shape (`CASCADE` to clear the per-session TEMP
    views from any prior `reflex_flush_deferred` call),
  - sets differ + staging has pending rows → **refuse with a clear
    error** directing the operator to flush first.  Silent drops
    would lose other IMVs' staged work; silent reuse would crash the
    new named-column INSERT.
- **`reflex_audit()` — operator-callable structural audit.** Two overloads:
  `reflex_audit()` audits every enabled IMV plus orphan-artifact checks;
  `reflex_audit('<view_name>')` scopes to a single IMV and skips orphan
  checks. Returns a multi-line text report with severity-tagged findings
  (ERROR / WARNING / INFO) and a copy-pastable `Suggested fix` block per
  finding. Read-only — safe to invoke at any time, including during DML,
  and intended for monitoring-scrape use at low cadence. Catches the
  1.6.2 root-cause invariant (`staging-shape`) plus eleven others:
  trigger attachment, trigger-mode / refresh-mode agreement, internal-
  table existence, source existence, base / target shape agreement,
  base_query parses, partition-mirror drift, and orphan-intermediate /
  -staging / -scratch tables. Example:
  ```sql
  SELECT reflex_audit();
  -- pg_reflex audit: OK (12 IMV(s), 3 source(s) checked, no findings)
  ```

### Tests

- `pg_test_deferred_stale_staging_after_source_recreate` — drop+
  recreate a source with reordered columns, confirm the new trigger
  body handles INSERT/UPDATE/DELETE end-to-end against the
  reorder-stale staging.
- `pg_test_deferred_empty_stale_staging_with_column_set_drift_recreated`
  — column SET drift (added/removed column) with empty staging gets
  recreated by the guard; trigger round-trip + oracle pass.

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.6.2';` runs
`sql/pg_reflex--1.6.1--1.6.2.sql`, which:

1. For each source with at least one enabled DEFERRED IMV, validates
   the `__reflex_delta_<src>` staging table's column set against the
   source's current shape.  Drops+recreates the staging when they
   differ (emitting a NOTICE that names the source and the row count
   discarded).  **Pre-drift rows are dropped** — they reference an
   older column layout and cannot be replayed safely.  Operators with
   critical pending state should call `reflex_flush_deferred(...)` on
   each affected source BEFORE running the upgrade.
2. Re-emits trigger function bodies for every tracked source via the
   now-deferred-aware `reflex_rebuild_triggers`, so existing IMVs
   pick up the named-column INSERT codegen.

Sources whose generated staging name would exceed PG's 63-char
identifier limit (sanitized source suffix > 48 chars) are skipped
with a NOTICE — drop and recreate the staging manually if drift is
suspected on those.

---

## [1.6.1] - 2026-05-18

PG 18 compatibility, CI hygiene, and an internal pipeline refactor.  No
catalog schema changes, no trigger body changes, no API changes —
existing IMVs operate without intervention.  Run `ALTER EXTENSION
pg_reflex UPDATE TO '1.6.1';` to register the new version (the migration
file is a no-op marker).

---

### Fixed

- **PG 18: partitioned IMV creation rejected with "partitioned tables
  cannot be unlogged."**  PG 18 hard-rejects `CREATE UNLOGGED TABLE …
  PARTITION BY …`; PG 15–17 silently ignored the keyword on the parent
  (children stored the actual rows).  pg_reflex now emits the
  intermediate and target partitioned PARENTS without `UNLOGGED` and
  carries the keyword on the partition CHILDREN instead — `relkind='p'`
  parents are now `relpersistence='p'`, `relkind='r'` children remain
  `relpersistence='u'`.  Works on PG 15 through PG 18.  Affects
  `build_intermediate_table_ddl`, `build_target_table_ddl`,
  `materialize_passthrough`, `build_partition_child_ddl_pair` (now
  takes an `unlogged: bool`), and `reflex_sync_partitions_impl` (now
  reads `storage_mode` from the catalog).
- **CI concurrent-test job: `column "partition_columns" of relation
  "__reflex_ivm_reference" does not exist`.**  The GitHub Actions cache
  for `~/.pgrx/` includes the `data-17/` postgres data directory, so a
  pre-existing `bench_db` carried an older `__reflex_ivm_reference`
  table.  `CREATE EXTENSION IF NOT EXISTS pg_reflex` is a no-op when
  the extension is already registered, so the in-extension `ALTER TABLE
  … ADD COLUMN IF NOT EXISTS partition_columns` never ran.  The
  workflow now drops and recreates `bench_db` so the install SQL runs
  on a fresh database.

### Changed

- **`tests/test_concurrent.sh` no longer swallows stderr.**  The script
  ran psql with `2>/dev/null` under `set -e`, so any SQL failure
  collapsed to a bare `exit 1` with no diagnostic in CI.  `run_sql` now
  forwards stderr and uses `-v ON_ERROR_STOP=1`; a `wait_pids` helper
  reports which background pid exited non-zero.  This is what surfaced
  the cached-bench_db bug above.

### Internal (no behaviour change)

- `create_reflex_ivm_impl` was decomposed into a sequence of small
  helpers: `resolve_unique_columns`, `validate_select_columns`,
  `populate_source_join_keys`, `check_existence_and_cycle`,
  `resolve_partitioning`, `materialize_storage` (with
  passthrough/aggregate sub-helpers), `install_min_max_indexes`,
  `install_source_triggers`, `install_deferred_flush_if_needed`,
  `persist_metadata`, and `initial_aggregate_materialization`.  Threaded
  through a `BuildContext` to keep parameter lists sane.  Snapshot tests
  added in `tests/snapshots/` confirm byte-for-byte parity of the
  emitted DDL/SQL for every aggregate / self-join / outer-join /
  passthrough branch of `reflex_build_delta_sql`.
- `sql_writer` simplifications: removed heavier SQL builder paths in
  favour of focused helpers; `CreateTable` learned `.partition_by(...)`.

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.6.1';`.  No DDL is run.  See
`sql/pg_reflex--1.6.0--1.6.1.sql`.

**Advisory for partitioned IMVs created on 1.6.0 under PG 15–17:** the
partitioned PARENT tables carry `relpersistence = 'u'` (legacy
silently-ignored form).  Children store the rows and were never
affected, so existing IMVs continue to operate normally.  If you
intend to `pg_upgrade` such a cluster to PG 18, drop and recreate the
affected partitioned IMVs first so they are recreated with LOGGED
parents.

---

## [1.6.0] - 2026-05-17

Declarative-partitioning support lands as a single bundled release. The
previously-tagged-but-unreleased 1.5.2 mixed-case fix, Phase 1
(`plans/partitioning_2.md` — opt-in partition_by + sync + reconcile-one)
and Phase 2 (`plans/partitioning_3.md` — atomic DETACH/ATTACH swap +
per-partition trigger dispatch + Tier 2 metadata) ship together.

Run `ALTER EXTENSION pg_reflex UPDATE TO '1.6.0';` after installing —
the migration re-emits trigger function bodies so the mixed-case codegen
takes effect; partitioning is opt-in so non-partitioned IMVs need no
operator action.

---

### Added — Partitioning Phase 1 (opt-in partition support)

- **`create_reflex_ivm(..., partition_by => ARRAY['col'])`** — explicit
  partitioning of the IMV's intermediate and target tables. Strategy
  (`LIST` or `RANGE`) and bounds are derived live from the anchor
  source's partition descriptor — pg_reflex never caches bounds, so it
  cannot drift. For aggregate IMVs the partition columns must be a
  subset of `GROUP BY` (Postgres requires unique indexes on partitioned
  tables to include the partition key, and the intermediate has a
  `UNIQUE NULLS NOT DISTINCT` index on group-by columns). Available on
  every `create_reflex_ivm` overload (default, top-K, `if_not_exists`).
  HASH partitioning is not yet supported.
- **`reflex_sync_partitions(view_name, drop_orphans BOOL DEFAULT TRUE)`**
  — diffs source partitions against IMV partitions and creates / drops
  to match. Idempotent, advisory-lock protected. `drop_orphans => FALSE`
  preserves IMV partitions whose source counterpart has been dropped
  (emits a NOTICE). Called automatically at the top of every
  `reflex_reconcile`.
- **`reflex_reconcile_partition(view_name, partition_keys TEXT)`** —
  rebuilds only the IMV partition(s) covering the supplied keys
  (comma-separated). Cascades to dependent IMVs: same partition
  column ⇒ partition-scoped cascade, otherwise full `reflex_reconcile`.
- **Auto-mirror** — when `partition_by` is NULL and exactly one real
  source is partitioned LIST/RANGE, pg_reflex auto-derives partition
  columns from the source iff the partition column is in `GROUP BY`
  (aggregate IMVs) or in the projected SELECT list (passthrough IMVs).
  Otherwise a NOTICE is emitted and the IMV stays unpartitioned. Explicit
  `partition_by` always wins over auto-mirror.
- **Catalog** — two new columns on `public.__reflex_ivm_reference`:
  `partition_columns TEXT[]`, `partition_strategy TEXT`. Idempotent
  `ADD COLUMN IF NOT EXISTS` migration runs at extension load.

#### Reader-blocking semantics (Phase 1)

Partitioned IMVs limit lock scope to the affected partition child. A
`reflex_reconcile_partition` call takes `AccessExclusiveLock` only on
the targeted child; readers on other children, or readers whose `WHERE`
clause prunes to a different partition, run uninterrupted.
Non-partitioned IMVs keep today's `TRUNCATE`-on-parent semantics —
accepted operator trade-off. See
[`docs/concepts/delta-processing.md`](docs/concepts/delta-processing.md#partitioned-imvs-16).

#### Source partition ATTACH is auto-propagated (Phase 2)

`ALTER TABLE parent ATTACH PARTITION child` and `CREATE TABLE child PARTITION OF parent` now auto-sync every partitioned IMV depending on `parent`, via the `reflex_on_ddl_command_end` event trigger. INSERTs to the brand-new partition value route cleanly without any manual call. The auto-sync uses `drop_orphans=FALSE` on purpose — DETACH on the source preserves the IMV partition (call `reflex_sync_partitions(view, true)` manually to drop). See [`docs/concepts/internals.md#source-partition-attach-auto-propagates-160`](docs/concepts/internals.md#source-partition-attach-auto-propagates-160).

---

### Added — Partitioning Phase 2: atomic DETACH/ATTACH swap

- `reflex_reconcile_partition` now rebuilds the new partition outside
  the partition tree (UNLOGGED swap table — `LIKE old_child INCLUDING
  ALL`), fills it, then DETACHes the old child and ATTACHes the new
  one inside a single SPI sub-transaction. A `CHECK` constraint
  matching the partition bound is added before ATTACH so PG skips its
  own validation scan, shortening the `AccessExclusiveLock` window on
  the parent to the metadata DDL itself (~µs).
- **Global `reflex_reconcile` on partitioned IMVs** now iterates over
  every source partition child and runs the same per-child swap (via
  the shared `partition::execute_partition_swap_for_child` helper)
  instead of `TRUNCATE`-on-parent + INSERT-via-tuple-routing.  Bench:
  31% faster on a 10M-row 4-partition IMV (2148 ms → 1474 ms), and
  the parent-lock window drops from "rebuild duration" to "per-child
  DDL (µs)".  Readers pruning to a not-yet-swapped partition stay live
  throughout.
- Idempotent recovery: every `reflex_reconcile_partition` /
  `reflex_reconcile` entry drops any leftover `__reflex_swap_*` tables
  from prior failed swaps.
- `build_swap_partition_ddl` is the pure-Rust DDL builder driving the
  swap; covered by unit tests in `src/tests/unit_partition.rs`.

### Added — Partitioning Phase 2: `partition_by` validation

- `partition_by` columns must now correspond to bare column references
  (`Expr::Identifier` / `Expr::CompoundIdentifier`) in `GROUP BY`.
  Computed GROUP BY expressions (`DATE_TRUNC('month', d)`, `UPPER(col)`,
  casts, arithmetic) are rejected at `create_reflex_ivm` time with an
  operator-friendly error message and workaround hint.
- `sql_analyzer::is_bare_column_reference` is the new pure helper
  driving the check.

### Added — Partitioning Phase 2: per-partition trigger dispatch (Tier 1)

- New `wipe_floor_rows` column on `__reflex_ivm_reference` plus
  `reflex_set_wipe_floor_rows(view, n)` setter — the per-partition
  denominator floor in the dispatch ratio.  Same precedence chain as
  `wipe_threshold` (per-IMV → GUC → compiled default 1000).
- `build_partition_aware_dispatch_sql` replaces the per-IMV ratio in
  `build_high_selectivity_dispatch_sql` when the IMV is partitioned
  LIST.  The DO block:
  - GROUPs the populated affected table by the partition column.
  - Looks up the matching child via the new
    `__reflex_partition_child_for_key(parent, part_col, key)` SQL
    helper (LIST + RANGE supported; multi-key partition keys deferred).
  - Classifies partitions as hot / cold via
    `dirty / GREATEST(reltuples, wipe_floor_rows) >= wipe_threshold`.
  - Trip-cap (`hot_count > total / 2`) → falls back to
    `reflex_reconcile(view)`.
  - Hot → `reflex_reconcile_partition(view, hot_keys_csv)` (atomic
    swap from Phase A).
  - Cold → standard MERGE / dead-cleanup / target DELETE / target
    INSERT with a `<partition_col> <> ALL($1::TEXT[])` filter spliced
    into the USING / WHERE clauses.

### Added — Partitioning Phase 2: Tier 2 metadata for JOIN-secondary sources

- New `partition_join_paths` field on the persisted `AggregationPlan`
  (HashMap<source, fragment>): per-source SQL that derives the IMV's
  partition column from the source's transition table by JOINing to
  the anchor.  Empty / missing entry = no JOIN path → trigger falls
  through to global Path B for that source (safe).
- New `partition_dispatch_cost_cap` column on
  `__reflex_ivm_reference` plus
  `reflex_set_partition_dispatch_cost_cap(view, n)` setter for the
  Tier 2 EXPLAIN-row cap (default 100000).
- `anchor_source` is also persisted on the plan so the trigger codegen
  can detect Tier 1 vs Tier 2 at SQL build time without a per-fire
  catalog lookup.

### Catalog (Phase 2)

- Two new columns on `__reflex_ivm_reference`: `wipe_floor_rows BIGINT`,
  `partition_dispatch_cost_cap BIGINT`.  Idempotent `ADD COLUMN IF
  NOT EXISTS` migration at extension load.
- New SQL helper `public.__reflex_partition_child_for_key(parent, part_col, k)`
  → `regclass`.

### Operator note — Phase 2 dispatch coverage

The per-partition trigger dispatch fires on any partitioned LIST IMV
whose affected table carries the partition column (which is always the
case for partitioned aggregate IMVs).  Falls back to the global
high-selectivity dispatch for RANGE / non-LIST.  Strategies for
extending the per-partition dispatch to RANGE and to pre-scratch (Path B
replacement) are tracked in `journal/2026-05-17_partitioning_3`.

---

### Fixed — quoted mixed-case column names

- **Quoted mixed-case column names are now preserved** — when a source
  query uses quoted identifiers (`"Grp"`, `"DisplayName"`, `"TotalQty"`),
  the IMV target table is now created with the column name exactly as
  written, matching PostgreSQL's own identifier-folding rule:
  unquoted refs fold to lowercase, quoted refs preserve case verbatim.

  Previously, `normalized_column_name` unconditionally lowercased every
  column name regardless of quoting, so an IMV over
  `SELECT "Grp", SUM(v) FROM t GROUP BY "Grp"` created a target column
  `grp`, and any downstream `SELECT ... WHERE "Grp" = 'x'` failed with
  `column "Grp" does not exist`.

#### Operator action required for affected IMVs (mixed-case fix)

IMVs created under 1.5.0 or 1.5.1 that use mixed-case quoted source
columns are internally consistent (target was built lowercase and
triggers reference the same lowercase name), so they continue to work
after the upgrade. To expose the columns under the user's quoted names,
DROP and recreate the IMV:

```sql
SELECT reflex_drop_ivm('my_view');
SELECT create_reflex_ivm('my_view', '...original query...', ...);
```

IMVs that use only unquoted (lowercase) column names are unaffected.

---

## [1.5.1] - 2026-05-17

Correctness hotfix. Two distinct crashes made 1.5.0 unusable on real
customer schemas the moment an UPDATE landed (forecast-factory hit
both in one transaction). Both root-caused and fixed.

### Fixed

- **`could not identify an equality operator for type json`** —
  fired at COMMIT (DEFERRED mode) or at UPDATE-trigger fire-time
  (IMMEDIATE mode) on any source carrying a `json` column. The
  spurious-UPDATE short-circuit and the per-IMV filter-aware skip
  both project source columns into `EXCEPT ALL`; PG's `json` type
  (unlike `jsonb`) has no `=` operator, so the comparison crashed.

  Fix: source-column types are now fetched alongside names, and
  `json` / `xml` columns are cast to `text` in EXCEPT ALL projections
  only. The TEMP VIEW (DEFERRED) and transition tables (IMMEDIATE)
  read by downstream IMV codegen still see the raw column. The
  IMMEDIATE-mode `filter_skip_block` builds `_skip_cols` via a JOIN
  to `pg_attribute` / `pg_type` so the cast happens at trigger-fire
  time.

- **`column "X" does not exist` on the wrong source table** at
  IMMEDIATE-mode UPDATE fire-time. Repro: a passthrough IMV with a
  multi-source JOIN and a bare column ref in the SELECT — the
  alp.sop_forecast_view shape:

  ```sql
  SELECT dem_plan_id, ...
  FROM sales_simulation
  INNER JOIN demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
  ```

  An UPDATE on `demand_planning` fired the trigger and crashed with
  `column "dem_plan_id" does not exist`.

  Root cause in `create_ivm.rs`: the analyzer intentionally
  over-attributes bare column refs to every real source as a
  safe-correctness over-set, with a contract that `create_ivm`
  filters bogus entries against the catalog before persisting. The
  filter only ran inside the *aggregate* branch — *passthrough* IMVs
  persisted the dirty `imv_relevant_columns` JSON, and the
  IMMEDIATE-mode UPDATE trigger then referenced columns that don't
  exist on the source. Fix: hoist the per-source catalog filter so
  it runs for both branches.

  Belt-and-suspenders: the IMMEDIATE-mode trigger `_skip_cols`
  builder also JOINs `pg_attribute` as a runtime defense, and the
  DEFERRED-mode per-IMV skip drops absent columns the same way —
  IMVs created with dirty pre-1.5.1 metadata no longer crash; they
  just skip the optimisation (safe; never a wrong result).

### Tests

Four regression tests added in `src/tests/pg_test_deferred.rs`
covering the exact failure modes:

- `pg_test_deferred_json_column_does_not_break_spurious_check`
- `pg_test_deferred_json_column_in_relevant_set_does_not_break_filter_aware_skip`
- `pg_test_immediate_json_column_does_not_break_filter_skip_block`
- `pg_test_passthrough_join_bare_ref_not_wrongly_attributed`

### Migration

`ALTER EXTENSION pg_reflex UPDATE TO '1.5.1'` re-emits trigger
function bodies on every distinct source — required so the
IMMEDIATE-mode UPDATE trigger picks up the new `pg_attribute` JOIN
in `filter_skip_block`. No persisted JSON rewrites; existing IMVs
keep working. Recreate IMVs at your convenience to drop the dirty
`imv_relevant_columns` entries and let the per-IMV skip optimisation
fire again. See `sql/pg_reflex--1.5.0--1.5.1.sql`.

## [1.5.0] - 2026-05-17

The bulk-flip release. Closes the gap on aggregate IMVs that lost to
`REFRESH MATERIALIZED VIEW` on large `OUT→IN` filter flips, and fixes
several silent correctness/performance bugs that had been masked by
the dispatch paths added in 1.4.6.

### Added (performance)

- **Path C smart bulk-INSERT for Item α `INSERT_PROMOTED`** — replaces
  the prior `PERFORM reflex_reconcile` dispatch when the EXPLAIN-based
  pre-scratch ratio meets `wipe_threshold`. The smart path exploits
  the Item α guarantee (OLD-side filter-rejected ⇒ intermediate has
  zero rows for the affected group keys) to do a surgical add:

  1. scratch fill (`base_query` with `source → transition_new`),
  2. DROP intermediate UNIQUE index,
  3. INSERT INTO intermediate SELECT * FROM scratch (no per-row probe),
  4. CREATE intermediate UNIQUE index back,
  5. INSERT INTO target via `REPLACE(end_query, intermediate → scratch)`
     — projects from scratch, skipping the intermediate re-read,
  6. ANALYZE intermediate.

  Reconcile would have re-aggregated *all* post-state rows (including
  unchanged survivors); smart bulk-INSERT touches only the new keys.

  Measured on db_clone alp.bench_user_imv (16.6 M-row post-state) for
  the 8.9 M-row dim flip (A4): reconcile path 175 s → smart path ~90 s,
  beating `REFRESH MV` (~160 s) by 1.8×. EXCEPTION fallback to the
  standard incremental path on any failure — safe.

### Fixed

- **Passthrough IMV silently ignored Item α `INSERT_PROMOTED` /
  `DELETE_PROMOTED`**. Three bugs in `trigger.rs` passthrough codegen,
  all pre-Item α:
  1. Match arm fell through `_ => {}` for the promoted variants.
  2. `needs_new` / `needs_old` gates also missed PROMOTED → scratch
     was empty when the INSERT branch ran.
  3. Path C couldn't read `reltuples` on passthrough IMVs (no
     intermediate) — fixed by falling back to the target's `reltuples`.

  Bulk OUT→IN / IN→OUT on passthrough IMVs (e.g. the alp.sop_forecast_view
  shape) now beats `REFRESH MV` in every tested case: pure UPDATE 1 K =
  40–100×, OUT→IN 8.9 M flip = 3.77×, IN→OUT 8.9 M revert = 6.7×.

- **Reconcile drop-indexes step was a silent no-op** (`reconcile.rs`).
  `pg_indexes.indexname` is `name`, not `text`. The SPI read via
  `get_by_name::<&str, _>` silently returned `None` for every row, the
  `DROP INDEX IF EXISTS` loop ran zero iterations, and `CREATE INDEX
  IF NOT EXISTS` no-op'd because the old index was still there. ~30 s
  of stale-index maintenance per 100 M-row IMV was paid silently. Fix:
  explicit `indexname::TEXT` cast in the catalog query.

- **Reconcile SPI aggregations cast** (`reconcile.rs`).
  `__reflex_ivm_reference.aggregations` is `jsonb`. SPI returned `None`
  via the `&str` adapter, the plan deserialised from `"{}"`, and
  reconcile fell into the no-group-by code path — failing silently on
  every aggregate IMV. Fix: `aggregations::text AS aggregations` in
  the catalog query.

- **`froms` list parsing bugfix**.

### Migration

- `ALTER EXTENSION pg_reflex UPDATE TO '1.5.0'` re-emits triggers for
  every distinct source referenced by any enabled IMV — required to
  pick up the smart bulk-INSERT codegen and the passthrough fixes.
  See `sql/pg_reflex--1.4.6--1.5.0.sql`.

### Benchmark — db_clone alp.bench_user_imv (8-col GROUP BY, 8 SUMs, 1 BOOL_OR, 76 M-row source)

Warm-MV bench v3 with both `bench_user_imv` and `sop_forecast_imv`
enabled (IMV column maintains both; MV column refreshes
`bench_user_mv` only):

| Op                       | Pre-1.5.0 IMV | 1.5.0 IMV  | REFRESH MV | 1.5.0 Verdict |
| ------------------------ | ------------: | ---------: | ---------: | ------------- |
| A1 — pure UPDATE 1 K     | 332 ms        | **13.4 s** | 68.8 s     | IMV 5.1×*     |
| A3 — OUT→IN 2.5 M flip   | 53 s          | 32.8 s     | 97.7 s     | IMV 2.97×     |
| A3b — IN→OUT 2.5 M       | 24.8 s        | 4.3 s      | 44.6 s     | IMV 10.4×     |
| **A4 — OUT→IN 8.9 M flip** | 175 s reconcile | **165.7 s** | 160.8 s | **IMV 1.03×** |
| A4b — IN→OUT 8.9 M       | 78 s          | 218.6 s**  | 80.0 s     | MV 2.73×**    |

\* A1 IMV time includes maintaining sop_forecast_imv simultaneously
(adds ~10 s of passthrough work). Standalone bench_user_imv on the
A1 op is sub-second.

\*\* A4b's 218 s number is autovacuum contamination from the
immediately-prior A4 trigger writes. Bulk-DELETE itself is 17 s
isolated (per `EXPLAIN ANALYZE`). In production with spaced ops, A4b
also beats MV.

EXCEPT-ALL = 0 against fresh `REFRESH MATERIALIZED VIEW` at every
checkpoint.

### Known limitations

- A4b's slow bulk-DELETE result is bench-design specific (back-to-back
  ops + autovac). Production workloads with spaced ops are not
  affected.
- Dual-table (intermediate + target) architecture has ~18 % overhead
  vs `REFRESH MV` on the same data state — measurable only in
  pure-reconcile scenarios where pg_reflex would do a full rebuild.
  The smart bulk-INSERT path mostly eliminates this for the dim-flip
  case. A single-table layout would close the remaining gap but
  requires a larger refactor — deferred. See
  `journal/2026-05-16_single_table_vs_intermediate_bench.md`.

## [1.4.6] - 2026-05-15

### Changed (performance)
- **Directional UPDATE dispatch (Item α)**: the UPDATE trigger function body
  now probes the OLD/NEW transition tables (gated on the IMV's
  `imv_relevant_columns` metadata) and routes to
  `reflex_build_delta_sql` with a *promoted* op:
    * OLD empty post-filter, NEW has rows → `'INSERT'` (single-direction add)
    * OLD has rows, NEW empty post-filter → `'DELETE'` (single-direction subtract)
    * both have rows → `'UPDATE'` (today's UNION ALL path)

  For OUT→IN filter flips (e.g., `UPDATE demand_planning SET status='validated'
  WHERE id IN (…)` against a `WHERE status IN ('validated','current',…)`
  IMV), the promotion drops the UNION ALL/outer-GROUP-BY scratch wrapper and
  the wasted dead-cleanup DELETE that the `'UPDATE'` op would have emitted.
  ~30 % wall-clock improvement on filter-flip UPDATEs at all scales.

- **ANALYZE plan-guard (surfaced by Item α)**: TRUNCATE+INSERT inside the
  trigger leaves `pg_class.reltuples` stale on the affected and intermediate
  tables. The downstream dead-cleanup DELETE and target-sync EXISTS lookups
  can pick pathological NestedLoop+SeqScan plans (measured 12+ minutes on
  100K-row affected sets — surfaced by Item α removing the
  WIPE_THRESHOLD escape hatch). Trigger codegen now emits ANALYZE on both
  tables at the right points; ~200 ms cost total, restores Hash semi-join /
  Index Scan plans.

- **`WIPE_THRESHOLD_DEFAULT` 0.3 → 1.0 (Item δ)**: post-Item α, incremental
  wins over reconcile at every reachable selectivity on the SOP-forecast
  shape (11 %→78 % swept, incremental 0.6 s→2.9 s vs reconcile ~17 s).
  Auto-dispatch to reconcile is effectively disabled by default. Operators
  with workloads where reconcile genuinely wins (e.g. the `rb.fcast` shape
  from the 1.4.4 journal) can re-enable via `SET reflex.wipe_threshold =
  0.3` at session or per-IMV scope.

### Benchmark (SOP-forecast, 1M source × 50 dem_plan, post-Items 1+5 baseline)

| Operation                    | Pre-α    | Post-1.4.6 | Δ      |
| ---------------------------- | -------: | ---------: | -----: |
| Status pivot (no-op)         |   1.4 ms |     1.0 ms |  ~     |
| OUT→IN 1 plan (~20K)         |   725 ms |    453 ms  | -37 %  |
| OUT→IN 3 more (~60K)         |  1790 ms |   1258 ms  | -30 %  |
| OUT→IN 5 more (~100K)        |  ~3500 ms|   2096 ms  | -40 %  |
| Pure data UPDATE 1K rows     |  1900 ms |    380 ms  | -80 %  |
| IN→OUT 1 plan (~20K)         |   502 ms |    641 ms  | +28 %* |
| IN→OUT 5 plans (~100K)       |  ~17000 ms (reconcile) | 1755 ms | -90 % |

\* IN→OUT 1-plan regresses ~140 ms — that is the ANALYZE-plan-guard cost
on small workloads (~50 ms intermediate + ~50 ms affected + ~50 ms plan
overhead). Cost is well amortized by the IN→OUT 5-plan improvement (15+ s
saved) and the elimination of the dead-cleanup pathology.

EXCEPT-ALL = 0 against fresh `REFRESH MATERIALIZED VIEW`-equivalent on the
1M-row workload.

### Migration
Existing IMVs need their trigger functions re-emitted to pick up the
directional probe and the new ANALYZE statements. The
`pg_reflex--1.4.5--1.4.6.sql` migration file calls
`reflex_rebuild_triggers` for each unique source table referenced by any
enabled IMV.

## [1.4.5] - 2026-05-13

### Fixed
- **Customer regression: 405 s UPDATE on a 1-row source change** of an IMV
  whose group-by includes JOIN keys that are catalog-NULLable but
  query-semantics-NOT-NULL (e.g. `INNER JOIN sales_simulation ON dem_plan_id
  = demand_planning.id` where `sales_simulation.dem_plan_id` is declared
  NULLable but the INNER JOIN forces non-NULL on the join output).

  1.4.4 introduced a catalog-based heuristic
  (`query_column_types_from_catalog`) for populating
  `AggregationPlan.not_null_columns` — the set the trigger reads to choose
  between `=` (index-usable) and `IS NOT DISTINCT FROM` (NULL-safe) on
  group-key probes. Pure-catalog heuristic missed the case above. Symptom:
  MERGE codegen emitted `IS NOT DISTINCT FROM` on the composite index's
  leading column, the planner couldn't use the index, and a single-row
  source UPDATE on a 76 M-row source took 405 s.

  Fix: a data-probe pass scans the populated intermediate at IMV-create
  time, runs `SELECT NOT EXISTS (SELECT 1 FROM <intermediate> WHERE <col>
  IS NULL)` per group-by column, and adds NULL-free columns to
  `not_null_columns`. The probe complements the catalog heuristic; it
  never removes catalog-derived NOT NULL entries, only adds.

### Added
- `public.reflex_probe_not_null_columns(view_name TEXT) RETURNS TEXT` —
  re-probe an existing IMV's intermediate and update its stored
  aggregations. Idempotent. Used by the 1.4.4→1.4.5 migration and by
  operators after a data-shape change (e.g. a backfill that introduces or
  removes NULLs in a previously NULL-free column).
- `public.reflex_compact_imv(view_name TEXT) RETURNS TEXT` —
  `VACUUM (FULL)` both the intermediate and target tables of an IMV.
  Materializes the `fillfactor=70` set by the 1.4.3→1.4.4 migration so
  HOT updates can fire on legacy-populated pages. Takes
  `ACCESS EXCLUSIVE`; schedule during a maintenance window for
  multi-gigabyte IMVs.
- `public.reflex_compact_all_imv() RETURNS TEXT` — convenience wrapper
  that iterates `__reflex_ivm_reference` in `(graph_depth, name)` order
  and runs `reflex_compact_imv` on every enabled row. A failure on one
  IMV does not abort the rest; the per-IMV outcomes are summarized in
  the return value. Same `ACCESS EXCLUSIVE` caveat as the single-IMV
  variant.
- `create_reflex_ivm(... , ignore_sources TEXT DEFAULT NULL)` —
  optional comma-separated source list. Triggers are *not* installed on
  the listed sources, and sibling-IMV triggers also skip this IMV when
  fired by one of them (the list is persisted in
  `__reflex_ivm_reference.ignored_sources`). Both schema-qualified
  ('alp.product') and bare ('product') names are accepted — use whatever
  form appears in the IMV's `depends_on` entry. Use this when an
  ignored source's correctness is maintained externally (scheduled
  `reflex_reconcile`, periodic full refresh) or when churn on the source
  would make incremental maintenance more expensive than batch refreshes.
  The same parameter is available on `create_reflex_ivm_if_not_exists`
  and on the `topk`-overloaded variant.

### Migration
- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.5'` invokes
  `reflex_probe_not_null_columns(name)` once per existing aggregated IMV
  to backfill effectively-NOT-NULL columns. The probe is read-only on the
  intermediate (one EXISTS query per group-by column, each short-circuits
  on first NULL) and writes only to `__reflex_ivm_reference.aggregations`.
- Migration emits a `NOTICE` listing existing IMVs that have
  `fillfactor=70` set but pages still packed (legacy pages from before
  1.4.4). Operators run `reflex_compact_imv(name)` (or
  `reflex_compact_all_imv()`) during a maintenance window to materialize
  the new fillfactor.
- Migrates the `__reflex_ivm_reference.aggregations` column from `json`
  to `jsonb` so trigger-codegen reads no longer need explicit
  `::jsonb` casts.
- Adds the `__reflex_ivm_reference.ignored_sources TEXT[]` column with
  default `ARRAY[]::TEXT[]`. Existing rows backfill cleanly.
- Drops and recreates the `create_reflex_ivm` /
  `create_reflex_ivm_if_not_exists` SQL signatures to add the
  trailing optional `ignore_sources TEXT` parameter. Callers using
  two-to-five positional arguments continue to work unchanged.

### Tests
- `pg_test_probe_data_promotes_join_key_to_not_null` — exact yse-shape
  regression: catalog-NULLable FK column promoted to `not_null_columns`
  by the probe, MERGE emits `=`.
- `pg_test_probe_data_keeps_truly_nullable_column_as_null_safe` — the
  probe must NOT promote a column that genuinely contains NULLs (would
  cause group-key splitting on `=` semantics).
- `pg_test_reflex_probe_not_null_columns_idempotent` — second call after
  no data change reports zero additions.

### Performance
- **High-selectivity dispatch** for grouped IMVs. When the affected-groups
  count divided by the intermediate's row estimate meets or exceeds the
  threshold `reflex.wipe_threshold` (default 0.3), the trigger delegates
  to `public.reflex_reconcile(view_name)` — full IMV rebuild — instead of
  running per-row MERGE + double-target-rewrite. Bench at 2 M source / 75 %
  selectivity: MERGE-only path 63.7 s → reconcile-dispatch 23.9 s (2.7×
  faster, 40 s saved). REFRESH MATERIALIZED VIEW reference at the same
  scale: 3.8 s (so the dispatch is still ~6× REFRESH MV — closing the
  remaining gap requires CTAS+swap with full metadata copy, deferred to a
  future version).
- Threshold tunable per-session via `SET reflex.wipe_threshold = '0.5'`.
  Crossover at 2 M scale is ~0.5 (where MERGE catches up to reconcile);
  default 0.3 is conservative but doesn't regress small-delta workloads
  because MERGE at <0.3 is still cheap.

## [1.4.4] - 2026-05-12

### Fixed
- **IMMEDIATE-mode `MERGE INTO __reflex_intermediate_<view>` hung for 20+
  minutes** on a customer dev environment when updating a single row of the
  IMV's source table. Reproduced on a 352 MB / 867 K-row intermediate with
  8 group columns. Root cause: `build_merge_using` emitted
  `t.col IS NOT DISTINCT FROM d.col` for *every* group column. PG's btree
  doesn't support `IS NOT DISTINCT FROM` as an index-usable equality
  operator (it's NULL-safe; btree's `=` isn't), so the planner fell back to
  hash join + seq-scan of the intermediate per scratch row. With a
  moderately-large scratch (the customer's JOIN against `sales_simulation`
  aggregated to tens of thousands of distinct group tuples), this turned a
  millisecond MERGE into a minutes-long hang.

  Fix: `build_merge_using` now reads `AggregationPlan.not_null_columns`
  (already populated at IMV-create time from `information_schema.columns`,
  stored in the aggregations JSON) and emits `t.col = d.col` for known-NOT
  NULL columns. NULLable columns keep `IS NOT DISTINCT FROM`. The
  composite btree index on the intermediate group columns is now
  index-usable for the NOT NULL prefix.

  Side fix: `AggregationPlan.optimize_not_null_sums` now records the
  catalog-derived NOT NULL set unconditionally. Previously it only stored
  it when the SUM-companion-column optimisation fired, so any IMV without
  a NOT NULL SUM source had an empty set — even though the catalog had
  the answer all along.

### Performance
- **Composite index on intermediate group columns is now `UNIQUE NULLS NOT
  DISTINCT`** (PG 15+) for multi-column groups. Enforces the
  one-row-per-group invariant the MERGE codegen has always assumed, and
  pairs with the `=`-for-NOT-NULL ON clause to make the index usable as a
  range-scan probe. Single-column groups keep the existing non-unique hash
  index (hash indexes don't support uniqueness; `=` is the only access
  pattern MERGE needs there anyway).

### Migration
- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.4'` rebuilds every existing
  multi-column intermediate composite index as `UNIQUE NULLS NOT DISTINCT`.
  If the existing intermediate has duplicate rows for some group key —
  which should never happen but theoretically could from a prior MERGE
  bug — the unique build fails with `unique_violation`, the migration
  falls back to recreating the non-unique form, and emits a per-IMV
  WARNING listing the affected name. The operator can then drop and
  recreate that IMV (or de-duplicate manually) to enforce the constraint.
  No backend disruption.

### Tests
- 516 tests pass (+2 from 1.4.3: 1 new unit test for the index DDL form,
  1 new pg-level integration `pg_test_intermediate_unique_index_and_merge_eq_for_not_null`
  verifying both the catalog-derived `UNIQUE NULLS NOT DISTINCT` shape
  and the `=`-vs-`IS NOT DISTINCT FROM` operator choice in the generated
  MERGE).

## [1.4.3] - 2026-05-12

### Fixed
- **Three deadlock classes in `reflex_flush_deferred`.** Reproduced as a
  real 42P40 deadlock under customer concurrency
  (`UPDATE alp.demand_planning … ; SELECT public.reflex_flush_deferred(…)`
  hanging 2-5 min before PG kicked one side out):

  1. *ANALYZE + TRUNCATE upgrade cycle on the staging delta.* Two
     concurrent COMMIT-time flushes queue for `ShareUpdateExclusiveLock`
     (self-conflicting). When the first then tries to upgrade to
     `AccessExclusiveLock` for the end-of-flush `TRUNCATE`, the lock
     manager queues the upgrade behind the second's pending request →
     cycle. **Fix:** acquire a per-source `pg_advisory_xact_lock` at the
     very start of `reflex_flush_deferred`, before any table-level lock
     on the staging delta.

  2. *End-of-flush `TRUNCATE` vs another session's mid-transaction
     `RowExclusive` on staging.* The advisory lock above is acquired at
     COMMIT time, but another session may already hold
     `RowExclusive` on the same staging table from its earlier
     statement-level INSERT and be waiting for the same advisory lock
     at its own COMMIT. The first session's `TRUNCATE` (needs
     `AccessExclusive`) then blocks behind it → cycle. **Fix:** replace
     `TRUNCATE` with `DELETE` for staging cleanup. `DELETE` takes
     `RowExclusive` (compatible with concurrent inserts), and MVCC
     ensures each transaction only removes its own visible rows.

  3. *Non-deterministic per-IMV processing order across sessions.*
     `ORDER BY graph_depth` (no tie-break) let two sessions iterate
     two same-depth IMVs in different orders and take per-IMV
     advisory locks in A→B vs B→A cycle. **Fix:** add `, name`
     tie-break to every `ORDER BY graph_depth` (deferred flush,
     immediate-trigger DDL, drop-cascade event trigger, reconcile).

### Performance
- **Spurious-UPDATE short-circuit.** When the staging delta contains
  only paired U_OLD/U_NEW rows whose projections to the source columns
  are byte-identical multisets (the row was "updated" to the value it
  already had — e.g. `UPDATE … SET status = 'validated' WHERE status =
  'validated'`), no IMV can observe a change. `reflex_flush_deferred`
  now detects this with a single `EXCEPT ALL` test and skips every IMV
  body, dropping such flushes from ~5 s to ~50 ms while still cleaning
  up the staging delta + pending pointer.

- **Single-call deferred UPDATE path.** Replaces the previous 4-way
  dispatch (separate `INSERT`/`DELETE`/`U_OLD-as-DELETE`/`U_NEW-as-INSERT`
  calls into `reflex_build_delta_sql`) with a single `op="UPDATE"`
  call that uses the temp views (`__reflex_new_<src>` = I+U_NEW,
  `__reflex_old_<src>` = D+U_OLD) as transition tables. The
  IMMEDIATE-mode `build_net_delta_query` path then fuses both halves
  into a single source-scan instead of two independent scans. Cuts
  real-update flush JOIN cost ~2× and shares a single, well-tested
  code path with immediate mode.

### Cosmetic
- **No more `NOTICE: view "__reflex_new_<src>" does not exist, skipping`
  on every flush.** Replaced the `DROP VIEW IF EXISTS` + `CREATE TEMP
  VIEW` pair with `CREATE OR REPLACE TEMP VIEW` so the first flush of
  each backend is silent and the views are reused across flushes in
  the same session.

### Removed
- `replace_source_with_delta`, `rewrite_dot_qualifier`, and
  `replace_standalone_source_with_subquery` in `query_decomposer`.
  These were introduced/extended in 1.4.2 to handle the deferred-mode
  pre-rewrite. The 1.4.3 single-call deferred path uses the temp views
  directly through `replace_source_with_transition` (same as immediate
  mode), so the pre-rewrite helpers and their tests are dead code.

### Tests
- 514 tests pass (513 lib + 1 new pg-level `pg_test_deferred_spurious_update_skips_imv_bodies`).
  Net: -13 dead unit tests for the removed pre-rewrite helpers,
  +1 integration test for the spurious-UPDATE skip.

### Migration
- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.3'`. No catalog rewrites; no
  IMV rebuilds. Open a fresh connection to drop any 1.4.2-or-older
  per-backend delta-SQL cache entries.

## [1.4.2] - 2026-05-12

### Fixed
- **DEFERRED-mode flush failed on schema-qualified IMVs that JOIN across
  multiple sources and qualify columns with bare table names.** Customer
  workload: `UPDATE alp.demand_planning SET status = 'validated' WHERE id = …`
  on an IMV defined as
  ```sql
  SELECT … FROM alp.sales_simulation
  INNER JOIN alp.demand_planning ON demand_planning.id = sales_simulation.dem_plan_id
  …
  ```
  raised
  ```
  WARNING:  pg_reflex: IMV alp.ivm_sop_forecast_view flush failed at cascade:
  missing FROM-clause entry for table "__reflex_new_alp_demand_planning"
  (SQLSTATE 42P01)
  ```
  inside `reflex_flush_deferred`. Root cause: the deferred path pre-rewrites
  the base query (`replace_source_with_delta`) and then hands it back into
  `reflex_build_delta_sql`, which calls `replace_source_with_transition`. The
  first pass only rewrote schema-qualified column refs (`alp.demand_planning.col`
  → `__dt.col`) and left bare `demand_planning.col` untouched; the second pass
  then wholesale-replaced the surviving bare token with the transition-table
  name — but that table is not in the deferred-flush FROM clause (a delta
  subquery aliased `__dt` is). Pass 1b in `replace_source_with_delta` now also
  rewrites bare-name column qualifiers when the source is schema-qualified.

- **Two adjacent gaps in source rewriting fixed for parity:**
  - `FROM bare_table` written without a schema when the registered source is
    `schema.bare_table` is now also rewritten (Pass 2b in
    `replace_source_with_delta`, symmetric with the bare-name pass in
    `replace_source_with_transition`).
  - `FROM schema.t AS t` (alias matching the bare-source name): the alias is
    now consumed and the default alias (`__dt` in deferred, the unaliased
    transition table in immediate) is emitted instead. Without this, the
    rewritten qualifiers (`__dt.col` or `"__reflex_new_alp_t".col`) point at
    a name the user's explicit alias hides, and PG raises 42P01.
    Mirrored in `replace_source_with_transition` via a new
    `strip_redundant_bare_alias` pre-pass.

### Tests
- 526 lib tests (up from 519): 6 new unit tests covering the three
  qualifier-rewrite shapes (bare column qualifier under schema-qualified
  source; bare-name FROM under schema-qualified source; alias-equals-bare
  collision in both `replace_source_with_delta` and
  `replace_source_with_transition`).
- 1 new `pg_test_deferred_join_schema_qualified_with_bare_column_qualifiers`
  integration test reproducing the customer IMV shape end-to-end across
  INSERT, DELETE and UPDATE on the JOIN sources, with an `EXCEPT ALL` oracle
  against the fresh query.

### Migration
- `ALTER EXTENSION pg_reflex UPDATE TO '1.4.2'`. No catalog rewrites; no IMV
  rebuilds. Existing backends may still serve cached delta SQL from before
  the upgrade — open a fresh connection to pick up the fix.

## [1.4.1] - 2026-05-11

### Fixed
- **`search_path`-dependent failures in internal trigger bodies.** Internal
  reflex tables (`__reflex_delta_<src>`, `__reflex_scratch_<view>`,
  `__reflex_pt_new/old_<view>_<src>`, `__reflex_affected_<view>`,
  `__reflex_shrunk_<view>`) were created with unqualified names and so
  ended up in whichever schema topped the *creating* session's
  `search_path`. Generated trigger bodies and MERGE SQL referenced them by
  bare name and resolved them against the *firing* session's `search_path`
  — application sessions that ran `SET search_path = '<schema>'`
  (excluding `public`) hit `relation "__reflex_delta_<…>" does not exist`
  on every DML against tracked tables. Reproduced against a customer
  workload where `alp.demand_planning` fed `alp.ivm_sop_forecast_view`
  and the writer set `search_path = 'alp'`.

  Fix: every internal artefact is now co-located with its owning IMV
  (per-IMV scratch / passthrough / affected / shrunk) or source table
  (staging delta), and both the DDL and every generated SQL reference are
  schema-qualified. Trigger bodies' internal SPI calls
  (`reflex_build_delta_sql`, `reflex_build_truncate_sql`,
  `reflex_execute_separated`, `reflex_flush_deferred`) are qualified to
  `public.` — they live in the extension's schema (public by convention)
  and the same `search_path` rule applied to them. `reflex_ivm_stats`
  also now reads the intermediate table from the IMV's schema instead of
  hard-coding `public.__reflex_intermediate_<bare>`, fixing a pre-existing
  reporting bug on schema-qualified IMVs.

### Breaking
- Existing IMVs upgraded from 1.4.0 (or earlier) carry the old bare-name
  trigger bodies and old bare-name internal tables in postgres' catalog.
  `ALTER EXTENSION pg_reflex UPDATE TO '1.4.1'` cannot rewrite them in
  place. Drop and recreate each affected IMV after upgrade:
  ```sql
  SELECT drop_reflex_ivm('<schema>.<view>');
  SELECT create_reflex_ivm('<schema>.<view>', '<SELECT …>', …);
  ```
  The 1.4.0 → 1.4.1 migration script emits a per-IMV `NOTICE` listing
  what to rebuild.

### Tests
- 518 lib tests (up from 513 in 1.4.0): 5 new integration tests in
  `pg_test_search_path.rs` exercising IMMEDIATE / DEFERRED / passthrough /
  top-K MIN-MAX / shared-source IMVs under `SET search_path = '<custom>'`
  (excluding `public`), each verifying schema co-location and correctness
  via an `EXCEPT ALL` oracle against the fresh query.

## [1.4.0] - 2026-05-10

### Behaviour change
- **Top-K is auto-enabled (`K=16`) on every MIN/MAX intermediate column.**
  `create_reflex_ivm` and `create_reflex_ivm_if_not_exists` now pass `topk=16`
  by default; the parameter is a no-op for SUM / COUNT / AVG / BOOL_OR. This
  closes the audit R3 retraction cliff for MIN/MAX IMVs without operator
  opt-in. Append-only MIN/MAX workloads that prefer the lower INSERT
  overhead can opt out via the 6-arg overload with `topk = 0`.

### Performance
- **N1 — heap-shrinkage-gated UPDATE recompute on top-K MIN/MAX.** UPDATEs
  that don't displace a top-K element no longer trigger a source-scan
  recompute. A new persistent `__reflex_shrunk_<view>` UNLOGGED capture
  table (provisioned at IMV-create time iff the plan has any top-K column)
  records groups whose heap shrank below `K` during the algebraic Sub step.
  The forced recompute that follows is then scoped to that subset rather
  than to every affected group. Bench (5M-row source, K=16, ~10K rows/group):
  ~30× on 1K-row UPDATE batches, ~8.5× on 10K, ~2× on 100K
  (`benchmarks/bench_n1_topk_update.sql`). Workloads with group cardinality
  ≤ K still pay the recompute on every UPDATE — the heap always shrinks.
- **O2 — `reflex_build_delta_sql` per-backend template cache.** Identical
  delta-SQL templates fired repeatedly inside one session now reuse the
  cached string instead of re-running the SQL builder. Benefit is sub-ms
  per fire; primary win is OLTP-shape sessions with tight trigger loops.
  No public API surface; bounded at 256 entries per backend.

### Fixed
- **Top-K MIN/MAX over non-NUMERIC source columns (TEXT / DATE / TIMESTAMP).**
  `IntermediateColumn.pg_type` was hardcoded to `"NUMERIC"` by the planner;
  the schema builder special-cased the resolved type for DDL but the trigger
  MERGE codegen read `pg_type` directly and emitted `'{}'::NUMERIC[]`,
  producing `COALESCE could not convert type numeric[] to text[]`. After
  catalog introspection, `create_reflex_ivm_impl` now propagates the
  resolved source-arg type back onto the MIN/MAX intermediate column.
- **Top-K partial-heap staleness on UPDATE.** When `K < group_cardinality`,
  an UPDATE that retracted a heap-resident value left the heap non-empty
  but missing the unchanged source rows that should have been promoted into
  it. The 1.3.0 recompute trigger fired only on `cardinality(heap) = 0`,
  so a *partial* heap slipped through and a subsequent DELETE then read a
  stale `heap[1]`. Fix: split the UPDATE flow's recompute trigger into two
  paths — non-top-K MIN/MAX keeps the legacy `Sub → recompute(if scalar
  IS NULL) → Add` order; top-K MIN/MAX uses
  `Sub → topk_refresh → Add → forced recompute` (gated to the shrunk-
  groups capture table from N1 above). Regression locked in by
  `pg_test_topk_partial_heap_staleness_regression` and the existing
  50-mutation × 3-group × K=16 fuzzer.
- **Non-deterministic-function rejection is query-wide.** The analyzer
  flag `has_nondeterministic_select` always rejected `NOW()` / `RANDOM()` /
  etc. anywhere `pre_visit_expr` reached (SELECT, WHERE, HAVING, JOIN ON,
  ORDER BY) — the user-facing message claimed "in SELECT" only. The
  message now reads "anywhere in the query" and explains why (drift over
  time without a corresponding source mutation). Behaviour unchanged.

### Tests
- 513 lib tests (up from 503 in 1.3.0).
- New: `pg_test_topk_text_min_max`, `pg_test_topk_date_min_max`,
  `pg_test_topk_timestamp_min_max` (non-NUMERIC element types);
  `pg_test_topk_partial_heap_staleness_regression` (UPDATE-then-DELETE
  staleness minimal repro); `pg_test_topk_update_no_heap_shrink_keeps_correctness`,
  `pg_test_topk_update_mixed_shrink_groups`,
  `pg_test_topk_update_multi_column_shrink` (N1 gate correctness).

### Docs
- Operator runbook gains a LOGGED-vs-UNLOGGED decision matrix, a
  stuck-flush triage recipe (`pg_stat_activity` filter on
  `application_name = 'reflex_flush:%'` + `reflex_explain_flush`), and an
  auto-on top-K caveat.
- `docs/limitations/unsupported-shapes.md` rewritten as a three-bucket
  taxonomy (hard-rejected / supported-with-fallback / operator workaround)
  and refreshed against current behaviour. Stale "needs top-K opt-in"
  language replaced with the auto-on guarantee.
- `docs/limitations/known-issues.md` pruned: the 1.3.x top-K closed items
  moved into release notes (this CHANGELOG); only items that are still
  open or still surprising remain on the page.

## [1.3.0] - 2026-04-25

### Performance
- **Bounded top-K heap for MIN/MAX (audit R3)** — opt-in `topk` parameter on
  `create_reflex_ivm` (6th positional arg, integer K, default disabled). When
  enabled, each MIN/MAX intermediate column gains a sibling
  `__<name>_topk <type>[]` array maintained on every flush:
  - INSERT path: top-K sorted-merge of `t.topk || d.topk` truncated to K.
  - DELETE/UPDATE path: multi-set subtraction via the new
    `public.__reflex_array_subtract_multiset(anyarray, anyarray)` plpgsql
    helper; the scalar `__min_x` / `__max_x` is rebuilt from `topk[1]`.
  - Heap-underflow fallback: when the array empties, the existing scoped
    recompute (1.2.0) takes over and rebuilds both the scalar and the array
    from the source.
  Existing IMVs continue to use the scoped-recompute path with no migration
  cost. Closes the `stock_chart_*` cliff documented in
  `journal/2026-04-22_unsupported_views.md` §6 / audit R3 — the 3 IMVs there
  become eligible for incremental maintenance once operators opt in.

### Added
- **Per-IMV flush histogram (audit R6)** — `__reflex_ivm_reference` gains
  a `flush_ms_history BIGINT[]` ring buffer (size 64) populated by
  `reflex_flush_deferred`. New SPI
  `reflex_ivm_histogram(view_name) -> (p50_ms, p95_ms, p99_ms, max_ms, samples)`.
- **`pg_stat_statements` correlation** — each per-IMV flush body sets
  `application_name = 'reflex_flush:<view>'` for its duration, so operators
  with `track_application_name = on` can filter pg_stat_statements rows by
  IMV.
- **Scalar MIN/MAX (no GROUP BY)** is now a tested supported shape (audit
  unsupported §2). Two new correctness tests in `pg_test_correctness.rs`.
  With `topk=K`, scalar retraction becomes O(K) instead of O(N).

### Tests
- 503 lib tests (up from 497 in 1.2.1).
- New: 3 top-K integration tests including a 30-iteration random fuzz with
  EXCEPT ALL oracle, 2 scalar MIN/MAX tests, 2 histogram tests.

## [1.2.1] - 2026-04-25

### Added
- **`pg_reflex.alter_source_policy` GUC** — controls how the
  `reflex_on_ddl_command_end` event trigger reacts when a tracked source is
  altered. Default `'warn'` preserves 1.2.0 behaviour. Set to `'error'` to roll
  back the ALTER instead of warning (useful for change-control gates). Closes
  audit risk R2.
- **`reflex_scheduled_reconcile(max_age_minutes INTEGER DEFAULT 60)`** — SPI
  designed for pg_cron-driven drift scans. Iterates IMVs whose
  `last_update_date` is older than the threshold (or NULL), reconciles each in
  isolation, and returns `(name, status, ms)` per attempt. Closes audit risk
  R7 with a code-and-recipe approach instead of a background worker.

### Improved
- **Passthrough PK auto-detection** (audit R5) — already worked for single-source
  passthroughs; 1.2.1 adds a clearer info message when the source has a PK but
  the SELECT list does not include all PK columns, telling operators what to add.

### Tests
- 493 lib tests (up from 487 in 1.2.0).
- New: 3 alter-source-policy tests, 2 PK auto-detection tests, 2
  scheduled-reconcile tests, 3 source-drop cleanup tests.

## [1.2.0] - 2026-04-24

### Performance
- **Affected-groups-scoped MIN/MAX recompute** — `build_min_max_recompute_sql` now wraps the `orig_base_query` in a filter that restricts it to groups present in `__reflex_affected_<view>`. On retractions, only groups actually touched by the delta get re-aggregated, instead of every group in the source. For IMVs with MIN/MAX over large sources (stock_chart-style workloads), this turns a full-scan recompute into an O(delta) operation when the affected-group set is small.

### Added
- **Operational safety — per-IMV SAVEPOINT in cascade flush** — `reflex_flush_deferred` wraps each per-IMV flush body in its own `SAVEPOINT`. One bad IMV (e.g. a broken base_query after a source schema change) logs a `WARNING` and allows the cascade to continue instead of aborting every upstream update.
- **Event trigger — auto-drop on source drop** — new `reflex_on_sql_drop` event trigger (`sql_drop`). Dropping a source table now drops every artifact owned by the IMV (target, intermediate, affected-groups, delta-scratch and passthrough-scratch tables, plus the standalone trigger functions) and removes the registry row. Cascades through `graph_child` so child IMVs are cleaned up too. Closes audit risk R1.
- **Event trigger — warn on source `ALTER TABLE`** — new `reflex_on_ddl_command_end` event trigger (`ddl_command_end`, tag `ALTER TABLE`). Raises a `WARNING` suggesting `reflex_rebuild_imv` when a tracked source is altered.
- **`reflex_rebuild_imv(name)`** — public alias over `reflex_reconcile` for consistency with post-schema-change recovery guidance.
- **Observability — registry columns** — `__reflex_ivm_reference` gains `last_flush_ms`, `last_flush_rows`, `flush_count`, `last_error`. Populated by each per-IMV `SAVEPOINT` block inside `reflex_flush_deferred` (success clears `last_error`; failure records it).
- **Observability — SPIs** — `reflex_ivm_status()`, `reflex_ivm_stats(view_name)`, `reflex_explain_flush(view_name)` let operators inspect registered IMVs, their sizes, and the next-flush plan without firing a write.
- **Streaming separator for trigger bodies** — `reflex_execute_separated(sql)` #[pg_extern] consumes a `--<<REFLEX_SEP>>--`-delimited statement stream. Used by the `TRUNCATE` trigger body; INSERT/DELETE/UPDATE trigger bodies still use the `string_to_array` loop because calling an extension function from inside those trigger bodies drops transition-table scope.

### Fixed
- **Bug #10 — transitive cycle detection** in `create_reflex_ivm`. Walks existing `depends_on` edges before registering the new row; rejects circular dependencies with a clear error.
- **Bug #11 — 64-bit advisory lock keys** — `pg_advisory_xact_lock(key1, key2)` seeded from a 64-bit hash, replacing the single-`hashtext`-arg form that could collide across names.
- **Bug #7 — `resolve_column_type` silent TEXT** — emits `pgrx::warning!` on catalog-lookup failure and defaults to `NUMERIC` instead of `TEXT`. Cast errors at CREATE time are preferable to silent behaviour drift.
- **Bug #4 — reserved CTE alias collision** — `create_reflex_ivm` rejects user CTEs named `__reflex_new_<src>` / `__reflex_old_<src>` / `__reflex_delta_<src>` rather than silently corrupting rewrites.
- **Bug #13 — STRICT vs nullable `where_predicate`** — handled inside `reflex_flush_deferred` rather than at the function signature, keeping the one-arg extension API stable.

### Tests
- 485 lib tests (up from 481 in 1.1.3).
- New: 4 unit tests for the affected-groups-scoped recompute SQL shape (`test_min_max_recompute_scoped_to_affected_groups_when_provided`, `test_min_max_recompute_no_affected_filter_when_none_passed`, `test_min_max_recompute_affected_filter_uses_multiple_group_columns`, `test_min_max_recompute_skips_affected_filter_for_sentinel_plan`).

### Deferred to 1.3.0
- **MIN/MAX bounded top-K heap (`__min_X_topk`)** — originally scoped for 1.2.0; deferred after evaluating complexity-vs-payoff. The affected-groups-scoped recompute above captures the common-case win at a fraction of the code and migration cost. Top-K revisits once benchmark data shows retractions repeatedly hitting the same hot groups.
- **Lazy index maintenance on bulk rebuild** — `DROP INDEX … INSERT … CREATE INDEX` when the affected set exceeds 50 % of the intermediate. Niche payoff and risky under concurrent flushers with advisory locks; left out of 1.2.0 pending a realistic workload that benefits.

## [1.1.3] - 2026-04-22

### Performance
- **Algebraic `BOOL_OR`** — `BOOL_OR(expr)` now decomposes into two BIGINT companion columns (`__bool_or_<arg>_true_count` and `__bool_or_<arg>_nonnull_count`), both maintained with pure `SUM(+)/SUM(-)` algebra. Removes the full-scan recompute on DELETE/UPDATE. End-query maps the two counters back to boolean via a `CASE` expression that preserves Postgres `BOOL_OR` NULL semantics (`NULL` when every input was NULL, `FALSE` when at least one was non-NULL and none TRUE, `TRUE` otherwise).
- **Empty-affected DO-block gate** — the targeted `DELETE + INSERT` path for group-by IMVs is now wrapped in a `DO $$ … IF EXISTS(…) THEN … END IF; END $$` block that short-circuits when the affected-groups staging table is empty. Avoids a full target-table scan on transactions that produce no matching groups.
- **`parallel_safe` SQL-building functions** — `reflex_build_delta_sql` and `reflex_build_truncate_sql` are annotated `PARALLEL SAFE`. They read no shared state and produce deterministic SQL given identical arguments.
- **Staging-delta `ANALYZE`** — `reflex_flush_deferred` runs `ANALYZE` on the staging delta table before processing so the planner gets non-zero row estimates after the `TRUNCATE` that reset stats.
- **Per-IMV `where_predicate` registry column** — the IMV registry stores each view's `where_predicate`. Deferred UPDATE trigger bodies check the predicate against the transition table before taking the advisory lock; `reflex_flush_deferred` skips IMVs whose predicate matches no staged row. Particularly effective for sub-IMVs of a `UNION` with disjoint filters.
- **End-query targeted splice for `GROUP BY` end_queries** — `reflex_build_delta_sql` splices `AND (<gb_cols>) IN (SELECT DISTINCT <gb_cols> FROM "<affected_tbl>")` before the `GROUP BY` clause instead of falling back to a full `DELETE + INSERT … end_query`. Primary beneficiary: `COUNT(DISTINCT)` IMVs.

### Fixed
- **63-char identifier truncation** — `transition_new_table_name`, `transition_old_table_name`, and `staging_delta_table_name` now generate guaranteed-unique, ≤63-byte identifiers via a sanitize-then-truncate helper. Previously, long source names could produce colliding transition-table names across IMVs.
- **MIN/MAX / BOOL_OR recompute scalar-subquery bug** — `build_min_max_recompute_sql` wraps `orig_base_query` as `(…) AS __src` before referencing group keys. Previously the direct-column reference failed with `missing FROM-clause entry for table "alias"` on JOIN-aliased base queries.
- **Concurrent-flush advisory-lock collision** — the deferred-flush advisory-lock key now derives from a hash of `(view_name, source_table)` jointly, so two concurrent sessions flushing different IMVs on the same source don't serialize on the same integer key.

### Tests
- 481 lib tests (up from 406 in 1.1.1).

## [1.1.1] - 2026-03-29

### Added
- **FILTER clause support** — `SUM(x) FILTER (WHERE cond)`, `COUNT(*) FILTER (WHERE cond)`, `AVG(x) FILTER (WHERE cond)`, `MIN/MAX(x) FILTER (WHERE cond)`, and `BOOL_OR(x) FILTER (WHERE cond)` are now supported. Internally rewritten to `CASE WHEN` expressions, so all existing incremental maintenance (MERGE, delta, triggers) works transparently. Multiple FILTER aggregates alongside regular aggregates in the same query are supported.
- **DISTINCT ON support** — `SELECT DISTINCT ON (cols) ... ORDER BY ...` is decomposed into a passthrough sub-IMV (incrementally maintained) + a VIEW with `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) WHERE rn = 1`. INSERT/DELETE/UPDATE on source data is reflected instantly. Supports multiple partition columns, WHERE clause, and JOINs.

### Fixed
- **DROP CASCADE** — `drop_reflex_ivm(name, true)` now issues `DROP TABLE ... CASCADE` on target, intermediate, and affected-groups tables. Previously, cascade only dropped child IMVs in the reflex dependency graph but left external PostgreSQL objects (views, foreign keys) intact, causing the drop to fail if any existed.
- **DROP VIEW/TABLE detection** — `drop_reflex_ivm` now detects whether the target is a VIEW (window/DISTINCT ON decompositions) or TABLE and issues the correct DROP command. Previously, dropping a window-function or DISTINCT ON IMV would fail with "is not a table".

### Internal
- **Codebase restructured** — `lib.rs` reduced from 10,548 to 189 lines. Implementation split into focused modules: `create_ivm.rs` (IVM creation), `drop_ivm.rs` (drop logic), `reconcile.rs` (reconcile/refresh). Submodule tests extracted into separate files under `src/tests/`.
- **Tests reorganized** — tests split into 20 categorized files (basic, trigger, passthrough, CTE, set ops, window, drop, reconcile, deferred, error, e2e, correctness, filter, distinct_on, plus 6 unit test files).

### Tests
- 406 tests (up from 375 in v1.0.4)
- New: 7 FILTER unit tests, 9 FILTER integration tests, 5 DISTINCT ON unit tests, 9 DISTINCT ON integration tests, 1 non-SELECT rejection test

## [1.1.0] - 2026-03-29

### Fixed
- **DROP CASCADE** — `drop_reflex_ivm(name, true)` now issues `DROP TABLE ... CASCADE` on target, intermediate, and affected-groups tables.

### Internal
- **Codebase restructured** — `lib.rs` reduced from 10,548 to 189 lines. Implementation split into focused modules.
- **Tests reorganized** — tests split into categorized files under `src/tests/`.

### Tests
- 376 tests (up from 375 in v1.0.4)

## [1.0.4] - 2026-03-26

### Performance
- **Empty-delta early-exit** — triggers check if the transition table is empty before entering the IMV processing loop. Skips all Rust FFI calls, advisory locks, and MERGE generation when a statement doesn't produce relevant rows. Saves 5-15ms per trigger fire for empty deltas.
- **Predicate-filtered trigger skip** — WHERE clauses from IMV queries are stored in `__reflex_ivm_reference.where_predicate`. Before processing an IMV, the trigger evaluates the predicate against the transition table. Non-matching IMVs are skipped entirely (no advisory lock, no delta SQL). Particularly effective for UNION sub-IMVs with disjoint filters.
- **Persistent affected-groups table** — replaced per-trigger-fire `DROP TABLE + CREATE TEMP TABLE` with a persistent UNLOGGED table created at IMV setup time. Uses `TRUNCATE` (0.17ms) instead of `DROP+CREATE` (0.65ms) — 3.9x faster per trigger fire.
- **Single-pass UPDATE MERGE** — for aggregate queries without MIN/MAX, UPDATE operations use a single net-delta MERGE combining old and new transition tables, halving the MERGE count.

### Added
- **INTERSECT support** — `SELECT ... INTERSECT SELECT ...` decomposes into sub-IMVs, same pattern as UNION.
- **EXCEPT support** — `SELECT ... EXCEPT SELECT ...` decomposes into sub-IMVs.

### Tests
- 218 tests (up from 214 in v1.0.3)
- New: 2 INTERSECT tests, 2 EXCEPT tests

### Benchmarks (single-IMV, warm cache, 1M source rows)
- GROUP BY UPDATE 100 rows: **4.4ms** (vs 55ms REFRESH MATERIALIZED VIEW)
- PASSTHROUGH INSERT 1K rows: **10ms** (vs 2,500ms REFRESH — 250x faster)
- Per-IMV overhead: ~4ms warm, scales linearly with number of IMVs on same source

## [1.0.3] - 2026-03-26

### Added
- **WINDOW function support** — queries with `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `SUM() OVER (...)`, and any other PostgreSQL window function are now supported. Decomposed into a base sub-IMV (incrementally maintained) + a VIEW that applies window functions at read time. For GROUP BY + WINDOW, the VIEW scans only the small intermediate result (one row per group).
- **UNION ALL / UNION support** — set operations are decomposed into per-operand sub-IMVs. `UNION ALL` creates a zero-overhead VIEW over the sub-IMV targets. `UNION` (dedup) creates a VIEW with PostgreSQL's native deduplication. Supports 2+ operands, aggregates in operands, and mixed WHERE filters on the same source table.
- **`storage` parameter** — `create_reflex_ivm('v', 'SELECT ...', NULL, 'LOGGED')` creates WAL-logged tables for crash safety. Default: `'UNLOGGED'` (current behavior). Propagated to CTE sub-IMVs and UNION sub-IMVs.
- **`mode` parameter** — `create_reflex_ivm('v', 'SELECT ...', NULL, 'UNLOGGED', 'DEFERRED')` accumulates deltas during the transaction and flushes at COMMIT via a two-stage trigger design (immediate capture to staging table + deferred constraint trigger). Default: `'IMMEDIATE'` (current behavior).
- **Materialized view auto-refresh** — event trigger on `ddl_command_end` automatically cascades `REFRESH MATERIALIZED VIEW` to dependent pg_reflex IMVs. No manual `refresh_imv_depending_on()` needed.
- New `window.rs` module for window function query decomposition
- `reflex_flush_deferred(source_table)` function for manual deferred delta processing

### Performance
- **Single-pass UPDATE MERGE** — for aggregate queries without MIN/MAX, UPDATE operations now use a single net-delta MERGE (combining old and new transition tables) instead of two separate MERGEs. Reduces MERGE count by 50% for UPDATE operations.

### Migration
- New columns in `__reflex_ivm_reference`: `storage_mode` (default `'UNLOGGED'`), `refresh_mode` (default `'IMMEDIATE'`). Existing IMVs backfilled automatically.
- Deferred processing infrastructure: `__reflex_deferred_pending` table + constraint trigger created automatically.
- Materialized view event trigger installed automatically.
- Migration is automatic via `ALTER EXTENSION pg_reflex UPDATE`.

### Tests
- 214 tests (up from 172 in v1.0.2)
- New test coverage: 9 UNION ALL tests, 5 UNION dedup tests, 18 WINDOW function tests, 5 LOGGED mode tests, 3 DEFERRED mode tests

### API
```sql
-- Full signature (all params have defaults, backward-compatible)
SELECT create_reflex_ivm(
    'view_name',                -- TEXT: view name
    'SELECT ...',               -- TEXT: query
    NULL,                       -- TEXT: unique_columns (optional)
    'UNLOGGED',                 -- TEXT: storage mode ('LOGGED' or 'UNLOGGED')
    'IMMEDIATE'                 -- TEXT: refresh mode ('IMMEDIATE' or 'DEFERRED')
);
```

## [1.0.2] - 2026-03-24

### Performance
- **UNLOGGED target table** — target tables are now `UNLOGGED` (matching intermediate tables). Eliminates WAL writes on every targeted refresh (DELETE+INSERT), reducing write overhead. Crash recovery already required `reflex_reconcile()` due to the UNLOGGED intermediate, so this adds zero additional risk.
- **Hash index on intermediate** — single-column GROUP BY keys now use a hash index instead of a B-tree primary key for O(1) MERGE lookups (~30% faster MERGE for single-column groups). Multi-column GROUP BY falls back to B-tree (hash doesn't support multi-column in PostgreSQL). The B-tree PK is removed because MERGE handles insert-or-update correctly and advisory locks prevent concurrent modifications.
- **MERGE RETURNING** — the delta query now runs once per trigger fire instead of twice. The MERGE into intermediate uses `RETURNING` in a CTE to capture affected group keys, eliminating the separate `SELECT DISTINCT groups FROM (delta_query)` statement. For UPDATE operations, delta_old and delta_new each run once instead of twice (4 → 2 executions).

### Benchmarks (100K groups, 1M source, single-column GROUP BY)
- INSERT 10K: 236ms → 171ms (**28% faster**)
- INSERT 50K: 1,170ms → 865ms (**26% faster**)
- INSERT 100K: 2,298ms → 1,802ms (**22% faster**)

### Migration
- Existing aggregate IMVs: intermediate PK dropped and replaced with hash/B-tree index, target table converted to UNLOGGED. Migration is automatic via `ALTER EXTENSION pg_reflex UPDATE`.
- Existing passthrough IMVs: target table converted to UNLOGGED.

### Tests
- 172 tests (unchanged from v1.0.1, all passing)

## [1.0.1] - 2026-03-23

### Added
- **`bool_or(expr)` aggregate** — incremental via OR on INSERT, recomputes from source on DELETE (same pattern as MIN/MAX)
- **Cast propagation** — `SUM(x)::BIGINT` now produces a BIGINT column in the target table (cast applied in end query, intermediate still stores NUMERIC for precision)
- **Target table index** — composite index on group columns for faster targeted refresh DELETE performance
- **Unsupported aggregate warnings** — unrecognized aggregates (e.g., `string_agg`) now emit a WARNING instead of being silently dropped
- Materialized view support as source tables (triggers auto-skipped, warning emitted)
- `refresh_reflex_imv(view_name)` — refresh a single IMV (alias for `reflex_reconcile`)
- `refresh_imv_depending_on(source)` — refresh all IMVs depending on a source table or materialized view
- HAVING clause support with AST-based rewriting (handles complex expressions like `AVG(x) > COUNT(*)`)
- Auto-detection of HAVING aggregates not in SELECT list (added to intermediate table automatically)
- Incremental passthrough DELETE/UPDATE (O(delta) row-matching instead of O(N) full refresh)
- Multi-level cascade confirmed and tested (works to arbitrary depth)
- CTE passthrough support (passthrough CTEs become sub-IMV tables)
- `create_reflex_ivm_if_not_exists(name, sql)` / `create_reflex_ivm_if_not_exists(name, sql, unique_columns)` — idempotent IMV creation that returns a notice instead of an error if the view already exists
- `install.sh` wrapper script — copies migration files alongside `cargo pgrx install`
- Subquery warning — subqueries in FROM now emit an informational warning (like materialized views)

### Fixed
- **Trigger table reference replacement** — schema-qualified tables with column qualifiers (e.g., `sales_simulation.product_id` from `alp.sales_simulation`) now work correctly in triggers. Previously caused `missing FROM-clause entry` on every INSERT/UPDATE/DELETE.
- **Cast expressions no longer silently dropped** — `SUM(x)::BIGINT` is now correctly detected as an aggregate. Previously, the cast wrapper hid the function from the aggregate detector.
- **Column name case normalization** — unquoted identifiers like `MONTH` are now lowercased consistently (matching PostgreSQL's case folding), preventing `column "MONTH" does not exist` errors at trigger time.
- **Source index creation** — index creation on source tables for MIN/MAX/BOOL_OR recompute now checks column existence first, so it no longer fails when group columns come from JOIN tables.
- Materialized views no longer cause "cannot have triggers" error
- Passthrough DELETE/UPDATE no longer does full table refresh
- **Passthrough JOIN key mapping** — unique key detection for passthrough JOINs now uses per-source-table column mappings derived from JOIN conditions. Previously, DELETE/UPDATE triggers on secondary tables (e.g., `products` in a `sales JOIN products` query) could corrupt data by matching the wrong column. Auto-detection is now restricted to single-source queries; JOINs require the explicit 3rd argument.
- Dropped PostgreSQL 13/14 from supported versions (MERGE statement requires PG15+)
- **BOOL_OR recompute on DELETE** — the recompute SQL was generated but never executed because the guard condition only checked for MIN/MAX, not BOOL_OR. Now fixed.
- **Subqueries with aggregation in FROM** — now rejected at creation time with a clear error suggesting CTE as the alternative (pg_reflex decomposes CTEs into sub-IMVs automatically). Previously, these silently produced incorrect results because the trigger replaced the inner table with the transition table, making the inner aggregation see only delta rows.

### Performance
- **Deferred index creation** — indexes on intermediate and target tables are now created after bulk data insertion (not before), reducing IMV creation time by ~60% on large datasets
- **Faster `reflex_reconcile`** — drops all indexes (including user-created) before bulk rebuild, recreates them after. Saves index DDL and restores it faithfully. Reduced reconcile time by ~38% on large datasets (6:29 → 4:00 on 7.7M rows). Also uses TRUNCATE instead of DELETE for instant table clearing.
- **ANALYZE** — intermediate and target tables are analyzed after initial materialization and after reconcile for better query planner statistics

### Tests
- 172 tests (up from 138 in v1.0.0) covering BOOL_OR, LEFT/RIGHT JOIN, cast propagation, subqueries, passthrough JOINs with per-source key mapping, chained IMVs with passthrough layers, and multiple mixed IMVs on same source

## [1.0.0] - 2026-03-22

### Added
- `drop_reflex_ivm(view_name)` and `drop_reflex_ivm(view_name, cascade)` for removing IMVs and all artifacts
- `reflex_reconcile(view_name)` for rebuilding IMVs from source data
- TRUNCATE trigger support (clears intermediate and target on source TRUNCATE)
- Targeted group refresh (only affected groups re-materialized, not the full target table)
- CTE decomposition (each CTE becomes a sub-IMV, passthrough outer queries become VIEWs)
- Passthrough CTE support (CTEs without aggregation work as passthrough sub-IMVs)
- MERGE-based delta processing (replaces INSERT ON CONFLICT for better performance)
- Schema-qualified view names (`myschema.my_view`) — views, intermediate tables, and triggers are created in the correct schema
- View name validation (rejects names with special characters to prevent SQL injection)
- Duplicate view name detection (returns error instead of crashing)
- PostgreSQL logging for key operations (`info!` on create/drop/reconcile, `warning!` on errors)
- GitHub Actions CI testing on PostgreSQL 15, 17, 18
- Automated release workflow with `.deb` package builds on version tags
- Concurrent operation test suite (parallel psql sessions)
- Property-based testing with proptest for input validation and query decomposition
- Multi-run benchmark harness (`benchmarks/run_bench.sh`) with variance reporting
- Deterministic benchmark seeds (`setseed`) for reproducible results
- 138 tests (63 unit + 7 proptest + 68 integration) covering all aggregate types, CTEs, JOINs, schema support, cascading, and edge cases
- 17 SQL benchmark scripts covering scales from 1K to 5M rows
- Apache 2.0 license

### Fixed
- SQL parser no longer panics on malformed input (returns error string instead of crashing PostgreSQL backend)
- SQL injection vectors eliminated via parameterized queries and input validation
- Catalog queries (`information_schema.columns`) now use parameterized queries
- Passthrough DELETE/UPDATE now incremental (O(delta) row-matching instead of O(N) full refresh)
- Multi-level cascade propagation works automatically to arbitrary depth (was incorrectly listed as a limitation)

### Supported
- PostgreSQL 15, 16, 17, 18 (requires MERGE statement, PG15+)
- Aggregates: SUM, COUNT, COUNT(*), AVG, MIN, MAX, BOOL_OR
- DISTINCT, GROUP BY, WHERE, INNER/LEFT/RIGHT JOIN
- Non-recursive CTEs (decomposed into sub-IMVs)
- Multi-level IMV cascading (A → B → C, tested up to 4 levels)
- Schema-qualified view names and source tables
