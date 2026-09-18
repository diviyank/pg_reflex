# Hand-off — 1.11.4 ignored-source heal, N-round (2026-09-18)

Branch `reflex-1.11.4`. Everything below is committed on it. This note is the
single source of truth for the next session: the review artefacts it summarises
lived in a machine-local scratchpad and do not travel.

## State

| Commit | Contents |
|---|---|
| b123a53 | heal after an ignored source changes; swap retires its rebuild anomaly |
| ec966b3, 77538da | docs for the above |
| 3b125ad | F-round hardening (F1–F10) |
| 9a74d08 | N-round fixes N1–N7 (code + tests) — **contains the defects X1–X5 below** |
| bc95b44 | N-round docs |
| this commit | release files (version bump, changelogs, migration) + this note |

Verified at bc95b44: `cargo pgrx test pg17` = 1673 passed / 0 failed; `cargo fmt`
clean; clippy clean bar 4 pre-existing warnings in `src/tests/pg_test_audit.rs`;
`mkdocs build --strict` passes. 25 of 26 mutations RED (the exception is an
equivalent mutant, see below).

Release files are **uncommitted by policy** normally; they are committed here
only so the work travels. Before tagging, reorder/squash as the release process
wants: `Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`, `docs/changelog.md`,
`sql/pg_reflex--1.11.3--1.11.4.sql`.

## The work in one paragraph

An ignored source (`ignore_sources`) gets no maintenance trigger, so a change to
it leaves the IMV silently wrong — the omc `sop_forecast_view` incident. 1.11.4
maps an ignored source that joins by equality onto the IMV's first partition
column, puts statement triggers on it that queue the affected partition keys in
`public.__reflex_heal_pending`, reports the IMV (and every IMV reading it)
`known_stale`, and rebuilds exactly those partitions in
`reflex_heal_ignored_sources` / `reflex_scheduled_reconcile` / `reflex_doctor
(fix => TRUE)` F14.

## What must happen next (design already decided by the user)

The N-round split the trigger into a SECURITY INVOKER trigger plus three
SECURITY DEFINER helpers (`__reflex_heal_targets`, `__reflex_heal_enqueue`,
`__reflex_heal_mark_truncated`). A final adversarial review found that split
introduced worse defects than it closed. **The user chose: go back to a single
SECURITY DEFINER trigger function with no helpers**, keeping the cast-free diff
and the built-in key gate, so no user-defined code runs with the definer's
rights. A trigger function is not directly callable and needs no schema USAGE,
which is what closes X1, X3 and X5 structurally.

### Implementation plan (TDD — tests first, watch them RED for the intended reason)

In `src/lib.rs` bootstrap DDL, and byte-identically (dedented) in
`sql/pg_reflex--1.11.3--1.11.4.sql`:

1. **Delete the three helpers.** They never shipped (1.11.4 is unreleased), so
   no `DROP FUNCTION` is needed in the migration; just remove them.
2. **`__reflex_heal_on_ignored_change` becomes** `LANGUAGE plpgsql SECURITY
   DEFINER SET search_path = pg_catalog, pg_temp SET extra_float_digits = 3`.
   Inline what the helpers did:
   - target loop: `SELECT r.name, k.key, k.value->>'source_column', watched …
     FROM public.__reflex_ivm_reference r CROSS JOIN LATERAL jsonb_each(
     COALESCE(r.aggregations->'ignore_heal_keys','{}'::jsonb)) k WHERE
     COALESCE(r.enabled,TRUE) AND to_regclass(k.value->>'relation') = TG_RELID`;
   - enqueue: the `INSERT … ON CONFLICT (imv_name, partition_key) DO UPDATE SET
     enqueued_at = clock_timestamp(), source = EXCLUDED.source, last_error =
     NULL` that `__reflex_heal_enqueue` holds today;
   - TRUNCATE: the `UPDATE public.__reflex_ivm_reference` that
     `__reflex_heal_mark_truncated` holds today, **unconditionally** (no
     emptiness check — that check is X4 and X1), keeping the N7 behaviour:
     append `' | '` + reason when already stale, skip when the reason is already
     contained, `stale_since = COALESCE(stale_since, now())`.
3. **Keep** the N5 `CONTINUE WHEN` for a missing mapped/watched column (no
   registry write — `reflex_ivm_status` and `reflex_doctor` derive it), and the
   key expression `to_jsonb(%I) #>> '{}'`.
4. **X2 — NULL-aware watched diff.** Today: `format('format(''%%s'', %I)', c)`.
   `format('%s', NULL)` is `''`, so NULL ↔ `''` is invisible. Emit a pair per
   watched column: `format('(%I IS NULL), format(''%%s'', %I)', c, c)`.
5. **Fire-time key-type recheck (security).** The install gate requires the
   source column's type to be in `pg_catalog`, but `ALTER COLUMN … TYPE` to a
   user type afterwards would make `to_jsonb` consult that type owner's cast to
   json under the definer. Add to the `CONTINUE WHEN`: the source column's
   `atttypid`'s `typnamespace` is not `'pg_catalog'::regnamespace`.

Then: `cargo fmt`, clippy, full `cargo pgrx test pg17`, mutation-check each new
guard, sync the migration (script below), update the docs listed under
"Docs to correct", commit code+tests and docs separately (**no mention of Claude
in commit messages**), and re-run the adversarial review.

### Tests to write first

- Replace `ish_direct_truncate_helper_call_marks_nothing` (its helper is being
  deleted) with a test that **no heal function is directly callable**: every
  `pg_proc` row named `__reflex_heal%` has `prorettype = 'trigger'::regtype`.
  This is the X1 regression pin.
- **X2:** watched text column NULL → `''` queues the key (see repro below).
- **X4:** `TRUNCATE ONLY` of an inheritance parent whose child still has rows
  marks the IMV `known_stale`.
- **X3:** a writer with no USAGE on schema `public` can still write the ignored
  source. Needs the source, fact and IMV in another schema (`app`), with
  `REVOKE ALL ON SCHEMA public FROM PUBLIC`; each `#[pg_test]` runs in its own
  rolled-back transaction, so the revoke is local. If `create_reflex_ivm` turns
  out not to take a schema-qualified IMV name, adapt the fixture — this test was
  never written, so it is not a "modified after the fact" test.

## The five confirmed findings (from the final review, reproduced verbatim)

**X1 — security, critical.** `__reflex_heal_mark_truncated` is SECURITY DEFINER,
owned by the superuser that installed the extension, EXECUTE granted to PUBLIC,
and runs `EXECUTE format('SELECT NOT EXISTS (SELECT 1 FROM %s)', _relid)` before
validating `_relid`. A view's functions run as `current_user` = the definer:

```sql
SET ROLE rv_evil;                              -- NOSUPERUSER, no pg_reflex grants, no IMV
CREATE FUNCTION pwn() RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$
BEGIN EXECUTE 'ALTER ROLE rv_evil SUPERUSER'; RETURN false; END $$;
CREATE VIEW pwn_v AS SELECT 1 AS x WHERE pwn();
SELECT public.__reflex_heal_mark_truncated('pwn_v'::regclass);
RESET ROLE;  -- rv_evil is now SUPERUSER
```
`pg_temp` works when the role has CREATE on no schema. Closed by deleting the
helpers (a trigger function cannot be called directly).

**X2 — silent wrong result.** `format('%s', NULL) = format('%s', '')`, so a
watched column changing between NULL and `''` is never queued; the IMV stays
wrong and reports fresh. Repro: an IMV filtered on `x.note IS NULL`, then
`UPDATE nd SET note = '' WHERE id = 1` → queue 0, `known_stale = f`, oracle
diverges by 1 row. Fix: the NULL flag in item 4 above.

**X3 — availability.** The invoker trigger calls `public.__reflex_heal_targets`,
and calling a function needs USAGE on its schema (firing a trigger does not).
With `REVOKE ALL ON SCHEMA public FROM PUBLIC` — common hardening — every
INSERT/UPDATE/DELETE on the ignored source fails with `permission denied for
schema public`. Closed by the definer trigger with no helper calls.

**X4 — silent wrong result.** The emptiness guard reads the relation *including*
inheritance children, so after `TRUNCATE ONLY parent` (children still populated)
nothing marks the IMV stale. Closed by dropping the guard (the TRUNCATE branch
marks unconditionally again).

**X5 — availability, non-converging remedy.** Any role can call
`__reflex_heal_enqueue` with an unparseable key (`'not-a-number'` for a bigint
partition column). The heal then fails on every run, the IMV reports
`known_stale` forever, F14 reports `failed:` every run, and `reflex_reconcile`
does not clear `__reflex_heal_pending`. `__reflex_heal_targets` also discloses
the registry mapping to any role. Closed by deleting the helpers.
*Optional extra credit, independent of X5:* let a successful full
`reflex_reconcile` delete that IMV's queue rows stamped before it started — the
only remaining way a stuck queue row can be cleared without SQL surgery.

## What the review confirmed as sound (do not re-litigate)

N2, N4, N5, N6, N7 are closed and each is pinned by a mutation that turns its
test RED. Float/numeric/array/jsonb diffs can only over-queue
(`extra_float_digits = 3` round-trips exactly). Composite and enum watched
columns evaluate only I/O functions, which a non-superuser cannot define.
Transition tables are ENRs with no ACL check, so column privileges and RLS do
not affect the trigger. The migration's four function bodies are byte-identical
to the bootstrap DDL once comments and indentation are stripped.

**Known equivalent mutant:** `ish_heal_trigger_runs_no_user_code_as_another_role`
stays green if only the invoker layer, or only the `format('%s')` diff, is
reverted — each alone prevents the cast from running. The cast property is
pinned by `ish_type_owner_cast_neither_runs_nor_hides_a_change`; removing both
layers turns the N1 test RED. After the rewrite, re-check which test pins what.

## Docs to correct after the rewrite

`docs/api/reflex_heal_ignored_sources.md` — the "Writes to the ignored source"
section currently describes the invoker trigger, the three helpers and the
"helpers are callable by any role" residual; all of that goes. `CHANGELOG.md`
and `docs/changelog.md` 1.11.4 entries say "runs as the writer" and list the
three helpers in the migration bullet. `sql/pg_reflex--1.11.3--1.11.4.sql`
header item 6 says the same. `untreated_bugs/2026-09-08_status_window_swap_
wipes_slice_silently.md` residual 5 (helpers callable by any role) goes;
residual 4 (a second read of the source hidden in a SQL function body is not
detected) stays and is still real.

## Environment notes

- `cargo pgrx test` installs into the shared `~/.pgrx/<ver>/pgrx-install`; never
  run two sessions' tests concurrently, and re-run a single failure before
  believing it.
- Always `CARGO_TARGET_DIR=/private/tmp/rfx-<tag>` (a long path overflows the
  103-byte Unix socket limit and produces hundreds of spurious failures).
- Features pg15–pg18, so `pg_input_is_valid` (PG16+) is unavailable.
- Migrations in `sql/*--*.sql` are never executed by `cargo pgrx test`; only the
  bootstrap DDL in `src/lib.rs` is. Keep the two in sync — sync script:

```python
lib = open("src/lib.rs").read(); p = "sql/pg_reflex--1.11.3--1.11.4.sql"; mig = open(p).read()
def span(s, ind):
    a = s.index(ind + "-- <first comment line of the heal block>")
    b = s.index(ind + "$fn$;\n", s.index("__reflex_heal_on_ignored_change()", a)) + len(ind + "$fn$;\n")
    return a, b
a, b = span(lib, "    ")
block = "\n".join(x[4:] if x.startswith("    ") else x for x in lib[a:b].split("\n"))
ma, mb = span(mig, ""); open(p, "w").write(mig[:ma] + block + mig[mb:])
```

## Still open before release

- pg18 run (`cargo pgrx test pg18`).
- 1.11.3 → 1.11.4 upgrade smoke test on a real instance.
- The narrowed report `untreated_bugs/2026-09-08_status_window_swap_wipes_slice_
  silently.md` stays open: the window between a status returning and the next
  sweep, unmappable ignored sources, and direct partition writes.
