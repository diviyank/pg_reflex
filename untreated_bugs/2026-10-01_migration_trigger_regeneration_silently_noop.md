# Migration-time trigger regeneration through `reflex_rebuild_triggers` is a silent no-op — S2 (silent, upgrade heals never applied)

Nine migration scripts regenerate per-source trigger bodies by calling
`reflex_rebuild_triggers(src)` inside `ALTER EXTENSION pg_reflex UPDATE`, each source in its own
`BEGIN … EXCEPTION WHEN OTHERS` block that only raises a NOTICE or WARNING:
1.4.4→1.4.5, 1.4.5→1.4.6, 1.4.6→1.5.0, 1.5.0→1.5.1, 1.5.1→1.6.0, 1.6.1→1.6.2, 1.7.5→1.7.6,
1.7.7→1.8.0, 1.10.11→1.11.0 (PS-6). 1.7.0→1.7.1 deliberately does not call it, because of the
first cause below. 1.11.4→1.11.5 rewrites the one changed statement in place instead (Task 10).
In each of these scripts a failure leaves the source's live trigger function on its old body, and
the upgrade still reports success.

## Three independent causes (reproduced on pg17, 1.11.5 library)

1. **Healthy passthrough scratch tables are refused.** Since 1.11.0 the library's
   `reflex_rebuild_triggers` also heals the passthrough scratch pair with
   `CREATE UNLOGGED TABLE IF NOT EXISTS __reflex_pt_{new,old}_<imv>_<src>`
   (src/create_ivm/admin.rs, PS-6 heal). Inside an extension script PostgreSQL refuses
   `CREATE … IF NOT EXISTS` on an existing object that is not an extension member
   (CVE-2022-2625 hardening): `table __reflex_pt_new_pt_v_pt_src is not a member of extension
   "pg_reflex"`. The ERROR rolls back the whole call, including the trigger-function
   `CREATE OR REPLACE` that ran first. Any upgrade run with a ≥ 1.11.0 library therefore fails
   for **every source that feeds an enabled passthrough IMV**. Repro: a throwaway update script
   containing the 1.11.0 loop gave `0 ok, 2 failed` (one passthrough DEFERRED source, and one
   source feeding a DEFERRED aggregate plus an IMMEDIATE passthrough). In both, a marker comment
   injected into the bodies survived the update. A schema-qualified source feeding only an
   aggregate IMV was regenerated (`1 ok`).
2. **Non-member trigger functions are refused** (not re-run here; documented in
   sql/pg_reflex--1.7.0--1.7.1.sql:99). Before 1.8.0's adoption step, trigger functions created by
   `create_reflex_ivm` were not extension members. `CREATE OR REPLACE FUNCTION` on them inside
   `ALTER EXTENSION` errors with `function … is not a member of extension`. So on PG minors with
   the 2022 hardening, the 1.4.5–1.7.6 regenerations probably failed for every source. 1.8.0's own
   rebuild (step 4) runs before its adoption steps, so it probably failed there too.
3. **Bare-name sources are never regenerated, even outside an update.** `depends_on` stores
   public sources bare (`pt_src`), and `create_reflex_ivm` names the trigger set from that
   string (`__reflex_trigger_ins_on_pt_src` → `__reflex_ins_trigger_on_pt_src`).
   `reflex_rebuild_triggers('pt_src')` resolves the name to `public.pt_src` and creates a
   **second** trigger set, `__reflex_trigger_*_on_public_pt_src`. That set is inert: its body
   matches `'public.pt_src' = ANY(depends_on)`, which no IMV satisfies, and its flavour lookup
   picks the IMMEDIATE body. The live `*_on_pt_src` triggers keep their old body, although the
   function returns `rebuilt 8 trigger DDL(s)`. Reproduced outside `ALTER EXTENSION`: 8 stale
   bodies before, 8 after, plus 8 new inert trigger and function pairs. An INSERT and a
   TRUNCATE + INSERT on the source still maintain the IMVs correctly (only the old triggers act).
   The 1.11.5 migration's WARNING advises `reflex_rebuild_triggers('<src>')` with the bare name
   it extracted, so that advice has the same defect.

## Which heals likely never applied

- Upgrades executed with a ≥ 1.11.0 library (every multi-step `ALTER EXTENSION … UPDATE` run today
  runs the newest library, so every replayed 1.4.5–1.8.0 step too): no regeneration for sources
  feeding a passthrough IMV. The 1.11.0 PS-6 heal only "succeeds" where the scratch pair is
  missing. There it creates the pair inside the extension script, which makes it an extension
  member, so a later `DROP EXTENSION` drops it.
- Upgrades executed at the time with a 1.4.5–1.7.6 library: likely none (non-member functions).
- Every public bare-name source, through any path that calls `reflex_rebuild_triggers` (migrations,
  operators following the documented remedy, `reflex_doctor` prescriptions): never.

The impact depends on what each regeneration carried. 1.6.2 made the rebuild deferred-aware,
1.7.1 added Path C, and the 1.4.5/1.4.6/1.5.x bodies added the filter-aware skip and
schema-resolved sources. Bodies that call `reflex_build_delta_sql` at runtime pick up most
fixes from the library anyway. Fixes in the body template (`sql/*.plpgsql.in`) do not.

## How to verify on a live DB

1. Duplicates (cause 3): run
   `SELECT tgrelid::regclass, tgname FROM pg_trigger WHERE tgname LIKE '__reflex_trigger_%_on_public_%'`.
   If one of these sits beside a `__reflex_trigger_%_on_<bare>` trigger on the same table, a
   rebuild created an inert set.
2. Stale bodies: in a scratch database with the same library, run `CREATE EXTENSION pg_reflex` and
   the same IMV definitions. Compare `md5(prosrc)` of each `public.__reflex_{ins,del,upd,trunc}_trigger_on_*`
   function bound by a live trigger (`pg_trigger.tgfoid`) with the fresh install. The bodies
   embed only the source name and column list, so the same names give comparable bodies.
   Alternatively, on the live DB inside `BEGIN … ROLLBACK`: snapshot those md5s, call
   `reflex_rebuild_triggers('<schema>.<table>')` (qualified, so a bare public source still hits
   cause 3; compare the `_on_public_` functions then), and diff. This takes trigger locks on the
   source.
3. Upgrade logs: look for `could not rebuild triggers`, `reflex_rebuild_triggers(…) failed` or
   `could not heal passthrough scratch` NOTICEs and WARNINGs from past updates.

## Fix direction (not done)

- Do not call `reflex_rebuild_triggers` inside `ALTER EXTENSION`. Either rewrite the changed
  statement in place (the 1.11.5 approach), or ship a post-upgrade entry point that the operator
  runs in a normal session and that reports per-source success.
- Make the PS-6 scratch heal skip the `CREATE` when `to_regclass` finds the table, so a healthy
  source is not refused even if some caller still runs it in an extension script.
- Make `reflex_rebuild_triggers` name the trigger set from the same string that `create_reflex_ivm`
  used (the `depends_on` entry), or drop and replace the existing set for that table, so a bare
  public source regenerates its live triggers instead of adding an inert set. Ship a cleanup for
  the `*_on_public_*` duplicates already created.
- Fix the 1.11.5 migration's WARNING advice once the bare-name rebuild works.
