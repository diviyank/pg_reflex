# Seven places still match a source or an IMV by its name without the schema: work or reports of one schema can reach a same-named relation of `public` — S3 (extra work and locks, or a wrong line in a report; no wrong IMV content shown)

Found by reading, during the sweep that fixed the partition event trigger and the partition flush
(branch `fix/v112-partition-catalog-locks`, findings 5 and 6 of `p3-ac-review.md`). Those now use
`public.__reflex_reads_relation(depends_on, relation)`, which reads a spelling without a schema as
a relation of `public` and nothing else. The sites below still compare the part after the dot, so
`ta.v` also matches what is registered as `v`, which is `public.v`.

None of them is reproduced: each line states what the code would do, not what was observed.

| Site | What it matches | Effect across schemas |
|---|---|---|
| `src/reconcile.rs` `ignoring_dependents` (`depends_on && ARRAY[$1, $2]`, `$2` = bare name) | IMVs that read and ignore the IMV being reconciled | a reconcile of `ta.v` also reconciles IMVs of `public` that read and ignore `public.v`: extra work and locks in `public` |
| `src/create_ivm/admin.rs` `rebuild_triggers_named` (`$2 = ANY(depends_on)` and `split_qualified_name(dep).1 == bare_source`) | passthrough IMVs whose scratch tables are recreated | `reflex_rebuild_triggers('ta.t')` recreates the scratch tables of passthrough IMVs over `public.t` |
| `src/create_ivm/admin.rs` chain lookup (`name = $1 OR name = split_part($1, '.', 2)`) | the IMV whose chain is walked | `ta.v` also starts from a `public` IMV registered as `v` |
| `src/heal.rs` reach query (`ARRAY[o.origin, split_part(o.origin, '.', 2), 'public.' \|\| o.origin]`, twice) | IMVs downstream of a healed one | IMVs of `public` are listed as reached by a heal in `ta` |
| `src/introspect.rs` suspended-root reach (`ARRAY[c.source_root, split_part(c.source_root, '.', 2)]`, and the IMV-over-IMV step) | IMVs behind a suspended partition root | `reflex_ivm_status()` reports IMVs over `public.sim` as suspended when `ta.sim` is |
| `src/doctor.rs` pending-root query (`depends_on @> ARRAY[split_part(p.source_root, '.', 2)]`) | the oldest stale IMV of a pending root | `wedged_since` of `ta.sim` can come from an IMV of `public` |
| `src/lib.rs` `__reflex_on_ddl_command_end`, reconcile-root test (`split_part(_imv.name, '.', 2) = split_part(_reconcile_root, '.', 2)`) | consumers whose alter-source warning is suppressed during a reconcile | two names without a schema both give `''`, so the warning is suppressed for every consumer registered without a schema while any such IMV reconciles |

## Why they were left

- The first is on the reconcile cascade another branch is rewriting, and a narrower match there
  fails toward a stale IMV: it needs its own test with an IMV that reads and ignores a same-named
  IMV in two schemas before it is narrowed.
- The second and third are operator calls; the fourth to sixth are reports. Changing them without
  a test that shows the wrong line first would be an unproven change.
- The last one only decides whether a warning is printed; under
  `pg_reflex.alter_source_policy = 'error'` a narrower test can abort a reconcile.

## Fix direction

Use `public.__reflex_reads_relation(registered, relation)` (bootstrap SQL) or
`crate::reconcile::registered_spellings_of` (Rust) at each site, one at a time, each with a
two-schema fixture (same names in `public` and in another schema) that shows the cross-schema
effect before and its absence after. An IMV registered without a schema but created under another
`target_schema` must keep matching: compare with `target_schema` as the event trigger does for the
IMV-owned-root test.
