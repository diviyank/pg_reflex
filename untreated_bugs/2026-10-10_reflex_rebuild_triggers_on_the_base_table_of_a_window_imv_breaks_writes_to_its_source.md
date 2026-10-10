# `reflex_rebuild_triggers` on the base table of a window IMV breaks every later write to its source

**Severity:** S2 (write-blocking, loud). **Versions:** observed on 1.12.0 pre-release; not checked on
1.11.5. The 1.10.x changelog describes the same hazard for sub-IMVs under wrappers, so it is
probably old.

## Reproduction

Observed while testing the upgrade notes on PostgreSQL 17. The exact fixture was a throwaway
test and is not committed; the shape is:

```sql
CREATE TABLE src (id SERIAL PRIMARY KEY, grp TEXT, val INT);
INSERT INTO src (grp, val) VALUES ('a', 1), ('a', 2), ('b', 5);
-- a window IMV: decomposed into a base sub-IMV `<imv>__base` and a view
SELECT create_reflex_ivm('zzd_lag',
    'SELECT grp, SUM(val) AS total, LAG(SUM(val)) OVER (ORDER BY grp) AS prev FROM src GROUP BY grp');
SELECT reflex_rebuild_triggers('zzd_lag__base');   -- returns: rebuilt 8 trigger DDL(s)
INSERT INTO src (grp, val) VALUES ('a', 3);
-- ERROR: aggregations for 'zzd_lag' must be valid JSON
--        (catalog invariant violated: missing field group_by_columns)
```

Expected: `reflex_rebuild_triggers` refuses a name that is not a source carrying reflex triggers
(or is a no-op for it), and writes to `src` keep working.

## What was ruled out

- The same INSERT passes when `reflex_rebuild_triggers` is called only for sources that already
  carry a `__reflex_trigger_%` trigger (the generator documented in the 1.12.0 upgrade notes).
- The view layer of a window IMV has `aggregations = {}` in the registry; the installed triggers
  treat it as an aggregate IMV.

## Why it matters

Any "rebuild triggers for every `depends_on` entry" recipe prints this call, because the view
layer of a window IMV depends on `<imv>__base`. The upgrade notes avoid it and warn about it.

## Fix direction (hypothesis)

`reflex_rebuild_triggers` should install triggers only for dependents that are maintained by
triggers (skip a registry row whose layer is a plain view over its base), or refuse the call with
a message naming the real source.
