# Sources `s.t` and `s_t` share their trigger names and functions; the IMVs of one are never maintained — S1 (SILENT wrong result)

Met while testing the drop hook in the 1.12.0 fix round 3 (pg17). Not introduced there:
`source_trigger_suffix` (`src/schema_builder.rs`) is `source.replace('.', "_")` without the
quotes. Not checked on 1.11.5.

## Reproduction (pg17, one transaction; the same with both IMVs DEFERRED and flushed)

```sql
CREATE SCHEMA zzs;
CREATE TABLE zzs.t(id INT PRIMARY KEY, v INT);
CREATE TABLE zzs_t(id INT PRIMARY KEY, v INT);
SELECT create_reflex_ivm('a', 'SELECT id, v FROM zzs.t');
SELECT create_reflex_ivm('b', 'SELECT id, v FROM zzs_t');
INSERT INTO zzs.t VALUES (1, 1);
SELECT count(*) FROM a;      -- 0; zzs.t holds 1 row
```

No error. `b` is maintained. After the second create, `pg_trigger` listed the four reflex
triggers on `zzs_t` only.

## What is established and what is not

Established: `a` is stale after every write to `zzs.t`, IMMEDIATE and DEFERRED, when `b` was
created after it. Not established: the reverse creation order; whether dropping `b` drops the
functions `a` needs; what `reflex_audit` / `reflex_doctor` say. A schema `alp` with a table
`product` beside a public table `alp_product` is an ordinary layout, so this is not exotic.

## Fix direction (hypothesis to falsify)

Make the suffix injective: keep the bare form for a public table and add a hash of the exact
registered spelling for anything schema-qualified (as `source_trigger_function_name` already
does for upper case). The names are stored in deployed databases, so `drop_reflex_ivm`, the
rebuild paths and the audit must keep finding the old names: this needs a migration or a
dual-name lookup, and is not a one-line change.
