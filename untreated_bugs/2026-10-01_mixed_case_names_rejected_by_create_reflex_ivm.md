# `create_reflex_ivm` fails on three mixed-case name shapes — S3 (usability, loud)

Found while testing 1.11.5's dependent-safe rebuild with schema-qualified mixed-case names
(`pg_rbd_qualified_mixed_case_name` had to work around all three). Every failure is a loud
ERROR at create time; nothing is created half-way and no data is wrong. Tenants with
mixed-case schema or IMV names cannot use these shapes.

## Reproduction (1.11.5, pg17)
```sql
-- 1. Mixed-case schema
CREATE SCHEMA "Rbq"; CREATE TABLE "Rbq".src (k INT PRIMARY KEY, v INT);
SELECT create_reflex_ivm('Rbq.Up_V', 'SELECT k, v FROM "Rbq".src');
-- ERROR: schema "rbq" does not exist   (relation "rbq.src" once a schema rbq exists)

-- 2. Mixed-case IMV name with a unique key (explicit or PK-inferred), bare or qualified
CREATE TABLE mc_src (k INT PRIMARY KEY, v INT);
SELECT create_reflex_ivm('Mc_Keyed', 'SELECT k, v FROM mc_src', 'k');
-- ERROR: relation "mc_keyed" does not exist   (regclassin)
SELECT create_reflex_ivm('Mc_Pk', 'SELECT k, v FROM mc_src');
-- ERROR: relation "mc_pk" does not exist

-- 3. A schema-qualified mixed-case IMV used as a source
CREATE SCHEMA rbq; CREATE TABLE rbq.rel (product_id INT, location_id INT);
SELECT create_reflex_ivm('rbq.Up_V', 'SELECT product_id, location_id, COUNT(*) AS n FROM rbq.rel GROUP BY 1, 2');
SELECT create_reflex_ivm('rbq.dep_a', 'SELECT product_id, n FROM rbq."Up_V"');
-- ERROR: relation "rbq.up_v" does not exist   (RangeVarGetRelidExtended)
```
A bare mixed-case IMV as a source works (`SELECT ... FROM "Mc_Agg"`), keyed or not.

## Mechanism
- Case 2: `index_covers_prefix` (src/create_ivm/mod.rs:1494) casts the raw registry name,
  `'{view}'::regclass`, which folds `Mc_Keyed` to `mc_keyed`. Called from
  `install_secondary_key_indexes` (mod.rs:1535), i.e. for every passthrough IMV with a key
  mapping.
- Cases 1 and 3: some SQL built during create interpolates the registry name (`Rbq.Up_V`,
  `rbq.Up_V`) unquoted, so PostgreSQL folds it. Not pinned to a line; the error location is
  the parser's relation lookup, not a pg_reflex check.

## Fix direction
Everywhere a registry name reaches SQL, render it with the per-part quoting the registry
convention requires (`"Rbq"."Up_V"`), or resolve it once to an oid. Add a create test per
shape above with the `assert_imv_correct` oracle, and remove the workaround in
`pg_rbd_qualified_mixed_case_name`.
