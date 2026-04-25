# `drop_reflex_ivm`

Drops an IMV and all artifacts (target, intermediate, affected-groups, scratch tables, triggers, registry row).

## Signatures

```sql
drop_reflex_ivm(view_name TEXT) RETURNS TEXT
drop_reflex_ivm(view_name TEXT, cascade BOOLEAN) RETURNS TEXT
```

Returns `'DROP REFLEX INCREMENTAL VIEW'`.

## Without cascade

Refuses if the IMV has children (other IMVs that depend on it):

```sql
SELECT drop_reflex_ivm('daily_totals');
-- ERROR: IMV has children. Use drop_reflex_ivm(name, true) to cascade.
```

## With cascade

Recursively drops all child IMVs first:

```sql
SELECT drop_reflex_ivm('daily_totals', true);
```

## Source DROP TABLE

Since 1.2.0, the `reflex_on_sql_drop` event trigger automatically calls `drop_reflex_ivm(name, true)` for every IMV whose source table is being dropped. No manual cleanup needed.
