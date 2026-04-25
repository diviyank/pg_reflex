# Multi-tenant guards

pg_reflex is **admin-facing by design.** `create_reflex_ivm` and `drop_reflex_ivm` accept user-supplied SQL that gets parsed and (after validation) interpolated into trigger bodies. In a multi-tenant deployment, exposing these SPIs to untrusted users is a SQL-injection vector.

## What's validated

- `view_name` — `validate_view_name` rejects anything outside `[A-Za-z0-9_.]` and rejects names starting with a digit, leading/trailing/double dots, or empty strings.
- `sql` — parsed via [sqlparser](https://github.com/apache/datafusion-sqlparser-rs); rejected if it isn't a single SELECT statement.
- `unique_columns` — split on `,` and lowercased; column names are quoted at SQL-build time.
- `storage`, `mode` — uppercased and matched against an allowed set.

## What's not

The user SQL body itself is interpolated into trigger bodies. A clever user with `create_reflex_ivm` privileges can craft a SELECT that references catalogs they otherwise wouldn't have access to. **Treat `create_reflex_ivm` as equivalent to `CREATE FUNCTION`-level privilege.**

## Recommended deployment

Gate `create_reflex_ivm` / `drop_reflex_ivm` / `reflex_rebuild_imv` behind your own RPC layer:

```sql
-- Revoke from PUBLIC
REVOKE EXECUTE ON FUNCTION create_reflex_ivm FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION drop_reflex_ivm FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION reflex_reconcile FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION reflex_rebuild_imv FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION reflex_scheduled_reconcile FROM PUBLIC;

-- Grant only to your service role
GRANT EXECUTE ON FUNCTION create_reflex_ivm TO reflex_admin;
```

Tenants get to read from IMV tables and call `reflex_ivm_status` / `reflex_ivm_stats` / `reflex_ivm_histogram` (all read-only). Creation/drop/reconcile go through your validated RPC.

## Why not bake input sanitisation deeper?

The CLAUDE.md project rule prioritises **simplicity > performance > features** when it comes to the extension's surface area. Pushing tenant-facing validation into pg_reflex itself would couple the extension to a specific authz model. The boundary stays at the SQL function grant — flexible, deployable behind any RPC layer.

[See audit risk R8 :material-arrow-right-bold:](deployment-profile.md){ .md-button }
