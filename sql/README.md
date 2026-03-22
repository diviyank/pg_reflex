# Extension Migration Files

This directory holds **migration SQL files** for upgrading pg_reflex between versions.

## How PostgreSQL extension upgrades work

When a user runs:
```sql
ALTER EXTENSION pg_reflex UPDATE TO '1.1.0';
```

PostgreSQL looks for a file named `pg_reflex--1.0.0--1.1.0.sql` in its extension directory. This file contains only the **delta** between versions (ALTER TABLE, CREATE OR REPLACE FUNCTION, etc.).

pgrx auto-generates the full install file (`pg_reflex--1.1.0.sql`) for fresh installs. Migration files must be created **manually**.

## When you need a migration file

- **Schema changes** to `__reflex_ivm_reference` (new columns, type changes)
- **Function signature changes** (new parameters, different return types)
- **New functions** added to the public API

You do NOT need a migration file for:
- Bug fixes that don't change function signatures
- Internal logic changes (the `.so` binary is replaced on disk, PostgreSQL picks it up)
- Changes to `AggregationPlan` JSON fields (handled by `#[serde(default)]`)

## How to create a migration file

Use the helper script:
```bash
./sql/generate_migration.sh 1.0.0 1.1.0
```

This creates `sql/pg_reflex--1.0.0--1.1.0.sql` with a stub containing all current function definitions. Edit it to keep only what changed.

### Manual approach

1. Create the file: `sql/pg_reflex--OLD--NEW.sql`
2. Add the necessary SQL (examples below)
3. The release workflow automatically includes it in the `.deb` package

### Common migration patterns

**Add a column to the reference table:**
```sql
ALTER TABLE public.__reflex_ivm_reference
    ADD COLUMN IF NOT EXISTS new_column TEXT;
```

**Update a function** (pgrx functions use CREATE OR REPLACE):
```sql
-- Copy the function definition from the pgrx-generated pg_reflex--NEW.sql file
CREATE OR REPLACE FUNCTION create_reflex_ivm(view_name TEXT, sql TEXT) RETURNS TEXT
    LANGUAGE c
    AS 'MODULE_PATHNAME', 'create_reflex_ivm_wrapper';
```

## File naming convention

```
pg_reflex--1.0.0--1.1.0.sql    (1.0.0 → 1.1.0)
pg_reflex--1.1.0--1.2.0.sql    (1.1.0 → 1.2.0)
```

PostgreSQL can chain migrations: upgrading from 1.0.0 to 1.2.0 runs both files in sequence.
