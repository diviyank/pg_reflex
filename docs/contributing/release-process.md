# Release process

## Version bump

```bash
# 1. Update Cargo.toml version
sed -i 's/^version = ".*"$/version = "X.Y.Z"/' Cargo.toml

# 2. Add a CHANGELOG entry under [X.Y.Z] - YYYY-MM-DD
$EDITOR CHANGELOG.md

# 3. Add the migration file (if there are schema changes)
$EDITOR sql/pg_reflex--PREVIOUS--X.Y.Z.sql
```

## Migration file conventions

- Each migration is named `pg_reflex--FROM--TO.sql`.
- Inside, `CREATE OR REPLACE` for functions, `ALTER TABLE … ADD COLUMN IF NOT EXISTS` for columns. The migration must be idempotent.
- Lead the file with a comment block listing the themes and what changes operationally.
- When a new SPI lands, no manual SQL needed — pgrx schema generation handles registration.

## Pre-release checks

```bash
cargo fmt --check
cargo clippy --features pg17
cargo pgrx test pg17
cargo pgrx test pg18
./install.sh --release --pg-config $(which pg_config)
```

Smoke test on a 1.X.Y instance:

```sql
ALTER EXTENSION pg_reflex UPDATE TO 'X.Y.Z';
SELECT extversion FROM pg_extension WHERE extname = 'pg_reflex';
SELECT * FROM reflex_ivm_status();
```

## Tag and release

```bash
git tag -a vX.Y.Z -m "Release X.Y.Z"
git push origin vX.Y.Z
```

GitHub Actions (`.github/workflows/release.yml`) builds `.deb` artefacts for PG15 / PG16 / PG17 / PG18 on the tag push and attaches them to the GitHub release.

## Documentation

The docs site is auto-deployed from `main` by `.github/workflows/docs.yml`. Cut a docs PR alongside the release commit so the new SPIs / params show up in the API reference when the release tag is pushed.
