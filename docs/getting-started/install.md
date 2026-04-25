# Installation

pg_reflex requires **PostgreSQL 15, 16, 17, or 18** (the `MERGE` statement requires PG 15+).

## Option A: prebuilt `.deb` package

Download the package matching your PostgreSQL version from the [GitHub releases page](https://github.com/diviyank/pg_reflex/releases):

```bash
wget https://github.com/diviyank/pg_reflex/releases/download/VERSION/pg-reflex-VERSION-pg17-amd64.deb
sudo dpkg -i pg-reflex-VERSION-pg17-amd64.deb
```

Enable in your database:

```sql
CREATE EXTENSION pg_reflex;
```

## Option B: from source

```bash
git clone https://github.com/diviyank/pg_reflex.git
cd pg_reflex

# 1. Rust toolchain (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# 2. cargo-pgrx
cargo install cargo-pgrx --version '=0.18.0' --locked

# 3. Pull PG headers for your version (downloads from postgresql.org)
cargo pgrx init --pg17 download

# 4. Build & install into your PG instance
./install.sh --release --pg-config $(which pg_config)
```

Then in your database:

```sql
CREATE EXTENSION pg_reflex;
```

!!! warning "Permissions"
    If you get a `Permission denied` error during `install.sh`, the PostgreSQL extension directories need to be writable. On a dev machine:

    ```bash
    sudo chown -R $USER /usr/share/postgresql/extension/ /usr/lib/postgresql/*/lib/
    ```

## Verify

```sql
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_reflex';
```

Should return a single row with the version you just installed.

[Build your first IMV :material-arrow-right-bold:](first-imv.md){ .md-button .md-button--primary }
