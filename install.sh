#!/usr/bin/env bash
#
# Install pg_reflex: builds the extension and copies migration files.
# Usage: ./install.sh [--release] [--pg-config /path/to/pg_config]
#
set -euo pipefail

RELEASE=""
PG_CONFIG="$(which pg_config)"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --release|-r) RELEASE="--release"; shift ;;
        --pg-config|-c) PG_CONFIG="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# 1. Build and install via pgrx
cargo pgrx install $RELEASE --pg-config "$PG_CONFIG"

# 2. Copy migration files (if any exist)
EXT_DIR="$("$PG_CONFIG" --sharedir)/extension"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

shopt -s nullglob
MIGRATIONS=("$SCRIPT_DIR"/sql/pg_reflex--*--*.sql)
shopt -u nullglob

if [ ${#MIGRATIONS[@]} -gt 0 ]; then
    echo "  Installing ${#MIGRATIONS[@]} migration file(s) to $EXT_DIR"
    cp "${MIGRATIONS[@]}" "$EXT_DIR/"
fi
