#!/usr/bin/env bash
#
# Generate a migration stub for pg_reflex
# Usage: ./sql/generate_migration.sh <old_version> <new_version>
#
# Creates sql/pg_reflex--OLD--NEW.sql with function definitions
# extracted from the pgrx-generated install file.

set -euo pipefail

OLD_VERSION="${1:?Usage: $0 <old_version> <new_version>}"
NEW_VERSION="${2:?Usage: $0 <old_version> <new_version>}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT="${SCRIPT_DIR}/pg_reflex--${OLD_VERSION}--${NEW_VERSION}.sql"

if [ -f "$OUTPUT" ]; then
    echo "Error: $OUTPUT already exists"
    exit 1
fi

# Find the pgrx-generated install file for the new version
INSTALL_SQL=$(find ~/.pgrx -name "pg_reflex--${NEW_VERSION}.sql" -print -quit 2>/dev/null)

cat > "$OUTPUT" <<EOF
-- Migration: pg_reflex ${OLD_VERSION} → ${NEW_VERSION}
--
-- Run via: ALTER EXTENSION pg_reflex UPDATE TO '${NEW_VERSION}';
--
-- Instructions:
-- 1. Review and edit this file to include ONLY what changed
-- 2. For schema changes: use ALTER TABLE ADD COLUMN IF NOT EXISTS
-- 3. For function changes: copy CREATE OR REPLACE FUNCTION from the
--    pgrx-generated file (pg_reflex--${NEW_VERSION}.sql)
-- 4. Delete this comment block when done

-- === Schema changes ===
-- Example:
-- ALTER TABLE public.__reflex_ivm_reference
--     ADD COLUMN IF NOT EXISTS new_column TEXT;

-- === Function updates ===
-- Copy changed function definitions from:
EOF

if [ -n "$INSTALL_SQL" ]; then
    echo "-- ${INSTALL_SQL}" >> "$OUTPUT"
    echo "--" >> "$OUTPUT"
    echo "-- Functions defined in that file:" >> "$OUTPUT"
    grep -oP 'CREATE OR REPLACE FUNCTION \K[^(]+' "$INSTALL_SQL" 2>/dev/null \
        | sort -u \
        | while read -r fname; do
            echo "--   $fname" >> "$OUTPUT"
        done
else
    echo "-- (pgrx install file not found — run 'cargo pgrx install' first)" >> "$OUTPUT"
fi

echo "" >> "$OUTPUT"
echo "Created: $OUTPUT"
echo ""
echo "Next steps:"
echo "  1. Edit the file to include only the changes between ${OLD_VERSION} and ${NEW_VERSION}"
echo "  2. Test with: ALTER EXTENSION pg_reflex UPDATE TO '${NEW_VERSION}';"
echo "  3. Commit the migration file"
