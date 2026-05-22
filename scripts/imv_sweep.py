#!/usr/bin/env python3
"""Differential migration sweep for pg_reflex.

For every materialized view discovered in the base-db-anchor-evm SQL registry,
build a pg_reflex IMV from the SAME body and diff it row-for-row against the
existing materialized view on db_clone. Surfaces the same class of correctness
bugs the in-CI fuzz harness finds (docs/fuzz-findings.md), but on REAL view
shapes.

This is an EXTERNAL, MANUAL tool — not part of `cargo pgrx test`. It needs a
live Postgres with pg_reflex installed and the real views present.

Usage:
    python3 scripts/imv_sweep.py --dsn 'host=localhost dbname=db_clone user=postgres'
    python3 scripts/imv_sweep.py --dsn '...' --sql-dir /path/to/base_db/sql

Per-view status:
    PASS         IMV contents identical to the MV.
    LIMITATION   pg_reflex cleanly rejected the shape (tagged [reflex-unsupported]).
    CODEGEN-BUG  Postgres raised while building/maintaining the IMV (generated SQL).
    DIVERGED     IMV built but its contents differ from the MV.

Every CODEGEN-BUG / DIVERGED row is a real finding: minimize it into an
#[ignore]'d #[pg_test] in src/tests/pg_test_fuzz.rs (mod findings) and a
docs/fuzz-findings.md entry, then fix on a branch.

All work runs inside a savepoint and is rolled back — the script leaves no
artifacts in the database.
"""
import argparse
import glob
import os
import re
import sys

try:
    import psycopg2
except ImportError:
    sys.exit("psycopg2 is required: pip install psycopg2-binary")

DEFAULT_SQL_DIR = "/home/diviyan/fentech/algorithm/api/base-db-anchor-evm/base_db/sql"
UNSUPPORTED_TAG = "[reflex-unsupported]"

# NOTE: this regex extracts `CREATE MATERIALIZED VIEW <name> AS <body>;` blocks.
# The real registry layout may differ (templated DDL, IF NOT EXISTS, schema
# qualification, trailing WITH DATA/indexes). Refine on first contact — print
# what it finds with --list and adjust.
MV_RE = re.compile(
    r"create\s+materialized\s+view\s+(?:if\s+not\s+exists\s+)?([\w\.\"]+)\s+as\s+(.*?);",
    re.IGNORECASE | re.DOTALL,
)


def discover_views(sql_dir):
    views = []
    for path in sorted(glob.glob(os.path.join(sql_dir, "**", "*.sql"), recursive=True)):
        try:
            text = open(path, encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        for m in MV_RE.finditer(text):
            name = m.group(1).strip()
            body = m.group(2).strip()
            # strip a trailing WITH [NO] DATA clause that is not part of the SELECT
            body = re.sub(r"\s+with\s+(no\s+)?data\s*$", "", body, flags=re.IGNORECASE)
            views.append((name, body, path))
    return views


def short_name(qualified):
    return qualified.split(".")[-1].strip('"')


def diff_count(cur, mv, imv):
    cur.execute(
        f"SELECT count(*) FROM ("
        f"  (SELECT * FROM {mv} EXCEPT SELECT * FROM {imv}) UNION ALL "
        f"  (SELECT * FROM {imv} EXCEPT SELECT * FROM {mv})) d"
    )
    return cur.fetchone()[0]


def sweep(dsn, sql_dir, unique_key):
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    report = []
    views = discover_views(sql_dir)
    if not views:
        print(f"No materialized views found under {sql_dir} — refine discover_views().")
        return 0
    for name, body, _path in views:
        imv = f"_sweep_{short_name(name)}"
        status, detail = "PASS", ""
        with conn.cursor() as cur:
            try:
                cur.execute("SAVEPOINT sw")
                cur.execute(f"DROP TABLE IF EXISTS {imv} CASCADE")
                # unique_key heuristic: caller-supplied, else first column of the MV
                key = unique_key
                if key is None:
                    cur.execute(f"SELECT * FROM {name} LIMIT 0")
                    key = cur.description[0].name
                cur.execute("SELECT create_reflex_ivm(%s, %s, %s)", (imv, body, key))
                msg = cur.fetchone()[0]
                if UNSUPPORTED_TAG in msg:
                    status, detail = "LIMITATION", msg.split(UNSUPPORTED_TAG, 1)[-1].strip()
                elif not msg.startswith("CREATE REFLEX"):
                    status, detail = "CODEGEN-BUG", f"unexpected return: {msg}"
                else:
                    d = diff_count(cur, name, imv)
                    status, detail = ("PASS", "") if d == 0 else ("DIVERGED", f"{d} rows")
            except psycopg2.Error as e:
                status = "CODEGEN-BUG"
                detail = str(e).splitlines()[0]
            finally:
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT sw")
                except psycopg2.Error:
                    conn.rollback()
        report.append((name, status, detail))
        conn.rollback()

    for name, status, detail in sorted(report, key=lambda r: r[1]):
        print(f"{status:12} {name:55} {detail}")
    findings = [r for r in report if r[1] in ("CODEGEN-BUG", "DIVERGED")]
    print(f"\n{len(findings)} finding(s), {len(report)} views swept")
    return 1 if findings else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dsn", required=True, help="psycopg2 connection string")
    ap.add_argument("--sql-dir", default=DEFAULT_SQL_DIR, help="dir of *.sql view definitions")
    ap.add_argument(
        "--unique-key",
        default=None,
        help="unique key column for create_reflex_ivm (default: first MV column)",
    )
    args = ap.parse_args()
    sys.exit(sweep(args.dsn, args.sql_dir, args.unique_key))


if __name__ == "__main__":
    main()
