#!/usr/bin/env bash
# ============================================================================
# Build and install pg_reflex from an arbitrary commit, attributably.
# ============================================================================
#
#     ./benchmarks/build_at_commit.sh <commit-ish> [expected_so_sha16]
#
# Exports the commit to a fixed scratch path, builds it, installs it into the
# cluster named by PGCONFIG, and prints the sha256 prefix of the installed
# shared library.  Pair it with bench_partition_scaling.sh --label <commit> so
# every number is traceable to a build:
#
#     ./benchmarks/build_at_commit.sh 2f8b786
#     PGBIN=/opt/homebrew/opt/postgresql@17/bin \
#       ./benchmarks/bench_partition_scaling.sh --label 2f8b786
#
# TWO TRAPS THIS EXISTS TO CLOSE
#
# 1. `git archive` stamps extracted files with the COMMIT time, which is older
#    than the artifact already sitting in CARGO_TARGET_DIR.  Cargo then judges
#    the crate fresh, skips the rebuild, and `cargo pgrx install` cheerfully
#    reinstalls the PREVIOUS commit's shared library while reporting success.
#    Every benchmark number afterwards is attributed to the wrong commit.  The
#    fix is the `touch` below; the guard is the sha comparison.
#
# 2. A debug build embeds its source path, so the same commit built from two
#    different directories yields two different sha256 values.  Everything is
#    therefore built from ONE fixed path, which makes "same sha" mean "same
#    commit" and "different sha" mean "a real rebuild happened".
#
# Environment:
#   PGCONFIG           pg_config of the target cluster
#                      (default: homebrew postgresql@17)
#   CARGO_TARGET_DIR   default /private/tmp/rfx-bench — keep it SHORT (a long
#                      path breaks PostgreSQL's 103-byte socket limit elsewhere
#                      in this repo's tooling) and private to your session, so
#                      a concurrent `cargo pgrx test` cannot share fingerprints
#                      with it.
#
# Do NOT point PGCONFIG at ~/.pgrx/<ver>/pgrx-install while another session is
# running `cargo pgrx test`: they share one install prefix and would overwrite
# each other's .so.
# ============================================================================
set -euo pipefail

SHA="${1:?usage: $0 <commit-ish> [expected_so_sha16]}"
EXPECT="${2:-}"

REPO="$(git -C "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" rev-parse --show-toplevel)"
SRC="${RFX_BUILD_SRC:-/private/tmp/rfxsrc}"
PGCONFIG="${PGCONFIG:-/opt/homebrew/opt/postgresql@17/bin/pg_config}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/private/tmp/rfx-bench}"
export CARGO_INCREMENTAL=0

DYLIB="$("$PGCONFIG" --pkglibdir)/pg_reflex.dylib"
[ -f "$DYLIB" ] || DYLIB="$("$PGCONFIG" --pkglibdir)/pg_reflex.so"

echo "=== disk ==="
df -h "$(dirname "$CARGO_TARGET_DIR")" | tail -1

BEFORE="$( [ -f "$DYLIB" ] && shasum -a 256 "$DYLIB" | cut -c1-16 || echo none )"
echo "installed .so before: $BEFORE"

rm -rf "$SRC"
mkdir -p "$SRC"
git -C "$REPO" archive "$SHA" | tar -x -C "$SRC"
find "$SRC" -type f -exec touch {} +
echo "=== exported $(git -C "$REPO" rev-parse "$SHA") -> $SRC ==="

cd "$SRC"
cargo pgrx install --pg-config "$PGCONFIG" --no-default-features --features pg17 2>&1 \
    | grep -iE 'Compiling pg_reflex|Copying shared library|^error|Finished installing'

AFTER="$(shasum -a 256 "$DYLIB" | cut -c1-16)"
echo "installed .so after:  $AFTER"

if [ -n "$EXPECT" ] && [ "$AFTER" != "$EXPECT" ]; then
    echo "FAIL: expected .so sha $EXPECT, got $AFTER" >&2
    exit 1
fi
if [ -z "$EXPECT" ] && [ "$AFTER" = "$BEFORE" ]; then
    echo "FAIL: the installed .so is byte-identical to the one already there." >&2
    echo "      Either this commit does not differ from the last one in src/," >&2
    echo "      or cargo skipped the rebuild — in which case the next benchmark" >&2
    echo "      run would be attributed to the wrong commit.  Investigate before" >&2
    echo "      trusting any number produced against it." >&2
    exit 1
fi

echo "=== disk ==="
df -h "$(dirname "$CARGO_TARGET_DIR")" | tail -1
