#!/usr/bin/env bash
# ============================================================================
# pg_reflex — partition-count scaling benchmark driver
# ============================================================================
#
# Sweeps N = number of partitions and reports, per metric, whether cost is
# FLAT, LINEAR or SUPERLINEAR in N — with the arithmetic that justifies the
# call, not just raw totals.
#
# WHY IT IS BUILT THIS WAY
#
# The fixture keeps total data constant while N varies (see the header of
# bench_partition_scaling.sql).  So for every metric the log-log slope
#
#     e = d(log time) / d(log N)
#
# is a direct read-out of the per-child cost shape:  e ~ 0 constant,
# e ~ 1 linear in the number of children, e ~ 2 quadratic.  Doubling N and
# seeing time quadruple is e = 2 and is printed as such.
#
# Each N runs in its own freshly created database.  pg_reflex keeps process-
# and catalog-level state (the pending queue, the source-partition snapshot,
# the IMV registry), and reusing one database across N leaks that state into
# the next point and destroys comparability.
#
# USAGE
#
#     ./benchmarks/bench_partition_scaling.sh --label <commit-ish> [options]
#
#   --label  <s>   identifies the build under test; goes in the results file
#                  name.  Use the commit the loaded .so was built from.
#   --n-list <s>   partition counts to sweep   (default "10 25 50 100 200")
#   --roots  <s>   root counts for the subxid metric (default "10 25 50 100")
#   --rows   <n>   total source rows, held CONSTANT across N (default 20000)
#   --reps   <n>   repetitions per measurement (default 6); one extra warm-up
#                  repetition is always run and discarded
#
# NOISE
#
# Every measurement reports the MINIMUM over its repetitions, plus a median and
# a coefficient of variation so the reader can see how trustworthy the column
# is.  The verdicts are within-run comparisons across N, which is what makes
# them survive a busy machine: background load inflates every N in a sweep,
# but it does not change the SHAPE of the curve.  Comparing absolute times
# BETWEEN two labels does require a comparably loaded machine — check the CV
# column before reading much into a small between-label difference.
#   --skip-subxid  omit the subtransaction-XID sweep
#   --out    <f>   results TSV (default benchmarks/results_partition_scaling_<label>.tsv)
#
# SERVER PREREQUISITE
#
#     max_locks_per_transaction >= 2048
#
# A full reflex_reconcile of a partitioned IMV holds several locks per leaf
# (source child, intermediate child, target child, and the swap tables), all to
# end of transaction.  At the PostgreSQL default of 64 it runs out of shared
# memory somewhere between N=100 and N=200 and the sweep loses its top point:
#
#     ALTER SYSTEM SET max_locks_per_transaction = 2048;   -- then restart
#
# This is a benchmark prerequisite, not a workaround: any deployment running
# hundreds of partitions has to raise it for PostgreSQL's own sake.
#
# Connection is taken from PSQL_BIN / PGBIN / standard libpq environment
# variables:
#
#     PGBIN=/opt/homebrew/opt/postgresql@17/bin ./benchmarks/bench_partition_scaling.sh --label abc1234
#
# GUARDING A RELEASE — the whole point of the script
#
#     git checkout <baseline>
#     cargo pgrx install --pg-config "$PGBIN/pg_config" --no-default-features --features pg17
#     ./benchmarks/bench_partition_scaling.sh --label baseline
#
#     git checkout <candidate>
#     cargo pgrx install --pg-config "$PGBIN/pg_config" --no-default-features --features pg17
#     ./benchmarks/bench_partition_scaling.sh --label candidate
#
#     ./benchmarks/bench_partition_scaling.sh --compare baseline candidate
#
# Do NOT use `cargo pgrx install` against ~/.pgrx while another session runs
# `cargo pgrx test` — they share one install prefix and will overwrite each
# other's .so, making every number unattributable.  Point --pg-config at a
# separate cluster.
# ============================================================================

set -euo pipefail

LABEL=""
N_LIST="10 25 50 100 200"
ROOT_LIST="10 25 50 100"
TOTAL_ROWS=20000
REPS=6
SKIP_SUBXID=0
OUT=""
COMPARE_A=""
COMPARE_B=""
DB_PREFIX="${DB_PREFIX:-rfxscale}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

while [ $# -gt 0 ]; do
    case "$1" in
        --label)        LABEL="$2"; shift 2 ;;
        --n-list)       N_LIST="$2"; shift 2 ;;
        --roots)        ROOT_LIST="$2"; shift 2 ;;
        --rows)         TOTAL_ROWS="$2"; shift 2 ;;
        --reps)         REPS="$2"; shift 2 ;;
        --out)          OUT="$2"; shift 2 ;;
        --skip-subxid)  SKIP_SUBXID=1; shift ;;
        --compare)      COMPARE_A="$2"; COMPARE_B="$3"; shift 3 ;;
        -h|--help)      sed -n '2,70p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

PGBIN="${PGBIN:-}"
if [ -n "$PGBIN" ]; then
    PSQL="${PGBIN}/psql"; CREATEDB="${PGBIN}/createdb"; DROPDB="${PGBIN}/dropdb"
    PGCONFIG="${PGBIN}/pg_config"
else
    PSQL="$(command -v psql)"; CREATEDB="$(command -v createdb)"; DROPDB="$(command -v dropdb)"
    PGCONFIG="$(command -v pg_config)"
fi

# --------------------------------------------------------------------------
# report(): the whole analysis lives here so --compare can reuse it.
# stdin is the raw TSV: metric <TAB> n <TAB> rep <TAB> ms
# --------------------------------------------------------------------------
report() {
    awk -F'\t' -v unit="${1:-ms}" -v noise="${2:-1}" '
    function stat(key, what,   i, c, tmp, t, j, s, ss, mu) {
        c = 0
        for (i = 1; i <= cnt[key]; i++) tmp[c++] = val[key SUBSEP i]
        if (c == 0) return -1
        for (i = 1; i < c; i++) { t = tmp[i]; j = i - 1
            while (j >= 0 && tmp[j] > t) { tmp[j+1] = tmp[j]; j-- }
            tmp[j+1] = t }
        if (what == "min") return tmp[0]
        if (what == "median") {
            if (c % 2) return tmp[int(c/2)]
            return (tmp[c/2 - 1] + tmp[c/2]) / 2
        }
        s = 0; for (i = 0; i < c; i++) s += tmp[i]
        mu = s / c
        if (mu <= 0) return 0
        ss = 0; for (i = 0; i < c; i++) ss += (tmp[i] - mu) ^ 2
        return 100 * sqrt(ss / c) / mu
    }
    # Growth (largest N relative to the cheapest point) is checked alongside the
    # fitted slope because a curve that is flat at small N and then climbs fits a
    # deceptively low slope.  The 1.11.1 flush regression landed exactly there:
    # e = 0.55 but a 8x rise from its cheapest point.  A row cannot be called
    # FLAT while it grows by more than 60%.
    function verdict(e, growth) {
        if (e == "na")                     return "unclear"
        if (e < 0.35 && growth < 1.6)      return "FLAT"
        if (e < 0.35)                      return "rising"
        if (e < 1.35)                      return "linear"
        if (e < 1.75)                      return "SUPERLINEAR"
        return "QUADRATIC+"
    }
    {
        m = $1; n = $2 + 0; ms = $4 + 0
        key = m SUBSEP n
        cnt[key]++
        val[key SUBSEP cnt[key]] = ms
        if (!(m in seenm)) { seenm[m] = ++nm; morder[nm] = m }
        if (!(n in seenn)) { seenn[n] = 1; nn++; nlist[nn] = n }
    }
    END {
        for (i = 1; i < nn; i++) for (j = i + 1; j <= nn; j++)
            if (nlist[j] < nlist[i]) { t = nlist[i]; nlist[i] = nlist[j]; nlist[j] = t }

        printf "\n"
        if (unit == "ms") {
            printf "MEDIAN TIME (ms) BY PARTITION COUNT N   [total data held CONSTANT across N]\n"
            printf "  The MEDIAN, not the minimum.  A regression can be a shift in the DISTRIBUTION\n"
            printf "  rather than in the floor: when a predicate stops being prunable, the first few\n"
            printf "  executions still get a custom plan that prunes on actual values and\n"
            printf "  stay fast, while later generic-plan executions do not.  The minimum then picks\n"
            printf "  the lucky rep and reports no regression at all.  Measured: on the 1.11.1 flush\n"
            printf "  regression at N=200 every rep was worse than every rep of the fixed build, but\n"
            printf "  min showed 14.0 vs 4.5 ms where median showed 53.2 vs 4.9 ms.\n"
            printf "  The min and the CV are in the NOISE CHECK table below.\n"
        } else {
            printf "LOCK FOOTPRINT (locks held at end of the call) BY PARTITION COUNT N\n"
            printf "  A pure count: no timer, no cache, no competing process can move it.  This is\n"
            printf "  the metric to trust when the machine is busy, and the one operators must size\n"
            printf "  max_locks_per_transaction against.\n"
        }
        printf "  slope e = d(log y)/d(log N), least squares over all N.\n"
        printf "  e ~ 0 constant in N   e ~ 1 linear in N   e ~ 2 quadratic in N\n\n"
        printf "%-26s", "metric"
        for (i = 1; i <= nn; i++) printf "%12s", "N=" nlist[i]
        printf "%9s%9s  %s\n", "slope e", "growth", "verdict"
        printf "%-26s", ""
        for (i = 1; i <= nn; i++) printf "%12s", "--------"
        printf "%9s%9s  %s\n", "-------", "------", "-------"

        for (mi = 1; mi <= nm; mi++) {
            m = morder[mi]
            printf "%-26s", m
            sx = 0; sy = 0; sxx = 0; sxy = 0; np = 0; lo = 0; top = 0
            for (i = 1; i <= nn; i++) {
                v = stat(m SUBSEP nlist[i], (unit == "ms") ? "median" : "min")
                if (v < 0) { printf "%12s", "-"; continue }
                printf "%12.2f", v
                best[m, nlist[i]] = v
                if (lo == 0 || v < lo) lo = v
                top = v
                if (v > 0) { x = log(nlist[i]); y = log(v)
                             sx += x; sy += y; sxx += x*x; sxy += x*y; np++ }
            }
            # Growth is anchored at the LARGEST N, not at whichever point happened
            # to be highest: a single noisy mid-sweep N would otherwise veto a FLAT
            # verdict, and a genuine trend in N always peaks at the largest N anyway.
            g = (lo > 0) ? top / lo : 0
            if (np >= 2 && (np*sxx - sx*sx) != 0) {
                e = (np*sxy - sx*sy) / (np*sxx - sx*sx)
                printf "%9.2f%8.1fx  %s\n", e, g, verdict(e, g)
            } else printf "%9s%8.1fx  %s\n", "na", g, "unclear"
        }

        printf "\n"
        printf "SUCCESSIVE-N RATIOS   time(N2)/time(N1)  and the local slope it implies\n"
        printf "  \"doubled when N doubled\" is 2.00x / 1.00 ; \"quadrupled\" is 4.00x / 2.00\n"
        printf "%-26s", "metric"
        for (i = 2; i <= nn; i++) printf "%16s", nlist[i-1] "->" nlist[i]
        printf "\n"
        for (mi = 1; mi <= nm; mi++) {
            m = morder[mi]
            printf "%-26s", m
            for (i = 2; i <= nn; i++) {
                a = best[m, nlist[i-1]]; b = best[m, nlist[i]]
                if (a > 0 && b > 0)
                    printf "%9.2fx %5.2f", b/a, log(b/a) / log(nlist[i]/nlist[i-1])
                else printf "%16s", "-"
            }
            printf "\n"
        }

        printf "\n"
        printf "COST PER PARTITION (%s / N)   falling row = sublinear; flat row = linear; rising row = SUPERLINEAR\n", unit
        printf "%-26s", "metric"
        for (i = 1; i <= nn; i++) printf "%12s", "N=" nlist[i]
        printf "\n"
        for (mi = 1; mi <= nm; mi++) {
            m = morder[mi]
            printf "%-26s", m
            for (i = 1; i <= nn; i++) {
                v = best[m, nlist[i]]
                if (v > 0) printf "%12.4f", v / nlist[i]
                else printf "%12s", "-"
            }
            printf "\n"
        }

        if (noise == 0) { printf "\n"; exit }
        printf "\n"
        printf "NOISE CHECK   best rep (ms) and coefficient of variation per N\n"
        printf "  A CV above ~30%% means that column is not trustworthy on its own; re-run.\n"
        printf "  A best rep FAR below the median is not just noise: it is the signature of a\n"
        printf "  plan that prunes on some executions and not others.  Compare against the table\n"
        printf "  above rather than reading either number alone.\n"
        printf "%-26s", "metric"
        for (i = 1; i <= nn; i++) printf "%16s", "N=" nlist[i]
        printf "\n"
        for (mi = 1; mi <= nm; mi++) {
            m = morder[mi]
            printf "%-26s", m
            for (i = 1; i <= nn; i++) {
                v = stat(m SUBSEP nlist[i], "min")
                if (v < 0) { printf "%16s", "-"; continue }
                printf "%10.2f %4.0f%%", v, stat(m SUBSEP nlist[i], "cv")
            }
            printf "\n"
        }
        printf "\n"
    }'
}

if [ -n "$COMPARE_A" ]; then
    for lbl in "$COMPARE_A" "$COMPARE_B"; do
        f="${SCRIPT_DIR}/results_partition_scaling_${lbl}.tsv"
        [ -f "$f" ] || { echo "missing results file: $f" >&2; exit 1; }
        echo "############################################################"
        echo "# $lbl"
        grep '^#' "$f" | sed 's/^/# /'
        echo "############################################################"
        grep -v '^#' "$f" | grep -v '^locks_' | report ms 1
        grep -v '^#' "$f" | grep '^locks_' | report locks 0
    done
    exit 0
fi

[ -n "$LABEL" ] || { echo "--label is required (use the commit the .so was built from)" >&2; exit 2; }
OUT="${OUT:-${SCRIPT_DIR}/results_partition_scaling_${LABEL}.tsv}"

DYLIB="$("$PGCONFIG" --pkglibdir)/pg_reflex.dylib"
[ -f "$DYLIB" ] || DYLIB="$("$PGCONFIG" --pkglibdir)/pg_reflex.so"
DYLIB_SHA="$(shasum -a 256 "$DYLIB" 2>/dev/null | cut -c1-16 || echo unknown)"
DYLIB_MTIME="$(date -r "$DYLIB" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null || echo unknown)"
GIT_HEAD="$(git -C "$SCRIPT_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
GIT_DIRTY="$(git -C "$SCRIPT_DIR" status --porcelain 2>/dev/null | wc -l | tr -d ' ')"

# A full filesystem distorts every timing here without raising a single error:
# I/O contention and failed writes are invisible in the numbers. Record free
# space and load with the results so an anomaly can be attributed afterwards,
# and refuse to start below a floor rather than produce numbers nobody can trust.
DISK_FREE_KB="$(df -k /private/tmp 2>/dev/null | awk 'NR==2 {print $4}')"
DISK_LINE="$(df -h /private/tmp 2>/dev/null | tail -1)"
LOADAVG="$(uptime | sed 's/.*load averages*: //')"
MIN_FREE_KB=$((5 * 1024 * 1024))
if [ -n "$DISK_FREE_KB" ] && [ "$DISK_FREE_KB" -lt "$MIN_FREE_KB" ]; then
    echo "REFUSING TO RUN: less than 5 GB free on /private/tmp ($DISK_LINE)." >&2
    echo "Timings taken on a near-full filesystem are noise reported as signal." >&2
    exit 1
fi

{
    echo "# label:        $LABEL"
    echo "# git HEAD:     $GIT_HEAD (uncommitted files: $GIT_DIRTY)"
    echo "# .so:          $DYLIB  sha256:$DYLIB_SHA  built:$DYLIB_MTIME"
    echo "# server:       $("$PSQL" -d postgres -tAc 'select version()')"
    echo "# params:       n_list='$N_LIST' roots='$ROOT_LIST' rows=$TOTAL_ROWS reps=$REPS"
    echo "# server cfg:   max_locks_per_transaction=$("$PSQL" -d postgres -tAc 'show max_locks_per_transaction') shared_buffers=$("$PSQL" -d postgres -tAc 'show shared_buffers')"
    echo "# disk before:  $DISK_LINE"
    echo "# load before:  $LOADAVG"
    echo "# run at:       $(date '+%Y-%m-%dT%H:%M:%S')"
} > "$OUT"

echo "=== pg_reflex partition scaling — label '$LABEL' ==="
echo "    .so   $DYLIB (sha256:$DYLIB_SHA, built $DYLIB_MTIME)"
echo "    HEAD  $GIT_HEAD"
echo "    out   $OUT"
echo ""

for n in $N_LIST; do
    db="${DB_PREFIX}_${n}"
    echo "--- N=$n ---"
    "$DROPDB" --if-exists "$db" >/dev/null 2>&1 || true
    "$CREATEDB" "$db"
    "$PSQL" -q -d "$db" -c "CREATE EXTENSION pg_reflex" >/dev/null
    # A failure at one N must not discard the points already collected, so the
    # sweep records it and carries on rather than aborting under `set -e`.
    "$PSQL" -q -d "$db" -v n="$n" -v total_rows="$TOTAL_ROWS" -v reps="$REPS" \
            -f "${SCRIPT_DIR}/bench_partition_scaling.sql" 2>&1 \
        | tee "/private/tmp/rfxbench_${LABEL}_n${n}.log" \
        | grep -E 'RFXBENCH\||RFXCHECK\|' \
        | sed -e 's/^.*RFXBENCH|/RFXBENCH|/' -e 's/^.*RFXCHECK|/RFXCHECK|/' \
        | while IFS='|' read -r tag a b c d; do
              if [ "$tag" = "RFXCHECK" ]; then
                  if [ "${b}" != "0" ]; then
                      echo "  !! CORRECTNESS FAILURE: $a differs from base query by $b rows" >&2
                      echo "# CORRECTNESS FAILURE n=$n $a diff=$b" >> "$OUT"
                  fi
              else
                  printf '%s\t%s\t%s\t%s\n' "$a" "$b" "$c" "$d" >> "$OUT"
              fi
          done
    if grep -q '^psql:.*ERROR:' "/private/tmp/rfxbench_${LABEL}_n${n}.log"; then
        echo "  !! N=$n did not complete; first error:" >&2
        grep -m1 '^psql:.*ERROR:' "/private/tmp/rfxbench_${LABEL}_n${n}.log" >&2
        echo "# INCOMPLETE n=$n $(grep -m1 -o 'ERROR:.*' "/private/tmp/rfxbench_${LABEL}_n${n}.log")" >> "$OUT"
    fi
    "$DROPDB" "$db" >/dev/null 2>&1 || true
done

if [ "$SKIP_SUBXID" -eq 0 ]; then
    echo ""
    echo "=== subtransaction-XID consumption of a multi-root flush ==="
    SUBOUT="${SCRIPT_DIR}/results_partition_subxid_${LABEL}.tsv"
    {
        echo "# label: $LABEL   .so sha256:$DYLIB_SHA built:$DYLIB_MTIME   HEAD:$GIT_HEAD"
        printf 'roots\txids_consumed\txids_per_root\troots_until_64_overflow\n'
    } > "$SUBOUT"
    for r in $ROOT_LIST; do
        db="${DB_PREFIX}_sx_${r}"
        "$DROPDB" --if-exists "$db" >/dev/null 2>&1 || true
        "$CREATEDB" "$db"
        "$PSQL" -q -d "$db" -c "CREATE EXTENSION pg_reflex" >/dev/null
        line="$("$PSQL" -q -d "$db" -v roots="$r" \
                 -f "${SCRIPT_DIR}/bench_partition_subxid.sql" 2>&1 \
                 | grep -oE 'RFXSUBXID\|[0-9]+\|[0-9.]+' | tail -1)"
        "$DROPDB" "$db" >/dev/null 2>&1 || true
        xids="$(echo "$line" | cut -d'|' -f3)"
        [ -n "$xids" ] || { echo "  roots=$r: no measurement" >&2; continue; }
        printf '%s\t%s\t%s\t%s\n' "$r" "$xids" \
            "$(awk -v x="$xids" -v r="$r" 'BEGIN{printf "%.2f", x/r}')" \
            "$(awk -v x="$xids" -v r="$r" 'BEGIN{p=x/r; if(p<=0){print "inf"}else{printf "%d", int(64/p)}}')" \
            >> "$SUBOUT"
        echo "  roots=$r  xids=$xids  per-root=$(awk -v x="$xids" -v r="$r" 'BEGIN{printf "%.2f", x/r}')"
    done
    echo ""
    echo "SUBTRANSACTION XIDs PER MULTI-ROOT FLUSH   (PGPROC_MAX_CACHED_SUBXIDS = 64)"
    grep -v '^#' "$SUBOUT" | column -t -s $'\t'
    echo ""
    echo "  'roots_until_64_overflow' is the cliff: past it, every OTHER backend"
    echo "  reading this transaction's rows falls back to pg_subtrans lookups."
fi

echo ""
echo "# disk after:   $(df -h /private/tmp | tail -1)" >> "$OUT"
echo "# load after:   $(uptime | sed 's/.*load averages*: //')" >> "$OUT"

echo ""
echo "############################################################"
echo "# RESULTS — label '$LABEL'"
grep '^#' "$OUT" | sed 's/^/# /'
echo "############################################################"
grep -v '^#' "$OUT" | grep -v '^locks_' | report ms 1
grep -v '^#' "$OUT" | grep '^locks_' | report locks 0
echo "raw: $OUT"
