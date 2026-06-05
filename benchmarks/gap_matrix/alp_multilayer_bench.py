"""Multi-layer IMV cascade vs full MV REFRESH on the alp base-db view DAG.

Answers: when a base source changes, what does it cost to bring the derived
view layer up to date — incremental IMV maintenance (cascading through the
layers) vs refreshing every materialized view from scratch?

Fair + non-destructive: both variants run inside BEGIN ... ROLLBACK, so db_clone
is never mutated. Each applies the SAME mutation from the SAME baseline and we
time only the maintenance step:
  * IMV variant  — reflex_flush_deferred(source) within the txn (the DEFERRED
    capture trigger staged the delta on the UPDATE; the explicit flush replays
    it through every dependent IMV layer in graph order).
  * MV  variant  — REFRESH MATERIALIZED VIEW for each of the 7 top views in
    topological order (full recompute).

Prereq: the IMV DAG is already built (setup_alp_mvs.py). The MV variant builds
the 7 top views as MVs ONCE (slow initial build, NOT timed) then measures the
REFRESH after the mutation.

Run from the base-db-anchor-evm repo so the framework imports resolve:
  cd .../base-db-anchor-evm
  DBUSER=postgres DBPASS=postgres DBHOST=localhost DBPORT=5432 DBNAME=db_clone \\
    DB_CLONE_SCHEMA=alp uv run python <path-to>/alp_multilayer_bench.py
"""
import os
import sys
from time import perf_counter

from sqlalchemy import create_engine, text
from dataclasses import replace

from base_db.db_utils import get_url
from base_db.view_registry import load_registry, Registry, ViewKind
from base_db.view_registry.executor import (
    _render_create_mv,
    _render_create_imv,
    _render_index_sql,
)

SCHEMA = os.environ.get("DB_CLONE_SCHEMA", "alp")

# Mutation scenarios: (label, source_table, mutation_sql, flush_sources).
# Each mutation must be self-reversing under ROLLBACK (it is — we never COMMIT).
# Batch size kept modest so the incremental delta is small relative to the views.
# NOTE: current_assortment_activity_view is empty on this clone (its filter
# `assortment_id = (SELECT ... FROM sop_current_view)` resolves to NULL because
# sop_current_view is empty), so a caav->sop_forecast cascade scenario is
# degenerate here. We focus on sales_simulation, which sop_forecast_view (33.7M
# rows) reads directly.
# CORRECTION (2026-06-05): an earlier version used
#   WHERE ctid IN (SELECT ctid FROM sales_simulation LIMIT n)
# which is BROKEN on a partitioned table — ctid is not unique across partitions, so
# a "1,000-row" update actually changed 216,001 rows (the same block/offset in every
# one of the ~50 partitions). That artifact produced bogus 17s/101s/44min flush
# times and a fake "super-linear gap". Verified ground truth: a correctly bounded
# delta flushes in milliseconds with a pruned, index-driven DELETE — there is NO
# maintenance gap. See journal/2026-06-05_sop_forecast_flush_analysis.md.
#
# Correct mutation: re-forecast a whole demand plan (one dem_plan_id partition) —
# prunes to a single partition, realistic. SCENARIOS are filled at runtime from a
# few dem_plan_ids of differing size (see main()).
def _bounded_update(dp, n):
    # Update exactly n rows within ONE dem_plan partition via a full-PK key-join
    # (index-driven, partition-pruned, deterministic-bounded). NOT ctid (collides
    # across the 1,062 leaf partitions) and NOT composite-IN-over-self (bad plan).
    return (
        f"UPDATE {SCHEMA}.sales_simulation s SET qty_sales = qty_sales + 1 "
        f"FROM (SELECT order_date, dem_plan_id, id FROM {SCHEMA}.sales_simulation "
        f"      WHERE dem_plan_id = {dp} LIMIT {n}) k "
        f"WHERE s.order_date=k.order_date AND s.dem_plan_id=k.dem_plan_id AND s.id=k.id"
    )


SCENARIOS = []  # populated in main(): bounded deltas within one large dem_plan


def imv_specs_in_order(r: Registry):
    """The 7 top-level IMV specs in topological (dependency) order."""
    return [s for s in r.sorted() if s.kind == ViewKind.IMV]


def time_imv_variant(engine, mutation_sql, flush_sources):
    """Time flushing the deferred delta through every IMV layer, then ROLLBACK."""
    with engine.connect() as c:
        try:
            c.execute(text(f"SET search_path = {SCHEMA}, public"))
            c.execute(text(mutation_sql))
            t0 = perf_counter()
            for src in flush_sources:
                c.execute(text("SELECT public.reflex_flush_deferred(:s)"), {"s": src})
            ms = (perf_counter() - t0) * 1000
        finally:
            c.rollback()
    return ms


def time_mv_variant(engine, imv_order, mutation_sql):
    """Time REFRESH of every top view (as MV) in topo order, then ROLLBACK."""
    with engine.connect() as c:
        try:
            c.execute(text(f"SET search_path = {SCHEMA}, public"))
            c.execute(text(mutation_sql))
            t0 = perf_counter()
            per_view = []
            for spec in imv_order:
                v0 = perf_counter()
                c.execute(text(f'REFRESH MATERIALIZED VIEW "{SCHEMA}"."{spec.name}"'))
                per_view.append((spec.name, (perf_counter() - v0) * 1000))
            ms = (perf_counter() - t0) * 1000
        finally:
            c.rollback()
    return ms, per_view


def build_top_views_as_mv(engine, imv_order):
    """Drop the 7 IMVs and (re)build them as plain MVs (+ indexes). One-time,
    NOT timed. Returns once all 7 materialized views exist."""
    with engine.connect() as c:
        c.execute(text(f"SET search_path = {SCHEMA}, public"))
        for spec in imv_order:
            qname = f"{SCHEMA}.{spec.name}"
            try:
                c.execute(text(f"SELECT drop_reflex_ivm('{qname}', TRUE)"))
                c.commit()
            except Exception:
                c.rollback()
                c.execute(text(f"SET search_path = {SCHEMA}, public"))
            c.execute(text(
                f"DELETE FROM public.__reflex_ivm_reference WHERE name LIKE '{qname}%'"
            ))
            c.execute(text(f'DROP TABLE IF EXISTS "{SCHEMA}"."{spec.name}" CASCADE'))
            c.execute(text(f'DROP MATERIALIZED VIEW IF EXISTS "{SCHEMA}"."{spec.name}" CASCADE'))
            c.commit()
            t0 = perf_counter()
            c.execute(text(_render_create_mv(spec, SCHEMA)))
            for i, idx in enumerate(spec.indexes):
                c.execute(text(_render_index_sql(spec.name, idx, i, SCHEMA)))
            c.commit()
            print(f"  built MV {spec.name:<42} {perf_counter()-t0:6.1f}s", flush=True)


def build_top_views_as_imv(engine, imv_order):
    """Rebuild the 7 top views back as IMVs (partition_by stripped, like
    setup_alp_mvs). One-time, NOT timed."""
    with engine.connect() as c:
        c.execute(text(f"SET search_path = {SCHEMA}, public"))
        for spec in imv_order:
            qname = f"{SCHEMA}.{spec.name}"
            c.execute(text(f'DROP MATERIALIZED VIEW IF EXISTS "{SCHEMA}"."{spec.name}" CASCADE'))
            c.execute(text(f'DROP TABLE IF EXISTS "{SCHEMA}"."{spec.name}" CASCADE'))
            c.execute(text(
                f"DELETE FROM public.__reflex_ivm_reference WHERE name LIKE '{qname}%'"
            ))
            c.commit()
            opts = spec.imv_options.model_copy(
                update={"partition_by": None, "if_not_exists": False}
            )
            t0 = perf_counter()
            c.execute(text(_render_create_imv(replace(spec, imv_options=opts), SCHEMA)))
            c.commit()
            print(f"  built IMV {spec.name:<41} {perf_counter()-t0:6.1f}s", flush=True)


def main() -> int:
    engine = create_engine(get_url())
    r = Registry(load_registry())
    imv_order = imv_specs_in_order(r)
    print(f"Top-level IMV views ({len(imv_order)}), topo order:")
    for s in imv_order:
        print(f"  - {s.name}")

    # Pick the largest dem_plan_id and sweep bounded delta sizes within it, so the
    # IMV cost curve (delta-proportional) is visible against the constant MV
    # refresh-all baseline. The 'all' scenario updates the whole partition.
    with engine.connect() as c:
        dp, dp_rows = c.execute(text(
            f"SELECT dem_plan_id, count(*) FROM {SCHEMA}.sales_simulation "
            f"GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
        )).fetchone()
    print(f"\nmutating within dem_plan_id={dp} ({dp_rows:,} rows)")
    for n in (1_000, 100_000, 1_000_000):
        if n > dp_rows:
            break
        SCENARIOS.append((
            f"delta {n:,} rows in dem_plan {dp}",
            f"{SCHEMA}.sales_simulation",
            _bounded_update(dp, n),
            [f"{SCHEMA}.sales_simulation"],
        ))

    # Phase 1 — IMV variant (DAG already built as IMVs by setup_alp_mvs.py).
    print("\n=== IMV variant (incremental cascade flush) ===", flush=True)
    imv_results = []
    for label, _src, mut, flush_srcs in SCENARIOS:
        ms = time_imv_variant(engine, mut, flush_srcs)
        imv_results.append((label, ms))
        print(f"  {label:<60} {ms:9.1f} ms", flush=True)

    # Phase 2 — MV variant: build the 7 top views as MVs, then time REFRESH-all.
    # KNOWN LIMITATION: build_top_views_as_mv drops IMV targets with CASCADE, which
    # can remove a dependency MV (e.g. history_sales_view) that a sibling top view
    # needs — the full 7-view refresh-all then fails. A proper fix rebuilds the MV
    # DAG dependency-aware. Until then, the per-view REFRESH baseline is each view's
    # build time (a fair full-recompute proxy), and we hard-restore via setup_alp_mvs.
    mv_results = []
    try:
        print("\n=== building top views as MVs (one-time, not timed) ===", flush=True)
        build_top_views_as_mv(engine, imv_order)
        print("\n=== MV variant (full REFRESH of all top views in topo order) ===", flush=True)
        for label, _src, mut, flush_srcs in SCENARIOS:
            ms, per_view = time_mv_variant(engine, imv_order, mut)
            mv_results.append((label, ms))
            print(f"  {label:<60} {ms:9.1f} ms  (refresh-all)", flush=True)
            for name, vms in sorted(per_view, key=lambda x: -x[1])[:5]:
                print(f"      {name:<46} {vms:9.1f} ms", flush=True)
    except Exception as e:
        print(f"\nMV phase failed: {str(e).splitlines()[0]}", flush=True)
        print("DAG is now partially MV-converted — restore with:", flush=True)
        print("  DB_CLONE_SCHEMA=alp uv run python setup_alp_mvs.py", flush=True)

    # Best-effort restore of the IMV DAG (insufficient if a dependency MV was
    # CASCADE-dropped above — then run setup_alp_mvs.py as printed).
    print("\n=== restoring top views as IMVs ===", flush=True)
    try:
        build_top_views_as_imv(engine, imv_order)
    except Exception as e:
        print(f"  in-script restore incomplete ({str(e).splitlines()[0]}); "
              f"run setup_alp_mvs.py", flush=True)

    # Summary.
    print("\n" + "=" * 78)
    print(f"{'scenario':<60} {'IMV ms':>8} {'MV ms':>10} {'speedup':>9}")
    print("=" * 78)
    for (label, imv_ms), (_, mv_ms) in zip(imv_results, mv_results):
        speed = mv_ms / imv_ms if imv_ms else float("inf")
        print(f"{label:<60} {imv_ms:8.1f} {mv_ms:10.1f} {speed:8.1f}x")
    return 0


if __name__ == "__main__":
    sys.exit(main())
