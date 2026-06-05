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
def _batch_update(n):
    return (
        f"UPDATE {SCHEMA}.sales_simulation SET qty_sales = qty_sales + 1 "
        f"WHERE ctid IN (SELECT ctid FROM {SCHEMA}.sales_simulation LIMIT {n})"
    )


# FINDING (2026-06-05, db_clone): IMV incremental maintenance of sop_forecast_view
# (5-table-join passthrough, 33.7M rows) is super-linear and slow —
#   1k-row delta flush  = 17.4 s
#   10k-row delta flush = 101.4 s  (≈ the full MV-refresh cost of ~110 s)
#   100k-row UPDATE     = the capture-trigger statement alone ran >44 min (killed)
# i.e. the IMV/MV crossover for this wide-join view is at a *tiny* ~10k-row delta,
# and large updates are impractical — a real gap, not a win. Default scenarios are
# kept small so a re-run completes; raise with care.
SCENARIOS = [
    (f"sales_simulation +1 qty ({n:,} rows)",
     f"{SCHEMA}.sales_simulation",
     _batch_update(n),
     [f"{SCHEMA}.sales_simulation"])
    for n in (100, 1000)
]


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

    # Phase 1 — IMV variant (DAG already built as IMVs by setup_alp_mvs.py).
    print("\n=== IMV variant (incremental cascade flush) ===", flush=True)
    imv_results = []
    for label, _src, mut, flush_srcs in SCENARIOS:
        ms = time_imv_variant(engine, mut, flush_srcs)
        imv_results.append((label, ms))
        print(f"  {label:<60} {ms:9.1f} ms", flush=True)

    # Phase 2 — MV variant: build the 7 top views as MVs, then time REFRESH-all.
    print("\n=== building top views as MVs (one-time, not timed) ===", flush=True)
    build_top_views_as_mv(engine, imv_order)
    print("\n=== MV variant (full REFRESH of all top views in topo order) ===", flush=True)
    mv_results = []
    for label, _src, mut, flush_srcs in SCENARIOS:
        ms, per_view = time_mv_variant(engine, imv_order, mut)
        mv_results.append((label, ms))
        print(f"  {label:<60} {ms:9.1f} ms  (refresh-all)", flush=True)
        for name, vms in sorted(per_view, key=lambda x: -x[1])[:5]:
            print(f"      {name:<46} {vms:9.1f} ms", flush=True)

    # Restore the IMV DAG so db_clone is left as we found it.
    print("\n=== restoring top views as IMVs ===", flush=True)
    build_top_views_as_imv(engine, imv_order)

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
