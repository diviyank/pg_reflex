# 2026-09-08 — ignore-soundness does not flag an ignored source referenced only in the projection or GROUP BY

**Status: untreated — accepted false negative, deliberately deferred from 1.11.4.**
Raised by the final whole-branch review of `reflex-1.11.4`.

Severity: **medium.** The query is unsound in principle; the refusal/audit is silent about it.

---

## Mechanism

`unsound_ignored_sources_parsed` (`src/create_ivm/ignore_soundness.rs`) walks the
content-determining clauses — `WHERE`, `HAVING`, `JOIN … ON`, `JOIN … USING`/`NATURAL`
(unattributable) — and flags inner/cross participation. It does **not** walk the
projection or `GROUP BY`. So:

```sql
SELECT ss.a, dp.label FROM ss LEFT JOIN dp ON TRUE
```

with `dp` in `ignore_sources` is judged sound. It is not: a change to `dp.label` never
refreshes the IMV, so the projected column silently diverges. A `GROUP BY dp.label` is
unflagged for the same reason, and there a change can also merge or split groups.

The behaviour is intentional and pinned by `projection_only_reference_is_allowed`
(the "M7" decision in the 1.11.4 plan).

## Why it was not fixed in 1.11.4

Widening the walk turns this into a create-time **refusal**. Real base-db definitions
project columns from their ignored sources (`pricing.base_price`, `location.canal_id`
in `sop_forecast_view`), so the widening would newly refuse IMV creates in the field.
That needs its own development cycle with a measured blast radius across every existing
`ignore_sources` IMV, not a late fix wave.

## What makes the widening cheap now

`reflex_ack_ignore_source(imv, source)` and the `'!source'` create-time marker already
exist (1.11.4), so an operator can acknowledge each newly refused ignore in one call and
the refusal converges.

## Fix direction

1. Walk `SelectItem` expressions and `GROUP BY` expressions with the same attribution
   rules (unqualified → unattributable).
2. Before shipping, run `reflex_audit()` on a copy of every tenant and count the new
   `ignore-soundness` findings; ship the acks for base-db in the same release.
3. Flip `projection_only_reference_is_allowed` to assert the refusal.
