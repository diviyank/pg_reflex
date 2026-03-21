

● Opt B results: No measurable difference (235ms vs 233ms at 50K). The target table has only 10 rows — DELETE is trivially fast. TRUNCATE adds unnecessary AccessExclusiveLock overhead. Reverting Optimization B.

-> in real cases, target tables are millions of rows. Could we rebuild a test case and reevaluate this case?



  The real picture: pg_reflex adds only ~6% overhead to INSERT operations. The "slower than REFRESH" perception comes from comparing apples to oranges:

  ┌───────┬──────────────────────────────┬───────────────────────────────┬─────────────────────┐
  │ Batch │ pg_reflex (INSERT + trigger) │ INSERT (no trigger) + REFRESH │ pg_reflex advantage │
  ├───────┼──────────────────────────────┼───────────────────────────────┼─────────────────────┤
  │ 50K   │ 240 ms                       │ 223 + 106 = 329 ms            │ 27% faster          │
  ├───────┼──────────────────────────────┼───────────────────────────────┼─────────────────────┤
  │ 100K  │ 475 ms                       │ 446 + 115 = 561 ms            │ 15% faster          │
  ├───────┼──────────────────────────────┼───────────────────────────────┼─────────────────────┤
  │ 500K  │ 2,342 ms                     │ 2,201 + 160 = 2,361 ms        │ ~same               │
  └───────┴──────────────────────────────┴───────────────────────────────┴─────────────────────┘

  pg_reflex is actually faster than INSERT+REFRESH at every batch size. The comparison the user was likely making — INSERT time (240ms) vs REFRESH time (106ms) — doesn't account for the INSERT cost that both approaches must pay.

  That said, there is one real optimization opportunity: the delta GROUP BY on 500K rows takes 142ms, which is close to REFRESH's 160ms on 2.6M rows. If the aggregation itself could be made cheaper, that would help. But PostgreSQL's hash aggregate is already very
  efficient — the NUMERIC type for SUM is the expensive part.

  Let me update the benchmark file and plan to reflect these accurate findings.

● Updated plan
  ⎿  /plan to preview

  Wrote 1 memory (ctrl+o to expand)

● So to answer your question directly: the plpgsql/SPI overhead claim was wrong. The actual trigger overhead is just the delta GROUP BY, which is inherent and minimal (~6%).

  The real question is: what scenario are you seeing where the IMV is slower? Possible explanations:

  1. The comparison was INSERT+trigger vs REFRESH alone — this is unfair because REFRESH doesn't include the INSERT cost. When you add the INSERT cost, pg_reflex wins.
  2. High-cardinality GROUP BY with many groups — the target DELETE+INSERT step (currently <1ms with 10 groups) could become significant with 100K+ groups. That's where Optimizations C/D (targeted refresh, UPSERT on target) would matter.
  3. MIN/MAX aggregates — these require full group rescans on DELETE/UPDATE, which is O(group_size) per affected group.
  4. JOINs — multi-table queries with JOINs might have different performance characteristics.

  Could you share the specific query/scenario where you're seeing the slowdown? That would help me target the right bottleneck.
