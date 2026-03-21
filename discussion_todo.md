

● Opt B results: No measurable difference (235ms vs 233ms at 50K). The target table has only 10 rows — DELETE is trivially fast. TRUNCATE adds unnecessary AccessExclusiveLock overhead. Reverting Optimization B.

-> in real cases, target tables are millions of rows. Could we rebuild a test case and reevaluate this case?
