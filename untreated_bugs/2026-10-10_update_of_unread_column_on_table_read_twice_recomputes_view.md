# UPDATE of a column the view does not read, on a table the view reads twice, now recomputes the view in full — perf (new in 1.12.0, correctness unaffected)

Review finding M3. Versions: 1.12.0 pre-release (0324f47) only; 6946400 (1.11.5) skipped these updates. Result is correct on both; the cost is not.

## Observed (from the review, section 4, cause 3b)
Commit 6e7dc16 "keep the UPDATE skip off for a source read more than once". An UPDATE that changes no column the IMV reads used to be skipped; for a source read more than once (self-join, two aliases of one dimension, a table also read by a subquery) it now runs the full recompute that these shapes already had for real changes. Rows of the IMV rewritten per salary / unrelated update, 6946400 -> 0324f47:

| Def | Shape | rewritten / total |
|---|---|---|
| j26 | employee / manager self LEFT JOIN, salary update | 0 -> 22 of 22 |
| j43 | two joins to `customers` | 0 -> 60 of 60 |
| j50, r14, s08 | (definitions not quoted in the review) | same direction |
| r01 | transfers with two account roles | 0 -> 40 of 40 |

Measured by the `xmin` of the registered tables' rows newer than the step. The SQL of j26 is not printed in the review beyond "employee / manager self LEFT JOIN"; no minimal reproduction is available from it. Reproduce by self-joining `employees` on `manager_id` and running `UPDATE employees SET <column the view does not read> = ...`, then comparing row `xmin`.

## Status
Documented only in commit 6e7dc16: not in any user-facing page (the review asks for release-notes text, M3). This report is the tracking record.

## Fix direction (hypothesis)
The skip is unsound for these shapes only if the unread column can still change what the second read sees; if the set of columns read through every alias of the table is collected once, the skip is sound whenever the changed columns are outside that union. Falsify with the oracle on j26/j43 for UPDATE of an unread column and of a column read through only one alias.
