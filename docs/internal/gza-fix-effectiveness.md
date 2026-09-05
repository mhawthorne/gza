# Does `gza fix` lead to a passing review?

Generated 2026-09-04 from `.gza/gza.db` (all 80 `fix` tasks ever created). Verdicts parsed with `gza.review_verdict._extract_verdict`, not string matching.

## Cohorts

`fix` serves two unrelated jobs. They must not be pooled.

| cohort | trigger_source | n | what it does |
|---|---|---|---|
| churn-rescue | `manual`, and `NULL` (pre-dates the column) | 66 | `gza fix` — rescue a task stuck in review/improve churn |
| watch-rework | `watch`, `watch-pre-merge-integration-verify-rework` | 14 | merged tree failed verify; repair the branch |

## churn-rescue

| outcome of the next review | n |
|---|---|
| CHANGES_REQUESTED | 35 |
| APPROVED | 11 |
| no-review-after | 6 |
| review-failed | 4 |
| fix-failed | 4 |
| APPROVED_WITH_FOLLOWUPS | 3 |
| fix-dropped | 2 |
| review-dropped | 1 |

**Reached a verdict: 49. Approved: 14 (29%).**

## watch-rework

| outcome of the next review | n |
|---|---|
| fix-failed | 4 |
| no-review-after | 4 |
| fix-dropped | 2 |
| review-dropped | 2 |
| APPROVED_WITH_FOLLOWUPS | 1 |
| CHANGES_REQUESTED | 1 |

**Reached a verdict: 2. Approved: 1 (50%).**

## Did the task eventually merge? (churn-rescue only)

- impls that received at least one `gza fix`: **25/44 (57%)** merged
- impls that never needed one: **815/1054 (77%)** merged
- impls needing more than one fix: **11/44** (max 6)

> Not a controlled comparison: you only reach for `fix` on a task that is already churning, so this cohort is harder by construction. The gap is mostly selection.

## Why the next review fails

Ten before/after blocker pairs were read by hand (automated prose and symbol matching proved unreliable, giving 79% and 6% on the same data).

- ~7 of 10: the new review's blockers concerned **different code** than the original complaint.
- ~3 of 10: a genuine repeat of the same issue.

The fix generally does close the blocker it was handed. The next reviewer, by design independent and with no memory of the last round, then finds something else. Chains like `gza-2969 -> 2974 -> 2980 -> 3008` show each round's new blocker becoming the next round's input.

## Conclusion

`gza fix` does not reliably produce a passing review, but the cause is not the fix agent. It is an unbounded re-reviewer: the blocker set was resampled from the whole diff every round instead of shrinking, so the loop had no reason to terminate.

Addressed in `2dab013c`, which scopes blocker criterion (3) on a re-review to code changed since the round that already reviewed it. These numbers are the pre-change baseline; re-run this to measure the effect.

## Convergence: reviews per merged unit

The churn this report describes shows up here. A unit that converges is reviewed once or twice; one that does not accumulates reviews. `2dab013c` narrowed what a re-review may block on, so p90 in particular should fall for units merged after it landed (`2026-09-04T15:08:45`).

| month merged | units | p50 reviews | p90 reviews | max |
|---|---|---|---|---|
| 2026-05 | 151 | 2 | 8 | 20 |
| 2026-06 | 261 | 1 | 7 | 17 |
| 2026-07 | 94 | 2 | 9 | 29 |
| 2026-08 | 121 | 3 | 10 | 23 |
| 2026-09 | 29 | 3 | 7 | 24 |

**Before the bound:** 652 units, p50 2 / p90 8 reviews.
**After the bound:** 4 units, p50 1 / p90 5 reviews.

> Only 4 units have merged since the change. Too few to conclude anything; re-run once this reaches ~50.

## Per-task detail (churn-rescue)

| fix | impl | created | next review | outcome |
|---|---|---|---|---|
| gza-1232 | gza-1204 | 2026-04-20 | gza-1233 | CHANGES_REQUESTED |
| gza-1245 | gza-1204 | 2026-04-21 | gza-1246 | CHANGES_REQUESTED |
| gza-1257 | gza-1204 | 2026-04-21 | gza-1259 | CHANGES_REQUESTED |
| gza-1260 | gza-1204 | 2026-04-21 | gza-1276 | CHANGES_REQUESTED |
| gza-1275 | gza-1204 | 2026-04-22 | gza-1276 | CHANGES_REQUESTED |
| gza-1277 | gza-1204 | 2026-04-22 | gza-1279 | CHANGES_REQUESTED |
| gza-1336 | gza-1292 | 2026-04-22 | - | no-review-after |
| gza-1382 | gza-1341 | 2026-04-23 | - | no-review-after |
| gza-1564 | gza-1533 | 2026-04-27 | gza-1565 | APPROVED_WITH_FOLLOWUPS |
| gza-2196 | gza-1467 | 2026-05-03 | - | no-review-after |
| gza-2346 | gza-2329 | 2026-05-05 | gza-2347 | CHANGES_REQUESTED |
| gza-2969 | gza-2887 | 2026-05-11 | gza-2971 | CHANGES_REQUESTED |
| gza-2974 | gza-2887 | 2026-05-11 | gza-2975 | CHANGES_REQUESTED |
| gza-2980 | gza-2887 | 2026-05-12 | gza-2981 | CHANGES_REQUESTED |
| gza-3008 | gza-2887 | 2026-05-12 | gza-3010 | CHANGES_REQUESTED |
| gza-3288 | gza-3030 | 2026-05-14 | - | fix-dropped |
| gza-3289 | gza-3030 | 2026-05-14 | gza-3291 | CHANGES_REQUESTED |
| gza-3293 | gza-3030 | 2026-05-14 | gza-3294 | CHANGES_REQUESTED |
| gza-3296 | gza-3030 | 2026-05-14 | gza-3299 | CHANGES_REQUESTED |
| gza-3301 | gza-3030 | 2026-05-14 | gza-3302 | CHANGES_REQUESTED |
| gza-3553 | gza-3403 | 2026-05-15 | gza-3560 | CHANGES_REQUESTED |
| gza-3756 | gza-3310 | 2026-05-17 | gza-3763 | CHANGES_REQUESTED |
| gza-3785 | gza-3668 | 2026-05-18 | gza-3788 | CHANGES_REQUESTED |
| gza-3792 | gza-3310 | 2026-05-18 | gza-3793 | review-failed |
| gza-3808 | gza-3668 | 2026-05-18 | gza-3809 | APPROVED |
| gza-4906 | gza-4834 | 2026-06-13 | gza-4907 | CHANGES_REQUESTED |
| gza-5096 | gza-4988 | 2026-06-15 | gza-5097 | APPROVED_WITH_FOLLOWUPS |
| gza-5302 | gza-5237 | 2026-06-18 | gza-5306 | CHANGES_REQUESTED |
| gza-5303 | gza-5238 | 2026-06-18 | gza-5309 | review-failed |
| gza-5304 | gza-5177 | 2026-06-18 | gza-5321 | CHANGES_REQUESTED |
| gza-5380 | gza-5177 | 2026-06-19 | gza-5381 | CHANGES_REQUESTED |
| gza-5513 | gza-5501 | 2026-06-21 | gza-5515 | CHANGES_REQUESTED |
| gza-5581 | gza-5501 | 2026-06-23 | gza-5583 | CHANGES_REQUESTED |
| gza-6342 | gza-6236 | 2026-06-26 | gza-6778 | APPROVED |
| gza-6344 | gza-5704 | 2026-06-26 | gza-6746 | CHANGES_REQUESTED |
| gza-6345 | gza-6242 | 2026-06-26 | gza-6347 | APPROVED |
| gza-6348 | gza-6175 | 2026-06-26 | gza-6350 | APPROVED_WITH_FOLLOWUPS |
| gza-6351 | gza-4691 | 2026-06-26 | gza-6355 | CHANGES_REQUESTED |
| gza-6363 | gza-6339 | 2026-06-26 | - | no-review-after |
| gza-6368 | gza-6251 | 2026-06-26 | gza-6375 | APPROVED |
| gza-6369 | gza-6278 | 2026-06-26 | - | fix-failed |
| gza-6371 | gza-4615 | 2026-06-26 | gza-6389 | APPROVED |
| gza-6372 | gza-4611 | 2026-06-26 | gza-6388 | APPROVED |
| gza-6376 | gza-4858 | 2026-06-26 | gza-6381 | APPROVED |
| gza-6379 | gza-4414 | 2026-06-26 | gza-6387 | APPROVED |
| gza-6380 | gza-6169 | 2026-06-26 | gza-6384 | APPROVED |
| gza-6382 | gza-4413 | 2026-06-26 | gza-6533 | APPROVED |
| gza-6385 | gza-4624 | 2026-06-26 | - | fix-dropped |
| gza-6531 | gza-6278 | 2026-06-27 | - | fix-failed |
| gza-6536 | gza-6287 | 2026-06-27 | - | fix-failed |
| gza-6767 | gza-6059 | 2026-06-27 | - | no-review-after |
| gza-8152 | gza-4691 | 2026-07-06 | gza-8155 | APPROVED |
| gza-8158 | gza-7956 | 2026-07-06 | gza-8179 | review-failed |
| gza-8159 | gza-7337 | 2026-07-06 | - | no-review-after |
| gza-8160 | gza-4691 | 2026-07-06 | - | fix-failed |
| gza-8357 | gza-8310 | 2026-07-07 | gza-8361 | CHANGES_REQUESTED |
| gza-8590 | gza-8530 | 2026-08-17 | gza-8591 | CHANGES_REQUESTED |
| gzaserver-38 | gzaserver-2 | 2026-08-17 | gzaserver-41 | review-failed |
| gza-8816 | gza-8530 | 2026-08-18 | gza-8817 | CHANGES_REQUESTED |
| gza-8854 | gza-8530 | 2026-08-18 | gza-8857 | CHANGES_REQUESTED |
| gzaserver-64 | gzaserver-4 | 2026-08-19 | gzaserver-65 | CHANGES_REQUESTED |
| gzaserver-66 | gzaserver-4 | 2026-08-19 | gzaserver-67 | CHANGES_REQUESTED |
| gza-9008 | gza-8911 | 2026-08-21 | gza-9011 | CHANGES_REQUESTED |
| gza-9995 | gza-9545 | 2026-08-28 | gza-10003 | review-dropped |
| gza-10098 | gza-10059 | 2026-09-03 | gza-10101 | CHANGES_REQUESTED |
| gza-10107 | gza-8926 | 2026-09-03 | gza-10109 | CHANGES_REQUESTED |
