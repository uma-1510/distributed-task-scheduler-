# Chaos Testing — Plan

This tracks the work to add automated chaos testing to the scheduler: repeatedly
killing a worker mid-job and verifying the coordinator detects the failure and
reassigns the job, the way [README.md](../README.md#simulate-worker-failure)
currently describes doing by hand.

No application code lands in this branch — it's repo setup only (issues, labels,
branch protection, contributing guide). Everything below is implemented as a
separate branch/PR so each piece can be reviewed on its own.

## Work items

| # | Branch | Issue | What it does |
|---|--------|-------|---------------|
| 1 | `chore/setup-issues-and-labels` | [#12](https://github.com/uma-1510/distributed-task-scheduler-/issues/12) | This branch: issues, labels, this plan doc |
| 2 | `chore/branch-protection-and-collaborators` | [#12](https://github.com/uma-1510/distributed-task-scheduler-/issues/12) | Branch protection on `main`, `CONTRIBUTING.md` |
| 3 | `feat/chaos-test-script-v1` | [#13](https://github.com/uma-1510/distributed-task-scheduler-/issues/13) | First working `chaos_test.py` (hardcoded) |
| 4 | `feat/chaos-test-cli-args` | [#14](https://github.com/uma-1510/distributed-task-scheduler-/issues/14) | `argparse` for runs/workers/sleep |
| 5 | `fix/chaos-test-error-handling` | [#15](https://github.com/uma-1510/distributed-task-scheduler-/issues/15) | try/except around subprocess & requests calls |
| 6 | `feat/chaos-test-run-and-results` | [#16](https://github.com/uma-1510/distributed-task-scheduler-/issues/16) | Run at scale, commit `chaos_test_results.json` |
| 7 | `feat/chaos-test-summary-report` | [#17](https://github.com/uma-1510/distributed-task-scheduler-/issues/17) | JSON → markdown summary table |
| 8 | `docs/chaos-test-readme-metrics` | [#18](https://github.com/uma-1510/distributed-task-scheduler-/issues/18) | Metrics table + explanation in README |
| 9 | `test/chaos-test-unit-tests` | [#19](https://github.com/uma-1510/distributed-task-scheduler-/issues/19) | Unit tests for summary/parsing logic |
| 10 | `fix/chaos-test-address-review-feedback` | — | Only opened if review comments land after the above merge |

## Workflow

Each work item above is its own branch, cut from an up-to-date `main`, with its
own PR. See [CONTRIBUTING.md](../CONTRIBUTING.md) (added in work item 2) for the
full branch/PR/review conventions used on this repo.
