---
course: DS 3022, Systems II: Advanced Data Science Systems
term: Fall 2026, UVA in Valencia
artifact: Data Project 2, Grading Rubric
project: Data Project 2, The SQS Puzzle (Prefect + local SQS emulator + dbt)
assigned: Lesson 9, Monday 05 October 2026
due: Monday 19 October 2026, 23:59 (after the Lesson 11 working session)
total: 100 points
weight: 20% of the course grade
---

# Data Project 2: Rubric

**The SQS Puzzle** · brief: [README.md](README.md) · checkpoints: [CHECKPOINTS.md](CHECKPOINTS.md)

## Point structure at a glance

| Section | Points | Mostly built in |
|---|---:|---|
| §1 Pipeline correctness | 35 | Lessons 9-10 (Checkpoints A, B, D) |
| §2 Robustness and logging | 20 | Lessons 10-11 (Checkpoints B, F) |
| §3 dbt quality gate | 20 | Lesson 10 (Checkpoint C) |
| §4 README, design justification and repository hygiene | **25** | Throughout; finished in Lesson 11 |
| **Total** | **100** | |

> There is no interview for this project. You explain and defend your design **in writing** in your README, so §4 rewards your reasoning, not only your instructions.

Three things worth noticing before you plan your time:

1. **The README is a quarter of the grade.** A pipeline that works but whose README does not explain the polling strategy, the receive → persist → delete order and the chaos-round fixes cannot score above 75.
2. **The dbt gate is core, not optional.** A pipeline that reassembles the phrase in Python only is capped at 80.
3. **Evidence matters.** The full-delay run (2.4), the chaos round (2.5), the failing-then-passing dbt test (3.6) and the receipt code (1.7) are graded from screenshots and logs in the repository.

## How to use this

**Before you submit,** mark the Self column `Y` (done and verified), `P` (partial) or `N` (not attempted) for every row and submit this file (or the Canvas copy) with your repository link. Partial credit is real; an honest `N` costs you that row and nothing more.

**How it is marked.** The Evidence column says what the grader looks at. Your fork is cloned fresh, your README is followed step by step, and your pipeline is run in fast mode (`DP2_FAST=1`), and in chaos mode for row 2.5. Your receipt code is checked with `python kit/check_submission.py --verify <uvaid> prefect <RECEIPT>`.

---

## §1 Pipeline correctness (35 pts)

| # | Requirement | Pts | Evidence to look for | Self | Score |
|---|---|---:|---|:--:|:--:|
| 1.1 | **Runs end to end** without manual steps: one command (or one dashboard trigger) goes from POST to submission | 5 | Fresh clone + README steps + `python prefect-flow.py` completes in fast mode | Y | |
| 1.2 | **Populates the queue** with an HTTP `POST` to the scatter API, **once per run**, never inside a retry loop or on every scheduled run | 4 | `populate_queue` task; no retries that could re-scatter mid-run, or a guard if served on a schedule | Y | |
| 1.3 | **Parses messages**: requests `MessageAttributeNames=["All"]`, reads `order_no` and `word` from `StringValue`, keeps them paired | 5 | Receive/parse code; rows in `raw.fragments` | Y | |
| 1.4 | **Lands every fragment in DuckDB** (`raw.fragments`) **before** deleting its message | 5 | Insert happens before `delete_message` in the code | Y | |
| 1.5 | **Deletes all messages** by `ReceiptHandle`; no dangling messages | 5 | All three queue counters at 0 after a run (log line or screenshot) | Y | |
| 1.6 | **Submits** to `dp2-submit` with `uvaid`, `phrase` and `platform` attributes, and **checks/logs HTTP 200** | 5 | `submit_solution` code and log line | Y | |
| 1.7 | **Correct answer**: `check_submission.py` PASS; receipt code in the README is valid | 6 | Receipt checked with `check_submission.py --verify` | Y | |

**Subtotal: ___ / 35**

---

## §2 Robustness and logging (20 pts)

| # | Requirement | Pts | Evidence to look for | Self | Score |
|---|---|---:|---|:--:|:--:|
| 2.1 | **Polling strategy** uses `get_queue_attributes` (visible, not visible, delayed) and copes with random delays | 4 | Monitoring loop; counters logged during the run | Y | |
| 2.2 | **Empty responses handled**: no crash when `receive_message` returns no `Messages` key | 3 | `resp.get("Messages", [])` or equivalent | Y | |
| 2.3 | **Timeout** with a clear failure message if fragments never arrive (no infinite loop) | 2 | Timeout parameter; explicit error in the log | Y | |
| 2.4 | **Full-delay evidence run** (`DP2_FAST=0`, 30-900 s) completed end to end | 4 | Prefect dashboard screenshot in `docs/` or the README | Y | |
| 2.5 | **Chaos round** (`DP2_CHAOS=1`) passes: duplicates counted once; malformed message logged or stored **and deleted**; counters at 0 | 5 | A re-run in chaos mode, or two consecutive PASS runs evidenced in your README | Y | |
| 2.6 | **Logging** uses Prefect's built-in logger (`get_run_logger` / `log_prints`); no log files committed | 2 | Task logs in the dashboard; repo file list | Y | |

**Subtotal: ___ / 20**

---

## §3 dbt quality gate (20 pts)

| # | Requirement | Pts | Evidence to look for | Self | Score |
|---|---|---:|---|:--:|:--:|
| 3.1 | **Project structure**: source in `sources.yml`, a staging model and a mart model (`assembled_phrase` or equivalent) | 4 | `dbt/models/` tree; `dbt build` runs from the repo root | Y | |
| 3.2 | **Staging model** casts `order_no` to an integer and is idempotent (a duplicated raw row is counted once) | 4 | `stg_fragments.sql` (e.g. `distinct` or dedupe on `order_no`) | Y | |
| 3.3 | **Generic tests** (`not_null`, `unique`, ...) on the staging columns | 3 | `schema.yml` | Y | |
| 3.4 | **Singular test** that fails unless there are exactly 21 contiguous fragments | 4 | File in `dbt/tests/` | Y | |
| 3.5 | **Gate wired into the flow**: a failed `dbt build` stops the flow before submission | 3 | `dbt_build` task raises on failure | Y | |
| 3.6 | **Proved the tests can fail**: evidence of a test failing on purpose and then passing | 2 | Two screenshots in `docs/` | Y | |

**Subtotal: ___ / 20**

---

## §4 README, design justification and repository hygiene (25 pts)

| # | Requirement | Pts | Evidence to look for | Self | Score |
|---|---|---:|---|:--:|:--:|
| 4.1 | **Setup and run instructions** that work from a fresh clone (emulator, scatter API, `.env`, run command) | 5 | The grader follows your README alone | Y | |
| 4.2 | **Architecture / DAG diagram** with task boundaries and dependencies | 3 | `docs/dag.jpg` (or similar) referenced in the README | Y | |
| 4.3 | **Polling strategy justified**: what you chose, why, and what you rejected | 3 | README section | Y | |
| 4.4 | **Receive → persist → delete** order and idempotency explained (what happens if the flow crashes between steps) | 3 | README section | Y | |
| 4.5 | **"Chaos round" section**: what broke, why, and how you fixed it | 4 | README section | Y | |
| 4.6 | **Tool roles**: what Prefect, dbt and DuckDB each do in this pipeline, and what you would lose by dropping one | 2 | README section | Y | |
| 4.7 | **Results shown**: dashboard screenshot, dbt output, receipt code | 2 | Screenshots and receipt in the README | Y | |
| 4.8 | **Repository hygiene**: a fork of the assignment repo; no `.env`, `*.duckdb`, `dbt/target/` or log files committed; `kit/` unmodified | 3 | Repo page and file list | Y | |

**Subtotal: ___ / 25**

---

## Total

| Section | Score |
|---|---:|
| §1 Pipeline correctness | ___ / 35 |
| §2 Robustness and logging | ___ / 20 |
| §3 dbt quality gate | ___ / 20 |
| §4 README, design justification and repository hygiene | ___ / 25 |
| **Total** | **___ / 100** |

Late submissions (after 23:59 on Monday 19 October 2026) are penalised under syllabus §3.4.
