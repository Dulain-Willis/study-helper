Beginner·Pipeline Architecture · 20 min · 4 challenges

# Making It Repeatable: Beginner

Ines is 40 minutes into her data engineering loop at Airbnb, the company whose data team open-sourced a workflow orchestrator now used across the industry. On the shared screen are 5 boxes: pull yesterday's bookings, load them raw, build a nightly revenue table, check it, refresh the finance dashboard. "Right now these are separate cron jobs at fixed times," the interviewer says. "Last Tuesday the load ran long, the revenue job started on schedule anyway, and finance saw half a day of bookings. Someone reran it by hand and revenue doubled. Then a fix shipped straight to production and broke the dashboard for a morning. How would you make this repeatable?" Ines recognizes the pattern under the story: order tasks by data readiness, make every rerun safe, and give changes a path to production that catches mistakes first.


<br>

### What you will be able to do

Draw a pipeline as a DAG and explain what the orchestrator does in each task state

Choose how a task waits on data produced in the same DAG, and how it waits when another team or an external system produces it

Make a task safe to retry by keying it on the logical date and overwriting its partition

Set up dev and staging environments that run the same code as prod but cannot write to prod

Describe the checks and deploy steps a pipeline change passes before and after it reaches prod


<br>

---

## DAG Orchestration

### The question behind a broken schedule

Orchestration questions rarely use the word DAG. They arrive as a broken schedule: job A at 01:00, job B at 02:00, and on a bad night B read an empty table because A ran for 70 minutes. The structure the interviewer wants named is a DAG, a directed acyclic graph. Each task is a node, and an arrow from task X to task Y means Y starts only after X succeeds. Directed means every arrow points in a single direction. Acyclic means that following arrows never leads back to the task you started from, so some tasks always have no upstream and can begin the run.

A cron schedule encodes a guess about duration. It bets the load finishes within an hour because it usually does, and on the night it runs long, the revenue job reads whatever has landed so far. A DAG removes the bet: `build_daily_revenue` starts when `load_raw_bookings` reports success, whether the load took 20 minutes or 3 hours. The diagram shows the nightly bookings pipeline that every section of this lesson builds on.

![Pipeline diagram 1](images/pipeline_making_it_repeatable_beginner_diagram_1.png)


<br>

### What the orchestrator owns on every run

Responsibilities you should be able to list

- **Scheduling** *a run per interval*\
  Creates a run for each day or hour and stamps it with the interval it covers.
- **Dependency resolution** *who may start now*\
  Starts a task only when its upstream tasks are in states its trigger rule accepts.
- **State tracking** *per task, per run*\
  Records every task's state in every run, so a rerun skips work that already succeeded.
- **Retries** *bounded and spaced out*\
  Reruns a failed task after a wait, up to a set number of attempts, then marks it failed.
- **Backfill** *runs for past dates*\
  Creates runs for a range of earlier intervals so history can be rebuilt with current code.


<br>

### Logical date and wall clock

Every run carries the interval it processes, called its logical date. The run for 2026-03-14 handles bookings made on March 14. That data is complete only after midnight, so the run starts at 01:00 on March 15. Retry it on March 17 and it still processes March 14. Interviewers probe this because a task that computes "yesterday" from the clock gets a different day each time it runs, which turns a harmless retry into a run against the wrong data.

| Run | Logical date | Starts (wall clock) | Reads |
| --- | --- | --- | --- |
| Scheduled | 2026-03-14 | 2026-03-15 01:00 | `ds = '2026-03-14'` |
| Retry after a timeout | 2026-03-14 | 2026-03-15 01:35 | `ds = '2026-03-14'` |
| Backfill after a fix | 2026-03-14 | 2026-03-20 09:12 | `ds = '2026-03-14'` |

Across the 3 rows the logical date stays fixed while the wall clock moves. That fixed date is what makes a run repeatable, and Safe Retries builds directly on it.


<br>

### Task states worth naming

| State | Meaning | What follows |
| --- | --- | --- |
| queued | Upstream finished; waiting for a free worker | Runs when a slot opens |
| running | Executing now | Ends in success or failed |
| success | Finished without error | Downstream tasks become eligible |
| failed | Errored on every allowed attempt | Downstream tasks become upstream_failed |
| upstream_failed | Never started because a dependency failed | Runs once the failed task is fixed and cleared |
| skipped | A branch condition left it out | Counts as finished under rules that allow skips |

When a task fails at 01:40, recovery starts from that task. Clearing it resets it and everything downstream of it, while tasks that already succeeded keep their state. State tracking belongs in an answer for that reason: it decides how much work a recovery costs.


<br>

### How the scheduler picks the next task

The scheduler repeatedly looks for tasks whose upstream tasks have all finished. The order that falls out is a topological sort. This sketch applies the same rule to the bookings DAG and prints each wave of tasks that can start together.

```sql
dag = {
    "extract_bookings": [],
    "extract_fx_rates": [],
    "load_raw_bookings": ["extract_bookings"],
    "build_daily_revenue": ["load_raw_bookings", "extract_fx_rates"],
    "check_daily_revenue": ["build_daily_revenue"],
    "refresh_finance_dashboard": ["check_daily_revenue"],
}

done = set()
wave = 1
while len(done) < len(dag):
    ready = [
        t for t, ups in dag.items()
        if t not in done and all(u in done for u in ups)
    ]
    if not ready:
        raise ValueError("cycle detected")
    print(wave, sorted(ready))
    done.update(ready)
    wave += 1
```

It prints 5 waves. Wave 1 holds `extract_bookings` and `extract_fx_rates` because neither has an upstream, so they run in parallel; every later wave holds a single task. The `raise` line is the reason cycles are banned: if X needs Y and Y needs X, neither is ever ready, the loop cannot progress, and a real scheduler would wait forever. Orchestrators reject a cycle when the DAG is loaded, so it never reaches a run.

> Try it
>
> Add `send_daily_summary` with `check_daily_revenue` as its only upstream. Which wave does it join? Then add an arrow from `refresh_finance_dashboard` back to `extract_bookings` and predict exactly what the sketch prints before it stops.

*The new task joins wave 5 beside the dashboard refresh. With the back arrow, wave 1 prints only extract_fx_rates, because extract_bookings now waits on the last task; the next pass finds nothing ready and raises the cycle error.*

> **TIP**
>
> A complete opening answer: model the jobs as a DAG, start each task on upstream success, stamp every run with its logical date, and let the orchestrator handle scheduling and retries while it keeps track of each task's state.


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

Three jobs (extract, transform, then load into the warehouse) each fire on their own cron, so when extract runs late the transform quietly reads yesterday's data and the warehouse ships stale numbers. Redesign the flow so each step starts only after the one before it has finished, and so a run that misses its daily deadline pages on-call instead of failing silently.

**Starting canvas** (already placed for you):

- Source (source, PostgreSQL)
- Extract (cron) (transform, Spark)
- Transform (cron) (transform, Spark)
- Warehouse (storage, Snowflake)

<details>
<summary><strong>Solution</strong></summary>

Strip the costume: this is not a data-quality bug, it is a **race condition** between three schedulers that have never been introduced to each other. Time-based `cron` says 'run `transform` at 02:00' and hopes `extract` finished first. Hope is not a dependency. This is DAG orchestration wearing an ETL-freshness disguise, and the trap is that everyone's instinct is to widen the gap between crons. That is the one move that never converges: the day `extract` runs 40 minutes late, `transform` still reads yesterday's partition and the warehouse ships stale numbers with a green checkmark.

#### Why staggering the crons never closes the gap

Each job's runtime is a distribution, not a constant. Pick a fixed offset big enough for the slowest `extract`, and on a normal day you burn hours of latency for nothing; on the bad day the source is slow, the distributions overlap, and your buffer is gone. Worse, nothing tells you the bet failed: the three jobs succeed independently, so the pipeline is green while the answer is wrong. You need `transform` to start because `extract` finished, not because a clock ticked.

| ✕ Time-triggered (cron) | ✓ Completion-triggered (DAG) |
| --- | --- |
| Three crons, three independent clocks. `transform` fires at a fixed time and reads whatever partition exists. A late `extract` is invisible downstream; failure is silent and the warehouse looks fresh. Tuning means guessing a bigger offset. | One DAG keyed by `execution_date`. `extract >> transform >> load`: each task starts only on upstream success, so a slow `extract` simply delays the run instead of corrupting it. A deadline on the whole run turns 'late' into a page, not a guess. |

#### The redesign

```mermaid
flowchart LR
    n0[("Source<br/>(PostgreSQL)")]
    n1["Orchestrator<br/>(Airflow)"]
    n2["extract<br/>(Spark)"]
    n3["transform<br/>(Spark)"]
    n4["load<br/>(Spark)"]
    n5[("Warehouse<br/>(warehouse)")]
    n6(["On-call page<br/>(alerting)"])
    n0 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n1 --> n2
    n1 --> n3
    n1 --> n4
    n1 --> n6
```

*One Airflow DAG schedules the chained tasks, each starting on upstream success, and pages on-call when the daily deadline is missed.*

1. **Bind the three jobs into one `execution_date`-keyed DAG**

   Replace the three crons with a single scheduled DAG and wire the dependency: `extract >> transform >> load`. Now `transform` cannot start until `extract` reports success for the same `execution_date`, so a slow `extract` delays the run instead of feeding `transform` yesterday's partition. The whole day is one logical unit with one run id, which is what lets the orchestrator show you the state of the entire pipeline at a glance.

2. **Let a failure stop the chain, not corrupt it**

   When `transform` fails, `load` does not run: Airflow marks it upstream failed, so the warehouse keeps yesterday's clearly dated numbers instead of half-built ones. Give each task a small number of retries so a transient PostgreSQL hiccup heals on its own; when it does not, on-call fixes the failed task and re-runs it, and the DAG picks up from that point rather than from the beginning.

3. **Put the deadline on the run and make a miss page on-call**

   Declare a deadline for the batch (say, warehouse fresh by 06:00) on the DAG and attach an `sla_miss_callback` that fires the on-call alert. The clock covers the end-to-end run, not one task, so a run where every task succeeds but retries eat the morning still surfaces as a page instead of a quiet late refresh.

| Trick: Trigger on completion, alert on the deadline |
| --- |
| The whole problem collapses once you separate 2 concerns cron fuses together: **ordering** and timeliness. The DAG's `>>` edges give you ordering (start on upstream success); a deadline on the run gives you timeliness (page when the end-to-end run is late). Cron pretended a single clock could do both, and did neither. |

| Pitfall: Failure alerts alone miss the slow green run |
| --- |
| Candidates wire `extract >> transform >> load`, add an on-failure alert, and stop. But the incident in this prompt is not a failure: every job succeeded, just late. Only a deadline on the whole run catches a pipeline that is healthy task by task and still misses the morning. |

| Interviewer: Naming what cron cannot do is the answer |
| --- |
| Asked 'why not just use cron?', the strong answer is precise: cron can schedule a job, but it cannot manage dependencies between jobs, retry a failed task, or show the state of the whole pipeline at a glance. Name all three, then point at the stale-data incident as the cost of the first. |

| Real world: Green pipelines that ship the wrong number |
| --- |
| The expensive incidents are rarely red pipelines; they are green ones serving stale data to a dashboard an exec trusts. A completion-triggered DAG plus an end-to-end deadline converts 'silently wrong' into 'loudly late', and 'loudly late at 06:00' is a page you can act on before the morning numbers go out. |

**Follow-up questions**

1. The `transform` step now takes 3 hours and keeps blowing the 06:00 deadline. What do you change before you move the deadline?\
   *Tests whether they look at the slow task rather than hiding the problem by relaxing the deadline.*
2. `transform` fails at 03:00 and you fix the bug at 09:00. What state is `load` in, and where does the re-run start?\
   *Tests that downstream tasks are marked upstream failed and the re-run resumes from the failed task, not from `extract`.*
3. Another team's DAG now needs this warehouse table before it can start. How do you express a dependency that crosses 2 DAGs?\
   *Probes the DAG of DAGs problem: cross-DAG dependencies are harder than dependencies inside one DAG.*

</details>


<br>

---

## Task Dependencies

### 3 places a dependency comes from

Arrows inside a single DAG are handled by the scheduler on its own. Dependency questions push past them to data owned by someone else, because that's where pipelines fail quietly.

| Kind | Example | How the task waits |
| --- | --- | --- |
| Inside the DAG | `build_daily_revenue` needs `load_raw_bookings` | An arrow; the scheduler handles it |
| Another team's pipeline | The payments team's `payments.refunds` for 2026-03-14 | A sensor on that partition's completion record |
| An external system | A partner drops the day's exchange-rate file around 00:30 | A sensor polling storage for the file and its marker |

A sensor is a task whose only job is to wait. It checks a condition every few minutes and succeeds once the condition holds. It also carries a timeout, and when the timeout passes it fails, which turns "the data never arrived" into an alert someone sees before the dashboard deadline.


<br>

### Wait for a readiness signal

The classic failure: a sensor checks that the refunds partition for 2026-03-14 exists. It does, because the payments job has written 3 of its 12 files. The revenue task reads a quarter of the day's refunds and still succeeds, so it publishes a wrong number with no error anywhere. The repair is a signal the producer writes last: a `_SUCCESS` marker file, a row in a partition status table, or a published event. Because the producer writes it after every data file, its presence means the data is complete.

```sql
SELECT
  table_name,
  ds,
  status,
  row_count
FROM ops.partition_status
WHERE table_name = 'payments.refunds'
  AND ds = '2026-03-14'
  AND status = 'COMPLETE'
```

Pair the marker with a volume check. If refunds usually run between 9,000 and 14,000 rows a day and the status row says 1,200, the producer may have marked a partial load complete. Stopping costs a late dashboard. Continuing costs a wrong one, and a wrong number already copied into a finance report is far more expensive to unwind.


<br>

### Fan-out, fan-in and trigger rules

Fan-out is a task feeding several others, such as `load_raw_bookings` feeding both the revenue table and a cancellations table. Fan-in is several tasks feeding one, such as `build_daily_revenue` waiting on bookings and currency rates. A trigger rule defines what "upstream is done" means for a task. Names differ between orchestrators; these 4 cover most interview cases.

| Trigger rule | Starts when | Use it for |
| --- | --- | --- |
| `all_success` (default) | Every upstream task succeeded | Any task that reads upstream output |
| `all_done` | Every upstream task finished, in any state | Cleanup, such as dropping temp tables |
| `one_failed` | At least 1 upstream task failed | Paging the on-call engineer |
| `none_failed` | No upstream failed; skips are allowed | The join after an if/else branch |

Interviewers like the cleanup case. Under `all_success`, a task that drops temp tables never runs on a night the build fails, so temp tables pile up and storage grows with every failure. Under `all_done` it runs after success and failure alike.


<br>

### The critical path sets the finish time

Parallel branches overlap, so a run lasts as long as its longest chain of dependent tasks, called the critical path. These are durations from a typical night, with the run starting at 01:00.

| Task | Duration | Upstream |
| --- | --- | --- |
| extract_bookings | 25 min | none |
| extract_fx_rates | 5 min | none |
| load_raw_bookings | 40 min | extract_bookings |
| build_daily_revenue | 30 min | load_raw_bookings, extract_fx_rates |
| check_daily_revenue | 5 min | build_daily_revenue |
| refresh_finance_dashboard | 10 min | check_daily_revenue |

The tasks on the bookings chain take 25 minutes, then 40, then 30, then 5, then 10. The currency branch is done after 5 minutes and then idles while the load finishes. Making `extract_fx_rates` faster saves nothing; trimming 15 minutes from `load_raw_bookings` moves the dashboard 15 minutes earlier.

- **110 min** — critical path length
- **02:50** — dashboard refreshed

> Try it
>
> `build_daily_revenue` now also needs `payments.refunds`, which usually completes at 02:30. The dashboard must be ready by 03:00. When does it refresh, which chain is critical, and what would you raise with the payments team?

*The build waits until 02:30 where it used to start at 02:05, so it ends at 03:00, the check at 03:05 and the refresh at 03:15, 15 minutes late. The refunds wait is now the critical path, so tuning the bookings load cannot help; the conversation is about when refunds can be complete.*

> **TIP**
>
> When asked what happens if upstream data is late, give 3 parts: wait on a readiness signal, fail at a timeout set from the deadline, and alert the owner. Running on whatever happens to be there is the wrong answer.


<br>

---

## Safe Retries

### Retries happen whether you plan for them

Most pipeline failures are transient. A source database drops a connection, a worker is preempted, an API returns 503. The same task usually succeeds a few minutes later, so orchestrators retry by default, and engineers rerun tasks by hand during incidents and backfills. That puts a requirement on every task: running it twice for the same logical date must leave the same result as running it once. The name for that property is idempotency.


<br>

### The append bug

Here is the revenue step written the way most first drafts are. On March 15 at 01:40 it wrote its rows, then the worker died before the orchestrator recorded success, so the orchestrator retried it.

```sql
INSERT INTO analytics.daily_revenue
SELECT
    ds,
    city,
    SUM(amount_usd) AS revenue_usd
FROM raw.bookings
WHERE ds = '2026-03-14'
GROUP BY
    ds,
    city
```

After the first attempt

| ds | city | revenue_usd |
| --- | --- | --- |
| 2026-03-14 | Lisbon | 48200 |
| 2026-03-14 | Austin | 61750 |

After the retry

| ds | city | revenue_usd |
| --- | --- | --- |
| 2026-03-14 | Lisbon | 48200 |
| 2026-03-14 | Austin | 61750 |
| 2026-03-14 | Lisbon | 48200 |
| 2026-03-14 | Austin | 61750 |

Any query summing March 14 revenue now returns 219,900 dollars where the true figure is 109,950. Nothing errored, so nothing alerted; finance finds out when the number looks too good.


<br>

### Overwrite the partition

The fix gives the task ownership of a slice of the table. A partition here is every row with a single `ds` value, and the task replaces its partition on every run: delete the rows for its date, then write them fresh. A second run replaces the first one's rows, so 2 attempts leave 2 rows for March 14, the same as 1 attempt.

```sql
DELETE FROM analytics.daily_revenue
WHERE ds = '2026-03-14';

INSERT INTO analytics.daily_revenue
SELECT
    ds,
    city,
    SUM(amount_usd) AS revenue_usd
FROM raw.bookings
WHERE ds = '2026-03-14'
GROUP BY
    ds,
    city;
```

Between the delete and the insert, a reader sees an empty partition. You can close that gap by building the new rows in a staging table and swapping it into place in a single step, or by using a table format that replaces a partition atomically. Either way a reader sees the complete old rows or the complete new rows.


<br>

### Key the work on the logical date

Overwriting protects you only if the retry targets the same partition. A filter on `current_date - INTERVAL '1' DAY` processes March 14 when it runs on March 15, and March 16 when someone retries it on March 17. The retry rebuilds March 16, a date it was never asked to touch, and the broken March 14 partition is never repaired.

| ✓ Filter reads the clock | ✗ Filter reads the run |
| --- | --- |
| • ds = CAST(current_date - INTERVAL '1' DAY AS varchar)<br><br>• Retry on March 17 rebuilds March 16<br><br>• Backfill of March 1 to 31 rebuilds yesterday 31 times | • ds = '{{ logical_date }}', filled in by the orchestrator<br><br>• Retry on March 17 rebuilds March 14<br><br>• Backfill of March 1 to 31 rebuilds each of the 31 days once |


<br>

### Which failures to retry

| Error | Transient? | Action |
| --- | --- | --- |
| Connection timeout to the source database | Yes | Retry with backoff |
| HTTP 503 or 429 from an API | Yes | Retry with backoff; honour `Retry-After` |
| Worker lost or preempted | Yes | Retry |
| Column not found after an upstream schema change | No | Fail fast and alert the owner |
| Bad cast or division by zero in the SQL | No | Fail fast and alert the owner |
| Quality check: 0 revenue rows for the day | No | Fail and block downstream tasks |

Retrying a logic error only delays the alert, and retrying a failed quality check can let bad data through if the second attempt reads different input. Backoff spaces retries out by doubling the wait: 1 minute, then 2, then 4, which gives a struggling source time to recover. Doubling alone keeps tasks that failed together in lockstep, so each wait also gets a few seconds of random jitter, and the waiting clients reach the source one at a time instead of all at the same moment.

- **7 min** — wait before the alert fires
- **3** — retries configured per task


<br>

### Side effects a delete cannot undo

Some tasks act outside the tables they own: emailing a report, calling a payout API, publishing an event. Deleting a partition does not unsend an email. Give each action a deterministic key built from the target and the logical date, such as `payout:host_8812:2026-03-14`. Record each key once its action is done, so a retry skips the ones already sent.

```sql
def send_payout(host_id, ds, amount_usd, sent_log, api):
    key = f"payout:{host_id}:{ds}"
    if key in sent_log:
        return "skipped"
    api.post(
        "/payouts",
        json={"host_id": host_id, "amount_usd": amount_usd},
        headers={"Idempotency-Key": key},
    )
    sent_log.add(key)
    return "sent"
```

If the worker dies after the post but before `sent_log.add`, the retry posts again. The key also travels to the API in the `Idempotency-Key` header for that reason: a receiver that honours such keys drops the second request.

> Try it
>
> An hourly task runs `INSERT INTO raw.clicks SELECT ... WHERE event_time >= current_timestamp - INTERVAL '1' HOUR`. Yesterday the 14:00 run failed and was retried at 14:20. Name both defects and the fix for each.

*The insert appends, so rows from the first attempt are counted twice; overwrite the hour's partition. The window moves with the clock, so the retry covered 13:20 to 14:20, missing 13:00 to 13:20 and overlapping the next run's hour; filter on the run's logical hour.*

> **TIP**
>
> Answer retry questions in this order: what makes the task idempotent, how the run's date is fixed, which errors are retried with what backoff, and how side effects outside the warehouse are deduplicated.


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

The nightly load into the warehouse has no real recovery plan: a brief network blip kills the task halfway, and the next morning an engineer reruns the whole job by hand. Put the load under a scheduler that owns the run and retries a failed task on its own with growing gaps between attempts, instead of waiting for a person. And because one malformed record currently fails every retry forever, send those repeatedly-failing records aside for review so they stop stalling the load.

**Starting canvas** (already placed for you):

- Nightly source (source, S3)
- Loader (transform, Spark)
- Target table (storage, Snowflake)

<details>
<summary><strong>Solution</strong></summary>

#### The trap: 2 failures that look the same and aren't

This is safe retries dressed up as a nightly load. Beneath the business words there are exactly 2 ways this job dies, and the whole problem is refusing to treat them alike. A network blip is **transient**: nothing is wrong with your data, the world will be fine in 30 seconds, so the right move is to retry the task. A malformed record is permanent: it will fail identically on attempt 1 and attempt 1,000, so retrying it is an infinite loop that starves every good record behind it. Anyone can bolt `retries=3` onto a task. The skill probed is seeing that retry cures the first failure and poisons the second, then wiring a scheduler that applies each remedy to the right one.

Get this wrong and you fail in one of 2 loud ways. Retry the poison record and the job never drains: the same 3 a.m. page every night on the same one bad row. Send the network blip to the side and you silently drop a good night's data. The starter pipeline (source to `Spark` loader to warehouse table) has neither guard, which is why a human is the recovery plan.

```mermaid
flowchart LR
    n0[("Nightly source<br/>(object storage)")]
    n1["Orchestrator<br/>(Airflow)"]
    n2["Loader<br/>(Spark)<br/>Backoff: exponential"]
    n3[("Target table<br/>(warehouse)")]
    n4[/"Failed records queue<br/>(dead letter queue)"/]
    n1 --> n2
    n0 --> n2
    n2 --> n3
    n2 --> n4
```

*The scheduler owns the run and retries the `Loader` with exponential backoff; records that fail every attempt go to a dead letter queue instead of stalling the load.*

#### How a strong answer builds it

1. **Give the run an owner, not a person**

   Put the load under an orchestrator (Airflow, or any scheduler that persists task state). The DAG owns 'did tonight's run succeed?' as durable state, so a mid-task death is a task the scheduler knows has failed and can act on, not a silence a human notices at 9 a.m. That is the difference between a script and a pipeline: the run's status lives somewhere the machine can query.

2. **Retry the transient failure with growing gaps**

   On the `Loader` task set bounded retries with exponential backoff, for example 3 attempts at 1 minute, 5 minutes and 30 minutes, with a little jitter. Backoff matters because the thing that failed you (the network, the warehouse) is often recovering, and hammering it every second delays its recovery and yours.

3. **Make the retry safe to run twice**

   A retry reruns the whole task, so if the first attempt half-loaded the batch before the blip, attempt 2 must not double-insert. Load idempotently: `MERGE` on a business key, or overwrite the run date's partition rather than appending. Without this, 'retry on failure' quietly becomes 'duplicate rows on every network hiccup'.

4. **Set the poison record aside instead of dying on it**

   Catch the per-record failure instead of throwing it. Write the offending record plus its error to a dead letter queue and keep processing the rest of the batch. The load finishes, the bad rows wait for a human on their schedule, and one broken record can no longer stall the other million.

| Pitfall: A blanket retry turns a poison pill into an outage |
| --- |
| The most common wrong answer wraps the entire batch in `retries` and stops there. That fixes the network blip, and it converts the one malformed record into a job that fails, retries, fails identically, and exhausts every attempt, every night. Retry count is a ceiling on transient failures, never a cure for permanent ones. |

| ✕ Retry (transient) | ✓ Set aside (permanent) |
| --- | --- |
| Failures that fix themselves: a dropped connection, a throttled warehouse, a brief storage 503. Same input, different outcome next time. Bounded `retries` with exponential backoff, and an idempotent load so rerunning cannot duplicate. | Failures that never fix themselves: a row that won't parse, a schema violation. Same input, same failure forever. Catch it per record, send it to the dead letter queue with its error, continue the batch. Retrying here is an infinite loop. |

| Interviewer: Naming idempotency before you're asked |
| --- |
| A junior says 'add retries.' A senior says 'add retries with backoff, and because the retry replays the whole task I'll load with `MERGE` so a half-finished attempt can't double-count, and bad records go to a dead letter queue.' Linking retry to correctness is the signal the interviewer listens for. |

| Real world: The dead letter queue is one you must actually drain |
| --- |
| A failed-records queue is not fire-and-forget. It needs an alert on depth (a sudden spike usually means an upstream schema change, not scattered bad rows) and a person or backfill process that reprocesses or discards. A queue nobody watches is just a slower data-loss bug. |

| Performance: Cap the retries so a real outage fails loudly |
| --- |
| Backoff is bounded on purpose. 3 attempts at 1, 5 and 30 minutes spends about half an hour riding out a blip, then gives up and pages the on-call. Unbounded retries mean that when the warehouse is down for hours the task sits 'running' and hides the incident instead of surfacing it. |

**Follow-up questions**

1. The nightly file is late one night and the run fires before it lands. How do you keep that from counting as a failure?\
   *Tests whether they wait on data availability rather than burning retries on an input that has not arrived.*
2. 10% of tonight's rows land in the dead letter queue because upstream changed a column type. What should the pipeline do differently than on a normal night?\
   *Tests queue-depth alerting and the instinct to stop the load, versus quietly setting aside a tenth of the data.*
3. The retry succeeds on attempt three, but attempt one had already written half the batch. Show why the target still has correct counts.\
   *Forces a concrete idempotency mechanism (`MERGE` key or partition overwrite), not just a claim that the load is safe.*

</details>


<br>

---

## Dev/Staging/Prod

### One pipeline, 3 environments

The question usually sounds like "how would you test a change to the revenue job without breaking the finance dashboard?" The answer is a set of environments, each running the same code against different targets, with rules about which way code and data may move.

| Environment | Purpose | Reads | Writes to | Deployed by |
| --- | --- | --- | --- | --- |
| dev | Build and debug a change | A 1% sample of prod sources, PII masked | `dev_<engineer>` schemas | The engineer |
| staging | Prove the change on full data, on the real schedule | Prod sources, read-only | `staging_analytics` | The deploy pipeline after merge |
| prod | Serve dashboards and other teams | Prod sources | `analytics` | The deploy pipeline after staging passes |

The rule interviewers listen for: code moves up from dev to staging to prod, and nothing outside prod can write to a prod table. Dev and staging read prod sources through read-only credentials, and no identity outside prod holds write access to `analytics`. A bug in staging can damage only `staging_analytics`.


<br>

### Same code, different config

If a query names `analytics.daily_revenue` directly, running it in dev overwrites prod. So queries name tables through variables, and a config file supplies the values for each environment. Only the config differs between environments. The SQL and DAG code are identical everywhere, and so are the tests, which means what staging tested is exactly what prod runs.

```sql
dev:
  source_schema: raw_sample
  target_schema: dev_priya
  schedule: manual
  alert_channel: none
staging:
  source_schema: raw
  target_schema: staging_analytics
  schedule: "0 1 * * *"
  alert_channel: data-staging-alerts
prod:
  source_schema: raw
  target_schema: analytics
  schedule: "0 1 * * *"
  alert_channel: data-oncall
```

Staging runs on the same 01:00 schedule as prod, so it meets the same late upstreams and a full day of bookings. A change that runs past the 03:00 dashboard deadline at full volume shows up in staging a night before it could reach finance. Dev runs on demand against the sample, which keeps the edit-and-run loop under 2 minutes.


<br>

### Getting data into lower environments

| Approach | Good for | Cost |
| --- | --- | --- |
| Read prod sources, read-only | Real volume and real edge cases | Needs a strict split between read and write credentials |
| Sample, such as 1% of bookings | Fast dev loops | Misses rare cases, like a city with 1 booking that day |
| Masked copy | Work near personal data | The masking job must rerun on every refresh |
| Synthetic rows | Unit tests and CI fixtures | Covers only the cases someone thought to write |

Copying all of prod into staging doubles storage and copies every privacy obligation attached to the data. Most teams let staging read prod sources in place and keep only its outputs separate.


<br>

### Where environment setups break

- A hardcoded analytics. table name in 1 query sends a dev test run's writes into prod.
- Staging shares prod's alert channel, so on-call is paged at 01:30 for a test failure.
- Dev credentials can write to prod for convenience, and a test truncates a prod table.
- Staging falls weeks behind prod, so a change passes against old schemas and fails on release.
- A backfill launched from a laptop with prod credentials skips review and the deploy pipeline.

Each failure on that list is a broken isolation boundary. When an interviewer describes an incident like these, name the boundary that leaked, then the control that restores it: a variable for the table name, a separate alert channel, a credential without write access.

> Try it
>
> An engineer changes `build_daily_revenue` to convert each booking at the hourly rate for its booking time, replacing the single day-end rate. Walk the change through the 3 environments. What does each one tell you that the one before it could not?

*Dev on the 1% sample shows the SQL runs and totals shift by a plausible amount. Staging on full prod sources at 01:00 shows the run still beats the 03:00 deadline and that an hourly rate exists for every currency booked in a full day. Prod is the first place finance sees it, after both have passed.*

> **TIP**
>
> For "how do you test without breaking prod": same code in 3 environments, config chooses the schemas, lower environments read prod read-only and write only to their own schemas, and promotion happens only through the deploy pipeline.


<br>

---

## CI/CD for Data

CI, continuous integration, runs automated checks on every proposed change before it merges. CD names 2 practices for what follows a merge. Continuous delivery keeps every merged change deployable, and the prod release can still wait for an approval. Continuous deployment releases to prod automatically once every check passes. The path below is continuous deployment: a change that passes its staging data tests reaches prod with no manual step. For an application, the checks cover code. A data pipeline adds 2 more things a change can break: the data it reads, whose shape it assumes, and the tables other teams read from it.


<br>

### The path from pull request to prod

![Pipeline diagram 2](images/pipeline_making_it_repeatable_beginner_diagram_2.png)


<br>

### What CI checks before merge

| Check | Catches | Runs in |
| --- | --- | --- |
| Lint and SQL parse | Syntax errors, banned patterns such as `SELECT \*` | Seconds |
| DAG validation | Cycles, missing upstream tasks, import errors | Seconds |
| Unit tests on transforms | Wrong logic on hand-written input rows | Under a minute |
| Fixture run | Whole DAG on a small fixed dataset, output compared to expected rows | A few minutes |
| Schema diff | A dropped or renamed column that downstream tables read | Seconds |


<br>

### Unit testing a transform

Logic that lives in a function can be tested on rows you write by hand, including the rows that break things. This conversion step is the one `build_daily_revenue` calls.

```sql
def to_usd(amount, currency, rates):
    if currency not in rates:
        raise ValueError(f"no rate for {currency}")
    return round(amount * rates[currency], 2)


def test_to_usd_converts_eur():
    assert to_usd(100.0, "EUR", {"EUR": 1.09}) == 109.0


def test_to_usd_rejects_missing_rate():
    try:
        to_usd(50.0, "JPY", {"EUR": 1.09})
    except ValueError:
        return
    assert False, "expected ValueError"
```

The second test records a decision: a booking in a currency with no rate fails the run, where silently counting it as zero would understate revenue with no error.


<br>

### Data tests that gate promotion

A data test is a query that returns the rows breaking a rule, and it passes when it returns 0 rows. After the staging run, these tests decide whether the change is promoted to prod.

```sql
SELECT
  ds,
  city,
  COUNT(*) AS row_count
FROM staging_analytics.daily_revenue
WHERE ds = '2026-03-14'
GROUP BY ds, city
HAVING COUNT(*) > 1
```

Other data tests on the revenue build

- Not null: revenue_usd has no nulls.
- Relationship: every currency in the day's raw.bookings rows has a row in the rates table.
- Volume: the day's row count is within 50% of the trailing 7-day average.
- Freshness: the latest ds equals the run's logical date.


<br>

### A logic change ships with its backfill

Merging a new revenue formula changes every partition built from tomorrow on. The 90 partitions of the current quarter still hold the old formula, so a dashboard comparing this month with last shows a jump that is only the code change. The release therefore includes a backfill: rerun `build_daily_revenue` for the affected dates. Because the task overwrites its own partition and reads its date from the run, the backfill can be paused halfway and resumed, or run twice, without duplicating a row.


<br>

### Rolling back

| ✓ Rolling back code | ✗ Rolling back data |
| --- | --- |
| • Revert the commit and redeploy<br><br>• Takes minutes<br><br>• Fixes every run from now on | • Partitions written by the bad version stay wrong<br><br>• Rerun the affected dates on the reverted code<br><br>• Safe only because each task overwrites its partition |

| ✓ Do | ✗ Don't |
| --- | --- |
| • Run the whole DAG on fixture data in CI<br><br>• Block promotion on failed staging data tests<br><br>• Diff output schemas against the queries downstream teams run<br><br>• Ship the backfill in the same release as the logic change | • Deploy to prod by copying files by hand<br><br>• Let a failed staging test be overridden without review<br><br>• Rename a column without a deprecation window<br><br>• Assume a code revert fixes data already written |

> Try it
>
> A pull request renames `city` to `city_name` in `daily_revenue`. It passes lint, and DAG validation and the unit tests pass too. Which check should stop it, and what happens the next morning if none does?

*The schema diff should flag it, because the finance dashboard query and a marketing table both read city. Without that check, the change deploys, the revenue task succeeds, and the failures surface the next morning in pipelines other teams own, where the cause is hardest to trace.*

> **TIP**
>
> For a CI/CD question, name 3 layers. Checks on code and schemas run before merge, and data tests on staging output run before promotion; the third layer is a backfill and rollback plan for partitions already written.


<br>

---

## Putting It All Together

A vacation rental marketplace's finance team closes each month from a daily revenue table built at 01:00 and needed on the dashboard by 03:00. A new rule converts foreign-currency bookings at the hourly rate for each booking time, it must apply to the whole current quarter, and the payments team's refunds table often lands late.

- The refunds table enters the DAG through a sensor on its completion record, so the revenue build never reads a partial day of refunds, and the sensor's timeout pages someone before 03:00.
- The new conversion rule lives in a unit-tested function, and the missing-rate test fixes the decision that an unknown currency fails the run.
- Staging runs the change on full prod sources at 01:00 but writes only to staging_analytics. Before promotion, the uniqueness and volume tests must pass.
- The release backfills the quarter's 90 partitions; because each run overwrites its own ds partition, the backfill can stop halfway and resume without duplicates.
- If the formula proves wrong, reverting the code and rerunning the same dates restores the old numbers. This works only because every task is idempotent.


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

A clinic network's nightly patient-visit rollup is three crontab entries an hour apart: extract from the scheduling database, clean, then aggregate into daily visit counts. When extract runs long, aggregate still fires on half-loaded data and the counts come out low. Make each step wait for the one before it to succeed, and let a brief connection drop to the scheduling database clear up without anyone stepping in. When a step truly fails, the on-call engineer re-runs from that step, not from extract, and the visit counts must match a clean night.

<details>
<summary><strong>Solution</strong></summary>

#### What this really is

This is **DAG orchestration** dressed up as a crontab cleanup. The three jobs already have an order; cron just never knew it, so the only thing keeping extract, clean and aggregate apart is a guess about how long each one takes. Anyone will say 'put it in Airflow.' What separates answers is the second half of the question: what the night looks like after a step fails. Widen the cron gaps and you only move the half-loaded morning to a slower night. Bolt retries onto steps that append and a single recovered blip counts every visit twice.

| Trick: Finished, not scheduled, is the trigger |
| --- |
| Three moves, each from a different section of the lesson:<br><br>1. Dependencies are edges: `clean_visits` starts when `extract_visits` succeeds, not at 2am.<br>2. A transient failure retries with exponential backoff before a human gets involved.<br>3. Every step replaces its night's output instead of adding to it, so a retry or a manual re-run from the failed step lands the same counts as a clean night. |

#### Walk the requirements

1. **Replace the clock with edges**

   The crontab encodes an order the scheduler cannot see. Move the three steps into one orchestrator-owned workflow where `extract_visits`, `clean_visits` and `aggregate_visits` are joined by edges. The only thing scheduled is the start of the run; every later step begins the moment its upstream succeeds, whether extract took 20 minutes or 2 hours.

2. **Let a failure stop the chain**

   With edges, a failed `clean_visits` means `aggregate_visits` never starts: it is waiting on a success that did not happen. That is exactly what the managers want. A missing number on the dashboard gets a question at 8am; a low number gets believed and quoted. Cron cannot give you this at any spacing, because it never knew clean existed.

3. **Retry the blip, with growing waits**

   The scheduling database dropping a connection a few times a week is the textbook transient failure. Put a retry policy on `extract_visits`: a few attempts, each waiting longer than the last. A two-minute blip clears on the second or third try, and a database that is genuinely down does not get hammered by instant retries.

4. **Make every step safe to run twice**

   A retry and an on-call re-run both mean running a step a second time. If `clean_visits` or `aggregate_visits` appends, the second run adds the night's visits again. Give each writing step a '`delete_write`' strategy scoped to the night being processed, and 'run it again' becomes a safe instruction. That is what lets the on-call resume from the failed step instead of re-extracting from scratch.

#### The shape that fits

```mermaid
flowchart LR
    subgraph L0["Source and control"]
        n0[("scheduling_db<br/>(PostgreSQL)")]
        n1["orchestrator<br/>(Airflow)"]
    end
    subgraph L1["Nightly steps"]
        n2["extract_visits<br/>(Pandas)<br/>SLA: #60; 24h · Idempotency: delete_write · On error: retry · Retries: 3 · Backoff: exponential"]
        n3["clean_visits<br/>(Pandas)<br/>SLA: #60; 24h · Idempotency: delete_write"]
        n4["aggregate_visits<br/>(Pandas)<br/>SLA: #60; 24h · Idempotency: delete_write"]
    end
    subgraph L2["Serve"]
        n5[("daily_visit_counts<br/>(PostgreSQL)<br/>SLA: #60; 24h")]
        n6(["clinic_dashboard<br/>(Metabase)<br/>SLA: #60; 24h"])
    end
    n1 --> n2
    n1 --> n3
    n1 --> n4
    n0 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n5 --> n6
```

*One start time, then success-driven edges: `extract_visits` retries its own blips, and every step replaces its night's output so a re-run from the failed step is safe.*

| ✕ Three crontab entries | ✓ One orchestrated workflow |
| --- | --- |
| Start times are guesses: extract at 1am, clean at 2am, aggregate at 3am. A slow extract means aggregate reads half a night. A failure anywhere is silent, and the next job fires anyway. Recovery means re-running everything by hand, in order, hoping nothing appends twice. | One start time; everything after it follows success. A failed step holds back only what depends on it. `extract_visits` retries its own blips. Each step replaces its night's rows, so the on-call resumes at the failed step and the counts match a clean night. |

| Pitfall: Retries on an append double the night |
| --- |
| The most common answer adds a `retryCount` and stops. It looks complete on the canvas, and it is worse than cron on the night extract dies at 90% loaded: the retry loads the whole night again and `clean_visits` sees most visits twice. Retries are only safe on top of writes that replace, which is why the lesson teaches the 2 together. |

| Interviewer: The re-run is the real question |
| --- |
| Every candidate draws the arrows. The follow-up that sorts them is 'clean failed at 3am, what does the on-call do?' The strong answer: fix the cause, re-run `clean_visits`, and let `aggregate_visits` follow on its own. No re-extract, because that night's extract output is still there, and every write replaces rather than adds. |

| Real world: Why the clock looked like it worked |
| --- |
| Cron gaps survive for months because extract usually finishes in 40 minutes, well inside its hour. The failure only shows on the slow nights, and it shows as numbers that are plausible but low, which is the worst kind of wrong: nobody notices until a manager asks. |

**Follow-up questions**

1. The scheduling system stops allowing a direct connection and instead drops a nightly export file at an unpredictable time. How does the workflow know when to start?\
   *Tests sensors from the dependency section: the run waits on a signal (the file exists) rather than a start time, the same move as the edges, one level up.*
2. A change to the cleaning rules ships next week. How do you know it will not drop visits before it touches the real counts?\
   *Tests environments and CI: run the change against a recent snapshot in staging and compare its counts with production before release.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

How would you make the daily PostgreSQL-to-warehouse pipeline repeatable, with each step starting only after the step it depends on has succeeded? The on-call engineer who owns it wants to be paged whenever a step fails or misses its daily SLA.

**Starting canvas** (already placed for you):

- Source (source, PostgreSQL)
- Orchestrated extract (transform, Airflow)

<details>
<summary><strong>Solution</strong></summary>

#### What this really is

This is DAG orchestration wearing a "just run it every morning" costume. The pattern the lesson taught is dependency-ordered scheduling: each step fires because the step it needs has **finished and succeeded**, not because a clock struck a time you hoped was late enough. The trap here is exactly that clock reflex: wire the extract to 2am, wire the load to 3am, and hope the extract is done. The day the source is slow, the load reads yesterday's data or a half-finished extract, lands it in the warehouse, and nobody pages. Repeatable means the pipeline notices that itself.

```mermaid
flowchart LR
    n0[("Source<br/>(PostgreSQL)")]
    n1["Orchestrated extract<br/>(Airflow)<br/>SLA: #60; 24h"]
    n2["Transform and load<br/>(SQL)"]
    n3[("Warehouse<br/>(warehouse)")]
    n4(["On-call alert<br/>(alerting)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n1 --> n4
    n2 --> n4
```

*Each edge is a dependency the orchestrator enforces, not a time you scheduled. Both tasks route a failure or an SLA miss to the on-call alert.*

#### Why this shape

1. **Make `Transform and load` depend on the extract, not on a clock**

   The edge from `Orchestrated extract` to `Transform and load` is the whole point. Inside the same Airflow DAG, the load has an upstream dependency on the extract, so the scheduler releases it only when the extract's state is `success`. Delete that edge and schedule both by time, and you have 2 independent jobs that happen to run near each other. That is the failure the question asks you to design out.

2. **Wait for a complete day before extracting**

   The extract has an upstream of its own: the source's day must be closed. If another pipeline fills that day, a sensor on the extract polls for the signal (the day's rows committed, the partition populated) before it reads. Downstream then only ever sees complete input, and a late source delays the run instead of corrupting it.

3. **Write the warehouse idempotently, keyed on the run date**

   The day's output lands in the `Warehouse` as a delete-then-insert (or `MERGE`) scoped to that run date, never a blind `INSERT`. A retry of a load that half-ran the first time therefore does not double-count. Repeatable is a property of the write, not only of the schedule: running today's task twice must leave the warehouse identical to running it once.

4. **Give each task bounded retries before it counts as failed**

   Each task gets `retries` with exponential backoff for the transient case, such as a source blip or a warehouse timeout. Because step 3 made the write idempotent, a retry has no side effects. The retry budget keeps a five-second network hiccup from waking a human at 3am, and it works only because the step it protects is safe to repeat.

5. **Page on failure AND on SLA miss, not just on exceptions**

   Both task nodes carry an edge to `On-call alert`, and 2 distinct triggers feed it. A task that exhausts its retries fires an on-failure callback. A run that has not reached `success` by the freshness deadline trips an SLA miss: that is the starter's `< 24h` target. A pipeline that alerts only on thrown exceptions goes silently stale the day a task just hangs.

| Trick: The edge is the dependency |
| --- |
| The one move that cracks this: in a DAG, the arrow between `Orchestrated extract` and `Transform and load` IS the ordering guarantee. You never write "wait 20 minutes" or "run at 3am". You declare the edge and let the scheduler hold the downstream task until the upstream's state is `success`. The retries, the idempotent write and the SLA all hang off getting that one idea right. |

| Pitfall: Retries without idempotency inflate the day |
| --- |
| The most common half-right answer adds `retries` but writes the warehouse with a plain `INSERT`. Attempt one inserts half the rows and fails; attempt 2 succeeds and inserts all of them. The daily total is inflated, and no error is ever raised. Retries are safe only on top of the run-date-keyed write from step 3. |

| ✕ Clock-scheduled (the trap) | ✓ Dependency-triggered (the answer) |
| --- | --- |
| Extract at 2am, `Transform and load` at 3am, as 2 independent schedules. This works until the extract runs 70 minutes; then the load reads a half-finished extract and writes it to the `Warehouse`. There is no dependency, so there is no failure signal. | `Transform and load` declares an upstream edge on the extract. A slow extract simply delays the load, which still reads the complete day. If the extract never finishes, the load never starts, and the `< 24h` SLA miss pages on-call. |

| Interviewer: What signals seniority here |
| --- |
| The tell is whether you alert on the SLA miss separately from the exception. Junior designs page on `raise`; the freshness deadline against `< 24h` catches the far nastier case, a task that is stuck rather than crashed. Mentioning idempotent re-runs unprompted is the second tell. |

**Follow-up questions**

1. The source drops yesterday's rows late, after today's run already loaded the `Warehouse`. How does a re-run fix that day without manual cleanup?\
   *Tests whether the run-date-keyed idempotent write turns a backfill into a plain re-run of one day.*
2. Every retry of the load re-reads the production database. Where would you keep the extracted day so that a re-run replays it instead of re-fetching it?\
   *Probes landing raw data in object storage between extract and load as the next step toward safe replay.*
3. The extract succeeds but lands zero rows. Should that page on-call, and where do you put the check?\
   *Probes a data-quality check as its own task edge, versus treating empty output as success.*
4. You now need this hourly instead of daily. What in the DAG changes, and what stays exactly the same?\
   *Tests whether the candidate sees the schedule interval and the partition granularity as the only knobs, with dependency ordering and idempotency untouched.*

</details>


<br>

---

## Key Takeaways

- A DAG replaces guesses about duration with success signals; the orchestrator schedules runs and resolves dependencies, and it tracks each task's state and retries the ones that fail.
- Every run is stamped with a logical date. Retries and backfills of that run keep the same date as the scheduled run.
- Wait on a readiness marker the producer writes last, never on a partition merely existing, and give the wait a timeout that raises an alert.
- A run finishes when its critical path does, so only tasks on that path are worth speeding up.
- A task is safe to retry when it reads its date from the run and replaces its output partition atomically.
- Retry transient errors with backoff; fail fast on logic errors and failed quality checks.
- Dev and staging run the same code as prod, with config picking the schemas, and only prod can write to prod tables.
- CI checks code and schemas before merge, staging data tests gate promotion, and a logic change ships with its backfill.
