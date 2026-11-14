Beginner·Pipeline Architecture · 25 min · 10 challenges

# Pipeline Operations: Beginner

An engineer is 3 weeks into a Google SRE on-call rotation. At 06:00 UTC her pager fires: yesterday's rows are missing from a revenue rollup she has never opened, and finance reads it at 08:00. Its author is asleep. Whether those 2 hours are enough depends on choices made months earlier: which signals the job emits, which alert paged her, and whether a runbook says what to try first.


<br>

---

## Script vs Operable Pipeline

Most pipelines begin as a script written by a single engineer and understood by the same engineer. This one reads yesterday's orders, sums revenue by region, writes the totals to a warehouse table and prints a line at the end. On its author's server it has worked every night for months. The first night it fails, the people who notice are the ones reading the revenue numbers.

```sql
import datetime

import db
import warehouse


def main():
    day = datetime.date.today() - datetime.timedelta(days=1)
    rows = db.query("SELECT region, amount FROM orders WHERE order_date = %s", day)
    totals = {}
    for row in rows:
        totals[row["region"]] = totals.get(row["region"], 0) + row["amount"]
    warehouse.insert("daily_revenue", day, totals)
    print("done")


main()
```

What the script leaves unanswered

- print("done") writes to a terminal nobody watches. Seen from anywhere else, a failed night and a good night look the same.
- date.today() ties the processed day to the clock, so once Monday has passed there is no way to ask for Monday again.
- warehouse.insert appends. Two runs for the same day write that day's totals twice, the duplicate-row bug from Idempotent Reruns.
- Nothing records the row count or the run time, so nobody can say whether a night's 312,880 orders is normal or a warning.

**orders.py fails at 02:00 on Tuesday. The Wednesday 02:00 run succeeds. At 09:00 on Wednesday an analyst spots a gap and runs the script once more by hand. What does daily_revenue now hold for Monday and Tuesday?**

- **A.** Monday and Tuesday once each, because the hand run filled in the day that failed to load.
- **B.** Monday is missing and Tuesday appears twice, because both Wednesday runs processed Tuesday and appended.
- **C.** Monday once and Tuesday missing, because the hand run went back and processed Monday's orders.
- **D.** Tuesday once and Monday missing, because the second insert overwrote the totals from the first.

<details>
<summary>Solution</summary>

**B.** Monday is missing and Tuesday appears twice, because both Wednesday runs processed Tuesday and appended.

Both Wednesday runs got Tuesday from today minus a day, so warehouse.insert loaded Tuesday's rows a second time. Only the failed Tuesday run would have loaded Monday.

**Why the others are wrong**

- **A.** The hand run read the clock like every other run, so it processed Tuesday again, and no run ever requested Monday.
- **C.** No input lets a run go back a day. On Wednesday, date.today() minus a day is Tuesday.
- **D.** warehouse.insert appends, so the second Wednesday run added another copy of Tuesday's totals.

</details>


<br>

### Operable means a stranger can run it

An operable pipeline can be run as a service by people who did not write it. Without opening the code, a stranger can find out whether last night's run happened, whether its output can be trusted, who must hear when it cannot, and what to do next. Each of those 4 answers is work the script never did.

✓Working script

- Started by cron on a single server
- Processes whatever day the clock says
- Appends on every run
- Reports success to a terminal
- Breakage found by the people reading dashboards

✗Operable pipeline

- Scheduled and retried by an orchestrator
- Takes the date to process as an input
- Replaces the day's rows, so reruns are safe
- Sends its logs and metrics somewhere searchable, along with the status of each run
- Monitors watch the output and alerts reach an owner, with a runbook to follow

> The 3 a.m. test
>
> Suppose the pipeline breaks at 03:00 and the only person awake has never looked at it. Can they tell it is broken before a customer says so, and how much it matters? Can they fix it, or hand it to the right team, without phoning the author? Every 'no' marks a missing piece.

The rest of this lesson works on `orders_daily`, the same job rebuilt as a DAG under an orchestrator. Its facts stay fixed in every section:

| Property | Value |
| --- | --- |
| Pipeline | `orders_daily` |
| Schedule | Daily at 02:00 UTC, processing the previous day |
| Tasks | `extract_orders`, `transform_orders`, `load_revenue` |
| Source | Orders database, read through a replica |
| Output | `daily_revenue`, a row per region per day |
| Normal runtime | About 40 minutes, done by 02:40 |
| Normal volume | 1.0 to 1.4 million orders a day |
| Freshness promise | Yesterday's rows present by 06:00 UTC |
| Consumer | Finance dashboard, opened at 08:00 UTC |

> **TIP**
>
> Name an owning team before the pipeline ships. An alert from a pipeline that nobody owns reaches nobody, however well it is written.

**Which change to orders.py lets a stranger recover Monday's revenue from that incident without making anything worse?**

- **A.** Wrap db.query in a retry loop, so a failed extract is attempted 3 times before the script exits.
- **B.** Print the row count next to done, so every run reports how many orders it loaded.
- **C.** Move the cron entry to a larger server, so the script has more memory and fails less often.
- **D.** Take the date as an argument and replace that date's rows on write, so Monday can be rerun safely.

<details>
<summary>Solution</summary>

**D.** Take the date as an argument and replace that date's rows on write, so Monday can be rerun safely.

The stranger can now ask for Monday on any day, and replacing the day's rows means a repeated run cannot double it.

**Why the others are wrong**

- **A.** Retries help with a brief outage on the night. Wednesday's hand run would still read the clock and append.
- **B.** The count still lands in a terminal nobody watches, and runs still process the clock's day.
- **C.** Fewer failures still leave each one with the same lost day and the same double count.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

`daily_orders_agg.py` runs from cron on its author's server: when the 02:00 run dies nobody reruns it, and last week's hand rerun appended a second copy of the day's revenue to `fct_orders`. Make it operable: run it under an orchestrator that retries a failed run, make the warehouse write safe to rerun, and check the totals before they reach `fct_orders`.

**Starting canvas** (already placed for you):

- Postgres orders (source, PostgreSQL)
- daily_orders_agg.py (transform, Pandas)
- fct_orders warehouse (storage, warehouse)
- Revenue dashboard (consumer, dashboard)


<br>

### Solution

#### From a script to a job someone else can run

An operable pipeline is one a stranger can run without its author: it starts on time, recovers from a passing failure, can be rerun without damage, and refuses to publish numbers nobody should trust. `daily_orders_agg.py` already computed the right totals. What it lacked was everything around the computation. Cron on one server starts it, but nothing retries a dead run, so the only recovery is a person running it again by hand. That hand rerun appended a second copy of the day to `fct_orders`.

```mermaid
flowchart LR
    n0[("Postgres orders<br/>(PostgreSQL)")]
    n1["orders orchestrator<br/>(Airflow)"]
    n2["daily_orders_agg.py<br/>(Pandas)<br/>Idempotency: delete_write · Retries: 3"]
    n3["totals check<br/>(custom)"]
    n4[("fct_orders warehouse<br/>(warehouse)")]
    n5(["Revenue dashboard<br/>(dashboard)"])
    n1 --> n2
    n0 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
```

*The data path is unchanged. The orchestrator starts and retries the job, the job replaces its own day instead of appending, and the totals check sits between the job and `fct_orders`.*

1. **Hand the schedule to an orchestrator**

   Cron fires the command and forgets it. An orchestrator records every run, so 'did last night happen' has an answer. It also retries a failed task a few times before giving up, which clears the brief database blips that kill most nightly runs.

2. **Make the write idempotent**

   Retries and hand reruns are only safe when running a day twice leaves the table as if it ran once. Deleting that day's rows and writing them fresh gives that guarantee. A plain append adds another copy of the day's revenue on every rerun.

| Pitfall: Retries on an appending job multiply the damage |
| --- |
| Adding retries to a job that appends turns one failure into several copies of the same day. Retry and an idempotent write are a pair: wire the first only once the second is in place. |

3. **Check the totals before they are published**

   The check runs between the job and `fct_orders`, so a day with zero orders or negative revenue stops before the dashboard shows it. Otherwise the people reading the numbers are the ones who discover the problem.

**Follow-up questions**

1. If the source database is down for 3 hours, how many retries and what spacing would you want before the run gives up?\
   *Extends retry handling to an outage longer than a blip.*
2. Finance corrects an order from 2 days ago. How would you reload that one day without touching the others?\
   *Applies the rerun-safe write to a deliberate backfill.*


<br>

---

## Logs, Metrics, and Traces

Once `orders_daily` runs under an orchestrator, every run can leave evidence behind. That evidence comes in 3 kinds called signals. Each kind answers a different question, and reaching for the wrong one is how a team ends up scrolling through thousands of log lines for an answer a graph would have shown at a glance.


<br>

### Logs: what happened, in detail

A log is a timestamped record of a single event, written by the code as it runs: a task started, a retry began, a connection was refused. Logs carry the detail an investigation needs: host, attempt number, exact error. Written as named fields, they can be searched, so `level = ERROR AND task = extract_orders` returns every extract failure at once.

```sql
{
  "ts": "2026-03-16T02:14:07Z",
  "level": "ERROR",
  "pipeline": "orders_daily",
  "task": "extract_orders",
  "run_date": "2026-03-15",
  "attempt": 4,
  "message": "connection refused",
  "host": "orders-replica",
  "port": 5432
}
```


<br>

### Metrics: how much, over time

A metric is a named number recorded on every run or every minute: rows loaded, minutes taken, runs failed. Each value is a few bytes, so years of history stay cheap and draw as a line on a graph. Metrics are where you notice that something changed. They hold none of the detail that explains the change.

| run_date | rows_loaded | duration_minutes |
| --- | --- | --- |
| 2026-03-04 | 1,198,402 | 41 |
| 2026-03-05 | 1,214,977 | 39 |
| 2026-03-06 | 1,302,115 | 43 |
| 2026-03-07 | 1,087,560 | 38 |
| 2026-03-08 | 1,121,833 | 40 |
| 2026-03-09 | 1,233,090 | 42 |
| 2026-03-10 | 312,880 | 12 |

The March 10 row stands out: about a quarter of the usual orders, loaded in under a third of the usual time. The metric cannot say why. The logs for that run can. Among them is a warning from `extract_orders` reading `replica lag 18h04m`: the replica the job reads from had stopped copying new orders at 07:56 on March 10.


<br>

### Traces: where the time went

A trace follows a single unit of work through each step it passes and records every step as a span with a start and an end. In a batch pipeline, a run is the trace and each task is a span. Traces answer the question the other 2 signals leave open: which step made a slow run slow? This is the trace for March 13, a run that ended at 04:55.

| Span | Start | End | Minutes | Usual minutes |
| --- | --- | --- | --- | --- |
| `extract_orders` | 02:00 | 02:24 | 24 | 24 |
| `transform_orders` | 02:24 | 04:49 | 145 | 10 |
| `load_revenue` | 04:49 | 04:55 | 6 | 6 |

The duration metric for that night reads 175 and says only that the run was slow. The trace puts all 135 extra minutes inside `transform_orders`, so the search starts in that task's logs and skips the other 2.

**Finance says revenue for the last 2 weeks of March looks low next to the first 2. Which signal do you open first?**

- **A.** The logs for every run in March, filtered to lines at the ERROR level.
- **B.** The trace for the most recent run, to see which task took the most time.
- **C.** The rows_loaded metric for March, to see when the daily count changed and by how much.
- **D.** The orchestrator's list of failed runs, since low revenue means some runs must have failed.

<details>
<summary>Solution</summary>

**C.** The rows_loaded metric for March, to see when the daily count changed and by how much.

A month of counts on a single graph shows the night the drop began, which tells you whose logs to read next.

**Why the others are wrong**

- **A.** A run that loads too few orders can finish without writing a single ERROR line.
- **B.** A single run's timing shows nothing about a trend across 4 weeks.
- **D.** March 10 succeeded and still loaded a quarter of its orders, so the failure list can be empty.

</details>


<br>

### Matching the signal to the question

| Question | Signal | Why that one |
| --- | --- | --- |
| Has the daily order count dropped this week? | Metric | A trend across many runs |
| Why did `extract_orders` fail at 02:14? | Log | The error text belongs to a single event |
| Which task stretched the March 13 run to 175 minutes? | Trace | Spans show each step's share of the time |
| Which order IDs did the transform reject? | Log | Per-record detail lives in events |

> **TIP**
>
> Investigations often use the signals in turn: a metric shows that something changed, a trace narrows it to a step, and that step's logs explain why.

**load_revenue failed on its last attempt in the run for March 16. You know the task and the run. Which signal tells you whether a rerun is worth trying?**

- **A.** Its log, because the error text shows whether the cause is transient, like a refused connection, or permanent, like a missing column.
- **B.** Its duration metric, because a long attempt shows the task got far enough that a rerun will finish.
- **C.** Its trace, because span lengths show which task failed, and that is enough to decide on a rerun.
- **D.** The failed-runs metric, because a high count this month marks the error as a known pattern to rerun.

<details>
<summary>Solution</summary>

**A.** Its log, because the error text shows whether the cause is transient, like a refused connection, or permanent, like a missing column.

Only the error text says which kind of failure this is, and a permanent one fails again on every rerun.

**Why the others are wrong**

- **B.** How long an attempt ran says nothing about why it stopped.
- **C.** You already know the task. Timing cannot tell a refused connection from a missing column.
- **D.** A count shows how often runs fail, without saying what failed this time.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

The nightly daily_orders_agg job loads the Snowflake revenue mart but logs only to stdout, so an overnight failure leaves no trace. Write its run events as structured logs to durable object storage an on-call engineer can read after the fact. Check its output for an empty or short load before it reaches the mart, and send a failed check to whoever is on call.

**Starting canvas** (already placed for you):

- Postgres orders database (source, PostgreSQL)
- daily_orders_agg (stdout only) (transform, Pandas)
- Snowflake fct_orders mart (storage, Snowflake)
- Revenue dashboard (consumer, Looker)


<br>

### Solution

#### Making a batch job answer for itself

Structured run logs and a row-count metric answer different questions at 3am. The logs say what one run did; the metric says whether its output looks like a normal day. A job that only prints to stdout gives neither once the terminal scrolls away. You added a durable home for the logs, a check on the row count before the mart, and a path from a failed check to a human, so `daily_orders_agg` is no longer a silent script.

```mermaid
flowchart LR
    n0[("Postgres orders<br/>(PostgreSQL)")]
    n1["daily_orders_agg<br/>(Pandas)"]
    n2[("run log object store<br/>(storage)")]
    n3["row count check<br/>(quality_gate)"]
    n4[("fct_orders mart<br/>(Snowflake)")]
    n5(["on-call alert<br/>(consumer)"])
    n6(["revenue dashboard<br/>(consumer)"])
    n0 --> n1
    n1 --> n2
    n1 --> n3
    n3 --> n4
    n3 --> n5
    n4 --> n6
```

*Run logs land in object storage; the row count check sits between the job and the mart and fans out to the on-call alert when it fails.*

1. **Persist run logs to a durable store**

   stdout dies with the process. Write each run's id, start time, row counts and outcome as one structured log record to object storage, so the responder can read it hours later. That is the difference between 'it broke' and 'it loaded 0 rows at 02:14'.

2. **Gate the mart on a row-count check**

   Put the check between the job and `fct_orders`, not after it. An empty or short load is the classic overnight failure, and the row count is the metric that exposes it. Catching it here stops the bad batch before it reaches the mart and the dashboard.

3. **Route a failed check to on-call**

   A gate that fails silently is no better than the stdout you replaced. Send the check's failure to a destination a human watches, so the page arrives before the revenue number is wrong rather than after someone notices it.

| Pitfall: A check that logs but does not block |
| --- |
| The common miss is a check that writes a warning to the log and lets the load proceed anyway. If the gate does not stop the write to `fct_orders`, you have a record of the damage but no protection from it. The check has to sit in the data path, not beside it. |

**Follow-up questions**

1. Should the alert carry the failure detail, or point at the run's log record?\
   *Shows how the alert and the log work together: the page says the run failed, the log says why.*


<br>

### Exercise 2 of 2

**● TASK**

The starter canvas is a new daily orders pipeline: an Airflow DAG scheduled at 2am loads a warehouse orders mart that feeds a revenue dashboard. Add the three day-one monitors: one that reads the orchestrator to catch a run that never started, one that reads it to catch a run that failed, and one that reads the mart to catch an output whose row count falls outside its expected band. Wire every monitor to an on-call alert destination.

**Starting canvas** (already placed for you):

- Orders database (source, PostgreSQL)
- Daily orders DAG (transform, Airflow)
- Orders mart (storage, warehouse)
- Revenue dashboard (consumer, dashboard)


<br>

### Solution

#### The three day-one monitors

A brand-new pipeline earns exactly three monitors, and they map to three questions you ask in order: **did it run, did it succeed, and was the output the right size**. Each one watches a different failure surface. Together they catch most of what breaks in a pipeline's first month. They are not redundant: a run can be missing entirely, present but failed, or succeeded but wrong, and only one signal tells you which. That is why each question gets its own monitor: a single "is it healthy" check cannot tell those three states apart.

```mermaid
flowchart LR
    n0[("Orders database<br/>(PostgreSQL)")]
    n1["Daily orders DAG<br/>(Airflow)"]
    n2[("Orders mart<br/>(warehouse)")]
    n3(["Revenue dashboard<br/>(dashboard)"])
    n4(["Run monitoring: did it run<br/>(consumer)"])
    n5(["Status monitoring: did it succeed<br/>(consumer)"])
    n6(["Volume monitoring: right row count<br/>(consumer)"])
    n7(["On-call alerting<br/>(consumer)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n1 --> n4
    n1 --> n5
    n2 --> n6
    n4 --> n7
    n5 --> n7
    n6 --> n7
```

*Two monitors read the orchestrator's run records, and the third reads the orders mart. All three alert the same on-call.*

1. **Did it run: read the orchestrator, not the data**

   The run monitor asks a question the data cannot answer: is there a run record for today at all? It queries the orchestrator's run history for the daily orders DAG and fires when no run exists by 2:30am. This is the monitor that catches a dead scheduler, a paused DAG, or a schedule that silently stopped firing. In those cases the mart still holds yesterday's rows, and every check on the table passes while the pipeline is dead.

2. **Did it succeed: the run exists but failed**

   The status monitor reads the same run record but checks its terminal state instead of whether it exists. Suppose a run starts, then throws on a source schema change or a bad credential. It leaves a record, so the run monitor is satisfied, but its state is 'failed'. This monitor alerts on that state. It catches code errors and upstream drift: the pipeline tried and told you it could not finish.

3. **Right size: read the destination**

   The volume monitor is the only one that reads the orders mart directly. It counts today's rows and fires when the count falls outside an expected band. This catches what the first two miss: a run that started on time and exited cleanly but wrote a tenth of the usual rows, because a silent filter dropped a category or a source extract was partial. A clean exit on a half-empty output is the classic silent failure.

| Pitfall: A single "is it healthy" check hides the state you need |
| --- |
| The tempting shortcut is one monitor that alerts if anything looks off. But "no run today", "run failed" and "wrong output" each demand a different response: restart the scheduler, fix the code, or hunt the silent filter. A merged check tells you none of them. Worse, a check that only reads the mart cannot see a missing run at all, because a stale table looks normal until someone compares dates. |

| Real world: Three now, the specific checks later |
| --- |
| On day one you do not yet know the pipeline's real failure modes, so blanketing it in column-level monitors just buys noise. Ran, succeeded and right size is the minimal set that covers the structural failures every pipeline shares. Add the specific checks, such as null spikes on `order_total`, once the pipeline has shown you where it actually hurts. |

**Follow-up questions**

1. The DAG is scheduled at 2am but a legitimately slow run finishes at 3am: how do you set the run monitor's window so it does not fire on a slow day?\
   *Freshness is measured against an expected start or completion time plus tolerance, not wall clock.*
2. How would you choose the row-count band for a pipeline whose volume swings strongly between weekdays and weekends?\
   *Pushes a static band toward a day-of-week baseline.*
3. If the orders database is down at 2am, which of the three monitors fires, and is that the signal you want?\
   *Whether a failed extract surfaces as a failed run or as a missing one.*


<br>

---

## Day-One Monitoring

Signals sit unread until something checks them. A monitor is a scheduled check that reads a signal and returns pass or fail. A new pipeline needs only the handful of monitors that between them catch its most common breakages, and each one should catch something the others cannot.


<br>

### 3 monitors to ship with

- **Run failure** *Reads run status from the orchestrator*\
  Fails when a task ends as failed with its retries used up. Fires earliest and names the task, but sees only runs that started.
- **Freshness** *Reads the newest date in daily_revenue*\
  Fails when yesterday's rows are missing at 06:00 UTC. Watches the output, so it notices runs that never started or never ended.
- **Volume** *Reads the order count for the run's date*\
  Fails when the count falls outside 1.0 to 1.4 million. Judges the data, so it notices runs that succeed with the wrong amount.

![Pipeline diagram 1](images/pa_pipeline_operations_beginner_diagram_1.png)

For the volume check to act as a gate, `transform_orders` now writes to a staging table, `stg_daily_revenue`, with the same columns as `daily_revenue`. A fourth task, `check_volume`, counts the orders in that table. `load_revenue` copies the day across only when the count passes.


<br>

### What each monitor catches alone

Each column below imagines a single monitor as the only one installed. Each row is a real way `orders_daily` breaks.

| What broke | Run failure alone | Freshness alone | Volume alone |
| --- | --- | --- | --- |
| `extract_orders` fails after 3 retries | Fires at 02:15 | Fires at 06:00 | Silent: check never runs |
| A deploy leaves the schedule paused | Silent: no run, no failure | Fires at 06:00 | Silent: check never runs |
| `transform_orders` hangs for 6 hours | Silent: still running | Fires at 06:00 | Silent: check never runs |
| Replica lag: 312,880 orders, run succeeds | Silent: run succeeded | Silent: the date is present | Fires before publish |

Freshness fires in 3 of the 4 rows, which makes it the monitor to keep if only a single one were allowed: it watches what the consumer will see, so it notices failures no task reported. Run failure fires 3 hours and 45 minutes sooner in the first row and names the broken task. Volume is the only check that judges the data; without it, the quarter-sized March 10 load reaches the dashboard.

**A Friday deploy leaves the orders_daily schedule paused. The team installed only the run-failure and volume monitors. When does anyone find out?**

- **A.** At 02:15 on Saturday, when the run-failure monitor sees that no run started and reports it.
- **B.** Before publish on Saturday, when the volume check counts 0 orders for Friday and holds the load.
- **C.** At 06:00 on Saturday, when the orchestrator marks the skipped run as failed and alerts.
- **D.** When someone notices stale numbers on the dashboard, since neither monitor fires without a run.

<details>
<summary>Solution</summary>

**D.** When someone notices stale numbers on the dashboard, since neither monitor fires without a run.

Both monitors need a run to happen. Only a check on the output would notice that nothing arrived.

**Why the others are wrong**

- **A.** Run failure reads the status of runs that exist. A paused schedule creates no run to fail.
- **B.** The volume check is a task in the paused DAG, so it never runs either.
- **C.** No run was scheduled, so the orchestrator has nothing to mark. Noticing missing output is what freshness does.

</details>


<br>

### Writing the data checks

![Schema diagram 2](images/pa_pipeline_operations_beginner_diagram_2.png)

The freshness query runs at 06:00 UTC against the table above and fails when `days_behind` is greater than 1.

```sql
SELECT
  MAX(revenue_date) AS latest_date,
  DATE_DIFF('day', MAX(revenue_date), CURRENT_DATE) AS days_behind
FROM daily_revenue
```

The volume query runs against the staging table after `transform_orders`. The orchestrator writes the run's date into it, so a rerun for March 10 counts March 10.

```sql
SELECT
  SUM(order_count) AS orders
FROM stg_daily_revenue
WHERE revenue_date = DATE '2026-03-10'
```

> **TIP**
>
> Only the freshness query reads the clock, because it asks about the present: is yesterday loaded yet? Every check that judges a run's data should use that run's date.

Set the range wide, from a few weeks of history; 1.0 to 1.4 million covers every normal March night. A 1.9-million-order sale day will trip it on good data, and the owner raises the ceiling for that date.

> What can wait until week 2
>
> - Duration: flag runs over 80 minutes, twice the usual 40, before they threaten 06:00
> - Null and uniqueness checks on `revenue_date` and `region`
> - Volume per region, once each region's normal range is known

**A volume check is written as below. On March 12, the owner reruns the pipeline for March 10. Which day's orders does this check count?**

```sql
SELECT
  sum(order_count) AS orders
FROM stg_daily_revenue
WHERE revenue_date = current_date - INTERVAL '1' DAY
```

- **A.** March 10, because the rerun passes its date to every task, including this check.
- **B.** March 11, because the query takes yesterday from the clock, whatever date the run is for.
- **C.** March 10 and March 11 together, because the interval covers the 2 days before today.
- **D.** No day at all, because current_date is undefined while a rerun is in progress.

<details>
<summary>Solution</summary>

**B.** March 11, because the query takes yesterday from the clock, whatever date the run is for.

On March 12, current_date minus a day is March 11, so the gate judges the wrong day's rows.

**Why the others are wrong**

- **A.** This query never reads the run's date. It computes yesterday from the clock.
- **C.** The condition is an equality on a single date. INTERVAL '1' DAY shifts that date by a day.
- **D.** current_date always returns today's date, so the query runs and counts March 11.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

The monitors on this daily orders pipeline raise three alerts: a customer-facing SLA was missed and needs action within the hour, a daily DAG failed but will retry on its own, and a row count has drifted 5 percent over the past week. Give the DAG's tasks 3 automatic retries so that failure recovers unattended, then route each alert to the destination whose urgency matches it, wiring a PagerDuty page, a Slack channel and an email digest off the monitors.

**Starting canvas** (already placed for you):

- Daily orders pipeline (transform, Airflow)
- Monitors: did-it-run, did-it-succeed, row-count drift (quality_gate, custom)


<br>

### Solution

#### One monitor, three severities, three channels

Severity-based alert routing is the discipline you just applied: each signal goes to a destination whose interruption budget matches how fast someone must act. A missed customer SLA has to wake someone inside the hour, so it earns a page. A DAG failure is only a heads-up once the tasks carry retries: three automatic attempts absorb a refused connection before any consumer feels it, so the failed attempt belongs in a chat channel on-call can glance at. A five-percent row-count drift over a week is a trend, so it rolls into an email digest nobody reads at 2am.

```mermaid
flowchart LR
    n0["Daily orders pipeline<br/>(Airflow)<br/>Retries: 3"]
    n1["Monitors: did-it-run, did-it-succeed, row-count drift<br/>(custom)"]
    n2(["Page for the SLA miss<br/>(PagerDuty)"])
    n3(["Chat channel for the auto-retry<br/>(Slack)"])
    n4(["Email digest for the weekly drift<br/>(Email)"])
    n0 --> n1
    n1 --> n2
    n1 --> n3
    n1 --> n4
```

*Retries on the DAG's tasks keep the run failure off the pager; the monitor block fans out to three destinations split by severity, not by which check produced the signal.*

| Key insight: Retries turn a failure into a record, not a page |
| --- |
| Setting `retryCount` on the DAG's tasks is what lets the run-failure alert sit in chat. Without retries, a refused connection at 02:00 leaves the orders table empty until someone reruns it by hand, and that failure would deserve a page. With them, most causes heal unattended, and if they do not, the SLA monitor still pages on the symptom the consumer feels. |

| Pitfall: Every alert on the pager trains you to ignore the pager |
| --- |
| The wrong build wires all three monitors to the pager because paging feels thorough. It is the opposite. When the auto-retry failure and the drift warning page alongside the real SLA miss, on-call learns most pages are noise and acks them on reflex, and that reflex is what swallows the twenty-first page. The loud channel only means something if it stays rare. |

| ✕ Split by who produced it | ✓ Split by how fast someone must act |
| --- | --- |
| Everything to the pager: the retry failure and the drift warning interrupt at the same volume as the SLA breach, so the alert that needs action in an hour looks identical to the one that fixes itself. | Three channels tiered by urgency: the pager carries only what a human must handle now, chat what they should notice today, email what they review weekly. Each channel's cost matches the response time it demands. |

| Real world: Severity is a routing key, not a text label |
| --- |
| This is why paging tools attach a severity to every alert and drive escalation off it. The check that emits a signal and the decision about who gets woken stay separate: the same drift check that emails a digest can be re-tiered to page once drift breaks a downstream contract. Severity on the alert makes routing a table you edit, not a rewiring job. |

**Follow-up questions**

1. If all three retries fail and the run is still down at 05:00, which alert should fire, and where does it go?\
   *Tests whether you route on exhausted retries rather than on the first failed attempt.*
2. A new monitor is added next quarter with no severity assigned. Where does it route by default?\
   *Tests whether routing fails safe: an unclassified alert should land somewhere visible but low-urgency, not silently page or vanish.*


<br>

### Exercise 2 of 2

**● TASK**

2 problems keep hitting the on-call for the orders_daily DAG: a run that finishes but writes a half-empty orders table, and a minor warning that pages someone who cannot act until morning. Extend the pipeline so each run is checked for completion and expected volume, failures that need action within the hour reach the on-call pager, and lower-severity warnings land in a channel the team reads during business hours.

**Starting canvas** (already placed for you):

- orders_daily DAG (Airflow scheduled 2am) (transform, Airflow)
- run + volume monitor (quality_gate, custom)
- on-call pager alert (consumer, alerting)


<br>

### Solution

#### The 2 checks a green run still needs

2 ideas carry the pipeline you just extended. A quality gate judges the output, not the exit status: an `orders_daily` run can finish cleanly and still write a half-empty table, so "did it finish" and "did it produce the right data" are separate questions. And routing by severity: a signal reaches the destination matched to how fast a human must act. The starter had one check and one destination; you split each in two.

```mermaid
flowchart LR
    n0["orders_daily DAG<br/>(Airflow)"]
    n1["run + volume monitor<br/>(custom)"]
    n2(["on-call pager alert<br/>(alerting)"])
    n3(["team channel alert<br/>(alerting)"])
    n0 --> n1
    n1 --> n2
    n1 --> n3
```

*One gate reads each `orders_daily` run, then fans out by severity: the pager for act-now, the team channel for act-later.*

1. **Check that the run completed**

   The signal the orchestrator hands you for free: did the `orders_daily` DAG reach its terminal task without erroring? This catches the crash, the timeout, the upstream that never landed. Necessary, but not enough, which is why the starter missed the second failure mode.

2. **Check that it produced its expected volume**

   Compare the run row count against a normal day. A run that writes 4k rows when yesterday wrote 400k passed every status flag and is still broken. This assertion is what turns a silent half-empty `orders` table into a signal instead of a next-morning surprise.

| Pitfall: A green run is not a good run |
| --- |
| Alerting only on task failure is tempting because that status is free. But partial writes, a truncated source, a filter that matched nothing all finish 'SUCCESS'. Without a volume or freshness assertion downstream of the run, the pipeline is blind to the failure that woke someone at 3am. |

| Key insight: Route by time-to-act, not by source |
| --- |
| Severity is a property of the consequence, not the check. Ask whether someone must act within the hour. A half-empty `orders` table blocks every downstream report, so it pages the on-call; a retried-and-recovered task is a warning for the team channel, read during business hours. Route everything to the pager and people learn to ignore it. |

**Follow-up questions**

1. How would you set the expected volume when `orders_daily` has strong weekday and seasonal swings?\
   *Pushes past a hardcoded threshold toward a rolling baseline, like this run versus a trailing 7-day median for the same weekday.*
2. A warning sits unacknowledged in the team channel for 2 days. Should it ever escalate to the pager?\
   *Tests whether severity is static or a function of age, and where that escalation logic lives.*


<br>

---

## Alerting That Stays Useful

A monitor decides pass or fail. An alert decides who hears about a failure, and how fast. Keeping the 2 decisions apart lets every monitor record its result while only a few results interrupt a person.


<br>

### 3 severities

| Severity | What happens | Use it when | In orders_daily |
| --- | --- | --- | --- |
| Page | Rings the on-call engineer at any hour until acknowledged | A consumer will be harmed soon and only a person can stop it | Freshness fails at 06:00 |
| Ticket | Opens a work item handled in working hours | A person must act, but a day's wait harms no one | 3 slow nights in a row |
| Record | Writes to a dashboard or channel, interrupting nobody | Context for later, or a problem already handled | A retry that succeeded |

Choosing between a page and a ticket comes down to 2 durations: how long until a consumer is harmed, and how long a fix takes. At 06:00 the dashboard is 2 hours away and a rerun takes about 40 minutes, so a 06:00 page leaves room to finish the repair.


<br>

### Page on what the consumer feels

Pages work best on symptoms, the effects a consumer would notice: data that is late or missing, or numbers that are wrong. Causes such as a refused connection or a slow task fit tickets and records better, since retries absorb many of them. A symptom page also stands in for the several cause alerts a single incident sets off.

| Monitor | Fires | Goes to |
| --- | --- | --- |
| Run failure | When a task runs out of retries | Team channel and a ticket |
| Volume gate | Before publish | Team channel and a ticket; the load is held |
| Freshness | 06:00 UTC | A page to the on-call engineer |
| Duration over 80 minutes | After the third slow night in a row | A ticket |
| Retry that succeeded | Any time | A record only |

With this routing, the extract outage in the run for March 15 produces a single page. The run-failure post from 02:15 is already in the channel when the freshness page arrives at 06:00, so the engineer starts with the cause named.

**A different pipeline feeds a report that opens at 08:00. A rerun takes 3 hours. Its only page is a freshness check at 06:00. What should change?**

- **A.** Page when a task fails, because a 06:00 page leaves 2 hours for a fix that needs 3.
- **B.** Keep the 06:00 page and add a ticket for each failure, so causes are reviewed the next day.
- **C.** Move the freshness check to 07:30, so it fires closer to the moment the report opens.
- **D.** Turn the freshness page into a ticket, since paging at 06:00 wakes people for nothing.

<details>
<summary>Solution</summary>

**A.** Page when a task fails, because a 06:00 page leaves 2 hours for a fix that needs 3.

The page has to arrive while the time left before harm still exceeds the time a fix takes. Paging on symptoms assumes the symptom leaves time to fix. Here it does not, so the earliest signal pages, even a cause.

**Why the others are wrong**

- **B.** The review happens after the report has opened without data, and the page still comes too late.
- **C.** At 07:30 only 30 minutes remain for a 3-hour fix, which makes the gap wider.
- **D.** The report is about to be harmed, which is when a page is justified. It needs to come sooner.

</details>


<br>

### Noise wears out the on-call

Each page that turns out to need no action teaches the on-call engineer that pages can wait. After enough of them, a real page waits too. Picture a team whose alert history for a week looked like this:

- **41** — pages in a single week
- **3** — pages that needed a person

Most of the rest were retries that later succeeded and volume ranges drawn too tight. Demoting the first to records and widening the second removes the noise and hides no real incident.




<br>

### What a page should say

✓Hard to act on

- Task failed: extract_orders
- No date, so the reader hunts for the run
- No impact, so the reader cannot judge urgency
- No link, so the reader starts from a blank screen

✗Ready to act on

- orders_daily: no rows in daily_revenue for 2026-03-15 at 06:00 UTC
- Impact: finance dashboard shows stale revenue from 08:00 UTC
- Cause so far: extract_orders failed at 02:14 after 3 retries
- Runbook: runbooks/orders_daily, freshness section

✓Do

- Page only when a consumer will be harmed and a person must act now
- Give every page the date and impact, plus a link to the runbook
- Demote any alert that fired twice with no action taken

✗Don't

- Page on a retry that went on to succeed
- Page more than once for a single incident
- Route alerts to a shared inbox nobody owns

**For the third night in a row, orders_daily ran over 80 minutes. Each run still finished before 03:40. How should this alert be routed?**

- **A.** Page the on-call engineer, because 3 slow nights in a row put the 06:00 promise at risk.
- **B.** Record it on the dashboard only, because the data arrived on time and needs no person.
- **C.** Open a ticket for the owning team, because a person should look but a day's wait harms no one.
- **D.** Page the source database team, because slow runs usually start in the system being read.

<details>
<summary>Solution</summary>

**C.** Open a ticket for the owning team, because a person should look but a day's wait harms no one.

The slowdown gets investigated in working hours, before it reaches 06:00, with no overnight page.

**Why the others are wrong**

- **A.** Every run finished more than 2 hours early. Nobody is harmed tonight, so waking someone buys nothing.
- **B.** A slowdown that keeps growing will miss 06:00 eventually, so a person should look at it.
- **D.** No trace has placed the slowdown in any step yet, and the source team does not own this alert.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Make the orders script operable before it ships tomorrow, keeping Looker reading the Snowflake `fct_orders` mart. Send run telemetry to a monitoring node that watches whether the job ran and succeeded, and add a row-count validation that checks the output is the right size. Wire both into three alert tiers, a PagerDuty page for a missed or failed run, a Slack channel for a size breach and an email digest for the rest. Connect every alert destination to an on-call runbook node.

**Starting canvas** (already placed for you):

- Postgres orders database (source, PostgreSQL)
- daily_orders_agg.py (script; ships tomorrow) (transform, Pandas)
- Snowflake fct_orders mart (storage, Snowflake)
- Revenue dashboard (consumer, Looker)


<br>

### Solution

#### Operability is a layer you build, not a property the script has

A batch job that runs is not the same as a batch job you can operate. `daily_orders_agg.py` already reads Postgres, aggregates, and writes the `fct_orders` mart. What it lacks is the **operational shell**: durable telemetry, monitors that notice when reality diverges from expectation, and a routed path from a failure to the human who fixes it. You are not changing the transform; you are wrapping it so the next 2 a.m. failure arrives as a page with a runbook link instead of a silent gap on the revenue dashboard.

```mermaid
flowchart LR
    subgraph L0["Production read path"]
        n0[("Postgres orders database<br/>(PostgreSQL)")]
        n1["daily_orders_agg.py (script; ships tomorrow)<br/>(Pandas)<br/>SLA: #60; 24h"]
        n2["Row-count validation<br/>(Great Expectations)"]
        n3[("Snowflake fct_orders mart<br/>(Snowflake)")]
        n4(["Revenue dashboard<br/>(Looker)"])
    end
    subgraph L1["Operational plane"]
        n5(["Run telemetry monitoring<br/>(Grafana)<br/>Alert: page if no successful run by 06:00"])
        n6(["On-call page<br/>(PagerDuty)"])
        n7(["Pipeline alerts channel<br/>(Slack)"])
        n8(["Daily alert digest<br/>(Email)"])
        n9(["On-call runbook<br/>(consumer)"])
    end
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n1 --> n5
    n5 --> n6
    n2 --> n7
    n5 --> n8
    n6 --> n9
    n7 --> n9
    n8 --> n9
```

*The read path still ends at Looker on `fct_orders`; the operational plane watches it. Every signal carries `run_id`, and every alert tier lands on the same runbook.*

| Key insight: `run_id` is the thread that ties the three signals together |
| --- |
| Logs, metrics, and traces are only useful if you can join them. Mint one `run_id` at the top of the script and stamp it on every log line, every metric point (`rows_written{run_id=...}`), and the run's trace span. When an alert fires, on-call pastes the `run_id` into the monitoring node and reads exactly that execution. |

1. **Give the job durable telemetry, not stdout**

   stdout dies with the process. Ship structured logs, counters such as `run_started`, `run_completed` and `rows_written`, and one trace span per run to the monitoring node. That node answers the first 2 day-one questions: did it run (no `run_started` by the expected time) and did it succeed (a missing `run_completed`).

2. **Check the output size where the data flows**

   The third question, was the output the right size, is about the data, not the process. A row-count validation between the script and `fct_orders` compares `rows_written` to a sane band from recent history and catches the half-load an exit code happily calls success.

3. **Route by severity so a page means act now**

   Missed or failed runs page through PagerDuty because the dashboard is going stale. A size breach goes to the Slack channel: degraded, worth eyes within hours, not adrenaline. Trends and near-misses go to the email digest. Three tiers keep a page meaning a real problem.

4. **Link every alert to one runbook**

   Every tier points at the runbook: what the pipeline owns, what each alert means, how to diagnose from the `run_id`, how to re-run safely without clobbering the live `fct_orders` rows, and who to escalate to. The fix arrives with the alert.

| Pitfall: The green run that shipped bad data |
| --- |
| Wiring only a success monitor is the classic gap: the job exits 0 after a Postgres hiccup wrote a fraction of the rows, the monitor reports all-clear, and revenue quietly drops on the dashboard. The row-count validation exists as its own signal for exactly this case. |

**Follow-up questions**

1. The `rows_written` band false-alarms on Black Friday and month-end. How would you make it seasonal?\
   *Moves the size check from a fixed band to a seasonal baseline.*
2. The script becomes three chained tasks. Where does `run_id` live now, and what does the did-it-run monitor watch?\
   *Extends one-run observability to a multi-task DAG.*
3. The size check fires after half a day of `fct_orders` is already on the dashboard. What is the safe recovery?\
   *Tests an idempotent re-run against a live read path.*


<br>

### Exercise 2 of 2

**● TASK**

A nightly Spark job loads about 2 million partner CSV rows from object storage into the warehouse and reports nothing about its runs. Add a check that rejects malformed rows before they reach the warehouse, and give the job the two signals it is missing: per-run metrics (rows read, rows rejected, run duration) charted on a monitoring dashboard, and structured JSON logs keyed by `run_id` in a searchable log store. Keep the two signals in separate stores.


<br>

### Solution

#### A number per run, a line per event

Logs and metrics are both things a pipeline says about itself. They answer different questions, so they belong in different stores. A metric is a number recorded once per run, like `rows_read` or run duration. It is tiny, cheap to keep for years, and made for a chart where a bad night stands out against the trend. A log is a timestamped record of one event: a JSON line with a level, a message and fields like `run_id` and `file_name`. Logs are high-volume and read by a person after something has already gone wrong. The row check sits between them: each rejection adds one to a count for the chart and writes one line saying why.

```mermaid
flowchart LR
    n0[("partner_csv_files<br/>(object storage)")]
    n1["nightly_spark_load<br/>(Spark)"]
    n2["reject_malformed_rows<br/>(quality_gate)"]
    n3[("warehouse<br/>(warehouse)")]
    n4[("structured_log_store<br/>(storage)")]
    n5(["pipeline_monitoring<br/>(Grafana)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n1 --> n4
    n1 --> n5
```

*The Spark load passes rows through a malformed-row check into the warehouse. Its per-run numbers, rejected count included, go to monitoring, and its event lines go to a separate log store.*

1. **Reject malformed rows before the warehouse**

   Rows read and run duration exist on any run, but rows rejected only means something once a check decides what a bad row is. A partner row with no partner id or a non-numeric amount is stopped before it reaches the warehouse. Every stop gives each signal something to report: one more in the run's count, and one log line naming the file and the reason.

2. **Send counts and duration to monitoring**

   3 numbers a night is about a thousand points a year, which is the scale a dashboard is built for. The morning glance shows a run that read its usual 2 million rows but rejected 40,000, without anyone opening a file.

3. **Send event lines to the log store**

   Fifty thousand JSON lines a run is too many to chart and too detailed to throw away. Keyed by `run_id`, they let you go from the odd point on the chart straight to that run's lines and read which files the rejections came from.

| Pitfall: Logs on a dashboard cost money and blur the chart |
| --- |
| Pushing every log line into the metrics backend turns each `run_id` or file name into a new series. Storage grows with log volume and the chart becomes noise. The reverse mistake, counting rejected rows by searching logs, turns the morning check into a search job. Count once inside the job and emit the count as a metric. |

| Real world: `run_id` stitches the signals together |
| --- |
| Traces, the third signal, follow one run through every step it passes. With a single job, the shared `run_id` on metrics and logs does that work. Once the load splits into several tasks, a trace shows which one took the time. |

**Follow-up questions**

1. The job splits into separate extract, validate and load tasks. Which signal tells you which task made last night's run slow?\
   *Where traces earn their place next to logs and metrics.*
2. A partner adds a column and rejected rows jump to 30 percent. Which signal shows it first, and which one explains why?\
   *Matching each question to the signal built to answer it.*


<br>

---

## A First Runbook

A runbook is the written procedure for a single alert. Its reader is the engineer the alert has reached, who may never have opened the pipeline's code. Its 5 sections run in the order that reader needs them: symptom and impact first, then diagnosis and response, with escalation last. Here is the runbook for the `orders_daily` freshness page.


<br>

### Runbook: orders_daily freshness page

#### Symptom

The freshness monitor found no rows in daily_revenue for yesterday at 06:00 UTC. A normal run finishes by roughly 02:40.

#### Impact

Finance opens the revenue dashboard at 08:00 UTC. Until the load lands, the newest day it shows is the day before yesterday. No other team reads this table.

#### Diagnosis

- **Step 1.** Open the orders_daily run for yesterday's date in the orchestrator. If no run exists, the schedule is paused or did not trigger: go to Response A.
- **Step 2.** If the run is still going, open its trace and compare each task with its usual minutes: extract 24, transform 10, load 6. Go to Response B.
- **Step 3.** If extract_orders, transform_orders or load_revenue failed, read the ERROR line from its last attempt. connection refused or timeout: the replica was unreachable, go to Response C. column not found or a type error: the source schema changed, go to Escalation and do not rerun.
- **Step 4.** If check_volume failed, run the volume query for yesterday's date and note the count. Go to Response D.

#### Response

- **A.** Unpause the schedule, then trigger a run with --date set to yesterday.
- **B.** If a task has run past 3 times its usual minutes, stop the run and start it again with --date. Otherwise check again at 06:30.
- **C.** Run SELECT 1 against orders-replica port 5432. If it answers, rerun with --date; load_revenue replaces the day's rows, so the rerun is safe. If refused, go to Escalation.
- **D.** Under 1.0 million usually means replica lag. Measure it on the replica with SELECT date_diff('minute', max(created_at), current_timestamp) FROM orders and rerun once it is under 10 minutes. Over 1.4 million on a known sale day: raise that date's ceiling, then rerun. Over 1.4 million otherwise: likely duplicated source rows, so page the orders database on-call and do not rerun.
- Finish every response by running the freshness query. days_behind must be 1.

#### Escalation

- No acknowledgement within 15 minutes: the secondary on-call is paged automatically.
- Not fixed by 07:30 UTC: tell the finance channel that yesterday's revenue is late, and when the next update will come.
- Replica down for over 30 minutes, or a schema change: page the orders database on-call.
- Owner: revenue data team. Last followed end to end on 2026-03-01.

**At 06:00 on March 21 the freshness page fires. In the run for March 20, extract_orders and transform_orders succeeded. Then check_volume failed at 02:12, so load_revenue never started. The volume query returns 402,115. What does the runbook have you do?**

- **A.** Step 3 applies because a task failed, so confirm the replica accepts connections and rerun at once.
- **B.** Step 4 applies, and Response D says to check replica lag, wait until it is under 10 minutes, then rerun.
- **C.** Step 4 applies, and the low count is real, so lower the floor to 400,000 and rerun the load.
- **D.** Step 1 applies because no data was published, so unpause the schedule and trigger a new run.

<details>
<summary>Solution</summary>

**B.** Step 4 applies, and Response D says to check replica lag, wait until it is under 10 minutes, then rerun.

A count far under 1.0 million points at replica lag. Rerunning before the replica catches up fails the gate again.

**Why the others are wrong**

- **A.** Step 3 is for a failed extract_orders or transform_orders, and it also covers load_revenue. A failed check_volume is Step 4.
- **C.** That would publish a fraction of the day's revenue. Response D moves the range only for a known sale day.
- **D.** The run exists and its tasks ran, so the schedule is working. Step 1 does not match.

</details>


<br>

### Writing for a stranger at 3 a.m.

- Name the exact query, command or screen; 'check the logs' names none of them
- Give each check its possible results and the next step for every result
- Say when a rerun would make things worse, as well as when it helps
- Have an engineer new to the pipeline follow it in working hours, and record the date
- Update it after each incident, while the missing steps are fresh



**A draft runbook's diagnosis section reads: 'Check the logs and fix the issue.' Which rewrite helps an engineer who has never seen the pipeline?**

- **A.** Add a paragraph on how the pipeline was designed, so the reader understands the code before acting.
- **B.** Rerun the pipeline first, and read the logs only if the rerun fails as well.
- **C.** Contact the pipeline's author, who knows the code best and can find the problem fastest.
- **D.** Name the log line to read, list the errors it may show, and give the next step for each.

<details>
<summary>Solution</summary>

**D.** Name the log line to read, list the errors it may show, and give the next step for each.

Each result now leads somewhere: a transient error to a rerun, a schema change to escalation.

**Why the others are wrong**

- **A.** Background slows the reader down at 03:00. They need a next action, and this gives none.
- **B.** A schema change fails again on rerun, and the wait comes out of the 2 hours before 08:00.
- **C.** A runbook exists so the fix does not depend on the author being awake.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

A new nightly job pulls about 500,000 shipment records from a carrier API at 2am, loads them with Spark under Airflow, and writes them to the warehouse. Add the three day-one monitors: a run check that flags no run by 2:30am, a success check on the latest run's final status, and an output-size check on the rows loaded. All three feed one monitoring view.


<br>

### Solution

Day-one monitoring is the smallest set of checks that makes a new pipeline operable: did it run, did it succeed, and was the output the right size. Each question has one honest source of truth. The design work is wiring each check to that source, not to whatever node happens to sit nearby.

```mermaid
flowchart LR
    n0[("carrier_api<br/>(API)")]
    n1["airflow_orchestrator<br/>(Airflow)"]
    n2["spark_load<br/>(Spark)"]
    n3[("warehouse<br/>(warehouse)")]
    n4["run_check<br/>(quality_gate)"]
    n5["success_check<br/>(quality_gate)"]
    n6["output_size_check<br/>(quality_gate)"]
    n7(["monitoring_view<br/>(consumer)"])
    n0 --> n2
    n1 --> n2
    n2 --> n3
    n1 --> n4
    n1 --> n5
    n3 --> n6
    n4 --> n7
    n5 --> n7
    n6 --> n7
```

*Airflow's run records feed the run and success checks, the warehouse feeds the size check, and all three land in one monitoring view.*

1. **Read run state from the orchestrator**

   If no run record exists by 2:30am, the scheduler never fired, and only Airflow can tell you that. The success check reads from the same place: the latest run's terminal status. A check attached to the warehouse cannot tell a run that never started from one that failed halfway.

2. **Read row counts from the warehouse**

   Size is a property of what landed, so the output-size check reads the loaded table and compares its row count against a band around the expected 500,000. A run can start on time and finish green yet still write 40 rows, for example when the carrier API returns an empty page.

| Pitfall: A green run can still load nothing |
| --- |
| Treating success as proof of correctness is the gap the third monitor closes. The status checks watch the job, and only the size check watches the data. Drop it, and consumers find the empty table before you do. |

| Key insight: One view, three separate answers |
| --- |
| Routing all three checks into one monitoring view means a single morning glance covers the pipeline. Keep them as three checks, though: a combined 'pipeline OK' flag hides which question actually failed. |

**Follow-up questions**

1. The carrier starts sending 2 million records on Mondays. How should the size check's expected band change?\
   *Whether the threshold follows the data's real shape rather than one fixed number.*
2. The job moves from 2am to 4am. What in the run check has to change?\
   *The run check's deadline is the schedule plus a tolerance, so it moves with the schedule.*


<br>

---

## Putting It All Together

A marketplace pays its sellers every Friday at 09:00 UTC from a nightly seller_payouts pipeline. On a Thursday night the source replica falls behind, and the run reads 40% of the usual orders while every task reports success.

- The volume check, run against Thursday's date, holds the load, so no seller is paid from partial numbers.
- The run-failure post names the gate, and the order-count metric shows the drop happened on a single night.
- Freshness pages at its deadline because sellers are harmed within hours and a rerun fits inside that window.
- The runbook's volume step sends the on-call engineer to check replica lag and wait for it to clear before rerunning; replacing the day's rows makes the rerun safe.
- If the replica stays down, the escalation section tells the payments team before 09:00 that payouts will be late.


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

An hourly Spark job loads inventory counts into a serving table the storefront reads, and once that table is two hours stale shoppers buy items that are gone. Apply the page-worthy test to three signals: staleness past two hours, a daily row-count drop someone should review next business day, and per-run duration, which needs no action. Route each to one tier: an on-call page, a team channel, or a dashboard only.


<br>

### Solution

#### Rationing pages with the page-worthy test

The page-worthy test asks three questions before an alert may wake anyone: does it need action within the hour, can that action not wait for business hours, and can the on-call engineer actually do something? An hourly inventory load makes the answers concrete. Two hours of staleness means the storefront sells stock that is gone, and the on-call can rerun the load, so it passes all three. Every other signal drops a tier.

```mermaid
flowchart LR
    n0[("inventory_counts<br/>(source)")]
    n1["inventory_load<br/>(Spark)"]
    n2[("storefront_inventory<br/>(serving store)")]
    n3["staleness_2h_check<br/>(quality_gate)"]
    n4["row_count_drop_check<br/>(quality_gate)"]
    n5(["on_call_page<br/>(alerting)"])
    n6(["team_channel<br/>(consumer)"])
    n7(["run_duration_dashboard<br/>(Grafana)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n2 --> n4
    n3 --> n5
    n4 --> n6
    n1 --> n7
```

*Only `staleness_2h_check` reaches `on_call_page`; the row-count drop goes to `team_channel` and run duration lands on a dashboard.*

1. **Give the page to staleness alone**

   `staleness_2h_check` reads the serving table and feeds `on_call_page`. It is the only edge into the page, and that is the point: when the page fires, the engineer knows shoppers are affected right now and a rerun will fix it.

2. **Send the volume drop to the team channel**

   A daily row-count drop is usually a supplier sending a short file, sometimes a real change in stock. Someone should look, but the next business morning is soon enough, so `row_count_drop_check` posts to `team_channel`.

3. **Chart run duration without notifying**

   A run creeping from six minutes to 9 is worth seeing on `run_duration_dashboard` when someone investigates, but no single reading calls for action. The dashboard notifies nobody.

| Pitfall: Every extra route to the pager spends the budget |
| --- |
| The tempting design wires all three checks to the page because each signal is real. Then the on-call is paged whenever a supplier sends a short file, learns that most pages can wait, and treats the staleness page the same way. A lower tier is not a weaker alert; it is why the page still gets answered. |

**Follow-up questions**

1. What changes if the load ran nightly and fed a report read at 9am?\
   *Applying the test when the deadline is hours away: a failed run has slack, so it may drop out of the page tier.*
2. If the row count fell by 90 percent instead of a few percent, should it still go to the channel?\
   *Whether a severe version of a lower-tier signal earns its own page-worthy threshold.*


<br>

### Exercise 2 of 2

**● TASK**

A nightly Airflow DAG uses Spark to load about 3 million smart-meter readings from object storage into the warehouse, and the 6am billing run reads them. Triage three failures by routing each to its tier: a run that fails or has not finished by 5am pages on-call; a load whose row count is far off the usual volume alerts the team channel without paging; malformed readings Spark skips go only to a log store, with no alert. Send the run and row-count metrics to a health dashboard.


<br>

### Solution

#### Severity decides where each signal goes

Triage means deciding, before anything breaks, how bad each failure is and who has to hear about it. Three failures share this one pipeline but earn three different responses. A run that misses 5am blocks billing for about 3 million meters, so it pages. A volume swing might be a real loss or just a holiday, so it goes to the team channel for a human to judge in daylight. A skipped malformed reading is expected noise, so it is only logged.

```mermaid
flowchart LR
    n0[("meter_reading_files<br/>(object storage)")]
    n1["nightly_dag<br/>(Airflow)"]
    n2["load_readings<br/>(Spark)"]
    n3["row_count_check<br/>(quality_gate)"]
    n4[("readings_warehouse<br/>(warehouse)")]
    n5(["billing_run<br/>(consumer)"])
    n6[("pipeline_log_store<br/>(log store)")]
    n7(["health_dashboard<br/>(consumer)"])
    n8(["oncall_page_alerts<br/>(alerting)"])
    n9(["team_channel_alerts<br/>(alerting)"])
    n0 --> n2
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n2 --> n6
    n1 --> n8
    n3 --> n9
    n1 --> n7
    n3 --> n7
```

*Run status leaves `nightly_dag` for the pager, `row_count_check` alerts the team channel, and skipped readings only reach `pipeline_log_store`.*

1. **Match each failure to its signal**

   Run status and lateness are metrics the orchestrator already emits. The row count is a metric the check produces. A malformed reading is a log line carrying the bad record's details. Metrics tell you something is wrong; logs tell you exactly what.

2. **Rate severity by who is blocked**

   A failed or late run stops billing at 6am, so a human must act now: page. A strange row count needs judgment, not a 3am wake-up: team channel. A few dozen skipped readings block nobody: no alert at all, just a searchable record.

3. **Give every tier its own destination**

   Keeping `oncall_page_alerts` and `team_channel_alerts` separate is what makes the tiers real: nothing non-urgent can ever reach the pager. Both metrics also land on `health_dashboard`, so whoever gets paged has context the moment they open it.

| Pitfall: One channel for every signal trains people to ignore it |
| --- |
| Wiring the row count and the skipped-reading count to the pager feels safe, but the on-call starts getting woken for holidays and routine noise. Within weeks the pages get skimmed, and the one that says billing is blocked gets skimmed too. |

**Follow-up questions**

1. Skipped readings jump from a few dozen to 200,000 in one night. Should that still only be logged?\
   *Turning a log-only signal into a counted metric with its own threshold and tier.*
2. The load slows from 40 minutes to three hours but still finishes before 5am. What shows where the time went, and should it page?\
   *Using a trace to locate slow steps, and keeping a degraded but on-time run off the pager.*


<br>

---

## Key Takeaways

- A script becomes an operable pipeline when a stranger can tell it ran, trust its output, know who to tell and know what to do, all without reading the code.
- Metrics show that something changed, traces show which step changed, and logs show why.
- Ship with 3 monitors: run failure for speed and detail, freshness for anything that keeps data from arriving, volume for runs that succeed with the wrong data.
- Checks on a run's data use the run's date; only the freshness check reads the clock.
- Page when a consumer will be harmed and only a person can stop it in time; send everything else to tickets and records.
- A runbook gives symptom, impact, ordered diagnosis checks, a response for each cause, and who to escalate to and when.
