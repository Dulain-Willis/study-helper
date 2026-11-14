Advanced·Pipeline Architecture · 35 min · 12 challenges

# Pipeline Anatomy: Advanced

Picture Spotify in October: the Wrapped team needs a listening-sessions table that a recommendations team built 4 years ago for its own model. Royalty reporting reads it too. Wrapped wants column names frozen until December, recommendations wants rows by 06:00, royalties wants every row. That is 3 promises from 1 pipeline, and none of them is written down. This lesson is about writing them down.


<br>

---

## Pipelines as Products

The intermediate lesson drew pipelines as boxes and arrows: sources on the left, a curated layer in the middle, consumers on the right. Those diagrams answer where data moves. A product framing asks a second question of every box on the right edge: who reads this, and what did we promise them? A pipeline is a product when it has a named consumer and a written contract, and when someone has planned how it will be retired. A pipeline with none of those is a script that happens to run on a schedule, and the first person to notice it broke is a stranger on another team.


<br>

### Name the consumer

Take a curated table of music listening sessions, one row per user per session, read by 3 teams. Each reads it for a different reason, so each breaks in a different way.

| Consumer | What they read | What breaks them |
| --- | --- | --- |
| Recommendations | sessions from the last 24 hours, by 06:00 | a table that lands at 09:00 misses the morning model refresh |
| Royalty reporting | every session in a calendar month | a day with 2% of rows missing understates payouts |
| Yearly recap campaign | 12 months of sessions, read once in December | a renamed `ms_played` column 3 days before launch |

Without this list, every design decision is a guess. Should the pipeline ship a partial day at 06:00 or a complete day at 09:00? Recommendations wants partial and royalties wants complete. The answer is 2 outputs: a `listening_sessions_early` snapshot at 06:00 with recommendations as its only consumer, and the final table at 09:00 for everyone else.


<br>

### Write the contract

A contract is the promise in writing: what a row means, which columns exist with which types, when the table is ready, how complete it is, how it may change, and who gets paged when any of that fails. It fits on one screen, and each consumer signs off on it before the first row lands. Here is the contract for the final table.

```sql
product: curated.listening_sessions
owner: listening-data-team
grain: one row per user per listening session
consumers:
  - royalty-reporting
  - yearly-recap
ready_by: "09:00 UTC"
completeness: at least 99.5 percent of the sessions landed in raw for that day
columns:
  user_id: bigint, never null
  session_start: timestamp, never null
  ms_played: bigint
  event_date: date, partition key
change_policy: additive only, 30 days notice before any removal or rename
paged: listening-data-oncall
```

Each line of the contract maps to a check the pipeline runs on itself. The `ready_by` and `completeness` lines become the query below, run at 09:00. It fails if the curated table holds under 99.5% of the sessions that landed in `raw.listening_sessions` for yesterday, or if yesterday's latest session started before 23:00, a sign the day was cut off. The `columns` line becomes a check on names and types.

```sql
SELECT
  COUNT(*) >= 0.995 * (
    SELECT
      COUNT(*)
    FROM raw.listening_sessions
    WHERE event_date = CURRENT_DATE - INTERVAL '1' DAY
  ) AS is_complete,
  MAX(session_start) >= CAST(CURRENT_DATE - INTERVAL '1' DAY AS TIMESTAMP) + INTERVAL '23' HOUR AS reaches_day_end
FROM curated.listening_sessions
WHERE event_date = CURRENT_DATE - INTERVAL '1' DAY
```


<br>

### Define the deprecation path

Every table is eventually replaced. The product framing plans that replacement on day 1, because the consumer list is the only way to know who the replacement will hurt. The path has 4 steps, and the contract's `consumers` field drives the 3rd.

- Ship the replacement under a new name, listening_sessions_v2, with its own contract
- Run both for a fixed window, 30 days, and publish the differences between them
- Contact each listed consumer and confirm they have moved; the list is the migration checklist
- Announce a final date for the old table, then drop it on that date, not 'soon'

> **TIP**
>
> Write the contract before the first row lands. A contract written after launch describes what the pipeline happens to do; a contract written before launch describes what the consumer needs, and every surprise a consumer hits comes from the difference between the 2.

**A team runs a nightly job that writes orders_daily. It has run for 3 years, 6 dashboards query it, and nobody on the team can say which 6. Which single change moves it furthest toward being a product?**

- **A.** Add a _v2 suffix to the table name so a future replacement has somewhere to go
- **B.** Query the warehouse's access logs, list the 6 dashboards and their owners, and record them with the table
- **C.** Add a row-count check so the team is paged when the nightly write produces 0 rows
- **D.** Move the job to a 09:00 schedule so it has time to finish before the dashboards refresh

<details>
<summary>Solution</summary>

**B.** Query the warehouse's access logs, list the 6 dashboards and their owners, and record them with the table

The other changes depend on the consumer list: the contract is written for those 6 owners, and the deprecation path uses the list as its checklist.

**Why the others are wrong**

- **A.** Versioning only helps once you know who to migrate. Without the consumer list, the suffix is a name change with no checklist behind it.
- **C.** A check belongs to a contract, but a check without a known consumer guards a promise nobody asked for. It is the 2nd step, after naming the consumer.
- **D.** Picking a time before knowing when the dashboards refresh is a guess; 1 of the 6 may refresh at 08:00 and break the moment the schedule moves.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Turn the curated fct_orders table into a written data contract: set its slaFreshness to < 1h so consumers know how fresh it is. Then add a Great Expectations quality_gate node and draw a data-flow edge from fct_orders into it, so the next upstream schema drift is caught before downstream teams read broken numbers.

**Starting canvas** (already placed for you):

- Postgres orders (source, PostgreSQL)
- fct_orders (storage, warehouse)
- Finance dashboard (consumer, dashboard)
- ML feature store (storage, Feast)
- Ops runbook (consumer, Grafana)

<details>
<summary><strong>Solution</strong></summary>

#### A table with a promise attached

Treating a pipeline as a product means `fct_orders` ships with a **data contract**: a written promise its readers can check. 2 clauses carry it. A freshness commitment says how stale the table may get before it counts as broken. A quality gate says what the rows must look like to be trusted. Freshness without the gate is half a contract, and upstream schema drift lives in exactly that missing half.

| Pitfall: Fresh rows can still be wrong |
| --- |
| When an upstream field changes meaning, the load still lands on schedule, so any lateness check stays green. A freshness SLA measures when data arrived, never what shape it arrived in. Only a check that reads the values catches an upstream field change before readers do. |

```mermaid
flowchart LR
    n0[("Postgres orders<br/>(PostgreSQL)")]
    n1[("fct_orders<br/>(warehouse)<br/>SLA: #60; 1h")]
    n2["Orders quality gate<br/>(Great Expectations)"]
    n3(["Finance dashboard<br/>(dashboard)"])
    n4[("ML feature store<br/>(Feast)")]
    n5(["Ops runbook<br/>(Grafana)"])
    n0 --> n1
    n1 --> n2
    n1 --> n3
    n1 --> n4
    n1 --> n5
```

*Reference design: `fct_orders` carries `slaFreshness` '< 1h', and a data-flow edge feeds it into a Great Expectations gate that watches the table all 3 readers depend on.*

1. **Set the SLA as a property, not a label**

   Put '< 1h' in the node's `slaFreshness` field. Writing 'SLA < 1h' into the tech label reads fine to a person but is an annotation, not a commitment: nothing can alert on it. Choose the bound from what the readers need, not from how often the job happens to run today.

2. **Wire the gate to the table it guards**

   A `quality_gate` node sitting beside `fct_orders` with no edge monitors nothing. The edge from `fct_orders` into the gate is what points its expectations (values in range, totals reconcile, unique order keys) at the curated output. That one edge turns a silent upstream field change into a failed check.

| Real world: The contract is versioned with the model |
| --- |
| In practice the SLA and the expectation suite live in the same repo as the `fct_orders` model, and a failing checkpoint blocks promotion instead of only sending email. The same shape guards a `dim_customers` table feeding CRM syncs: one freshness bound, one gate, one edge. |

**Follow-up questions**

1. Should a failing gate block the load entirely, or quarantine the bad partition and keep serving the last good data?\
   *Whether a gate is a circuit breaker or just a report.*
2. If an upstream field is legitimately new, how does the expectation suite evolve without accepting every future drift?\
   *Connects the contract to deliberate schema evolution.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

A daily Python script, triggered by cron, pulls from Postgres into Snowflake for a Looker dashboard. Before next week's compliance audit, keep those 4 roles as they are and give each concern the script leaves unowned its own node: a scheduler that owns the run, a monitor that checks the warehouse output lands on time and correct, a secrets manager holding the hardcoded database password, and a CI runner that tests code changes before they ship.

**Starting canvas** (already placed for you):

- Postgres app (source, PostgreSQL)
- Daily Python ETL (no orchestrator, no monitor, hardcoded creds) (transform, Pandas)
- Production warehouse (storage, Snowflake)
- BI dashboard (consumer, Looker)

<details>
<summary><strong>Solution</strong></summary>

#### 4 loose ends, 4 named owners

Undercurrents are the concerns that run beneath every role, and the rule for them is plain: **every operational concern a pipeline depends on needs a named owner**, not a side effect of the one script that happens to do the work. Your daily job moves data from `Postgres` into `Snowflake` and `Looker` reads the result, and that data path is fine. But 4 concerns were riding on nothing: what triggers the run, what confirms it landed on time and correct, where the password lives, and what proves a code change is safe before it ships. The fix is not more Python in the script. It is 4 nodes, each owning exactly one of those concerns, sitting beside the 4 data roles you kept in place.

```mermaid
flowchart LR
    n0[("Postgres app<br/>(PostgreSQL)")]
    n1["Daily Python ETL<br/>(Pandas)"]
    n2[("Production warehouse<br/>(Snowflake)")]
    n3(["BI dashboard<br/>(Looker)"])
    n4["Scheduler<br/>(Airflow)"]
    n5["Output monitor<br/>(Great Expectations)"]
    n6[("Secrets manager<br/>(storage)")]
    n7["CI runner<br/>(transform)"]
    n4 --> n1
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n2 --> n5
    n6 --> n1
    n7 --> n1
```

*The 4 data roles stay as they were. The scheduler, the output monitor, the secrets manager and the CI runner each own one loose end, and nothing on the data path had to move.*

1. **Give the run an orchestrator, not a cron line**

   A `cron` entry fires the script blindly: no retries, no dependency awareness, no record of whether yesterday's run even happened. A scheduler like `Airflow` owns the schedule as a DAG. It knows the run's start, its upstream dependencies, its retries and its history, so a failed 6am run gets retried and alerted instead of silently skipped. That is DAG orchestration, and it belongs to a tool, not a crontab.

2. **Watch the output, not just the exit code**

   A script that exits 0 tells you it ran, not that it was right. The monitor reads the warehouse and checks what the business actually uses: did the table land on time, is the row count in range, are the key columns non-null. Because it watches the output independently of the job that writes it, a broken transform surfaces before a stale dashboard does.

3. **Move the password out of the file**

   A hardcoded credential lives in every clone, every backup and the whole commit history. A secrets manager holds the password and hands it to the job at run time, so the code carries a reference instead of the secret. Rotating the password becomes a config change, with no code redeploy.

4. **Gate the change before it ships**

   Shipping straight to production means the first test of any change is the production run. A CI runner executes the test suite on every change and blocks the deploy when it fails, so the warehouse stops being the place you discover a broken transform. The pipeline's code gets the same gate every other service's code already has.

| Pitfall: More code in the script is not an owner |
| --- |
| The tempting wrong move is to answer all 4 with more Python: a `try/except` retry loop for scheduling, a row-count `assert` for quality, `os.getenv` for the password, a `pytest` call at the top of the file. That just relocates the 4 loose ends into the same unowned file. Each concern needs a node that owns it whether or not the script runs today. |

| Real world: Silent failure is the default without owners |
| --- |
| Most incidents where the pipeline was down and nobody noticed look exactly like this: the job had been failing for days, nothing watched the `Snowflake` output, and the first signal was a VP asking why `Looker` was stale. Naming an owner for each concern turns a script nobody maintains into a system somebody can be paged for. |

**Follow-up questions**

1. The scheduler adds failure modes of its own. What watches whether `Airflow` itself is running?\
   *Applies the monitoring concern to the orchestrator itself, not just the data output.*
2. If a second daily job also needs the `Postgres` password, what changes in how the secrets manager is set up?\
   *Extends the secrets concern to shared credentials and access scoping.*
3. The monitor flags a table that landed late. Should it page a human or block the dashboard from reading the stale data?\
   *Pushes on what monitoring should do once it detects a problem.*

</details>


<br>

---

## The Cross-Cutting Undercurrents

The beginner lesson had 4 roles: source and consumer at the ends, with transform and storage between them. The undercurrent map regroups them into 4 columns that read left to right. Source stays as it was. Ingest is the first transform, the one that pulls data out of the source. Transform covers the remaining steps and the storage they write. Serve is the stored output as consumers read it. The undercurrents run the other way. Each is a concern no single box owns, so each shows up in all 4 columns, and a design that handles it in 1 column and forgets the other 3 still fails.


<br>

### The 6 undercurrents

- **Reruns** *a step runs twice*\
  A nightly load fails at 02:10 and someone restarts it at 08:00. If the first attempt had already written 400,000 rows, does the table now hold 800,000?
- **Failures** *a step stops partway*\
  The transform step dies after writing 3 of 12 regions. Does the serving step wait, run on the 3, or run on yesterday's 12?
- **Change** *a source changes shape*\
  The orders service renames amount to amount_cents. Which step is the first to notice, and is that step a check or a dashboard?
- **Lateness** *a row arrives after its day closed*\
  A session that ended at 23:58 on Tuesday reaches the pipeline at 00:40 on Wednesday, after Tuesday's totals were served. Does Tuesday ever get corrected?
- **Visibility** *someone asks if the table is right*\
  At 09:00 an analyst asks whether yesterday is complete. Can anyone answer from a dashboard, or only by running queries by hand?
- **Cost** *the bill arrives*\
  The pipeline scans 2 TB a night. Is that because consumers need 2 TB, or because a rerun also rescans 30 days it did not need to?

Each undercurrent has a different answer in each role, which is why mapping them is worth a table. Read it one row at a time: the reruns row asks what the source does under a rerun, then what ingest does, then transform, then serve. A cell that nobody on the team can fill is a gap in the design, found before it is found in production.

| Undercurrent | Source | Ingest | Transform | Serve |
| --- | --- | --- | --- | --- |
| Reruns | can the source be read twice for the same window | does a 2nd pull for the same window replace the 1st | does rerunning a day overwrite that day's output | does a consumer see the half-written state mid-rerun |
| Failures | does the source return partial data or an error | is a partial pull discarded or kept | does a step that dies leave a marker the next step reads | does serving fall back to the last complete day |
| Change | does the source announce changes to its shape | are unknown columns kept, dropped, or rejected | does a renamed column fail the step or silently null the output | does the consumer's contract say which columns are stable |
| Lateness | how long after an event can the source still emit it | is the pull window wider than the lateness | are closed days reopened, and how many days back | does the consumer know a day's number may move |
| Visibility | does the source expose its own row counts | is the pulled row count recorded per run | is output row count compared with input | is each day's ready time recorded where consumers look |
| Cost | does each pull hit a production database | is the pull incremental or full | does a rerun touch 1 day or 30 | does each consumer's query read only the partitions it needs |


<br>

### Walking a design through the map

Take the listening sessions pipeline and walk the lateness row. Source: the mobile app buffers sessions while the phone is offline, so a session can arrive up to 3 days late. Ingest: the nightly pull reads yesterday only, so a 2-day-old session is never pulled. That cell already fails. Transform and serve never see the row, so nothing in their cells can recover it. The fix belongs in ingest: widen the pull to 3 days and rewrite those 3 days each night. That fix then changes the reruns row, because 3 days are rewritten and the write must replace rather than add, and the cost row, because 3 days are scanned instead of 1. One undercurrent pulled 2 others with it, which is the normal case.

> How the rows pull on each other
>
> - Lateness forces reruns: a day that reopens is a day that is written twice
> - Reruns force cost: every reopened day is rescanned, so the rerun window is a bill
> - Failures force visibility: a step that stops partway is safe only if the next step can see it stopped
> - Change forces contracts: a renamed column is harmless to a consumer whose contract never promised it

**A pipeline serves daily revenue. On Thursday a consumer notices that Monday's revenue figure changed by 0.4% overnight, with no alert and no note anywhere. Which cell of the map holds the gap?**

- **A.** Transform, lateness: the step should refuse rows for a day that has already closed
- **B.** Ingest, reruns: the 2nd pull for Monday should have replaced the 1st instead of appending
- **C.** Source, change: the revenue source renamed a column and the pipeline did not notice
- **D.** Serve, lateness: the consumer was never told that a closed day's figure can move

<details>
<summary>Solution</summary>

**D.** Serve, lateness: the consumer was never told that a closed day's figure can move

The number moved for a good reason. The gap is in the serve cell: the contract never said days reopen, so the consumer read a correction as a bug.

**Why the others are wrong**

- **A.** Refusing late rows removes the surprise by dropping real revenue. The map asks whether closed days reopen, and for revenue the answer is yes.
- **B.** An append bug doubles rows or adds a large block. A 0.4% move with no other symptom matches late rows arriving and being counted correctly.
- **C.** A renamed column fails a step or nulls a whole column; it does not nudge a figure by 0.4%. The shape of the symptom rules this out.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

One Airflow instance owns a 60-task mega-DAG spanning three teams: ingestion, analytics-engineering curation, and ML feature extraction. When the analytics dbt step fails, the unrelated ML feature extraction halts and pages cross team lines all night. Apply the section's split-DAG framing and split the orchestrator along ownership boundaries so a failure in one team's branch does not halt unrelated work in another.

**Starting canvas** (already placed for you):

- Postgres app (source, PostgreSQL)
- payments API (source, API)
- bronze object storage (storage, object storage)
- Airflow (single mega-DAG, all three teams) (transform, Airflow)
- silver warehouse (storage, warehouse)
- BI dashboard (consumer, dashboard)
- ML feature store (storage, Feast)

<details>
<summary><strong>Solution</strong></summary>

#### Split the DAG along ownership boundaries

Splitting by ownership means **one DAG per owning team**, with the teams connected by data-aware scheduling instead of task dependencies. A mega-DAG that spans ingestion, analytics-engineering and ML puts all 60 tasks under a single scheduler, a single retry policy, a single SLA and a single on-call. That is why a failure in the analytics dbt step reaches ML feature extraction: the two were never truly dependent, they only shared one DAG. When the orchestration boundary follows the team boundary, each branch fails, retries and pages inside the team that owns it.

| Pitfall: A cross-team sensor rebuilds the coupling |
| --- |
| A common mistake is to cut the mega-DAG into three DAGs and then chain them with an `ExternalTaskSensor` that makes `ml_features_dag` wait for the analytics DAG to finish. That reproduces the exact coupling you were trying to remove: a dbt failure still blocks ML. ML features read from the silver warehouse, not from the dbt marts. They should trigger off the same upstream signal as analytics, side by side, never behind it. |

```mermaid
flowchart LR
    subgraph L0["Ingestion team"]
        n0[("Postgres app<br/>(PostgreSQL)")]
        n1[("payments API<br/>(API)")]
        n2["ingestion DAG<br/>(Airflow)"]
        n3[("bronze object storage<br/>(object storage)")]
        n4[("silver warehouse<br/>(warehouse)")]
    end
    subgraph L1["Analytics-engineering team"]
        n5["analytics DAG<br/>(Airflow)"]
        n7(["BI dashboard<br/>(dashboard)"])
    end
    subgraph L2["ML team"]
        n6["ML features DAG<br/>(Airflow)"]
        n8[("ML feature store<br/>(Feast)")]
    end
    n0 --> n2
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n4 --> n6
    n5 --> n7
    n6 --> n8
```

*Three team-owned DAGs meet at the silver warehouse. Ingestion writes it, and analytics and ML each read it on their own schedule, so neither can halt the other.*

1. **Cut the DAG where the teams are**

   Give each team its own DAG: `ingestion_dag`, `analytics_dag` and `ml_features_dag`. Each gets its own schedule, `retries`, `sla` and `default_args` owner, so a page routes to the team that can fix it. The point of the split is not smaller files. It is a separate blast radius for each owner.

2. **Replace the internal edge with a shared signal**

   Where the mega-DAG used a task dependency to connect teams, publish a Dataset on the silver warehouse instead. The silver load in `ingestion_dag` declares `outlets=[silver_ready]`, and both `analytics_dag` and `ml_features_dag` set `schedule=[silver_ready]`. When silver lands, Airflow wakes both consumers as peers. A dbt failure inside `analytics_dag` never enters the `ml_features_dag` run at all.

**Data-aware wiring across the three DAGs**

```python
from airflow.datasets import Dataset

silver_ready = Dataset("warehouse://silver")

# ingestion_dag: the load task announces silver is fresh
load_silver = SQLExecuteQueryOperator(
    task_id="load_silver",
    outlets=[silver_ready],
    ...
)

# analytics_dag: runs when silver updates
with DAG("analytics_dag", schedule=[silver_ready], ...):
    run_dbt = BashOperator(task_id="dbt_run", ...)

# ml_features_dag: also runs when silver updates, in parallel
with DAG("ml_features_dag", schedule=[silver_ready], ...):
    build_features = PythonOperator(task_id="build_features", ...)
```

*The producer emits one Dataset, and each consumer schedules on it independently.*

| Real world: The orchestrator boundary tracks the org chart |
| --- |
| This is the standard shape once a platform grows past one team. The orchestrator boundary tracks the team boundary, and cross-team hand-offs become Datasets (or an event or queue). That also gives each DAG its own lineage and its own SLA clock. Two signs that a design is drifting back toward a mega-DAG: a cross-team `ExternalTaskSensor`, or a task in one team's DAG that reaches into another team's tables. |

**Follow-up questions**

1. If ML features genuinely need the dbt marts, not just silver, how do you express that without re-coupling to the whole analytics DAG?\
   *Emitting a second, narrower Dataset on just the marts table rather than sensing a whole DAG's completion.*
2. How would you let analytics run on a schedule AND when silver updates, whichever comes first?\
   *Combining time-based and Dataset schedules.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

Redraw this hand-rolled fintech reporting stack so ingestion, storage and cleaning run on bought or open-source components and the attribution model stays the only custom code. The bought CDC connector lands source rows unchanged straight in the warehouse, so decide whether the cleaning the Python extractor does today runs before the load or inside the warehouse. The attribution model reads the cleaned rows and keeps feeding the dashboard.

**Starting canvas** (already placed for you):

- Postgres app DB (source, PostgreSQL)
- Custom Python extractor (transform, Pandas)
- Postgres reporting DB (custom) (storage, PostgreSQL)
- Custom attribution Python script (transform, Pandas)
- Attribution dashboard (consumer, Metabase)

<details>
<summary><strong>Solution</strong></summary>

#### Buy the plumbing, load first, keep the model

Moving the cleaning from before the load to inside the warehouse is the shift from **ETL to ELT**, and buying the ingestion layer is what forces it. The Python extractor is ETL: it reshapes rows in flight, which is exactly why an engineer has to own it. A bought CDC connector only extracts and loads, so the cleaning has to run where the rows land, as SQL inside the warehouse. Run the same per-layer test on the rest: ingestion and storage differentiate nothing, so they are bought; the attribution model is what makes the company different, so it stays built.

```mermaid
flowchart LR
    n0[("Postgres app DB<br/>(PostgreSQL)")]
    n1[("CDC connector<br/>(CDC)")]
    n2[("Warehouse<br/>(warehouse)")]
    n3["In-warehouse cleaning<br/>(SQL)"]
    n4[("Cleaned warehouse tables<br/>(warehouse)")]
    n5["Attribution model<br/>(transform)"]
    n6(["Attribution dashboard<br/>(Metabase)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n5 --> n6
```

*The connector loads rows unchanged into the warehouse, SQL cleans them inside it into cleaned tables, and the attribution model is the only custom code.*

| ✕ ETL: clean before the load | ✓ ELT: clean inside the warehouse |
| --- | --- |
| The Python extractor cleans rows in flight and writes only the cleaned result, so the cleaning logic lives inside a custom ingestion job the team has to keep alive. | The connector lands rows unchanged, and SQL inside the warehouse turns them into cleaned tables. Ingestion is bought, and the cleaning is a few queries on a store you already pay for. |

| Pitfall: A transform before the load keeps a custom layer alive |
| --- |
| A Python step between the connector and the warehouse looks tidy, but it rebuilds the ETL layer you meant to buy. The connector only pays off when it writes straight to the warehouse and the cleaning follows the load. |

| Real world: Move the model without rewriting it |
| --- |
| Point the attribution code at the cleaned tables and keep its logic as it is. The win is freeing the layers around the model, not risking the one piece that differentiates the business. |

**Follow-up questions**

1. The attribution model needs a per-customer aggregation of the cleaned rows. Should that run as SQL in the warehouse after the load, or stay in the Python model?\
   *Where the line sits between in-warehouse transformation and the in-house model.*
2. A new source sends personal data that must never reach the warehouse. Does all of its cleaning still belong after the load?\
   *When transforming before the load is still the right call.*

</details>


<br>

---

## When to Split, When to Merge

A DAG, a directed acyclic graph, is one scheduled graph of steps that runs as a unit: the arrows point forward and no step waits on itself. When a step fails, every step downstream of it is skipped, and steps on other branches still run. So a fragile step upstream of an urgent one sits on its critical path. Put too much into 1 DAG and a step nobody cares about ends up upstream of the step everyone cares about. Split into too many and every boundary becomes a handoff that nobody monitors. 3 signals decide where the edges go. Ownership and cadence are the first 2; the third is what it costs to operate the boundary.


<br>

### The 3 splitting signals

| ✓ Split when | ✗ Merge when |
| --- | --- |
| • 2 teams own steps in the same graph, and 1 team's bug pages the other<br><br>• Steps need different cadences: an hourly step inside a nightly DAG runs 24 times or once, never right<br><br>• A failure in 1 consumer's steps should not block another consumer: 2 consumers of 1 source each get their own graph<br><br>• A rerun of 1 half rescans the other half for no reason | • A step exists only to feed the next, and no other consumer reads its output<br><br>• Both sides are owned by the same team and run at the same cadence<br><br>• The boundary would need its own contract and a ready-time check with an alert on it, and nobody would own them<br><br>• Each half would be fewer than 3 steps and the halves would always run together |


<br>

### Worked example: orders and inventory

An online store runs 1 nightly DAG: pull orders, pull inventory, clean both together, compute daily revenue, compute low-stock alerts. The low-stock alerts are wanted every hour, so the team schedules the whole DAG hourly. Now daily revenue is recomputed 24 times a day, the orders database is pulled 24 times, and when an orders region file is missing at 14:00, the shared cleaning step fails and no low-stock alert fires until the file appears. Cadence is the signal: 2 cadences in 1 DAG, and the slow half is on the fast half's critical path.

![Pipeline diagram 1](images/pa_what_is_a_pipeline_advanced_diagram_1.png)

After the split, the orders database is pulled once a night and the inventory database once an hour. The boundary costs almost nothing because the 2 graphs share no step, so there is no handoff to monitor. The decision gets harder when they do share one.


<br>

### When they share a step

Suppose low-stock alerts need `clean_orders` too, to know what sold in the last hour. Now that table has 2 consumers at 2 cadences. The split still holds, but the shared step either moves into the hourly graph, which then pulls orders hourly, or becomes a product of its own with a ready-time check that both graphs read. The rule from the product framing applies: a step read by another DAG is a product and gets a contract, even a 3-line one. Merging the 2 DAGs back together to avoid that contract trades 1 monitored handoff for the 24-times-a-day revenue problem, which is the worse deal.



**A single DAG owned by the marketing team has 9 steps. Step 7, finance_close, is owned by finance and is the only step finance uses. It runs nightly like the rest and fails about once a week on a finance-side input file. Should the DAG split, and on which signal?**

- **A.** Split on ownership: a finance file failure pages marketing, and marketing cannot fix it
- **B.** Keep it merged: the cadences match, so a split would add a boundary with no benefit
- **C.** Split on cadence: a weekly failure means finance_close wants a weekly schedule
- **D.** Split on cost: 9 steps is past the size at which a DAG should always be divided

<details>
<summary>Solution</summary>

**A.** Split on ownership: a finance file failure pages marketing, and marketing cannot fix it

Matching cadence does not save the design. Every weekly failure wakes the wrong team, and the 2 steps after finance_close wait on a file marketing has never seen.

**Why the others are wrong**

- **B.** Cadence is 1 of 3 signals. An ownership mismatch alone justifies a split, and the boundary here is cheap because finance's step has a single input.
- **C.** How often a step fails says nothing about how often it should run. The step is wanted nightly; its failures come from an input, not a schedule.
- **D.** There is no step count that forces a split. The cost signal is about operating the boundary, and a 40-step DAG with 1 owner and 1 cadence can stay whole.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Postgres, Stripe and Salesforce feed six point-to-point Python jobs that write straight into a finance dashboard, a product dashboard and an ML feature store, all under one orchestrator. Redesign it with a shared raw storage layer and a shared curated layer in a bought warehouse that all three consumers read, with a freshness commitment on it. Split the orchestrator in two at team boundaries and add a node that monitors the flow so failures stop being silent.

**Starting canvas** (already placed for you):

- 3 sources (Postgres, Stripe, Salesforce) (source, PostgreSQL)
- 6 custom point-to-point Python jobs (transform, Pandas)
- single_airflow (one orchestrator owns everything) (transform, Airflow)
- 3 consumers (Finance, Product, ML) (consumer, Tableau)

<details>
<summary><strong>Solution</strong></summary>

#### Untangling the graph along ownership seams

To redesign a tangled DAG graph, start by asking where ownership changes hands. The old canvas wired every job from a source straight to a consumer, and one orchestrator scheduled all of them. So a single Postgres schema change broke six jobs at once, and nobody clearly owned the failure. The redesign gives the data a shape: a shared raw layer, then a shared curated warehouse. It also gives each stretch of that shape **its own orchestrator**, run by the team that owns it.

```mermaid
flowchart LR
    subgraph L0["Raw ingest"]
        n0[("Postgres<br/>(PostgreSQL)")]
        n1[("Stripe<br/>(API)")]
        n2[("Salesforce<br/>(API)")]
        n3["Ingestion orchestrator<br/>(Airflow)"]
        n4[("Raw layer<br/>(object storage)")]
    end
    subgraph L1["Curated"]
        n5["Curated build orchestrator<br/>(Airflow)"]
        n6[("Curated warehouse<br/>(warehouse)<br/>SLA: #60; 2h")]
    end
    subgraph L2["Serving"]
        n7(["Finance dashboard<br/>(dashboard)"])
        n8(["Product dashboard<br/>(dashboard)"])
        n9(["ML feature store<br/>(Feast)"])
        n10(["Pipeline monitoring<br/>(monitoring)"])
    end
    n0 --> n3
    n1 --> n3
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n5 --> n6
    n6 --> n7
    n6 --> n8
    n6 --> n9
    n3 --> n10
    n5 --> n10
```

*The ingestion orchestrator lands each source once in the raw layer. The curated build orchestrator turns raw into the curated warehouse that all three consumers read. Both orchestrators report to monitoring.*

1. **Land every source once in a shared raw layer**

   The ingestion orchestrator runs one DAG with a task per source, and each task copies its source unchanged into the raw layer. No consumer reads from here. The raw layer exists so the curated tables can be rebuilt without pulling from Stripe or Salesforce again.

2. **Build the curated warehouse once and commit to its freshness**

   A second DAG, owned by the analytics team, transforms raw data into conformed tables in a bought warehouse. All three consumers read the same tables, and that is what stops the dashboards from disagreeing. The freshness commitment on the curated warehouse is the promise this team makes to its readers.

3. **Split the orchestrator where the teams split**

   Ingestion and curation now run in separate orchestrators. If a Stripe load fails, it fails inside the ingestion DAG and pages the team that owns it. The curated build waits for fresh raw data instead of breaking along with the load. Both orchestrators report to a monitoring node, so a failed run or a missed freshness target gets noticed.

| Pitfall: One bigger DAG keeps the old coupling |
| --- |
| It is tempting to add the new layers as more tasks in the single orchestrator, because it looks tidier. But every team still shares one failure domain and one deploy queue. Splitting only pays off when each orchestrator has exactly one owning team. |

**Follow-up questions**

1. The ML team wants to rebuild features from the curated warehouse every hour. Does that work go in the curated build orchestrator or a new one?\
   *Tests splitting by cadence as well as by ownership.*
2. The ingestion DAG runs late one night. How does the curated build orchestrator know to wait instead of building on stale raw data?\
   *Tests dependencies that cross a team seam.*
3. A bad Stripe deploy writes malformed rows into the raw layer. Where do you catch them before a dashboard shows them?\
   *Tests where quality checks sit once raw and curated are separated.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

A fintech runs 412 DAGs under one shared Airflow: 80 feed Finance, Product, and ML with no written contracts, and 332 have no consumer anyone can name, all built on hand-written extracts from Postgres, Stripe, Salesforce, and Kafka. Design the target: land the sources in a shared raw layer that feeds a curated layer with a stated freshness promise, with data quality checks and monitoring as their own stages. Split the one orchestrator so Finance, Product, and ML each own their scheduling. Track the 332 orphans in a data catalog with a notice period and a sunset date.

**Starting canvas** (already placed for you):

- 4 sources (Postgres, Stripe, Salesforce, Kafka) (source, PostgreSQL)
- Custom extracts per source (no shared raw) (transform, Pandas)
- 80 mission-critical ETLs (no contracts) (transform, Pandas)
- 332 orphan DAGs (no identified consumers) (transform, Pandas)
- single_airflow (owns every DAG) (transform, Airflow)
- 3 consumer surfaces (Finance, Product, ML) (consumer, Tableau)

<details>
<summary><strong>Solution</strong></summary>

#### Pipelines as products, at platform scale

Treating a pipeline as a **product** means every table has an owner, a written promise about what it delivers and how fresh it is, and a way to retire it. Fixing 412 drifting DAGs is that framing applied to the whole platform: sources land once in a shared raw store, a quality gate guards promotion to a curated store that promises freshness under an hour, each team runs its own orchestrator, and the orphans move through a catalog toward a sunset date.

```mermaid
flowchart LR
    n0[("Postgres<br/>(PostgreSQL)")]
    n1[("Stripe<br/>(API)")]
    n2[("Salesforce<br/>(API)")]
    n3[("Kafka<br/>(Kafka)")]
    n4[("raw store<br/>(Iceberg)")]
    n5["quality checks<br/>(Great Expectations)"]
    n6[("curated store<br/>(Iceberg)<br/>SLA: #60; 1h")]
    n7(["freshness monitoring<br/>(consumer)"])
    n8["finance airflow<br/>(Airflow)"]
    n9["product airflow<br/>(Airflow)"]
    n10["ml airflow<br/>(Airflow)"]
    n11(["finance dashboard<br/>(consumer)"])
    n12(["product analytics<br/>(consumer)"])
    n13(["ml training<br/>(consumer)"])
    n14["332 orphan DAGs<br/>(transform)"]
    n15(["catalog with notice period and sunset<br/>(DataHub)"])
    n0 --> n4
    n1 --> n4
    n2 --> n4
    n3 --> n4
    n4 --> n5
    n5 --> n6
    n6 --> n7
    n6 --> n8
    n6 --> n9
    n6 --> n10
    n8 --> n11
    n9 --> n12
    n10 --> n13
    n14 --> n15
```

*Shared raw and curated layers with a stated freshness, quality and monitoring as their own stages, one Airflow per team, and the orphans tracked in a catalog until sunset.*

1. **Land once, publish curated with a promise**

   The 80 critical tables stop depending on hand-written extracts once every source writes into one raw store and a single curated layer is built from it. The freshness setting on the curated store is the contract's number: Finance, Product, and ML read one promise instead of guessing from last night's run.

2. **Make quality and monitoring stages, not afterthoughts**

   A quality gate between raw and curated means a load that breaks a declared expectation (nulls, row counts, ranges) never reaches the curated tables. Freshness monitoring reads the curated store, so the owner sees a slipping promise before a consumer does.

3. **Split orchestration where ownership splits**

   With one Airflow per team, Finance can deploy, pause, or retry its own DAGs without touching Product's. The curated tables become the boundary between teams: depending on another team means depending on a published table, not on their DAG code.

| Pitfall: Silent for 90 days is not the same as unused |
| --- |
| The tempting move is to delete the 332 orphans outright. In a fintech some feed a report that runs only at quarter- or year-end, so no reads in 90 days does not prove no consumer. A catalog entry, a published notice period and a sunset date give a hidden owner the chance to speak up before anything is switched off. |

**Follow-up questions**

1. How would the curated store serve both a daily Finance close and a near-real-time ML feed without a second pipeline?\
   *Whether one curated layer can carry two freshness promises.*
2. When Stripe adds a field, where is the schema change absorbed, and what should the quality gate do with it?\
   *Schema evolution against declared expectations.*

</details>


<br>

---

## Build vs Buy at Each Layer

Every layer of a pipeline can be built in-house or bought as a managed service: the connectors that pull from sources, the storage the curated layer sits in, the transform logic that encodes your definitions, the engine that runs that logic, the scheduler that runs the DAGs, and the tools that serve the output. The decision is made per layer, and the same team reasonably builds 1 of the 6 and buys the other 5. Each decision comes down to 3 weights.


<br>

### The 3 weights

- **Differentiation** *does building it make the product better for the consumer*\
  A connector that pulls a payment provider's API is identical whoever writes it; the consumer never sees it. A transform that encodes how your company defines an active user is yours alone, and buying it means buying someone else's definition.
- **Total cost of ownership** *build cost plus every year of running it*\
  The build is the cheap part. A connector takes 6 engineer-weeks to write and 1 engineer-day a month forever to keep working as the source changes, which is 2.5 more engineer-weeks a year, every year.
- **Strategic risk** *what happens if the choice has to be undone*\
  A bought storage layer holds your data in its format; leaving means a migration measured in months. A bought connector holds nothing; leaving means rewriting 1 pull. Risk is highest where data and logic accumulate.

| Layer | Default | Exception |
| --- | --- | --- |
| Ingest connectors | buy | the source is internal and proprietary, so no vendor sells a connector for it |
| Storage | buy | regulation requires the bytes to stay on hardware you control |
| Transform logic | build | the logic is a commodity, such as currency conversion from a published rate table |
| Transform engine | buy | a query shape the managed engine cannot finish inside the contract's ready-by time |
| Scheduler | buy | schedules depend on signals from internal systems the managed scheduler cannot see |
| Serving | buy | the consumer is a product feature with latency needs no dashboard tool meets |


<br>

### A worked total cost

The listening data team needs sessions from a third-party podcast host's API. A managed connector costs $1,500 a month. Building takes 1 engineer for 6 weeks, and the API changes often enough that it needs 1 engineer-day a month after that. At a loaded cost of $15,000 a month per engineer, the build is $22,500 up front and $750 a month to maintain. Year 1 costs $31,500 built against $18,000 bought. Year 3 cumulative is $49,500 built against $54,000 bought. The crossover lands in month 30. Because the connector is undifferentiated, the team buys it and spends the 6 weeks on transforms instead. The same arithmetic with a $5,000 a month connector crosses over in month 6 and points the other way, so each layer needs the calculation redone with its own prices.

| ✓ Do | ✗ Don't |
| --- | --- |
| • Decide per layer; buying storage and building transforms is the common shape<br><br>• Count maintenance in the build cost, in engineer-days per month, forever<br><br>• Ask which layer your data and logic would be stuck in if the vendor vanished | • Build a connector to a public API because it looks like a 2-week job<br><br>• Buy transform logic; the active-user definition is the product<br><br>• Compare year-1 costs only; the crossover is usually in year 2 or 3 |

**A team is choosing where the transform that defines a 'paying customer' lives. Finance and sales disagree on that definition, and support has a third version. A vendor sells a package that computes paying customers from billing data. Which weight decides this, and which way?**

- **A.** Total cost of ownership: the vendor package is cheaper in year 1, so buy it
- **B.** Strategic risk: the vendor could shut down, so build an in-house copy of its package
- **C.** Differentiation: the definition is the product, and buying it adopts the vendor's answer to a question 3 teams have not settled
- **D.** Total cost of ownership: 3 teams disagreeing means the build will take years, so buy

<details>
<summary>Solution</summary>

**C.** Differentiation: the definition is the product, and buying it adopts the vendor's answer to a question 3 teams have not settled

Transform logic is the layer where the company's own definitions live. Buying it ends the argument by letting an outsider win it.

**Why the others are wrong**

- **A.** Year-1 cost is the wrong horizon, and cost is the wrong weight here. The package is cheap because it answers a question the company has not asked yet.
- **B.** Copying the vendor's definition in-house still adopts the vendor's definition. The risk weight is about being stuck, and this decision is about whose answer counts.
- **D.** The disagreement is a reason to settle the definition, and the settled definition is then a few lines of SQL. The cost of the argument is organizational, and buying does not pay it.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Design a pipeline that enforces the written contract for a daily customer-activity feed: the producer is the app's PostgreSQL database, the consumer is a churn model that retrains every morning, and each day's data must land in the warehouse within 24 hours. Enforce the contract's expectations (no null customer ids, row count within 20% of yesterday's) before the warehouse load, and when a check fails, stop the load and page the producing team.

<details>
<summary><strong>Solution</strong></summary>

#### A contract only counts when the pipeline enforces it

A data contract is only real when the pipeline enforces it. The written part names the producer (the app database), the consumer (the morning churn retrain), the delivery (one day of activity) and the cadence (within 24 hours). Data quality checks are how the last clause, what happens when a delivery fails, becomes behavior instead of a promise: each declared expectation turns into a check on every batch, and every check needs both a position in the flow and a destination for its failure.

```mermaid
flowchart LR
    n0[("app_postgres<br/>(PostgreSQL)")]
    n1["contract_checks<br/>(Great Expectations)"]
    n2[("activity_warehouse<br/>(warehouse)<br/>SLA: #60; 24h")]
    n3(["churn_model_retrain<br/>(consumer)"])
    n4(["producer_team_alerts<br/>(consumer)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n1 --> n4
```

*The contract enforced: `contract_checks` gates the load into `activity_warehouse` and pages the producing team when an expectation fails.*

1. **Put the checks in front of the load**

   The quality gate sits between the extract and the warehouse, so a batch with a null customer id or a row count far off yesterday's never becomes the table the model trains on. Because the gate blocks the load, yesterday's good data stays in place and the retrain runs on that instead of a broken day.

2. **Send every failure to its owner**

   The contract names the producer, so a failed check routes to the producing team's alerts. The people who can actually fix the extract hear about it that morning, before anyone downstream notices a strange churn score.

| Pitfall: A check after the load is a report, not a gate |
| --- |
| Running the checks on the warehouse table after it loads tells you the contract broke, but the churn model has already read the bad day. Enforcement means the failing batch never reaches the consumer. |

| Real world: Row-count drift catches what null checks miss |
| --- |
| A half-empty extract has no nulls at all: every row it did deliver is valid. Comparing against yesterday's count is what catches a source that silently dropped a chunk of the day, the most common way a daily feed breaks without throwing an error. |

**Follow-up questions**

1. A quarterly finance report starts depending on the `plan_tier` column. What changes in the contract and the checks?\
   *Adding a second named consumer, and a column-presence expectation that protects it.*
2. The producer wants to switch the extract to hourly to save cost. How should the contract surface that before it ships?\
   *Cadence as a contract term that every named consumer has to agree to.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

Each night a partner API export and a CRM file drop are loaded and combined into a revenue mart that a dashboard reads. Today each step starts at a fixed clock time, so a late API export leaves the mart built on half the data. Redesign it as a dependency graph run by an orchestrator: the mart build waits on both loads, each load retries its own failures, and any failed task pages on-call.

<details>
<summary><strong>Solution</strong></summary>

#### Ordering by dependency, not by clock

Orchestration is the undercurrent that decides when each step is allowed to run. A clock answers that with a time; an orchestrator answers it with a dependency: the mart build runs once both loads succeed, whenever that is. That one change turns a late partner export from a wrong dashboard into a late one, which is the failure you want.

1. **Draw the fan-in**

   Give each feed its own load task, then point both at the mart build. The 2 edges into `build_revenue_mart` are the whole contract: the orchestrator will not start it until `load_partner_api` and `load_crm_files` have both finished.

2. **Retry the task that failed**

   A timeout on the partner API should rerun that one load a few times while the CRM load stays done. Retries set per task mean a flaky source costs minutes, not a rerun of the whole night.

```mermaid
flowchart LR
    n0[("partner_api<br/>(API)")]
    n1[("crm_file_drop<br/>(source)")]
    n2["load_partner_api<br/>(Airflow)<br/>Retries: 3"]
    n3["load_crm_files<br/>(Airflow)<br/>Retries: 3"]
    n4["build_revenue_mart<br/>(Airflow)"]
    n5[("revenue_mart<br/>(warehouse)")]
    n6(["revenue_dashboard<br/>(consumer)"])
    n7(["on_call_alerts<br/>(consumer)"])
    n0 --> n2
    n1 --> n3
    n2 --> n4
    n3 --> n4
    n4 --> n5
    n5 --> n6
    n2 --> n7
    n3 --> n7
    n4 --> n7
```

*Both loads fan in to `build_revenue_mart`; each load retries on its own and every task pages `on_call_alerts` when it fails.*

| Pitfall: A start time is not a dependency |
| --- |
| Scheduling the build at 4am because the API is usually ready by 3am works until the night it is not. The build still succeeds, on half the data, and nothing fails. Moving the build later only shrinks the odds; an edge removes them. |

| Key insight: Failures left after retries have to reach a person |
| --- |
| Retries absorb the transient failures. The ones left are real, so every task routes to `on_call_alerts`: once retries run out, the run stops loudly instead of leaving yesterday's numbers on the dashboard. |

**Follow-up questions**

1. The CRM drop sometimes arrives with zero files. Should `load_crm_files` succeed?\
   *Whether a task can succeed while delivering nothing, and where a check belongs in the graph.*
2. A load half-wrote its rows before it failed. What has to be true for its retry to be safe?\
   *Whether task-level retries assume the write can be repeated without duplicating rows.*

</details>


<br>

---

## Redesigning a Tangled Graph

Here is a graph as it looks after 3 years of growth: 14 steps owned by 3 teams, all on 1 nightly schedule. Each step was added by whoever needed it at the time, reading from whichever step already had the data. Read it the way the intermediate lesson taught, left to right, then look for arrows that point the wrong way.

![Pipeline diagram 2](images/pa_what_is_a_pipeline_advanced_diagram_2.png)


<br>

### Diagnosis

> What the graph shows
>
> - `daily_revenue`, a finance figure, reads `attribution`, a marketing model: a change to the model moves the revenue number
> - `exec_dashboard` reads 4 steps across 3 teams, so any team's failure blanks the dashboard
> - `customer_360` has 3 consumers and no contract; it is a product nobody declared
> - Because there is one schedule, the attribution model reruns every night even though it is wanted weekly, and it sits on the critical path to finance
> - `refund_adjustments` runs after `finance_close`, so a refund posted at 23:00 reaches the dashboard but never the close

Apply the 3 framings in order. Products first: accounting reads the close, marketing reads its attribution report, and executives read the dashboard. Behind them sit 3 products with 3 contracts: `finance_close` and `attribution`, plus the curated layer that both DAGs and the dashboard read, which holds `orders_clean` and `sessions` along with `customers`. Undercurrents second: walk the failures row and the critical path shrinks to the steps finance needs. Split third: 3 owners and 2 cadences, so 3 DAGs around a curated layer the platform team owns.


<br>

### The redesign

![Pipeline diagram 3](images/pa_what_is_a_pipeline_advanced_diagram_3.png)

| Product | Owner | Consumers | Ready by | Change policy |
| --- | --- | --- | --- | --- |
| curated layer: `orders_clean`, `sessions`, `customers` | platform | finance DAG, marketing DAG, dashboard | 03:00 UTC | additive only, 30 days notice |
| `finance_close` | finance | dashboard, accounting export | 06:00 UTC | column set frozen for the fiscal year |
| `attribution` | marketing | dashboard | Monday 08:00 UTC | model version in a column, old column kept 2 cycles |

Check the result against the undercurrents. Failures: a marketing model failure now delays 1 weekly report, and finance closes on time. Reruns: a curated rerun clears the ready marker first, then writes each day aside and swaps it in at once, so nobody reads a half-written day at 03:00 or at 10:00. Lateness: late refunds reopen `refund_adjustments` and the close, and the finance contract says so. Change: `customer_360` was renamed `customers` through the deprecation path, with both names served for 30 days. Visibility: each product records its ready time, so the 09:00 question has a dashboard answer. Cost: the attribution model runs 1 night in 7 instead of 7.


<br>

### Capstone

**The marketing team, 6 months after the redesign, wants attribution to run nightly and asks to move it into the finance DAG so it can reuse the finance schedule. Using the 3 splitting signals, what is the right response?**

- **A.** Agree: the cadences now match, which was the signal that split them in the first place
- **B.** Decline: ownership still differs, so attribution gets its own nightly schedule reading the same curated layer
- **C.** Agree, as long as marketing writes a contract for attribution before the move
- **D.** Decline: the finance DAG already holds 3 steps and cannot take a 4th

<details>
<summary>Solution</summary>

**B.** Decline: ownership still differs, so attribution gets its own nightly schedule reading the same curated layer

A 2nd nightly DAG costs 1 schedule entry. Merging costs finance its independence from a model it does not own and cannot fix at 04:00.

**Why the others are wrong**

- **A.** Cadence was only one of the signals behind the split. Ownership still differs, and a marketing model failure inside the finance DAG puts the close back on marketing's critical path.
- **C.** A contract describes what attribution promises its consumers. It does nothing about who gets paged when a step inside the finance DAG fails.
- **D.** No step count forces or forbids a merge. The finance DAG could hold 30 finance-owned steps; the objection is about the owner, not the count.

</details>

Retiring a single table needs a finer list than the products table gives: every reader of that one table, including steps inside the owning DAG, the readers teams most often forget.

**The platform team wants to retire sessions and replace it with sessions_v2, which has a different grain. Which part of the redesign makes this possible without a surprise, and what does the team do first?**

- **A.** The curated layer's change policy; the team drops sessions and announces sessions_v2 the same day
- **B.** The finance DAG's 06:00 ready time; the team ships sessions_v2 before 06:00 so nothing downstream notices
- **C.** The 30-day notice in the contract; the team ships sessions_v2 beside sessions and works through every reader of sessions
- **D.** The weekly cadence of attribution; the team swaps the table on a Tuesday when the model is not running

<details>
<summary>Solution</summary>

**C.** The 30-day notice in the contract; the team ships sessions_v2 beside sessions and works through every reader of sessions

This is the deprecation path: new name, parallel window, the consumer list as checklist, then a final date. The per-table list for sessions is attribution and the platform's own customers step, which must move to sessions_v2 without changing its own grain within the 30 days. If it misses that window, finance_close and the dashboard break through it.

**Why the others are wrong**

- **A.** A grain change is a removal of the old shape, and the policy promises 30 days notice before any removal. Dropping first breaks the policy the team wrote.
- **B.** Ready time governs when a table lands, not what it contains. A table with a new grain landing on time still breaks every query written for the old grain.
- **D.** Timing the swap around 1 consumer's schedule avoids 1 failed run and leaves next Monday's run to fail instead. The cadence is not the mechanism.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

One Airflow DAG runs an hourly load of app events from Kafka and a daily revenue rollup together, so the rollup is scheduled hourly too and a slow rollup run holds back the next load. Split it at the cadence boundary: an hourly DAG that lands the events in the warehouse, and a daily DAG that reads the landed events and builds the revenue table a finance dashboard reads.

<details>
<summary><strong>Solution</strong></summary>

#### Split where the schedules disagree

Cadence is the second boundary the section names, and it is the one this pipeline crossed. A DAG has exactly one schedule, so an hourly load and a daily rollup sharing it force a bad choice: the rollup runs 24 times a day for no reason, or the hourly loads queue behind a slow rollup. Splitting at the cadence boundary gives each piece the schedule it needs, and the price is a seam between two DAGs that now has to be coordinated.

```mermaid
flowchart LR
    n0[("app_events<br/>(Kafka)")]
    n1["hourly_load_dag<br/>(Airflow)<br/>SLA: #60; 1h"]
    n2[("events_table<br/>(warehouse)")]
    n3["daily_rollup_dag<br/>(Airflow)<br/>SLA: #60; 24h"]
    n4[("revenue_table<br/>(warehouse)")]
    n5(["finance_dashboard<br/>(consumer)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
```

*Two DAGs, two clocks: `hourly_load_dag` lands events hourly, `daily_rollup_dag` reads `events_table` once a day.*

1. **Give each cadence its own DAG**

   The event load must finish every hour; the rollup only matters once a day. Each gets its own orchestrated DAG with its own schedule, so a long rollup can no longer delay the next hourly load, and a failed hourly run retries without touching revenue.

2. **Hand off through the landed table**

   The two DAGs never call each other's tasks. The hourly DAG writes `events_table`; the daily DAG reads it. That stored table is the contract at the seam: as long as its shape and completeness hold, either side can change internally.

| Pitfall: A daily task inside an hourly DAG runs on the wrong clock |
| --- |
| Leaving the rollup in the hourly DAG and guarding it with a 'only run at midnight' branch keeps the coupling: the rollup still shares the hourly DAG's runs, failures and retries. If the work has a different cadence, it belongs in a different DAG. |

| Key insight: Splitting buys independence and costs a wait |
| --- |
| The daily DAG must not start until the day's last hourly load has landed. That cross-DAG wait (a sensor, or a completeness check on `events_table`) is the coordination cost you accepted. Split when that cost is smaller than the coupling it removes. |

**Follow-up questions**

1. Another team wants hourly features from the same landed events. Do they get a task in `hourly_load_dag` or their own DAG?\
   *Applies the ownership boundary when the cadence already matches.*
2. The rollup drops to 5 minutes. Is the split still worth keeping?\
   *Weighs coupling against coordination once the symptom shrinks but the cadences still differ.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

Orders from an app's Postgres arrive through a bought change-data-capture connector and CRM accounts through a bought API connector; both land rows unchanged in the warehouse. A payment partner's legacy API has no connector, so you build its extractor, and raw card numbers from it must never reach the warehouse. Decide where each source is transformed, ELT or ETL, so a daily revenue dashboard reads SQL models built from all three.

<details>
<summary><strong>Solution</strong></summary>

#### The connector you buy decides where the transform lives

Whether a source is ELT or ETL is often settled by a build-versus-buy call made one layer earlier. A bought connector copies rows as they are, so every source it handles is ELT by construction: the reshaping moves into SQL models in the warehouse. The one source you build yourself is where you still get to choose, and here a contract makes that choice for you.

```mermaid
flowchart LR
    n0[("app_postgres_cdc<br/>(CDC)")]
    n1[("crm_api<br/>(API)")]
    n2[("partner_legacy_api<br/>(API)")]
    n3["mask_card_numbers<br/>(Pandas)"]
    n4[("raw_warehouse_schema<br/>(warehouse)")]
    n5["revenue_models<br/>(dbt)"]
    n6[("revenue_marts<br/>(warehouse)<br/>SLA: #60; 24h")]
    n7(["revenue_dashboard<br/>(dashboard)"])
    n0 --> n4
    n1 --> n4
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n5 --> n6
    n6 --> n7
```

*Bought connectors land raw; only the built partner extractor masks before load; `dbt` models in the warehouse build the daily revenue marts.*

1. **Let the bought connectors load raw**

   The Postgres change feed and the CRM connector land rows unchanged in a raw schema. That is not a compromise: raw history in the warehouse means a model bug is fixed by rebuilding the model, not by re-extracting from the source.

2. **Transform before load where a rule forbids raw**

   The partner extractor is code you own, so it can mask card numbers in flight. The contract says raw numbers never reach the warehouse, which rules out loading first and masking in a model: that one source is ETL, and everything after the raw schema is the same SQL modeling the other two get.

| Pitfall: Masking in a model is already too late |
| --- |
| Landing the partner rows raw and masking them in a `dbt` model looks tidy, since every source then follows one ELT pattern. But the raw card numbers sat in the warehouse between load and model run, and in its time travel and backups too. A rule about what may land is a rule about the load step. |

| Real world: Build the one connector, not the platform |
| --- |
| The extractor is the only built piece here, and it carries the hidden cost the section described: you now own its retries, the on-call page when the partner renames a field, and its eventual replacement. That is worth paying for one source no vendor covers; it is not worth paying for Postgres or the CRM. |

**Follow-up questions**

1. A connector vendor ships a column-hashing feature that runs before load and supports the partner API. Does the partner source change?\
   *Whether the build decision survives once a buy option can do the in-flight transform, weighed against the extractor's ongoing ownership cost.*
2. Analysts later need each card's issuing country, which comes from the leading digits of the raw number. Where does that derivation run?\
   *Whether a new derivation must move before load when the field it depends on is masked, rather than into the warehouse models.*

</details>


<br>

---

## Putting It All Together

A food delivery company has 1 nightly graph that feeds driver payouts, the restaurant partner dashboard, and the promotions model, which the growth team owns and needs refreshed only weekly. Payouts are late twice a month because the promotions model fails on a feature table it builds from the same deliveries data, and nobody on the payments team can say which dashboards read the deliveries table they maintain.

- Products: the 3 consumers with contracts are payouts and the partner dashboard, plus the promotions model. The deliveries table is a 4th product the payments team never declared.
- Undercurrents: the failures row shows the promotions model on the payout critical path, and the lateness row shows a delivery completed at 23:55 missing that day's payout with no contract saying so.
- Split: growth and payments are 2 owners. Payouts run nightly while the model runs weekly, so 2 DAGs read 1 contracted deliveries table with a ready-time check between them.
- Build versus buy: the company buys the connector to the third-party payment processor and the storage. It builds and owns the pull from its own courier app, which no vendor sells, and the definition of a completed delivery, because that definition decides who gets paid.
- Redesign: with the consumer list in hand, the deliveries table can later move to a new grain through the deprecation path without a surprise on payout day.


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Six DAGs each read Postgres orders directly and compute their own weekly active users, so one schema change breaks all six. Redesign it as layers: an orchestrated ingestion task with retries lands orders in a shared raw store, and one curated build writes the shared tables to a warehouse. The reporting dashboard reads only the curated layer, and a failed ingestion pages the owning team.

<details>
<summary><strong>Solution</strong></summary>

#### One owner per layer, not one DAG per team

Redesigning a tangled graph starts with fan-in. When six DAGs each pull Postgres orders, the source has six readers, so one column rename fails six runs at once and nobody knows whose page it is. The layered shape fixes the dependency graph itself: one ingestion task owns the source, one curated build owns the shared definitions, and everything downstream depends on the curated layer instead of the source.

1. **Give the source exactly one reader**

   Replace the six direct reads with one orchestrated ingestion task that lands orders unchanged in the raw store. A schema change now breaks one task with one owner, fixed in one place. Retries live on that task, because a source timeout is a task-level failure the orchestrator can rerun without touching anything downstream.

2. **Make consumers depend on curated, not raw**

   Weekly active users disagreed because each team wrote its own version. Move the definition into one curated build that reads raw and writes the shared tables to the warehouse, then point the dashboard at the warehouse. The edges now run raw, then curated, then consumers, and the orchestrator orders the tasks the same way.

```mermaid
flowchart LR
    subgraph L0["Raw"]
        n0[("orders_db<br/>(PostgreSQL)")]
        n1["ingest_orders<br/>(Airflow)<br/>Alert: page orders owner on failure · Retries: 3"]
        n2[("raw_orders<br/>(object storage)")]
    end
    subgraph L1["Curated"]
        n3["curate_shared_tables<br/>(dbt)"]
        n4[("curated_warehouse<br/>(warehouse)")]
    end
    subgraph L2["Serve"]
        n5(["reporting_dashboard<br/>(consumer)"])
    end
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
```

*One orchestrated ingestion lands orders raw; one curated build feeds the warehouse the dashboard reads.*

| Pitfall: Merging the six DAGs into one file changes nothing |
| --- |
| Consolidating code without changing the edges keeps six readers on the source. The test is the graph: if any consumer still has an edge to Postgres or to the raw store, the next schema change still fans out. |

| Key insight: A page should land on whoever owns the failing task |
| --- |
| Half the night pages had no owner because tasks crossed team lines. Once each layer has one task and one team, the failure alert on ingestion reaches the people who can actually fix the source. |

**Follow-up questions**

1. A billing API now has to join the curated tables. Where does it enter the graph?\
   *Whether a new source becomes a second ingestion task into the same raw layer rather than another point-to-point DAG.*
2. The curated build fails after ingestion succeeds. What should rerun?\
   *Task-level reruns: only the curated build reruns, reading raw data that already landed.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

A grocery-delivery platform runs one 48-task Airflow DAG holding the platform team's hourly Postgres orders load and the finance team's nightly payout reconciliation, so a failed finance task holds back the hourly load. The platform team also runs a separate hourly DAG for inventory from the fulfillment API, started 20 minutes later by clock. Redraw the boundaries as one DAG per owner and cadence. At the seam, the platform's hourly output must state a freshness under one hour and pass a contract check before the nightly side reads it, with the platform team paged on failure.

<details>
<summary><strong>Solution</strong></summary>

#### Draw the boundary where the owner or the cadence changes

Where one DAG ends and the next begins is a decision about failure and ownership, not task count. Two rules settle it: split wherever the owning team changes or the cadence changes, and **merge work that shares both**. The hourly orders load and the nightly payout reconciliation differ on both counts, so they belong in separate DAGs. The orders and inventory loads share an owner and an hour, so they belong in one.

1. **Split finance out of the hourly DAG**

   In one shared DAG a failed reconciliation task holds the run open, and the next hourly run waits behind it. Give finance its own nightly DAG and the hourly load stops sharing its fate. Each team now owns the retries and the alerts for its own graph.

2. **Merge the two hourly platform loads**

   An inventory DAG that starts 20 minutes later is a clock standing in for a dependency. Same owner, same cadence: fold it into the hourly DAG as a task downstream of the orders load, so it starts when orders actually finish rather than when they probably have.

3. **Publish the seam as a contract**

   The nightly DAG reads a hand-off table, never the hourly DAG's internals. The table states its freshness promise of '< 1h', a quality gate checks it before finance reads, and a failure pages the platform team because the producer owns the fix.

```mermaid
flowchart LR
    subgraph L0["Platform team hourly"]
        n0[("postgres_orders<br/>(PostgreSQL)")]
        n1[("fulfillment_inventory_api<br/>(API)")]
        n2["platform_hourly_dag<br/>(Airflow)"]
        n3[("hourly_handoff_table<br/>(warehouse)<br/>SLA: #60; 1h")]
        n4["handoff_contract_check<br/>(quality_gate)<br/>Alert: page platform team on failed check"]
    end
    subgraph L1["Finance team nightly"]
        n5["finance_nightly_dag<br/>(Airflow)"]
        n6[("payout_reconciliation<br/>(warehouse)<br/>SLA: #60; 24h")]
    end
    n0 --> n2
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n5 --> n6
```

*Two DAGs, one per owner and cadence, meeting at a checked hand-off table that pages its producer.*

| Pitfall: A split without a contract just moves the coupling |
| --- |
| Cutting the shared DAG in two and wiring the nightly DAG to wait on an hourly task by name keeps the hidden dependency: rename that task and finance breaks without warning. The seam has to be a table with a stated freshness and a check, so each side can change its internals freely. |

**Follow-up questions**

1. Inventory moves to a 15-minute cadence while orders stay hourly. Do the two loads still belong in one DAG?\
   *Applies the cadence rule on its own, with the owner held constant.*
2. The hand-off check fails at 1am. What should the nightly DAG do, and who reruns what?\
   *Tests that the contract names the failure behavior on both sides of the seam.*

</details>


<br>

---

## Key Takeaways

- The consumer list comes first: a pipeline becomes a product once it has a named consumer, and the written contract and the deprecation path both depend on that list.
- Reruns and failures, change and lateness, visibility and cost: each of these 6 undercurrents needs an answer in all 4 roles, and a fix in 1 row usually moves 2 others.
- Split a DAG where owners differ or cadences differ, and weigh what the boundary costs to run; merge when a step feeds only the next step under the same owner at the same cadence.
- Decide build versus buy per layer: buy connectors and storage by default, along with engines and schedulers, and build the transform logic because the definitions are the product.
- Untangle a grown graph in order: name the products, walk the undercurrents, then split on owners and cadences around a shared contracted curated layer.
