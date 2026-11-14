Beginner·Pipeline Architecture · 20 min · 4 challenges

# Keeping Data Fresh: Beginner

Ines is 40 minutes into a data engineering loop at Uber when the whiteboard question arrives. The interviewer sketches a trips table in the operational database: 2 billion rows, about 4 million edits a day as tips and corrections adjust fares and some trips get cancelled. Analysts want a dashboard that is never more than an hour behind. 'How do you keep the warehouse copy current?' Ines starts to say she would reload it every hour, then stops: copying 2 billion rows to pick up 4 million changes sounds wrong. But reading only the changes raises its own questions. How does the pipeline notice a cancelled trip that was deleted? What happens when a driver moves cities, when an engineer renames a column, or when a fare bug corrupts last month? Each follow-up is the same question in disguise: how does fresh data stay correct?


<br>

### What you will be able to do

Choose between full and incremental loading for a given table, and defend the choice with its cost and its failure modes

Explain how change data capture reads the transaction log and why it catches deletes that timestamp queries miss

Model a changing dimension as SCD Type 2 and write the point-in-time join that avoids double counting

Classify a schema change by whether it breaks readers, and whether it breaks them loudly or silently, then say what the pipeline should do in each case

Plan a backfill that can be rerun safely, and order its tables and partitions correctly


<br>

---

## Full vs Incremental Loading

Every freshness question starts with a single decision: on each run, how much of the source does the pipeline read? A full load reads the whole table and replaces the warehouse copy. An incremental load reads only the rows that changed since the previous run and merges them into the copy already in place. That choice sets the cost of each run and the way it fails. It also decides how much state the pipeline has to remember between runs.


<br>

### Full load: replace the whole copy

A full load runs the same query every time, reading every row of the source, and overwrites the target. Its strength is that it remembers nothing. If a run dies halfway, the next one starts from zero and still produces the right table. A row deleted in the source is absent from the new copy. A bug in yesterday's transform is repaired by today's run once the code is fixed. The price is that each run costs as much as the table is large, whether 10 rows changed or 10 million, and the source database carries that full read every time.


<br>

### Incremental load: read only what changed

An incremental load needs a column that moves forward whenever a row changes, usually updated_at. The pipeline stores a **watermark**, the highest updated_at it has loaded so far, and the next run asks only for rows above it. Those rows are then **upserted** into the target: a row whose key already exists replaces the old version, and a new key is inserted. Here is a run against 4 rows of the source, with the stored watermark at 09:00:00.

| trip_id | status | fare_cents | updated_at | Read this run? |
| --- | --- | --- | --- | --- |
| 101 | completed | 1850 | 2026-10-23 08:41:07 | No: below the watermark |
| 102 | completed | 2400 | 2026-10-23 09:12:30 | Yes: new trip, inserted |
| 103 | cancelled | 0 | 2026-10-23 09:47:02 | Yes: existing trip, replaced |
| 104 | completed | 3100 | 2026-10-23 07:15:55 | No: unchanged since 07:15 |

```sql
SELECT
  trip_id,
  status,
  fare_cents,
  updated_at
FROM trips
WHERE updated_at > TIMESTAMP '2026-10-23 09:00:00'
```

After the run, the new watermark is 09:47:02, the largest updated_at the run actually read. Setting it from the wall clock instead, say 09:48 when the run finished, opens a gap for any row stamped in between that the query did not see. Even the data-derived watermark has a gap of its own: a transaction that stamps a row 09:45:30 but commits at 09:50 was invisible to the 09:48 run, and it now sits below the watermark, so no later run will read it. The fix is a **lookback window**: each run reads from the watermark minus 30 minutes. Rows inside that window get read twice, which does no harm because the upsert replaces a row with itself.


<br>

### Where incremental loads break

- **Late commits** *Late data*\
  A row whose updated_at is earlier than its commit time can land below the watermark. A lookback window re-reads recent rows so the next run catches it.
- **Half-finished runs** *Retry*\
  A run that fails after writing part of its batch is retried with the same watermark. Appending would duplicate those rows; an upsert keyed on trip_id leaves 1 copy of each.
- **Hard deletes** *Invisible change*\
  A deleted row has no updated_at left to match, so the warehouse keeps it indefinitely. A full load or change data capture, the next section, is needed to see it.
- **Writers that skip the column** *Trust*\
  A one-off fix script that runs an UPDATE without setting updated_at changes data the watermark never sees. The pattern works only if every writer sets updated_at.


<br>

### Choosing between them

| ✓ Full load | ✗ Incremental load |
| --- | --- |
| • Cost grows with table size<br><br>• No state kept between runs<br><br>• Sees deletes automatically<br><br>• A retry reruns from scratch<br><br>• Self-heals once a bug is fixed | • Cost grows with rows changed<br><br>• Stores a watermark per table<br><br>• Misses hard deletes<br><br>• A retry needs an upsert to avoid duplicates<br><br>• Needs a lookback window for late commits |

A working rule: full-load a table when reading all of it fits comfortably inside the freshness target, or when deletes matter and nothing else reveals them. Load incrementally when the table is large, only a small fraction of it changes per run, and its change column is trustworthy. Dimension tables of a few thousand rows sit in the first group. The trips table, 2 billion rows with about 4 million inserted or edited per day, sits firmly in the second, and the gap is wide:

- **0.2%** — of trips rows touched on a typical day
- **500x** — less data read than a daily full load

> Try it: pick a strategy
>
> A `cities` table has 1,200 rows, is edited by hand a few times a month, and occasionally loses a row when a market closes. A `payments` table has 900 million rows, gains 3 million per day, and its rows are never updated after insert. Decide a load strategy for each, and the single failure you would design against, before reading the answer.

Cities: full load. Reading 1,200 rows costs almost nothing, and of the 2 strategies only the full copy notices the deleted market. Payments: incremental, and a cleaner case than trips, because insert-only rows mean `created_at` or an increasing `payment_id` works as the watermark and nothing needs replacing. The failure to design against is still the late commit, so payments keep a lookback window and an insert that skips payment ids already loaded.

> **TIP**
>
> Name the failure mode alongside the choice. 'Incremental on `updated_at` with a 30-minute lookback. The upsert is keyed on `trip_id`, so a late commit or a retry lands once.' That answer covers the next 2 follow-up questions before the interviewer asks them.


<br>

---

## Change Data Capture

The incremental query asks the source what its rows look like now. Change data capture, CDC, asks what happened. Before a relational database changes a table, it writes every row change to its transaction log, deletes included, so it can recover after a crash. A CDC reader follows that log as it grows and turns every entry into a change event the pipeline applies to its own copy.


<br>

### What the timestamp query cannot see

- Deletes and writers that skip updated_at: the 2 blind spots from the previous section.
- Intermediate states: a trip that goes requested, accepted, cancelled between 2 runs arrives as 1 row, its path lost.
- Source load: each run queries production while it serves live riders, and indexing updated_at slows every write.

Missed deletes decide most designs. A pipeline that misses deletes reports success on every run while the warehouse slowly fills with trips that no longer exist, and no error ever points at the cause.


<br>

### Anatomy of a change event

Each event carries the operation, the row's values after the change (many readers include the values before it as well), and a **log position**: a number the database assigns in commit order that only ever increases. Below are trip 103 from the previous section and a trip that is later deleted, as the reader emits them.

| log_position | op | trip_id | status | fare_cents |
| --- | --- | --- | --- | --- |
| 88120 | insert | 103 | requested | 2150 |
| 88131 | update | 103 | accepted | 2150 |
| 88140 | insert | 105 | requested | 1700 |
| 88152 | update | 103 | cancelled | 0 |
| 88167 | delete | 105 | (none) | (none) |

Applied in log order, these 5 events leave trip 103 cancelled with a fare of 0, and trip 105 gone from the warehouse. The timestamp query would have produced the first result and missed the second. It would also never have recorded that 103 was accepted before it was cancelled, which is exactly the fact a driver-cancellation metric needs.


<br>

### The pieces of a CDC pipeline

![Pipeline diagram 1](images/pipeline_keeping_data_fresh_beginner_diagram_1.png)

Freshness shrinks from the run interval to the time an event takes to cross the queue, typically seconds to minutes. The cost is operational: a reader that must stay connected to the database, a queue to run, and an apply step with rules of its own.


<br>

### Where CDC breaks

**Retries replay events.** The reader records the last log position it has handed off. Suppose the last offset it saved is 88120 and it crashes after sending 88131 through 88152. On restart it resends everything after 88120, so those 3 events arrive a second time. The apply step therefore has to be idempotent: store each row's log_position in the target and ignore any event whose position is not greater than the stored one. A delete must leave a **tombstone**, the key and its log_position with a deleted flag that queries filter out, in place of removing the row. Otherwise a replay that reaches back to 88140 finds nothing stored for trip 105 and recreates a trip the source deleted. Replayed events then change nothing.

**Order matters per key.** Applying 88152 before 88131 would leave trip 103 accepted. A queue split into partitions keeps order only within a partition, so it is partitioned by trip_id and all of a trip's events land together. **The log does not go back forever.** The database discards old log segments after a configured retention period, so a new CDC pipeline begins with a full snapshot of the table, notes the log position at the moment of the snapshot, and streams from there.

| ✓ Timestamp-based incremental | ✗ Change data capture |
| --- | --- |
| • Queries current table state<br><br>• Misses hard deletes<br><br>• Depends on every writer setting updated_at<br><br>• Sees only the latest state between runs<br><br>• Freshness bounded by the run interval<br><br>• A query plus a stored watermark | • Reads the database's own commit log<br><br>• A delete arrives as an event<br><br>• Captures every committed change<br><br>• Keeps intermediate states, in order<br><br>• Freshness in seconds to minutes<br><br>• A log reader and a queue to operate, plus ordering rules and a snapshot |

> Try it: replay after a restart
>
> The apply step holds trip 103 at log_position 88152, status cancelled, fare_cents 0. After a reader restart it receives, in order: 88131 update to accepted; 88152 update to cancelled; 88190 update to cancelled with fare_cents 500, a cancellation fee. What is trip 103's final row?

Cancelled, fare_cents 500, log_position 88190. The first 2 events carry positions at or below 88152 and are skipped; only 88190 is new. Without the position check, the replay would briefly move the trip back to accepted, and any query running at that moment would read a state the trip left long ago.

> **TIP**
>
> When asked 'why not query `updated_at`?', lead with deletes. It is the failure with no symptom: every run succeeds, and the warehouse is wrong with no error anywhere. Then mention replay and per-key ordering as the 2 rules CDC adds in exchange.


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

A nightly job re-copies the entire 500M-row orders table from production Postgres into the warehouse, running for hours and straining the live database even though only a few thousand orders change each day. Orders also get cancelled and hard-deleted, and the table has no reliable last-updated column to scan, so a timestamp-based incremental pull would silently miss both the deletes and the rows that change between 2 runs. Redesign the load to read inserts, updates, and deletes straight from the database's own commit log and carry that change stream onward continuously, so the warehouse stays current without the nightly full reload.

**Starting canvas** (already placed for you):

- Operational DB (source, PostgreSQL)
- Full nightly copy (hours) (transform, Spark)
- Warehouse (storage, Snowflake)

<details>
<summary><strong>Solution</strong></summary>

#### The trap: a full reload dressed up as freshness

Beneath the business story this is Change Data Capture (CDC), the exact pattern the lesson named: instead of re-reading state, you read the database's own commit log and replay its inserts, updates, and deletes downstream. The costume is 'the warehouse feels stale'; the real problem is that a full nightly copy re-ships 500M rows to move a few thousand that actually changed. Everyone reaches for the obvious fix, an incremental pull filtered on a `last_updated` column, and that is the trap. This table has no reliable modified-at, rows get **hard-deleted**, and a `WHERE updated_at > last_run` scan can only ever see rows that still exist and still carry a bumped timestamp. Deletes vanish and any row that changes twice between runs slips through. Get that wrong and the warehouse quietly accumulates orders that were cancelled months ago.

| Key insight: A delete leaves no row to SELECT |
| --- |
| This is why the load has to come off the log, not the table. A timestamp scan reads the table's current state, and a hard-deleted row is absent from current state, so there is nothing for the query to return. Only the commit log records that the row used to exist and is now gone. Deletes are the litmus test that separates a real CDC design from an incremental `SELECT`. |

#### The shape that works

```mermaid
flowchart LR
    subgraph L0["Capture"]
        n0[("Operational DB<br/>(PostgreSQL (WAL))")]
        n1["CDC connector<br/>(Debezium + replication slot)"]
    end
    subgraph L1["Transport"]
        n2[/"Change stream<br/>(Kafka topic (per table))"/]
    end
    subgraph L2["Apply"]
        n3["Stream sink / merge<br/>(Kafka Connect -#62; MERGE)"]
        n4[("Warehouse<br/>(warehouse)")]
    end
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
```

*The nightly full copy becomes a continuous log tail: capture from the WAL, buffer the change stream, apply upserts and deletes into the warehouse.*

1. **Capture from the write-ahead log, not the table**

   Point a CDC connector (Debezium) at a Postgres logical replication slot. It reads the WAL, the same ordered log Postgres already writes for durability and replication, so every `INSERT`, `UPDATE`, and `DELETE` arrives as an event with a before-image and an after-image. Crucially this puts near-zero query load on the live database: you are tailing a log it already produces, not running an hours-long full scan against production tables.

2. **Buffer the change stream in a durable log**

   Land the events on a Kafka topic keyed by primary key. This decouples capture from apply: if the warehouse is slow or down, the connector keeps reading the WAL and Kafka retains the backlog, so you never lose an event or force Postgres to hold WAL forever. The topic is the durable, replayable spine that makes the whole thing a stream instead of a fragile point-to-point pipe.

3. **Apply upserts and deletes into the warehouse**

   A sink consumes the topic and turns each event into a warehouse operation: insert/update become a `MERGE` on primary key, and a delete event becomes an actual delete or a soft-delete flag. Because events are keyed and ordered per key, applying them in offset order converges the warehouse to the source's current state, cancellations included. This is the step the timestamp approach can never do, because it never receives a delete.

4. **Run it continuously, checkpoint the offset**

   The connector persists its WAL position and the sink commits its Kafka offset, so a restart resumes exactly where it left off, no re-scan, no gap. The warehouse trails production by seconds to minutes instead of a day, and the nightly full reload is deleted outright rather than merely shrunk.

| Trick: Let the log be the source of truth |
| --- |
| The insight that cracks this is refusing to re-derive changes from table state at all. The database already computed the exact change set once, when it committed each transaction to the WAL. CDC just reads that log. Every hard problem here, deletes, double-updates, load on production, dissolves the moment you stop querying the table and start replaying its log. |

| ✕ Nightly full + timestamp incremental | ✓ CDC change stream |
| --- | --- |
| Batch. A full `SELECT` (or a `WHERE updated_at > last_run` filter) re-reads current table state on a schedule. Cannot see hard `DELETE`s, misses rows that change twice between runs, and hammers production with a long scan. Freshness capped at the batch interval, here 24h. | Streaming. Tails the WAL, so every `INSERT`/`UPDATE`/`DELETE` is an ordered event with a before-image. Deletes and repeat-updates are first-class, load on the source is negligible, and freshness drops to seconds because there is no window to wait for. |

| Interviewer: Say the word delete before they do |
| --- |
| Plenty of candidates propose 'incremental load on a timestamp' and stop there, satisfied they avoided the full copy. The tell of seniority is naming, unprompted, that a timestamp scan silently drops hard deletes and same-window double-updates, and reaching for the commit log to fix it. If you say `DELETE` out loud before the interviewer asks, you have shown you understand what incremental actually costs. |

| Real world: A dead connector fills the production disk |
| --- |
| A logical replication slot makes Postgres retain WAL until the consumer acknowledges it. If the connector dies and nobody notices, WAL piles up and can fill the production disk, the classic CDC outage. Monitor slot lag, alert on it, and treat the connector as a tier-1 service. This is the operational tax you accept in exchange for never running the full copy again. |

**Follow-up questions**

1. How do you seed the warehouse with the 500M existing rows before the stream takes over?\
   *Tests the snapshot-then-stream handoff: an initial consistent snapshot, then switch to WAL events from the snapshot's log position with no gap or double-count.*
2. The connector was down for 6 hours. What happens, and does the warehouse lose data?\
   *Probes durability: WAL retention via the slot plus Kafka backlog mean events replay from the checkpointed offset on restart.*
3. How do you guarantee correctness if the same event is delivered twice?\
   *Tests idempotency: keyed `MERGE` on primary key plus offset ordering makes reprocessing converge to the same state.*
4. When would a nightly batch load actually be the right call over CDC?\
   *Checks judgment: low freshness needs, no delete tracking required, or a source that exposes no usable log make the streaming machinery unjustified overhead.*

</details>


<br>

---

## Slowly Changing Dimensions

Trips are facts: each one happens once and stays put. Drivers are a dimension: they describe the facts, and their attributes change. Driver 42 drove in Austin until 2026-03-01, then moved to Denver. When an analyst asks for 2025 revenue by city, the answer depends on what the warehouse did with that move. The policy for that is called a slowly changing dimension, or SCD. Interviewers want to know which policy fits and what it costs.


<br>

### Type 1 overwrites, Type 2 keeps versions

Type 1 after the move

| driver_id | name | city |
| --- | --- | --- |
| 42 | Ana Ruiz | Denver |

Type 2 after the move

| driver_sk | driver_id | city | valid_from | valid_to | is_current |
| --- | --- | --- | --- | --- | --- |
| 9001 | 42 | Austin | 2024-06-10 | 2026-03-01 | false |
| 9377 | 42 | Denver | 2026-03-01 | 9999-12-31 | true |

Type 1 updates the row in place. It needs no extra columns and keeps 1 row per driver, but every past trip now reports as Denver, so 2025 revenue moves from Austin to Denver on the day the update lands. Type 2 closes the old row by setting its valid_to and inserts a new row under a new **surrogate key**, driver_sk, a warehouse-assigned id for a single version of the driver. History stays where it happened. Type 3, a previous_city column beside city, keeps exactly 1 prior value and mostly appears in interviews as a contrast.


<br>

### The Type 2 join

Once the dimension holds versions, a fact row has to join to the version that was true when the fact happened. There are 2 routes to that.

![Schema diagram 2](images/pipeline_keeping_data_fresh_beginner_diagram_2.png)

The first route stamps the surrogate key onto each fact as it loads: look up driver 42's current version and write its driver_sk into the trip row. Reports then join with a plain equality, and history is fixed at load time. The second route joins on the natural key driver_id plus a time range. That point-in-time join is the one interviewers ask candidates to write:

```sql
SELECT
  d.city,
  SUM(t.fare_cents) AS revenue_cents
FROM fact_trips AS t
JOIN dim_driver AS d
  ON t.driver_id = d.driver_id
    AND t.trip_ts >= d.valid_from
    AND t.trip_ts < d.valid_to
WHERE t.trip_ts >= TIMESTAMP '2025-01-01 00:00:00'
  AND t.trip_ts < TIMESTAMP '2026-01-01 00:00:00'
GROUP BY d.city
```

#### Two ways this join goes wrong

| Join condition | Rows per trip | Austin cents | Denver cents | Total cents |
| --- | --- | --- | --- | --- |
| driver_id only | 2 | 6,000 | 6,000 | 12,000 |
| driver_id and is_current = true | 1 | 0 | 6,000 | 6,000 |
| driver_id and the validity range | 1 | 6,000 | 0 | 6,000 |

Driver 42 completed 3 trips in 2025, fares of 1,800, 2,200 and 2,000 cents. Joining on driver_id alone matches each trip to both versions and doubles revenue, a **fan-out** that raises no error. Filtering on is_current repairs the total and repeats the Type 1 mistake. The second mistake happens at the boundary: with `BETWEEN`, a trip at exactly 2026-03-01 00:00:00 satisfies both versions, because Austin's valid_to equals Denver's valid_from. Half-open ranges, >= on the start and < on the end, place every instant in exactly 1 version.

> Try it: which version?
>
> Using driver 42's Type 2 rows above, which `driver_sk` does each trip join to? Trip A at 2026-02-28 23:59:59, trip B at 2026-03-01 00:00:00, trip C at 2026-10-23 14:00:00.

Trip A joins 9001, Austin: it falls before the boundary. Trip B joins 9377, Denver: it equals Denver's `valid_from` and fails Austin's strict less-than. Trip C also joins 9377, because 9999-12-31 is the open end of the current version, a far-future date that keeps the range comparison working without NULL checks.


<br>

### Where Type 2 loads break

- **Reruns create phantom versions** *Retry*\
  A load that opens a new version every time it sees a driver adds a duplicate on rerun. Compare the attributes, or a hash of them, with the current version and insert only on a real change.
- **Changes arrive late** *Late data*\
  A move effective 2026-03-01 that reaches the warehouse on 2026-03-09 must close Austin at 03-01. The range join then re-resolves those 8 days; stamped surrogate keys keep the old key until reloaded.
- **New attributes need a policy** *Schema change*\
  A column added to a Type 2 dimension forces a decision on whether its changes open a version. Choose per column: a phone number fix can be Type 1, a city change Type 2.

> **TIP**
>
> If asked 'Type 1 or Type 2?', answer with what the reports need. A corrected misspelling is Type 1. Type 2 is for attributes a report groups by over time, like city or pricing tier. The next question will be the join, so be ready to write the half-open range.


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

A customer's address changes. The current pipeline overwrites it, so a report run for last quarter now shows the new address and the historical numbers are wrong. Design the dimension load so history is preserved.

**Starting canvas** (already placed for you):

- Customer changes (CDC) (source, CDC)
- Overwrite loader (transform, Spark)
- Customer dim (overwrites) (storage, Snowflake)

<details>
<summary><strong>Solution</strong></summary>

#### What this really is

Strip the costume and this is the lesson's pattern by name: a CDC stream feeding a **slowly changing dimension, Type 2**. The current pipeline is Type 1. It does an in-place `UPDATE` of `address` on `customer_dim`, so the moment a customer moves, every historical fact that points at them reads the new address. Anyone can draw a dimension table. The trap is that preserving history is not one change. It is 2 coupled writes (close the old version, open the new one) plus a join rule on the fact side. Get any one of the 3 wrong and last quarter's report is silently wrong again.

| Trick: A row is a version, not the customer |
| --- |
| Add `effective_from`, `effective_to` and `is_current`, and give each version its own surrogate `customer_sk`. A change never overwrites: you close the open version (`effective_to = change_ts`, `is_current = FALSE`) and insert a new one. History survives because the old row still exists under its own key. |

```mermaid
flowchart LR
    n0[("Customer changes (CDC)<br/>(CDC)")]
    n1["Dedup + order by commit ts<br/>(Spark)"]
    n2["SCD2 merge (close old, open new)<br/>(Spark)"]
    n3[("Customer dim (versioned rows)<br/>(Delta Lake)")]
    n4["Fact loads (point-in-time join)<br/>(SQL)"]
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
```

*CDC feeds an SCD2 merge into a transactional store; facts bind to the `customer_sk` current at the event's timestamp, not to `customer_id`.*

#### Why overwrite loses history

| ✕ Type 1 (today, broken) | ✓ Type 2 (the design) |
| --- | --- |
| One row per `customer_id`. A move updates `address` in place. Facts join on the natural key `customer_id`, so they always resolve to the single surviving row. Nothing ever recorded the old address. | Many rows per `customer_id`, one per version, each with its own `customer_sk` and a `[effective_from, effective_to)` window. A move closes the current version and inserts a new one. Facts carry the `customer_sk` valid at transaction time, so last quarter re-runs correctly. |

1. **Land and order the change stream**

   CDC can deliver duplicates and, across partitions, out of order. Dedup on `(customer_id, commit_ts)` and order by the source commit time, not arrival time. Version in the wrong order and you close the wrong window, leaving overlapping intervals.

2. **Detect a real attribute change**

   Compare the tracked columns against the current version via `row_hash`. If nothing tracked changed, write nothing. This stops a heartbeat update or an untracked-column edit from spawning a spurious version.

3. **Close and open in one statement**

   Closing the current row and inserting the new one must commit together, or a reader between them sees a customer with no current version. A single `MERGE` gives you that atomicity, which is why the store must be transactional.

4. **Bind facts to the point-in-time key**

   The step candidates forget. When a fact loads, resolve `customer_sk` from the version whose window contains the event: `event_ts >= effective_from AND event_ts < effective_to`. Store that key on the fact, and old facts stay pinned to the address true when they happened.

**SCD2 upsert as a single MERGE**

```sql
/* union: the raw changes plus a synthetic row that forces
   the matching current version to be closed */
MERGE INTO customer_dim tgt
USING (
  /* open a new version for every real change */
  SELECT customer_id, customer_id AS merge_key, address, change_ts
  FROM staged_changes
  UNION ALL
  /* close the currently open version of a changed customer */
  SELECT customer_id, NULL AS merge_key, address, change_ts
  FROM staged_changes
) src
ON tgt.customer_id = src.merge_key
AND tgt.is_current = TRUE

WHEN MATCHED AND tgt.row_hash <> sha2(src.address, 256) THEN UPDATE SET
  tgt.effective_to = src.change_ts,
  tgt.is_current   = FALSE

WHEN NOT MATCHED THEN INSERT
  (customer_sk, customer_id, address, row_hash,
   effective_from, effective_to, is_current)
VALUES
  (uuid(), src.customer_id, src.address, sha2(src.address, 256),
   src.change_ts, TIMESTAMP '9999-12-31 00:00:00', TRUE);
```

*Rows keyed by `merge_key` match the open version and close it when the hash differs; the copies with a `NULL` `merge_key` never match, so they fall through to the INSERT and open the new version.*

| Pitfall: 3 ways to lose history anyway |
| --- |
| Joining facts on `customer_id` instead of `customer_sk`, which quietly recreates the Type 1 bug. Closing and opening in separate transactions, leaving no `is_current = TRUE` row for a moment. Stamping `effective_from` with load time instead of the CDC `commit_ts`, so version boundaries drift from when the change really happened. |

| Interviewer: Name the surrogate key and the join together |
| --- |
| A junior answer stops at 'add effective dates'. The senior tell is explaining why the fact must carry `customer_sk` rather than `customer_id`: Type 2 is a contract between the dimension and the facts, not a table trick. |

| Real world: Filter the insert branch to real changes |
| --- |
| As written, the `NULL`-keyed half of the `USING` set inserts a row for every staged change. In production, restrict that branch to customers whose incoming `sha2(address, 256)` differs from the current `row_hash` (or who have no current row), and chain intervals when one `customer_id` changes twice in a batch, so no-op events never spawn duplicate current versions. |

**Follow-up questions**

1. What changes if 2 updates for the same `customer_id` arrive in one batch?\
   *Tests chaining windows inside the batch instead of opening 2 current rows.*
2. A change arrives with a `commit_ts` earlier than the current version's `effective_from`. How do you handle it?\
   *Probes interval maintenance under out-of-order CDC.*
3. How would you answer 'what was this customer's address on 2026-03-31' from the dimension alone?\
   *Confirms they can query by the effective-date window.*
4. Which columns are Type 2 versus Type 1 here, and how do you load a mixed dimension?\
   *Tests that they don't version every column blindly.*

</details>


<br>

---

## Schema Evolution

The source tables a pipeline reads belong to other teams, and those teams change them to ship features. A schema change is safe when every existing reader and writer keeps producing correct results with no modification. It is breaking when any of them fails, or, worse, keeps running with wrong output. The interview question hands over a change and asks which kind it is and what the pipeline should do about it.


<br>

### Classifying the common changes

| Change to trips | Verdict | Why |
| --- | --- | --- |
| Add nullable column tip_cents | Safe | Readers that name their columns ignore it; old rows read as NULL |
| Widen fare_cents from INTEGER to BIGINT | Safe | Every existing value fits the wider type |
| Add NOT NULL column city_id, no default | Breaking for writers | The ALTER fails on 2 billion existing rows; once they are backfilled, inserts that omit it are rejected |
| Drop column promo_code | Breaking | Any query that names it fails |
| Rename status to trip_status | Breaking | Equivalent to a drop plus an add; the old name fails |
| Narrow driver_id from BIGINT to INTEGER | Breaking | Ids above 2,147,483,647 no longer fit |
| Start storing dollars in fare_cents | Breaking and silent | Same name and type, so nothing errors; revenue reads 100x low |

Bring up the last row by name in an interview. A **semantic change**, the same column carrying a new meaning, passes every type check, so the pipeline loads it without complaint and the dashboard shows revenue collapsing overnight. Only a check on values can catch it: a rule that the daily average fare stays within a band around last week's would fail on the first load and stop the bad data at the door.


<br>

### Safe can still break a careless pipeline

Whether an added column is harmless depends on how the load is written. A positional insert ties the target's column order to whatever the source happens to return.

```sql
INSERT INTO warehouse_trips
SELECT *
FROM staging_trips
```

```sql
INSERT INTO warehouse_trips (
  trip_id,
  status,
  fare_cents,
  updated_at
)
SELECT
  trip_id,
  status,
  fare_cents,
  updated_at
FROM staging_trips
```

Naming columns trades one risk for another: the new tip_cents column is safely ignored, and it is also missing from the warehouse until someone adds it. Pipelines that want new columns automatically add them to the target when they appear, which is a sound policy for nullable additions and for nothing else.


<br>

### Handling each kind

![Pipeline diagram 3](images/pipeline_keeping_data_fresh_beginner_diagram_3.png)

| ✓ Do | ✗ Don't |
| --- | --- |
| • Compare each batch's schema with the expected one before loading it<br><br>• Add new nullable columns to the target and log the change<br><br>• Hold the batch and alert on any column drop or rename, or any type narrowing<br><br>• Check value ranges to catch meaning changes that types cannot reveal<br><br>• Agree with source owners to rename by adding, migrating, then dropping | • Write positional inserts or SELECT \* into a fixed target<br><br>• Let a failed type cast write NULLs and report success<br><br>• Load a renamed column as new and strand its history under the old name<br><br>• Drop a column the moment the source does while readers still query it |

A rename handled well spans 3 releases. First the source adds trip_status and writes both columns. Next, every reader moves to the new name. Only then is status dropped. Throughout the overlap, every reader works. Describe this sequence when asked how a breaking change can ship without an outage.

> Try it: safe or breaking?
>
> - `surge_multiplier` changes from DOUBLE to VARCHAR
> - A new nullable column `pickup_zone` is added
> - `updated_at` is renamed to `modified_at`
> - `distance` starts arriving in kilometres instead of miles

Surge as text: breaking, since arithmetic on it fails or casts to NULL. Pickup zone: safe for named-column readers. The rename is breaking and worse than it looks: it removes the incremental load's watermark column, so the extraction query fails, or, if it falls back to a default, reloads all 2 billion rows. Kilometres: breaking and silent, caught only by a range check on average trip distance.


<br>

---

## Backfilling

A backfill recomputes data for past periods. Teams run one when a bug fix changes how a column is computed, a new column needs history, late data arrives for days already closed, or a new table needs its first year filled. The interview question is how to run it without corrupting the table or the live pipeline, and in what order.


<br>

### Running example: a September fare bug

Suppose a currency conversion bug wrote wrong values to `fare_cents` in the `trip_fares` table for every day of September 2026. The table is partitioned by `ds`, 1 partition per day, so the damage covers 30 partitions. The fix is merged; the history still has to be rebuilt.


<br>

### Overwrite, never append

| ds | Rows before | After appending the rerun | After overwriting |
| --- | --- | --- | --- |
| 2026-09-14 | 3,104,220 | 6,208,440 | 3,104,220 |
| 2026-09-15 | 2,987,615 | 5,975,230 | 2,987,615 |

Appending the rerun's output doubles every row, and a retry of that triples it. A backfill is safe when each unit of work, here a single partition, is **idempotent**: running it once or 5 times leaves the same result. Overwriting the partition gives that property, here as a delete followed by a rebuild.

```sql
DELETE FROM trip_fares
WHERE ds = '2026-09-14'
```

```sql
INSERT INTO trip_fares (
  trip_id,
  fare_cents,
  ds
)
SELECT
  r.trip_id,
  CAST(ROUND(r.fare_local_cents * x.usd_rate) AS BIGINT) AS fare_cents,
  r.ds
FROM trips_raw AS r
JOIN fx_rates AS x
  ON r.currency = x.currency
  AND r.ds = x.ds
WHERE r.ds = '2026-09-14'
```

The pair is safe to retry: if the insert fails, rerunning both statements removes whatever partial output landed and writes it again. What it lacks is atomicity. Between the delete and the end of the insert, readers see an empty or half-full day. Where that matters, write the rebuilt day to a staging table, compare its row count and fare total against expectations, and swap it in as a single operation, so readers see either the old day or the new one.


<br>

### Protecting the live pipeline

Today's daily job still has to meet its deadline while the backfill runs. Cap the backfill's concurrency, say 4 partitions at a time, and give it lower priority than the daily load. Launching all of September at once would compete for the same compute and could make today's data late in order to repair last month's.

- **8** — waves to rebuild all of September

![Pipeline diagram 4](images/pipeline_keeping_data_fresh_beginner_diagram_4.png)


<br>

### Choosing the order

Ordering rules, strongest first

- **Upstream before downstream**\
  daily_city_revenue reads trip_fares. Rebuilding it first recomputes from still-broken data and has to be redone. Repair tables in dependency order.
- **Oldest first when state carries forward**\
  Running totals, SCD Type 2 dimensions and anything built from the previous day's output need day N correct before day N+1 can be.
- **Newest first when partitions stand alone**\
  Independent days can go from 2026-09-30 back to 2026-09-01, because recent days are queried most and matter most to the people waiting.

The rules only appear to conflict. Dependency order is never optional. Within a single table, state decides the direction: independent partitions go newest first for the fastest relief, and stateful ones go oldest first for correctness. Late data follows the same logic: a day that receives late trips is reprocessed like any other damaged partition, and so is everything downstream of it.

> Try it: plan the rebuild
>
> A `driver_lifetime_earnings` table adds each day's fares to the previous day's running total, and it reads `trip_fares`. The September fix merges today, 2026-10-24. Which tables need a backfill over which dates, and in what order should they run?

First `trip_fares`, all 30 September days, newest first since its partitions are independent. Then the 2 tables that read it. `daily_city_revenue` gets the same 30 September days, newest first, because each day's revenue stands alone. `driver_lifetime_earnings` runs oldest first from 2026-09-01 through today, because each running total from October onward carries the wrong September sums inside it. The repair window for a stateful table ends today, well past the end of the bug.

> **TIP**
>
> Before giving the plan, list the partitions and downstream tables the bug touched. Then say whether any of them carry state forward. Most backfill mistakes come from stopping at the table where the bug was, or at the last day the bug was live.


<br>

---

## Putting It All Together

A ride-hailing company's finance team reports that September revenue by city does not match the payment processor's totals. The investigation finds 3 causes at once: a currency conversion bug in trip fares, a city report that joins drivers on is_current, and cancelled trips deleted in the source that the hourly incremental load never removed.

- The deleted trips are a timestamp-loading blind spot: moving trips to CDC turns each deletion into an event, after an initial snapshot replaces the stale copy.
- The wrong cities are an SCD join error: the report switches from is_current to the half-open range join on dim_driver.
- The fare bug is a backfill: trip_fares is rebuilt by partition overwrite, then city revenue, then running totals oldest first through today.
- Switching to CDC means the team adds log_position to the warehouse trips table on purpose, a nullable additive column that named-column readers ignore.
- Each fix can be retried safely. Upserts on trip_id give the same result on a second run, and so do partition overwrites and an apply step that skips replay positions.


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Our warehouse visit totals never go down when the clinic's scheduling database hard-deletes a cancelled appointment, and every visit is credited to the patient's current insurance plan instead of the plan they held on the visit date. Design how the warehouse copy keeps up with those deletes and plan switches from now on, and how the months already reported shed the cancelled visits they still count.

<details>
<summary><strong>Solution</strong></summary>

#### What this really is

This is 3 of the lesson's answers stacked on one visit table. The totals that never shrink are a **CDC** problem: a copy that asks the scheduling database what rows exist, or what changed since a timestamp, cannot see a row that is gone, so a cancelled appointment stays counted forever. The wrong plan is an SCD Type 2 problem: overwrite the patient's plan and every past visit quietly moves to it. The months already published are a backfill problem, because fixing the pipeline from today leaves every old total inflated. Miss any one and revenue is still right to distrust the numbers.

| Trick: A delete is an event, not an absence |
| --- |
| You cannot find a deleted row by looking for it. The only place a hard delete survives is the database's own change log, so the pipeline has to read that log and treat a delete as an instruction to remove the visit from the warehouse. |

#### Walk the requirements

1. **Read deletes from the change log**

   `scheduling_changes` captures every insert, update and delete the scheduling database writes. The `visit_loader` applies each one: a new appointment lands, an update overwrites, a delete removes the visit. This is incremental loading that actually sees cancellations, which a last-updated filter never will.

2. **Keep every plan a patient has held, with dates**

   When a plan change arrives, the loader closes the patient's current row in `patient_plan_history` by setting `valid_to`, then inserts the new plan with a fresh `valid_from`. A visit is credited to the row whose range contains its visit date, so a switch in June leaves March's visits under March's plan.

3. **Re-run the months already reported**

   The fix from step 1 only protects the future. The visit table is partitioned by visit month and carries `partition_overwrite`, so re-running a past month reloads it from the source, which no longer holds the cancelled rows. Overwriting makes the rerun safe to repeat; appending would double the month instead.

#### The shape that fits

```mermaid
flowchart LR
    subgraph L0["Ingest changes"]
        n0[("scheduling_changes<br/>(CDC)")]
        n1["visit_loader<br/>(Spark)"]
    end
    subgraph L1["Warehouse"]
        n2[("patient_plan_history<br/>(warehouse)")]
        n3[("visit_warehouse<br/>(warehouse)<br/>Backfill: partition_overwrite")]
    end
    subgraph L2["Serving"]
        n4(["revenue_report<br/>(dashboard)"])
    end
    n0 --> n1
    n1 --> n2
    n1 --> n3
    n2 --> n3
    n3 --> n4
```

*The change log carries deletes into `visit_loader`, plan switches become dated rows in `patient_plan_history`, and `visit_warehouse` overwrites a month's partition whenever a past month is re-run.*

| ✕ Overwrite the plan | ✓ Keep plan history |
| --- | --- |
| One row per patient in the plan table. The day a patient switches, `plan_id` changes and every past visit reports under the new plan. Last quarter's revenue by plan shifts without anyone touching it. | One row per plan per patient, bounded by `valid_from` and `valid_to`. A switch closes one row and opens another, and each visit keeps the plan whose range covers its date. Old reports stay stable. |

| Pitfall: A last-updated filter cannot see a deleted row |
| --- |
| The common first fix is to load rows where `updated_at` is later than the last run. It catches new and edited appointments and still misses every cancellation, because a deleted row has no `updated_at` left to compare. The totals keep growing exactly as before. |

| Performance: The extra state is small next to what it fixes |
| --- |
| The loader now handles 3 kinds of event instead of one, and plan history gains a row per switch. At about 40,000 appointments a month the nightly change set is a few thousand rows, and correcting a past month rewrites one partition, not the whole table. |

| Interviewer: Name the plan as of when |
| --- |
| The tell is a candidate who asks whether the source keeps old plans before promising history. It does not, so plan history starts the day capture starts, and they say so instead of claiming the backfill fixes plan attribution too. |

**Follow-up questions**

1. A patient switched plans a year ago, before `patient_plan_history` existed. Which of their visits can you attribute correctly, and which can't you?\
   *Tests whether the candidate sees that history begins when capture begins: the source only ever held the current plan, so earlier switches are lost unless another system recorded them.*
2. The clinic adds a `referral_source` column to appointments next week. What happens to this pipeline?\
   *Tests schema evolution: an added column is additive and should flow through automatically, while a rename or drop is breaking and needs a migration plan.*
3. The rerun of last March overlaps with tonight's load. What keeps March from being counted twice?\
   *Tests backfill safety: overwriting the month's partition replaces it wholesale, so a rerun is repeatable, and tonight's load writes a different partition.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

The operational PostgreSQL customers table holds 40 million rows, about 200,000 of which change each day with deletes included, and the source team adds a column every few months, so only changed rows may move and a new column must never break the load. Design the pipeline behind the analytics team's warehouse customer dimension so it can say which tier and region a customer held on any past date, and so one corrected day of changes can be reprocessed without reloading the rest or duplicating a version.

**Starting canvas** (already placed for you):

- Operational DB (source, PostgreSQL)

<details>
<summary><strong>Solution</strong></summary>

#### What this really is

Take off the warehouse costume and this is a **CDC-fed SCD2 dimension**: you turn a stream of row-level changes into a versioned history of each customer, not a fresh snapshot of today. The 4 parts the lesson named (CDC, SCD2, schema evolution, backfill) are not 4 stages. Each one depends on the others. The trap is picking the simplest tool at every step: a nightly full reload, an in-place overwrite, a hand-written `ALTER TABLE`, a merge you can only re-run by wiping the table. Any one of those quietly rules out the other three.

```mermaid
flowchart LR
    subgraph L0["Extract"]
        n0[("Operational DB<br/>(PostgreSQL)")]
        n1[("CDC capture<br/>(CDC)")]
    end
    subgraph L1["Land"]
        n2[("Raw change log<br/>(object storage)")]
    end
    subgraph L2["Transform"]
        n3["SCD2 merge<br/>(dbt)<br/>Idempotency: upsert"]
        n5["Backfill orchestrator<br/>(Airflow)"]
    end
    subgraph L3["Serve"]
        n4[("Warehouse customer dimension with schema evolution<br/>(Delta Lake)")]
    end
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n5 --> n3
```

*CDC lands only changed rows in a raw log partitioned by `extract_date`. The repeatable merge versions them into a Delta Lake dimension with schema evolution on, so a new source column is added on write. The orchestrator re-drives one partition when a day needs correcting.*

| ✕ Nightly full reload | ✓ CDC plus incremental merge |
| --- | --- |
| `TRUNCATE dim_customer` and re-select the whole source. Cost grows with table size instead of change volume, it scans the operational DB every night, and the truncate throws away every prior version. SCD2 cannot exist under it. | CDC reads the log and emits only rows that changed, deletes included, so merge cost follows change volume. Because the dimension carries history, incremental is not just the faster choice. It is the only choice that keeps that history. |

1. **Land changes before transforming them**

   Append CDC events to the raw log, partitioned by `extract_date`. Do not merge straight from the stream. A day of changes then becomes an immutable partition you can replay without touching the source. Partitioning is what makes backfill cheap later.

2. **Detect real change with `row_hash`**

   CDC fires on no-op writes too. Hash the tracked attributes in `stg_customer_changes` and open a new version only when `d.row_hash <> s.row_hash`. Without the hash, the dimension fills up with versions that differ only by a timestamp, and a replayed day would stack duplicates.

3. **Expire, then insert**

   Close the current row (`effective_to = s.change_ts`, `is_current = FALSE`), then insert the new current version. 2 plain statements are easier to read and audit than one clever `MERGE`, and re-running them over the same partition changes nothing. That is what lets the orchestrator backfill one day safely.

**The SCD2 merge**

```sql
/* 1. Expire the current row wherever a tracked attribute changed */
UPDATE dim_customer d
SET    effective_to = s.change_ts,
       is_current   = FALSE
FROM   stg_customer_changes s
WHERE  d.customer_id = s.customer_id
  AND  d.is_current
  AND  d.row_hash <> s.row_hash;

/* 2. Insert a new current version for brand-new and changed customers */
INSERT INTO dim_customer (
    customer_id, name, tier, region, row_hash,
    effective_from, effective_to, is_current
)
SELECT s.customer_id, s.name, s.tier, s.region, s.row_hash,
       s.change_ts, NULL, TRUE
FROM   stg_customer_changes s
LEFT   JOIN dim_customer d
       ON d.customer_id = s.customer_id AND d.is_current
WHERE  d.customer_id IS NULL          /* never seen this customer */
   OR  d.row_hash <> s.row_hash;       /* or an attribute changed */
```

*Expire changed current rows, then insert a fresh version for new and changed customers.*

| Pitfall: 2 statements need one transaction |
| --- |
| If the job dies between the `UPDATE` and the `INSERT`, a changed customer is left with no `is_current` row. That is why the dimension sits on an ACID table format (Delta Lake): run both statements in one transaction and a crash commits neither. Plain files in object storage cannot give you that guarantee. |

| Pitfall: An in-place update is SCD1 |
| --- |
| `SET tier = 'gold' WHERE customer_id = 42` answers which tier the customer is in now. The warehouse usually asks which tier they were in when they placed an order, and the overwrite has already erased that answer. |

| Key insight: Old versions should stay `NULL` for a new column |
| --- |
| When the source gains a column, a table format with schema evolution turned on adds it on write and the merge selects it by name. Do not pause the pipeline and reload to fill in the past. Older versions never had that attribute, so `NULL` is the accurate history, not a gap. |

| Interviewer: Show how the constraints depend on each other |
| --- |
| Senior candidates link the pieces: the partitioned log makes backfill cheap, the hash makes the merge idempotent, and keeping history rules out a full reload. Candidates who list the 4 terms one at a time sound like they are reciting a glossary. |

**Follow-up questions**

1. How do you handle a hard delete in the operational DB?\
   *Close the current version with `effective_to` instead of deleting it, so the history survives.*
2. 2 changes for one `customer_id` land in the same partition. Which version wins?\
   *Ordering by `change_ts` within each key so versions chain correctly.*
3. What breaks if an `extract_date` partition arrives late?\
   *Whether idempotency and the single `is_current` invariant survive out-of-order replay.*

</details>


<br>

---

## Key Takeaways

- Full loads trade cost for self-healing; incremental loads trade state and edge cases for a cost that tracks change.
- An incremental load needs a change column it can trust and a watermark taken from the data. A lookback window and an upsert then pick up late commits without loading them twice.
- CDC reads the commit log, which means a delete or an intermediate state arrives as its own event. The apply step skips positions it has already seen.
- SCD Type 2 keeps versions with half-open validity ranges; join on the natural key plus the range, or on a stamped surrogate key.
- A schema change is breaking if any reader or writer fails or gives wrong answers without an error; meaning changes need value checks.
- Backfills overwrite whole partitions under a concurrency cap, go upstream first, and go oldest first when state carries forward.
