Intermediate·Pipeline Architecture · 25 min · 4 challenges

# Streaming Systems: Intermediate

Renata is in the final round at Uber. The interviewer draws a box labelled ride requests and a second labelled surge pricing, then asks her to build the path between them: every city gets a request count each minute, and she must explain what happens when a worker dies halfway through a batch. She starts with partitions and a group of readers. The follow-ups keep coming. Finance wants any trip's fare history replayable; what is stored? A phone in a tunnel uploads its request 90 seconds late; does that minute get recounted? A deploy ships a parser bug and 40,000 events fail; where do they go, and how do they come back? Micro-batch engine or record-at-a-time? Each answer leans on the one before it, and this lesson walks the chain in the interviewer's order, with the actual offsets and timestamps at each step and actual failure counts where things break.


<br>

### What you will be able to do

Trace where a consumer crash or rebalance creates gaps or duplicates from the commit point, and pick the exactly-once pattern that closes it.

Decide whether a domain needs event sourcing and whether it needs CQRS, then state what each costs.

Pick a window type, then set the watermark bound and allowed lateness from a measured lateness distribution.

Classify streaming failures and design the retry topics and DLQ envelope that hold them, plus a redrive that is safe to run.

Pick between a micro-batch and a continuous engine by naming the requirement that decides it.


<br>

---

## Consumer Groups and Offsets

### How a group divides a topic

A consumer group is a set of processes sharing a group name, and the broker assigns each partition of a topic to a single member. The running example here is a topic called `ride_events` with 12 partitions, read by the group `surge-pricer`. With 4 members, each owns 3 partitions; with 6, each owns 2. Grow the group to 13 and the 13th member sits idle, because a partition is never split across members. Partition count caps parallelism within a group, and raising it later remaps keys to different partitions, breaking per-key ordering for records in flight.

| Members | Partitions per member | Idle members |
| --- | --- | --- |
| 4 | 3 | 0 |
| 6 | 2 | 0 |
| 12 | 1 | 0 |
| 13 | 1 | 1 |


<br>

### Offsets are positions the consumer commits

Every record in a partition has an offset that grows by 1 per record. The group stores a committed offset per partition: the next offset to read after a restart. The consumer decides when that commit happens, and its delivery guarantee follows from where the commit sits relative to the side effect.

#### Worked crash: offsets 1000 to 1499

Consumer C2 owns partition 7 and has committed offset 1000. It polls 500 records, offsets 1000 through 1499, adds them to per-city counters, writes the counters to the pricing store, and commits offset 1500. Kill it between 2 of those steps and restart it.

| Order of steps | Crash lands between | Restart reads from | Effect on counts |
| --- | --- | --- | --- |
| Commit, then write | commit and write | 1500 | Offsets 1000 to 1499 never counted |
| Write, then commit | write and commit | 1000 | Offsets 1000 to 1499 counted twice |
| Write and commit in a single transaction | no gap: 1 atomic step | The offset the store holds | Each record counted once |

Row 2 is the usual choice, and the right default: lost records vanish silently, while duplicates can be found and removed. At-least-once describes delivery. Duplicates become harmless only when processing a record twice has the same effect as processing it once, which is a property of the sink.


<br>

### Rebalances replay uncommitted work

Crashes are rare; rebalances happen on every deploy. When a member joins or leaves, the group reassigns partitions, and the same happens when a member misses its heartbeat deadline (a long garbage-collection pause is enough). Each new owner starts from the last committed offset. Records the old owner processed but had not committed run again. With commits every 5 seconds and partition 7 carrying 2,000 records per second, each move of partition 7 can repeat up to 10,000 records.


<br>

### Lag is the number that pages someone

Lag per partition is the log end offset minus the committed offset. If partition 7 ends at 88,400 and the group has committed 82,400, lag is 6,000 records. The drain rate tells you whether it matters: a consumer processing 2,500 records per second against 2,000 arriving clears those 6,000 in 12 seconds, while a drain rate of 0 never clears. Alert on lag that keeps growing for several minutes, since a fixed lag threshold pages during every normal burst.


<br>

### 3 ways to an exactly-once effect

No protocol can promise that a record reaches your code exactly once across crashes, because a sender cannot tell a lost acknowledgement from a lost message. Pipelines aim for an exactly-once effect instead: at-least-once delivery into a sink that absorbs repeats.

- **Idempotent writes** *A repeat overwrites itself*\
  Upsert the absolute count keyed by city_id and window_start, or dedupe on an event_id the producer assigns. Replaying offsets 1000 to 1499 rewrites identical rows. An increment such as count = count + n breaks this, because it doubles on replay.
- **Offsets stored with the output** *One transaction covers both*\
  Write results and the partition's next offset to the sink database in a single transaction, and on startup read the offset from that table instead of from the broker. The commit and the side effect can no longer diverge.
- **Transactional read-process-write** *For log-to-log pipelines*\
  The producer writes output records and the consumed offsets in a single log transaction, and downstream readers use read-committed isolation so aborted output stays invisible. It covers only effects that land in the log.

```sql
def run(consumer, db, partition):
    consumer.seek(partition, db.read_offset(partition))
    while True:
        batch = consumer.poll(partition, max_records=500)
        if not batch:
            continue
        counts = count_by_city_minute(batch)
        with db.transaction() as tx:
            for (city_id, window_start), n in counts.items():
                tx.increment_count(city_id, window_start, n)
            tx.save_offset(partition, batch[-1].offset + 1)
```

The counter write here is an increment, which pattern 1 forbids. Pattern 2 makes it safe because the offset update shares its transaction: a crash rolls back both, and the batch replays against the old counts. Move `save_offset` outside the `with` block and the same line double-counts on every rebalance.


<br>

### Where exactly-once ends

Each pattern stops at systems that can join the transaction or absorb a repeat. A card charge cannot be rolled back, and neither can a push notification or a partner API call. For those, derive an idempotency key from the record, such as `trip_id` plus event type, and have the receiving service reject keys it has seen. The strong answer to "make this exactly-once" names the boundary. It says which effects sit inside a transaction and which are idempotent on their own, then lists the external calls that carry a key.

> Disguises of this question
>
> - "We see double counts after every deploy." Rebalance replay into a sink that increments outside a transaction.
> - "Can we turn on exactly-once and stop worrying?" Only effects inside the log transaction are covered.
> - "Adding consumers did not raise throughput." Members beyond the partition count sit idle.
> - "Lag hit 1 million at 2am, then recovered." Check the drain rate before treating it as an incident.


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

A single consumer can't keep up with a 16-partition topic and lag is growing. Running 2 copies naively makes both read everything and double-process, and right now a single event that fails to parse stalls its whole partition. Design consumption that scales horizontally with committed offsets so a restart resumes cleanly, and route failed events aside so one bad record never blocks the partition or gets silently dropped.

**Starting canvas** (already placed for you):

- 16-partition topic (source, Kafka)
- Single consumer (lagging) (transform, Flink)
- Serving store (storage, Redis)

<details>
<summary><strong>Solution</strong></summary>

#### What this really is

Strip the 'lag is growing' costume and this is a partitioning problem: your one consumer isn't slow because it's underpowered, it's the only member draining all 16 partitions single-threaded. The trap is right there in the prompt. Launch a second copy the naive way and it reads the topic from the beginning too, so both process every event and Redis gets written twice. The move that actually scales is a **consumer group**: give every consumer the same `group.id` and Kafka assigns each of the 16 partitions to exactly one member. Partition count is your ceiling on parallelism. What separates a real answer from a diagram-drawer is the next 2 things nobody puts on the whiteboard until asked: what you commit and when, and what happens to the one record that won't parse.

```mermaid
flowchart LR
    subgraph L0["ingest"]
        n0[("orders topic (16 partitions)<br/>(Kafka)")]
    end
    subgraph L1["process"]
        n1["Consumer group (up to 16 members)<br/>(Flink)"]
    end
    subgraph L2["serve"]
        n2[("Serving store (idempotent upsert)<br/>(Redis)")]
    end
    subgraph L3["error path"]
        n3[("Dead-letter topic<br/>(Kafka)")]
        n4["DLQ monitor + replay<br/>(Flink)"]
    end
    n0 --> n1
    n1 --> n2
    n1 --> n3
    n3 --> n4
```

*One consumer group replaces the lone consumer; failed records fork to a dead-letter topic instead of blocking their partition.*

#### Scaling with a consumer group

1. **Put every consumer in one group**

   Give all consumers a shared `group.id`. Kafka's group coordinator now owns partition assignment: it hands each of the 16 partitions to exactly one live member and rebalances when a member joins or dies. This is the single line that turns 'two copies double-process everything' into 'two copies split the work', the difference between the naive fix and the correct one is literally whether they share a group.

2. **Size the group against the partition count, not the CPU**

   16 partitions means at most 16 members do useful work; a 17th sits idle because a partition is never split across 2 consumers. If lag is still growing at 16 consumers, you don't add a 17th, you repartition the topic to a higher count. Partitioning is the unit of parallelism here, so the partition count is a capacity decision you make upstream, at topic-creation time.

3. **Commit offsets after you process, never before**

   Commit the offset only once the event is safely in Redis. On restart or rebalance the new owner resumes from the last committed offset, so nothing between the crash and the last commit is lost. Turn off `enable.auto.commit` (which commits on a timer regardless of whether your write succeeded) and commit explicitly after the write. Commit-after-process is at-least-once, which sets up the next requirement.

| Trick: The whole thing hinges on offset timing |
| --- |
| Commit-before-process loses data on a crash (silent drop). Commit-after-process replays the in-flight batch on a crash (duplicates). There is no free lunch, you pick your failure mode. Pick at-least-once and make the sink idempotent, because silent loss in a serving store is the one you can't detect after the fact. |

At-least-once means the same event can hit Redis twice, so the write has to be **idempotent**, replaying it produces the same state. A keyed upsert like `SET order:{id} {state}` is naturally idempotent; a `INCR` counter is not, because a replayed event double-counts. If your logic genuinely needs counters, dedupe first by tracking a seen `event_id` per key, or move the count to a store that lets you write the derived total instead of an increment. This is why idempotency and committed offsets are one design, not two: the commit strategy is what forces the sink to be replay-safe.

| Pitfall: Auto-commit is the silent-loss factory |
| --- |
| With `enable.auto.commit=true` the client commits the latest polled offset every few seconds, even for events your handler is still processing or that just threw. Crash in that window and those offsets are already committed, the restarted consumer skips right past them. It looks fine in every demo and quietly drops records under real failure. Own the commit. |

| ✕ 2 copies, no shared group | ✓ One consumer group |
| --- | --- |
| Each consumer independently subscribes and reads all 16 partitions from its own offset. Every event is processed twice, Redis is written twice, and a `INCR` counter doubles. Throughput does not go up, you just paid for 2 machines to do the same work and corrupt the serving store. | All consumers share `group.id`; Kafka splits the 16 partitions across members, each event processed once. Add a member and the group rebalances to share load; lose one and its partitions reassign. Throughput scales linearly up to 16, and committed offsets make a restart resume cleanly. |

#### The poison message

Offsets within a partition are strictly ordered, so a consumer can't advance past record N until it's done with record N. One event that won't parse leaves you 2 bad options: block forever (that partition's lag climbs while the other 15 look healthy, a partial stall that's miserable to diagnose) or swallow the exception and move on (silent drop). The **dead-letter queue** is the third door: catch the failure, publish the raw record plus its error and metadata to a separate dead-letter topic, then commit the offset and keep going. The good partition keeps flowing, the bad record is preserved for inspection, and nothing is lost.

| Real world: A DLQ you never read is just a slow drop |
| --- |
| Routing to a dead-letter topic isn't the finish line, an unwatched DLQ is silent loss with extra steps. Alert on dead-letter depth, keep the original partition/offset and error on each dead-lettered record, and have a replay path that re-injects fixed records after you ship the parser change. The `DLQ monitor + replay` node exists so the error path closes the loop instead of accumulating forever. |

| Interviewer: What signals you've actually run this |
| --- |
| Anyone can say 'add more consumers.' The tells that you've operated a stream: you name the partition count as the parallelism ceiling, you say when you commit relative to the write, you make the sink idempotent because you chose at-least-once, and you give the poison message somewhere to go that is neither blocking nor `/dev/null`. Miss any one and the design silently corrupts or stalls under the first real failure. |

**Follow-up questions**

1. You add a 17th consumer to the group and throughput doesn't move. Why?\
   *Tests that partition count, not consumer count, is the ceiling on parallelism.*
2. One key gets 10x the traffic and its partition is hot while others idle. Do more consumers help?\
   *Tests understanding that partition-key skew, not consumer count, is the bottleneck.*
3. Your serving logic is a running total via `INCR`. How do you keep it correct under at-least-once redelivery?\
   *Tests idempotency when the sink operation isn't naturally replay-safe.*
4. How does a record get out of the dead-letter topic and back into normal processing without replaying the poison version forever?\
   *Tests that the DLQ has a closed loop: triage, fix, replay.*

</details>


<br>

---

## Event Sourcing Patterns

### State as a fold over facts

A conventional trip service keeps a row per trip and overwrites its status and fare on each change. Event sourcing makes the system of record an append-only log of events per entity, and computes current state by applying those events in order. The row you would have stored becomes a cache, rebuildable at any time.

| event_seq | event_type | occurred_at | payload |
| --- | --- | --- | --- |
| 1 | TripRequested | 12:00:05 | city_id=31, est_fare_cents=1840 |
| 2 | DriverAssigned | 12:00:19 | driver_id=880 |
| 3 | TripStarted | 12:04:52 | none |
| 4 | TripCompleted | 12:21:10 | fare_cents=2150 |
| 5 | FareAdjusted | 14:02:33 | delta_cents=-300, reason=route_dispute |

Folding trip 5521's 5 events gives status completed and a fare of 1,850 cents. A mutable row would hold those 2 values and nothing more. The log also answers what the row cannot: the fare at 13:00 (2,150 cents), who changed it and why, and how long the rider waited for a driver (14 seconds).

```sql
SELECT
  trip_id,
  MAX_BY(event_type, event_seq) AS last_event,
  SUM(COALESCE(fare_cents, 0) + COALESCE(delta_cents, 0)) AS fare_cents_as_of,
  MAX(occurred_at) AS last_changed_at
FROM trip_events
WHERE occurred_at <= TIMESTAMP '2026-10-24 13:00:00'
GROUP BY trip_id
```


<br>

### Snapshots bound the replay

Trip 5521 has 5 events. A driver's earnings account gains an event for every completed trip and payout, and an active driver can reach 20,000 within a few years. Replaying all of them on each read is too slow, so the store writes a snapshot of folded state every N events and replays only what follows it. With N set to 1,000, a read loads 1 snapshot and at most 999 events. Snapshots are caches as well, so a change to the fold logic means throwing them away and rebuilding them from the log.


<br>

### CQRS splits writes from reads

Command Query Responsibility Segregation separates the path that changes state from the paths that read it. A command such as AdjustFare is validated against the write model, which for an event-sourced trip is its folded state, and if accepted it appends events. Projections consume the log and maintain read models, each shaped for its queries: a status table for the rider app, a per-driver earnings table, a wide table for analysts. Each projection is its own consumer group with its own committed offsets, so rebuilding one means resetting its offset to 0 and replaying.

![Pipeline diagram 1](images/pipeline_streaming_systems_intermediate_diagram_1.png)

The cost CQRS adds is staleness. A rider who taps cancel may read the status table a few hundred milliseconds before the projection applies the event, and see the trip still active. The common mitigations are to return the new state in the command's response, or to have the client wait until the projection's committed offset passes the offset of the event it wrote.

| ✓ Use event sourcing when | ✗ Skip it when |
| --- | --- |
| • History is the product: ledgers, fares, balances, inventory<br><br>• Support or auditors must answer what state was at time T<br><br>• New read models keep appearing and must backfill from the first event<br><br>• Bugs in derived state are fixed by correcting code and replaying | • The domain is plain CRUD and nobody asks about past states<br><br>• Every read must see the latest write, with no lag allowed<br><br>• Personal data must be erasable and no key-deletion scheme exists<br><br>• Nobody will own event schema versioning for the log's lifetime |


<br>

### The costs interviewers probe

What goes wrong, and the standard answer

- **Event versioning.** Events live forever, so FareAdjusted v1, with a dollar amount as a float, and v2, with delta_cents as an integer, both stay in the log. An upcaster converts old versions on read so the fold handles a single shape.
- **Erasure.** Deleting a rider's address from an immutable log breaks its guarantee. Encrypt personal fields with a per-rider key and delete the key on request: the events remain, the personal data becomes unreadable.
- **Replay time.** Rebuilding a projection over 2 billion events at 50,000 events per second takes about 11 hours, so a new projection version runs beside the old one until it catches up, then reads switch over.
- **Ordering.** A trip's events must be applied in sequence. Keying the log by trip_id keeps each trip in 1 partition, so order holds per trip and nowhere else.

These patterns are separable. CQRS without event sourcing is common: an ordinary database holds the write model and change data capture feeds the read models. Event sourcing without CQRS is rare, because answering queries by folding the raw log is slow, so an event-sourced system grows projections almost at once.

> **TIP**
>
> If an interviewer asks how finance can reconstruct a fare at any past moment, that is the event sourcing question in disguise. If reads are slow because they contend with writes on the same table, that is the CQRS question.


<br>

---

## Windowing and Watermarks

### Pick the window from the question

A window assigns each event to finite buckets of event time so an unbounded stream can be aggregated. Surge pricing asks for requests per city per minute across 600 cities, which is a tumbling window: fixed size, no overlap. A smoothed rate covering the last 5 minutes, refreshed every minute, is a hopping window. Grouping a rider's taps until 30 minutes of silence is a session window, whose length depends on the data.

| Window | Parameters | Windows per event | State grows with |
| --- | --- | --- | --- |
| Tumbling | size 1 min | 1 | open windows times keys |
| Hopping | size 5 min, slide 1 min | 5 | 5 times the tumbling state |
| Session | gap 30 min | 1, merged as events arrive | active sessions, no fixed length |

Say the cost in the third column out loud: every event lands in 5 hopping windows, so across 600 cities the job keeps 5 times the state of the tumbling version and does 5 times the update work.


<br>

### Event time versus processing time

Each ride event carries `event_time`, stamped on the phone when the rider tapped, and receives a processing time when the engine reads it. Network delay and client retries open a gap between the 2, and so does a phone that buffers events while offline. Windowing by processing time is cheap and wrong for rides: the backlog after a consumer restart would pour 10 minutes of requests into the current minute and spike surge in every city at once. Windowing by event time puts each request in the minute it happened, and raises the question this section answers: when is a minute finished?


<br>

### A watermark is a claim about completeness

A watermark W asserts that no more events with `event_time` below W are expected. Once W passes a window's end, the window emits its result. The usual strategy is bounded out-of-orderness: W is the largest event time seen minus a fixed bound. With a 10 second bound, the window from 12:00:00 to 12:01:00 fires once an event stamped 12:01:10 or later arrives. The trace follows the events of a single partition for city 31.

| Arrival | event_time | Max seen | Watermark | Outcome |
| --- | --- | --- | --- | --- |
| 1 | 12:00:41 | 12:00:41 | 12:00:31 | Counted in 12:00 window |
| 2 | 12:00:58 | 12:00:58 | 12:00:48 | Counted in 12:00 window |
| 3 | 12:00:52 | 12:00:58 | 12:00:48 | Out of order, still counted |
| 4 | 12:01:12 | 12:01:12 | 12:01:02 | 12:00 window fires |
| 5 | 12:00:57 | 12:01:12 | 12:01:02 | Late: its window already fired |
| 6 | 11:59:42 | 12:01:12 | 12:01:02 | Tunnel phone, 90 s behind |

Row 3 is why the bound exists: out-of-order events within 10 seconds of the newest are absorbed. Rows 5 and 6 arrive after their windows fired, and what happens to them is a separate decision with 3 options.

- **Drop** *Cheapest*\
  Count them in a late-events metric and discard them. Acceptable when a correction would land after the surge multiplier it fixes has already expired.
- **Allowed lateness** *Keep window state open longer*\
  Keep each window's state for 2 minutes after it fires and emit an updated count for each late event. Row 5 joins the 12:00 window, held until the watermark passes 12:03:00. Row 6 joins the 11:59 window, held until 12:02:00, and the watermark is only 12:01:02. The sink must accept updates, so it needs the idempotent upsert on city_id and window_start.
- **Side output** *Route to a late stream*\
  Send late events to their own topic and let an hourly batch job fold them into a corrected table. The stream stays fast and the corrected total arrives later.


<br>

### Choose the bound from measured lateness

The bound trades latency for completeness, and it should come from data. Measure each event's lateness as the max event time seen when it arrives minus its own `event_time`, over a week of traffic. Suppose ride events show p50 of 1 second, p99 of 8 seconds and p99.9 of 75 seconds. A 10 second bound emits each minute around 10 seconds after it ends and counts at least 99% of events on time. An 80 second bound catches 99.9%, but every surge multiplier is then older than the minute it describes. Surge pricing takes the 10 second bound and lets allowed lateness correct the tail.

Both knobs cost memory. A window's state lives from its first event until the watermark passes its end plus the allowed lateness, so a larger bound keeps more windows per key open at once, and so does more allowed lateness.

- **190 s** — lifetime of each 1-minute window
- **2,400** — peak open windows across 600 cities


<br>

### Idle partitions freeze the clock

An engine reading 12 partitions tracks a watermark per partition and uses the minimum, so the slowest partition holds back every window. If partition 9 carries a small city that goes quiet at 4am, its watermark stops advancing, so no window anywhere can fire. An idleness timeout fixes this: after, say, 60 seconds with no records, partition 9 drops out of the minimum until it produces again. Set the timeout longer than normal gaps on busy partitions, or a brief stall turns their next records into late data.

Whatever the late-data choice, keep a batch query over the stored events that recomputes the same windows by event time. Comparing its counts with what the stream emitted shows whether the bound still fits as traffic changes.

```sql
SELECT
  city_id,
  DATE_TRUNC('minute', event_time) AS window_start,
  COUNT(*) AS requests
FROM ride_events_raw
WHERE event_time >= TIMESTAMP '2026-10-24 12:00:00'
  AND event_time < TIMESTAMP '2026-10-24 13:00:00'
GROUP BY city_id, DATE_TRUNC('minute', event_time)
```

> **TIP**
>
> Asked which watermark you would use, answer with the lateness distribution you would measure, the bound you would choose from it, and what the late tail does: dropped, corrected through allowed lateness, or reconciled by batch.


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

A clickstream sessionization job buckets events by processing time, so when ingestion lags the events land in the wrong session and the metrics jump around. Rework the windowing so session assignment stays correct no matter when an event is processed. A handful of events still arrive hours after their window has closed, far past any sane cutoff, and those stragglers must be captured for a later correction instead of silently dropped.

**Starting canvas** (already placed for you):

- Clickstream (source, Kafka)
- Processing-time windows (transform, Flink)
- Session metrics (storage, PostgreSQL)

<details>
<summary><strong>Solution</strong></summary>

#### What this really is

This is the **late-data pattern** dressed up as sessionization: event-time windows with a watermark, and a dead letter queue behind them. The starter windows on processing time, so session membership depends on Kafka lag rather than on what the user did. Everyone swaps in `EventTimeSessionWindows` and a watermark. The trap is stopping there. A watermark decides when a window fires. It does not decide what happens to the click that arrives 3 hours later. Flink drops that click by default, so session counts drift low and nothing tells you.

```mermaid
flowchart LR
    subgraph L0["Ingest"]
        n0[("Clickstream<br/>(Kafka)")]
    end
    subgraph L1["Stream"]
        n1["Timestamp and watermark assigner<br/>(Flink)"]
        n2["Event-time session windows<br/>(Flink)"]
    end
    subgraph L2["Serve and reconcile"]
        n3[("Session metrics<br/>(PostgreSQL)")]
        n4[/"Dead letter queue<br/>(Kafka)"/]
        n5["Correction job<br/>(Spark)"]
    end
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n2 --> n4
    n4 --> n5
    n5 --> n3
```

*Event-time sessions feed the metrics. Stragglers past the lateness cutoff go to a Kafka dead letter queue, and a correction job replays them from there.*

#### Building it

1. **Assign event time and a bounded watermark**

   Read `event_time_ms` from the payload instead of trusting the ingestion clock, and pair it with `for_bounded_out_of_orderness` set to 30 seconds. That bound is your promise about ordinary jitter. Set it too tight and windows fire before normal stragglers arrive. Set it too loose and every metric goes stale.

2. **Key by session, window on event-time gaps**

   Apply `key_by` on `session_key`, then `EventTimeSessionWindows.with_gap` with a 30-minute gap. The gap is measured in event time, so a lagged click merges back into its true session. It does not open a phantom new one.

3. **Re-fire for the tardy, dead-letter the rest**

   `allowed_lateness` of one hour keeps window state alive, so a tardy click re-fires the window with a corrected aggregate. Anything later goes through `side_output_late_data` to a side stream. That stream lands in a durable dead letter queue that a correction job replays.

**PyFlink: event-time sessions with a late-data side output**

```python
watermarks = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(30))
    .with_timestamp_assigner(lambda event, _: event.event_time_ms)
)

late_events = OutputTag("late-events", event_type_info)

sessions = (
    clickstream
    .assign_timestamps_and_watermarks(watermarks)
    .key_by(lambda event: event.session_key)
    .window(EventTimeSessionWindows.with_gap(Time.minutes(30)))
    .allowed_lateness(Time.hours(1))
    .side_output_late_data(late_events)
    .aggregate(SessionMetricsAggregator())
)

# happy path: correct, watermark-driven session metrics
sessions.sink_to(postgres_metrics_sink)

# stragglers past the 1h cutoff -> dead letter queue for later correction
sessions.get_side_output(late_events).sink_to(dead_letter_sink)
```

*The watermark absorbs jitter, `allowed_lateness` re-fires for the tardy, and the side output feeds the dead letter queue.*

| Trick: 3 bands of lateness, 3 dials |
| --- |
| The watermark decides when a window fires. `allowed_lateness` decides how long a fired window still accepts corrections. `side_output_late_data` catches only what falls past both. Each dial covers its own band: seconds of jitter, minutes of tardiness, and the rare event that is hours late. |

| ✕ Processing-time windows | ✓ Event-time windows plus DLQ |
| --- | --- |
| Session assignment depends on ingestion lag. A Kafka backlog splits or merges sessions at random, and no event is ever recognized as late. | Assignment follows `event_time_ms`, so a click always lands in its true session. Lateness has a precise definition, and the truly late events go to a queue instead of being dropped. |

| Pitfall: A huge `allowed_lateness` is not a DLQ |
| --- |
| Some candidates set `allowed_lateness` to many hours so nothing is ever late. That pins state for every keyed window across the whole horizon, so RocksDB balloons and checkpoints crawl on a high-cardinality `session_key`. Keep the window open for minutes and send the hours-late events to the side output, where they cost no window state. |

| Interviewer: Name who drains the dead letters |
| --- |
| Weak answers route stragglers to `dead_letter_sink` and stop there. A senior answer names the consumer: a correction job replays the queue, re-aggregates the affected sessions, and upserts by `session_key`, so a rerun cannot double-count. |

| Real world: A rising DLQ rate is an ingestion alarm |
| --- |
| Stragglers are rarely random. Usually it is a phone that buffered events offline, or a regional mirror that fell hours behind. If you alert on the dead-letter rate, you see the upstream problem before the dashboards visibly go wrong. |

**Follow-up questions**

1. How do you choose the watermark bound and `allowed_lateness` without guessing?\
   *Checks whether they measure the actual out-of-order distribution instead of picking round numbers.*
2. The correction job rewrites a session already in `session_metrics`. How do you avoid double-counting?\
   *Idempotent upserts versus at-least-once delivery.*
3. One Kafka partition goes silent and the watermark stalls. What breaks, and how do you detect it?\
   *Watermark idleness handling.*

</details>


<br>

---

## DLQ Patterns

### Classify before you route

A dead letter queue holds records the pipeline has given up on. Which failures belong there depends on a single test: would waiting change the outcome? A database timeout will probably succeed in a minute. A record with a malformed timestamp will fail identically forever. Handling both the same way either floods the DLQ with recoverable records or retries poison records until they jam the partition.

| Class | Example | Retry helps? | Route |
| --- | --- | --- | --- |
| Transient | Pricing store returns 503 or times out | Yes, within seconds to minutes | Short backoff, then retry topics |
| Dependency not ready | `driver_id` 880 missing from the driver lookup | Yes, once upstream catches up | Delayed retry topic |
| Permanent data error | Unparseable `event_time`, missing `city_id` | No | DLQ at once |
| Code defect | Valid record throws after a deploy | Only after a fix | DLQ, tagged with app version |

The 4th class is the hard one. A parser bug looks like a permanent data error from inside the consumer, yet the records are fine and the code is wrong. Tagging every DLQ record with the application version that failed it separates the two later: a burst from a single version, starting at a deploy timestamp, is a defect.


<br>

### Retry topics keep partitions moving

Retrying in place, by sleeping inside the consumer, blocks the partition: nothing behind the failing record moves. With 5 attempts and a backoff that doubles from 1 second up to 16, partition 7 stalls for 31 seconds, and at 2,000 records per second that is 62,000 records of lag behind a single bad record. Retry topics move the record aside instead: the consumer publishes it to a delay topic and commits its offset, so the partition keeps moving. A retry consumer reads each delay topic and processes a record only once its due time has passed.

![Pipeline diagram 2](images/pipeline_streaming_systems_intermediate_diagram_2.png)

The cost is ordering. If trip 5521's DriverAssigned event sits in a retry topic, its TripStarted can be processed first. Per-minute counts do not care. A projection that folds each trip's events in sequence does, so its consumer must also park later events for any key with a record in retry, by tracking a set of blocked keys and routing their new events to the retry path.


<br>

### The envelope makes reprocessing possible

A DLQ record holding only the payload is useless a week later: nobody knows which partition it came from, which code failed it, or how often it was tried. Keep the original bytes untouched, because a record that failed to parse cannot be safely re-serialized, and wrap them in metadata. The payload below decodes to `{"trip_id":5521}` with no `city_id`, so it reached the DLQ on its first attempt.

```sql
{
  "source_topic": "ride_events",
  "source_partition": 7,
  "source_offset": 1041,
  "key": "5521",
  "payload_base64": "eyJ0cmlwX2lkIjo1NTIxfQ==",
  "error_class": "PERMANENT_DATA",
  "error_message": "missing required field city_id",
  "app_version": "surge-pricer 4.17.2",
  "attempts": 1,
  "first_failed_at": "2026-10-24T12:00:59Z",
  "redrive_count": 0
}
```


<br>

### Redrive without making it worse

Say surge-pricer 4.18.0 ships a parser that rejects a new optional field, and 40,000 valid events reach the DLQ as code defects within 20 minutes. A summary over the DLQ table shows the incident's shape: 1 error class, 1 version, 1 start time.

```sql
SELECT
  error_class,
  app_version,
  COUNT(*) AS failed_events,
  MIN(first_failed_at) AS first_seen
FROM ride_events_dlq
WHERE first_failed_at >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY error_class, app_version
ORDER BY failed_events DESC
```

Redrive is a deliberate job, run once 4.18.1 is deployed. It filters the DLQ by error class and app version within a time range, republishes the original bytes to `ride_events` with `redrive_count` incremented, and leaves everything else untouched. Its safety depends on 3 properties. The main consumer must drop duplicates on the producer's `event_id` before counting, because a redrive job that crashes halfway is rerun and republishes part of its batch at new offsets, and an upsert of a count that already includes them stores the doubled number. Redrive must be rate limited, since 40,000 records arriving at once on partitions already carrying 2,000 per second trip the lag alert and delay live traffic. And a cap on `redrive_count`, 3 here, stops a record that still fails from cycling forever.

Redriven events are hours old, so the windows they belong to closed long ago and the watermark treats them as late. They take the side output from the windowing section into the hourly reconciliation, which is how the 12:00 counts finally include them.

| ✓ Do | ✗ Don't |
| --- | --- |
| • Classify errors in code and store the class on every DLQ record<br><br>• Keep the original bytes next to the topic-partition and offset they came from<br><br>• Alert on DLQ rate as a share of input, per error class<br><br>• Redrive by filter after a fix, rate limited, with a redrive cap | • Retry permanent data errors, which fail the same way every time<br><br>• Sleep in the consumer for long backoffs and block the partition<br><br>• Redrive the whole DLQ blindly after every deploy<br><br>• Let the DLQ grow with no owner and no retention policy |


<br>

---

## Spark Streaming vs Flink

The 2 engines in this section's title are the best-known examples of 2 execution models, and the interview question is about the models. Micro-batch cuts the stream into small batches and runs each as a short batch job. Continuous dataflow deploys long-running operators that pass records along as they arrive. Most tradeoffs follow from that difference.


<br>

### How each model runs the surge job

| ✓ Micro-batch | ✗ Continuous dataflow |
| --- | --- |
| • A coordinator plans a batch every trigger interval, say 5 seconds<br><br>• Each batch reads a fixed offset range from all 12 partitions<br><br>• Each batch writes its output before checkpointing offsets and state<br><br>• The watermark advances once per batch | • Operators are deployed once and run until the job stops<br><br>• Each record flows from source to sink as soon as it arrives<br><br>• Keyed state lives inside operators, often in a local embedded store<br><br>• Checkpoint barriers travel with the data to snapshot every operator consistently |


<br>

### Latency: where the floor comes from

A micro-batch job cannot emit a result until its batch commits, and the batch first has to be planned and run. End-to-end latency is the trigger interval plus processing time, which usually comes to a second or more. A continuous job can emit within milliseconds of a record arriving, limited by network hops and the sink. A fraud check that must answer within 200 milliseconds, before a ride is confirmed, rules micro-batch out.

For surge pricing the engine barely matters, and saying so is the strongest point you can make. With 1-minute tumbling windows and a 10 second watermark bound, no count leaves either engine until about 10 seconds after its minute ends, and a 5 second trigger adds at most another 5. The window and the watermark set the latency floor before the engine does, so check them first.

- **15 s** — worst surge count delay, micro-batch


<br>

### Timers and stateful logic

Both models support event-time windows and watermarks. They differ beyond built-in windows. The continuous model exposes keyed state and per-key timers directly: a function holds trip 5521's state, registers a timer for 15 minutes after TripRequested, and flags the trip if no DriverAssigned has arrived when it fires. The micro-batch model offers arbitrary stateful functions too, but they run per batch, so a timeout is noticed at the first batch after it falls due, and state is checkpointed every batch. Logic built from many timers and event patterns per key fits the continuous model more naturally.


<br>

### Exactly-once and back-pressure

Each model reaches an exactly-once effect with the patterns from the first section. Micro-batch uses a replayable source, a deterministic batch tied to a batch id, and a sink idempotent per batch id, so a rerun batch overwrites its own output. Continuous dataflow uses barrier checkpoints for internal state and a sink that pre-commits at each checkpoint and commits when the checkpoint completes, a 2-phase commit. Overload looks different too. A micro-batch job shows it as batches outlasting the trigger interval, so batches queue up behind each other and latency climbs. A continuous job pushes back-pressure upstream operator by operator until the source slows its reads and lag builds in the log.

| Requirement | Leans toward | Why |
| --- | --- | --- |
| Answer within 1 s per event | Continuous | No batch planning between arrival and result |
| Per-key timers, event patterns | Continuous | Timers and keyed state are first-class |
| Team already runs batch jobs on the same engine | Micro-batch | Shared API and code for batch and stream |
| Freshness of 1 minute or looser | Either | Window and watermark dominate latency |
| High throughput, relaxed latency | Micro-batch | Larger batches spread per-batch overhead |
| Small team, no streaming specialists | Micro-batch | Failures resemble the batch jobs they already debug |


<br>

### Answering the comparison question

A strong answer opens with requirements: the latency budget, whether logic needs per-key timers, the expected state size, and what the team already operates. Then it names the model those requirements point to and what that choice costs. "I'd pick the continuous engine because it's faster" is weak. "Our budget is 200 milliseconds and each trip needs a timeout, so continuous dataflow; we accept a codebase separate from our batch jobs and the work of upgrading stateful jobs safely" is strong.

> Follow-ups to expect
>
> - "What happens to state when the job's code changes?" Snapshot, migrate the state schema, restore.
> - "How do you backfill a day?" Replay the log from an offset, or run the same logic over a bounded range.
> - "What breaks first at 10x traffic?" The 12 partitions cap parallelism before either engine does.


<br>

---

## Putting It All Together

A food delivery platform streams order events (OrderPlaced, CourierAssigned, PickedUp, Delivered, Refunded) into a 24-partition topic keyed by order_id. Operations wants an alert within seconds when an order stalls, finance wants every refund reconstructable as of any past day, and a dashboard shows orders per restaurant per 5 minutes.

- The order log is event-sourced: refunds are adjustment events, and finance's as-of report is the fold cut off at a timestamp.
- Each read model (alerts, dashboard, finance table) is a projection with its own consumer group and offsets, rebuilt by replaying from offset 0.
- The dashboard's 5-minute windows take their watermark bound from measured courier-app lateness, with a side output feeding finance's hourly reconciliation.
- A stall alert needs a per-order timer 10 minutes after CourierAssigned with no PickedUp, which points to the continuous model for that job.
- A courier-app release that breaks parsing sends events to the DLQ as defects tagged with its version; redrive after the fix is safe because every projection upserts by order_id and event_seq, and the replayed events take the late path into reconciliation.


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

What does a usage-metering pipeline need so that growing its consumer group on the 24-partition API call topic, or a consumer dying mid-batch, never bills a call twice or drops one? The billing engineer on call wants the per-customer invoice line items it writes to survive exactly that, keep flowing past a malformed call without losing it from the bill, and stay recomputable from the original calls when a customer disputes an invoice months later.

<details>
<summary><strong>Solution</strong></summary>

#### What this problem really is

This is the consumer-group offset lifecycle wearing a finance hat. Every consumer in the group reads a batch, writes it, then commits, and a crash or rebalance can land between any 2 of those steps. The design question is which gap you can afford. Commit before the write and a crash drops calls you already acknowledged: silent under-billing. Commit after the write and a crash replays them, which is correct only if replaying a call rewrites the same line item instead of adding another. Anyone can say 'exactly-once'. The candidates who pass say where the commit sits and what makes the write safe to repeat, and the answer is the call's `request_id` as the line item's key.

| Trick: Commit last, write by key |
| --- |
| At-least-once delivery plus an idempotent sink is exactly-once in effect.<br><br>1. Read, write the line items, then commit the offset. A crash can only cause a replay, never a gap.<br>2. Key each line item on `request_id` and upsert, so the replay is a no-op.<br>3. Park bad records in a DLQ and keep the raw calls somewhere that outlives the topic. |

#### Walk the requirements

1. **Commit the offset only after the line items land**

   Adding a consumer triggers a rebalance: partitions move, and the new owner resumes from the last committed offset, not from where the old owner actually stopped. Whatever the old owner wrote after its last commit gets processed again. So the order is read, write, commit. Flink snapshots the source offsets in the same checkpoint as its operator state; Spark Structured Streaming logs a micro-batch's offsets before running it and marks the batch committed after the sink finishes. Either engine replays the uncommitted tail after a failure, and with invoices closing daily, micro-batch latency is no obstacle. Neither engine can stop your sink from counting that tail twice. That part is yours.

2. **Key every line item on `request_id`**

   The gateway stamps each call with a `request_id`, and a retried call reuses it. Make that the primary key of the line-item row and write with an upsert: a replayed batch rewrites rows that already exist, identically. The same key absorbs duplicates that reach the topic before any consumer sees them, when the gateway re-sends after a timeout. The per-customer total is computed at invoice close by summing those rows, never kept as a running counter the stream increments.

3. **Park malformed calls without skipping them**

   A record that fails to parse fails on every retry. If the processor throws, it never commits, the partition stalls, and every customer on that partition stops being metered. Send it to `dlq` with the original payload, the error, the retry count and the timestamp, then keep going. Skipping it is the opposite failure: that call never reaches the bill. Once the parser is fixed, the DLQ is replayed through the same upsert path, so reprocessing a call twice is harmless too.

4. **Keep the raw calls longer than the topic does**

   Disputes arrive up to 90 days later and the topic keeps seven. Land the raw calls in `raw_call_archive`, an object store written straight off the topic as an immutable log, untouched by pricing logic. A disputed invoice is then a replay: read that customer's calls for the period, price them again, and diff against what was billed. Line items are a projection of the calls; the calls are the record.

#### The shape that fits

```mermaid
flowchart LR
    subgraph L0["Ingest"]
        n0[("api_call_events<br/>(Kafka)")]
        n1[("raw_call_archive<br/>(object storage)")]
    end
    subgraph L1["Metering"]
        n2["metering_stream<br/>(Flink)<br/>Partitions: 24 tasks · On error: dlq"]
        n3[/"dlq<br/>(Kafka)"/]
    end
    subgraph L2["Serve"]
        n4[("line_item_store<br/>(PostgreSQL)<br/>Idempotency: upsert")]
        n5(["invoicing<br/>(API)"])
    end
    n0 --> n2
    n0 --> n1
    n2 --> n4
    n2 --> n3
    n4 --> n5
```

*`api_call_events` feeds `metering_stream`, which upserts line items keyed on `request_id` and commits offsets only after the write; bad records go to `dlq`, and `raw_call_archive` keeps the calls in object storage for dispute replay.*

| ✕ Commit, then write | ✓ Write, then commit |
| --- | --- |
| A crash after the commit but before the write loses the batch: the new owner starts past it. Calls vanish from the bill and nothing ever replays them. | A crash after the write but before the commit replays the batch. With line items upserted on `request_id`, the replay rewrites the same rows, so the bill comes out exactly once. |

| Performance: A rebalance costs replay, not correctness |
| --- |
| At about 300 million calls a day across 24 partitions, each partition carries roughly 145 calls a second. With a 10-second commit interval, a rebalance replays around 1,500 calls per moved partition, all of them no-op upserts. Commit more often and you pay more commit traffic; commit less and each rebalance replays more. Neither choice changes the bill. Past 24 consumers, extra instances sit idle: a partition has exactly one owner in a group. |

| Interviewer: Where the commit sits is the whole answer |
| --- |
| Strong candidates say the sequence out loud: read, write keyed on `request_id`, commit, then name what a crash does at each gap. They also know a group that rebalances every few seconds (usually a consumer blowing its poll interval on a slow write) is a symptom to fix, not load to scale through. Saying 'Kafka has exactly-once' without naming the sink is the tell of someone who has never run billing. |

| Pitfall: A running total cannot be replayed |
| --- |
| The version that ships first keeps a counter per customer and increments it for every call. It passes every test until the first rebalance, when the replayed tail increments the same counters again and every customer on a moved partition is overcharged by a few hundred calls. Nothing errors. Finance finds it at month end when invoices disagree with the gateway's own logs. Store the calls, key them, and sum at close. |

**Follow-up questions**

1. The gateway team wants to pack several calls into one event to cut volume. What breaks in this design?\
   *Tests whether the candidate keeps the key at call grain: the processor must unpack the event and key each call by its own `request_id`, or one bad call sends the whole event to `dlq` and its good calls go unbilled.*
2. Pricing changes mid-month and finance wants the month rebilled at the new rate. How do you do it without double-counting?\
   *Tests the event-log view: replay `raw_call_archive` for the period into a fresh version of the line items and swap it in, rather than pushing old calls back through the live topic.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

Our windowed totals from the 16-partition event topic come out doubled whenever a processor restarts, and last week one malformed event stalled its partition for an hour. We need consumption spread across a group of streaming processors that window on event time with watermarks and land output in a transactional warehouse, bad events parked in a DLQ backed by cold storage, and a dashboard showing consumer lag and DLQ depth.

**Starting canvas** (already placed for you):

- Topic (16 partitions) (source, Kafka)
- Stream processor group (transform, Flink)

<details>
<summary><strong>Solution</strong></summary>

#### What you actually just drew

Strip the marketing off and this is the horizontal-scaling problem for a stateful stream consumer: how do you add a second processor to a consumer group reading the same topic **without double-counting a window or losing one on a rebalance**. Anyone can drop a second Flink box on the canvas and draw an edge from the topic. What separates people who have run this in production is knowing that the second processor is only safe because the group commits offsets and the sink is transactional. Get those two wrong and a rebalance replays a partition, your windowed totals get counted twice, and the dashboard that was supposed to catch it is reading the same corrupted numbers.

```mermaid
flowchart LR
    subgraph L0["Ingest"]
        n0[("Topic (16 partitions)<br/>(Kafka)")]
    end
    subgraph L1["Process"]
        n1["Stream processor A<br/>(Flink)"]
        n2["Stream processor B<br/>(Flink)"]
    end
    subgraph L2["Serve"]
        n3[("Warehouse sink<br/>(warehouse)")]
        n4[("DLQ cold storage<br/>(object storage)")]
        n5(["Dashboard<br/>(dashboard)"])
    end
    n0 --> n1
    n0 --> n2
    n1 --> n3
    n2 --> n3
    n1 --> n4
    n2 --> n4
    n0 --> n5
    n4 --> n5
```

*The consumer group splits the 16 partitions across A and B. Both write windowed output to the transactional warehouse and send bad events to the cold-storage DLQ. The dashboard reads consumer lag off the topic and DLQ depth off the store.*

#### Why a group of two, not one bigger box

Partitions are the unit of parallelism. A consumer group hands each of the 16 partitions to exactly one member, so two processors split them 8/8, and you can keep adding members up to 16 before partitions become the ceiling. A single fat processor can't parallelize past its own cores and gives you zero failover: when it dies, everything stops. The group model is what lets you scale reads and survive a node loss at the same time.

| Interviewer: Processor count is bounded by partition count |
| --- |
| The tell of someone who has operated this: they tie the number of processors to the 16 partitions, not to a round number. A 17th member just sits idle. If you want more parallelism than 16, you repartition the topic first, and that is a real, disruptive operation, not a slider. |

1. **Add processor B to the same consumer group**

   B must share the group id so the coordinator assigns it a disjoint set of partitions. Put it in a different group and it gets all 16: a second full copy of your pipeline, not more throughput. Because offsets are committed per group, a rebalance re-hands a partition to whoever picks it up starting at the last committed offset, so no event is skipped.

2. **Land windows in a transactional warehouse sink**

   Exactly-once means the offset commit and the row write happen as one atomic unit, either through two-phase commit or a `MERGE` keyed on the window id. That is also your idempotency: a replayed partition upserts the same window key instead of appending a duplicate. Most people draw this edge carelessly, and it is where the double-count bug lives.

3. **Route bad events to a cold-storage DLQ**

   A parse or schema failure must not block its partition, because one poison event stuck at the head stalls every event behind it. Divert it to a dead letter store on cheap cold storage, with the error and retry count alongside, for later replay and audit. The rule: never silently drop a bad record, and never let one halt the stream.

4. **Feed a dashboard consumer**

   Monitoring reads two numbers: consumer lag (topic head minus committed offset, per partition) and DLQ depth. Lag climbing steadily means you are under-provisioned, so add a member. DLQ depth spiking almost always means upstream schema drift. Without this view you are blind to the two failure modes this whole design exists to contain.

| Pitfall: At-least-once delivery plus an appending sink double-counts |
| --- |
| Kafka gives you at-least-once by default: on a rebalance or replay, a partition's events are re-delivered from the last commit. If your sink just appends windowed rows, every replay inflates the totals. Committing more often does not fix this. Make the write idempotent (upsert on the window key) or transactional so the replay is a no-op. |

| Key insight: Watermarks bound both correctness and memory |
| --- |
| Event-time windows use watermarks to decide when a window is complete and can flush. That is what makes late data explicit: events past the allowed lateness go to a side output or the DLQ instead of silently corrupting a closed window. It is also your memory story, because the watermark is what lets the processor release window state. Skip it and window state grows unbounded until the processor runs out of memory. |

| ✕ True streaming | ✓ Micro-batch |
| --- | --- |
| Per-event processing (Flink here) commits on a continuous checkpoint boundary at the lowest latency, and the transactional sink ties each checkpoint to an offset commit. This is the right default when lag and DLQ depth must be watched in real time. | Micro-batch groups events into small intervals before writing. Exactly-once is simpler (one transaction per batch), but it adds latency equal to the batch interval. It is fine when the warehouse prefers larger commits and seconds of freshness are acceptable. |

**Follow-up questions**

1. A processor crashes mid-window and the group rebalances. Walk me through why the reassigned partition does not double-count.\
   *Tests whether they connect committed offsets to the transactional or idempotent sink, rather than just reciting 'consumer group'.*
2. Consumer lag is flat but DLQ depth is climbing fast. What is your first hypothesis?\
   *Separates lag (a throughput signal) from DLQ depth (a data quality or schema drift signal).*
3. The topic is repartitioned from 16 to 32. What breaks for a keyed window, and how do you replay safely?\
   *Probes whether they see that key-to-partition mapping and window state are coupled to partition count.*

</details>


<br>

---

## Key Takeaways

- Delivery guarantees come from where the offset commit sits relative to the side effect; an exactly-once effect is at-least-once delivery plus an idempotent or transactional sink.
- Event sourcing makes the log the system of record and state a fold over it; CQRS splits write and read models, and the price is staleness plus event versioning for the log's lifetime.
- Pick the watermark bound from measured lateness, decide separately what late events do, and remember that window state lives for window size plus bound plus allowed lateness.
- Route failures by whether waiting helps. Transient ones go through retry topics, while permanent errors and defects go straight to the DLQ, where a filtered redrive later sends them back into idempotent consumers.
- Choose an engine by naming the requirement first; at 1-minute windows the watermark sets latency before the engine does.
