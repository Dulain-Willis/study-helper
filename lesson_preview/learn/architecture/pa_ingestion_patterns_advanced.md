Advanced·Pipeline Architecture · 38 min · 11 challenges

# Ingestion Patterns: Advanced

Noon on Black Friday at Shopify: a celebrity sneaker drop opens and orders pour into a shared database. The event pipeline must copy those orders out without slowing checkout, hand every order to shipping even if the broker blinks, swallow a surge it can't stop, and keep every other merchant's data flowing while a single storefront eats the capacity.


<br>

---

## Reading Live Databases Safely

Every query an extract sends to a production database competes with the application the database serves. Checkout wants a few milliseconds per query and a cache full of hot rows, while a bulk extract wants every page of a big table once. Left alone, the extract wins because it asks for more, so a safe reader is built to lose.


<br>

### Four ways a reader hurts its source

| Mechanism | What the extract does | What the application feels |
| --- | --- | --- |
| Held snapshot | Reads a 400M-row table inside 1 transaction for 6 hours | Old row versions pile up, so every query reads more pages |
| Cache eviction | Pulls the cold pages of a full scan into memory | Hot rows get pushed out and are read from disk again |
| Disk and CPU contention | Reads sequentially at full speed | Every query sharing those disks waits longer |
| Locks and connections | Takes a table lock to dump, or opens 32 parallel connections | Writes queue behind the lock, or the app's connection pool runs dry |

Databases that keep multiple row versions leave each updated row's old version in place until no open transaction could still read it, so a 6-hour transaction makes every busy table carry 6 hours of dead versions. Live queries then scan more pages for the same rows, and latency climbs while CPU graphs look normal.


<br>

### Read from a replica, in short transactions

A read replica takes cache and CPU contention off the primary, making it the first move for any large extract. But a replica constantly replays the primary's changes, and a long query can hold rows a replayed change must remove, so the replica cancels the query or pauses replay and falls behind. Reporting the replica's open snapshots to the primary stops the cancellations by moving the old-version pileup back onto the primary. Either way, the extract must run as many short transactions.

#### Keyset chunks instead of OFFSET

Paging with OFFSET makes the database read and discard every row before the page it returns, so each page costs more than the last. Keyset paging starts each chunk after the last key it read: every chunk is an index range read of 10,000 rows in its own short transaction, and the reader can stop or resume between chunks. On a 400M-row orders table read 10,000 rows at a time, the totals compare like this:

- **8 trillion** — rows touched by OFFSET paging
- **20,000x** — OFFSET's work relative to keyset

```sql
SELECT
  order_id,
  customer_id,
  status,
  total_cents,
  updated_at
FROM orders
WHERE order_id > 184020000
ORDER BY order_id
LIMIT 10000
```


<br>

### Throttle on the application's own signals

A fixed pause between chunks is a guess that fails on every busy day. One workable rule instead: start at 5 chunks per second; when replica lag passes 10 seconds or checkout's p99 latency passes 200 ms, halve the rate; after a full minute with both healthy, add 1 chunk per second. Then add limits the throttle can never raise, such as 2 connections and a 30-second statement timeout, so a bad query plan dies in seconds.


<br>

### A consistent snapshot without a table lock

Starting log-based CDC on an existing table needs a full copy plus every change after it. The old recipe locks the table, records the log position and dumps the rows, blocking writes until the dump finishes. The watermark recipe keeps the log reader running and copies in keyset chunks, bracketing each chunk with 2 marker rows written to a small dedicated table: a low watermark before the SELECT, a high watermark after it. These are log positions, unrelated to event-time watermarks. Any key the log shows changing between the 2 markers is taken from the log and dropped from the chunk, since the log version is at least as new.

| Step | Reader action | Emitted downstream |
| --- | --- | --- |
| 1 | Insert low watermark L | Nothing |
| 2 | SELECT order_id 5001 to 5003; all 3 read as placed | Nothing yet |
| 3 | Insert high watermark H | Nothing |
| 4 | Read the log from L to H: order 5002 changed to shipped | 5002 shipped, from the log |
| 5 | Reach H in the log and drop 5002 from the chunk | 5001 placed, 5003 placed |

**A nightly extract on a replica pages through orders with OFFSET in 1 transaction lasting 5 hours. The replica kept canceling it, so the team turned on snapshot reporting to the primary. It now finishes, but checkout latency on the primary creeps up every night. What fixes the cause?**

- **A.** Turn reporting off again and give the replica more memory, so the long query stops getting canceled.
- **B.** Rewrite the extract as keyset chunks in short transactions, so no snapshot stays open for hours anywhere.
- **C.** Keep the single transaction and switch OFFSET to keyset paging, so each page reads far fewer rows.
- **D.** Move the extract back to the primary, where no replay happens and no query gets canceled.

<details>
<summary>Solution</summary>

**B.** Rewrite the extract as keyset chunks in short transactions, so no snapshot stays open for hours anywhere.

Reporting made the primary keep old versions for the 5-hour snapshot. Short chunks release each snapshot in seconds.

**Why the others are wrong**

- **A.** More memory leaves the replay conflict in place, so the cancellations come back.
- **C.** Keyset cuts scan work, but the 5-hour snapshot still pins old versions on the primary.
- **D.** The 5-hour snapshot then bloats the primary directly, and its reads compete with checkout for cache and disk.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

An order handler writes each order to Postgres and publishes to Kafka in the same function; about 0.4% of orders get no event, and rolled-back orders still trigger notifications. Redesign it so the handler commits to Postgres alone and each committed order is captured off the database and published to Kafka, with no distributed transaction. The recommendation and notification consumers keep reading Kafka and react to each event as it lands, not on a schedule.

**Starting canvas** (already placed for you):

- Order service handler (writes Postgres and publishes Kafka in one function) (source, API)
- Postgres orders database (storage, PostgreSQL)
- Kafka order_events topic (queue, Kafka)
- Recommendation service consumer (consumer, API)
- Customer notification service consumer (consumer, Email)

<details>
<summary><strong>Solution</strong></summary>

#### 2 commits, no shared transaction

A **dual write** is 2 independent commits in one handler: the order row to Postgres, then the event to Kafka. Every line can be correct and the pair is still not atomic, so 4 outcomes exist. Both succeed, or both fail, and nothing breaks. Postgres commits and the publish fails: an order with no event, the 0.4% that never reach the notification service. The publish succeeds and Postgres rolls back: an event for an order that does not exist, which is how customers get emails for shipments that never happened.

| ✕ Database committed, publish failed | ✓ Published, database rolled back |
| --- | --- |
| The order exists but no event was sent. Recommendations and notifications never hear about it, and nothing retries it later. | The event names an order id that is not in Postgres. Consumers act on a phantom order they cannot look up. |

```mermaid
flowchart LR
    n0[("Order service handler<br/>(API)")]
    n1[("Postgres orders database<br/>(PostgreSQL)")]
    n2["Change data capture publisher<br/>(transform)"]
    n3[/"Kafka order events<br/>(Kafka)"/]
    n4(["Recommendation consumer<br/>(API)"])
    n5(["Notification consumer<br/>(Email)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n3 --> n5
```

*The handler commits once, to Postgres. Change data capture turns each committed order into a Kafka event that triggers both consumers.*

The fix removes the second commit instead of trying to coordinate it. The handler writes Postgres and stops there. A change data capture stage reads committed changes from the database log, so it only ever sees rows that survived the commit, and publishes each one to Kafka. A rolled-back order never reaches the log. A committed order cannot be skipped, because the log is the record of the commit.

| Pitfall: A distributed transaction moves the risk instead of removing it |
| --- |
| Two-phase commit across Postgres and Kafka needs a coordinator that holds locks until both sides vote, so a stalled broker blocks order writes. Kafka's own transactions only cover writes inside Kafka, so they cannot include the Postgres commit. |

| Key insight: Consumers wake on the event, not the clock |
| --- |
| Both services stay event driven: each committed order arrives as a Kafka message, and that message triggers the work. A scheduled job that compares Postgres against Kafka could find the missing events, but only hours after the customer missed the email. |

**Follow-up questions**

1. If the change capture stage restarts mid-stream, could a consumer see the same order twice, and what makes that harmless?\
   *Extends the design to redelivery and to consumers that are idempotent on the order id.*
2. Which of the 4 outcomes would a nightly reconciliation job catch, and how late would it catch them?\
   *Contrasts event-triggered delivery with a schedule that only detects the gaps after the fact.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

Each order is written to Postgres and its order_placed event published to Kafka from one handler, so one order in 250 commits with no event and rollbacks leave phantom events. Replace the dual write with a transactional outbox: the handler writes the order and an outbox row in one transaction, and a separate publisher, named for whether it polls the table or tails the change log, drains it to Kafka. The consumer must ignore events resent after a publisher crash.

**Starting canvas** (already placed for you):

- Order service handler (currently dual-writes; producing ghost orders) (source, API)
- Postgres orders database (storage, PostgreSQL)
- Kafka order_events topic (queue, Kafka)
- Recommendation service consumer (consumer, API)

<details>
<summary><strong>Solution</strong></summary>

#### Why the two writes drift

The fix is the **transactional outbox** pattern. Instead of writing the order to Postgres and publishing to Kafka as two independent calls, the handler writes two rows to one database in a single transaction: the order into `orders`, and its event into an `outbox` table. They commit or roll back together, so a committed order always carries its event and a rolled-back one leaves no phantom. A separate publisher drains `outbox` to `order_events` afterward.

| Pitfall: Two systems cannot share one commit |
| --- |
| The original handler commits Postgres, then calls `produce()` on Kafka. Nothing binds those calls: a crash or broker timeout after the commit drops the event (your 1-in-250 gap), and producing before the commit and then rolling back leaves an event for an order that never existed. Reordering them just moves the failure; only folding the event into the order's transaction removes it. |

```mermaid
flowchart LR
    n0[("Order service handler<br/>(API)")]
    n1[("Postgres orders and outbox tables<br/>(PostgreSQL)")]
    n2["Outbox polling publisher<br/>(transform)"]
    n3[/"Kafka order events topic<br/>(Kafka)"/]
    n4(["Recommendation service consumer<br/>(API)<br/>Idempotency: upsert"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
```

*The handler's only write is one Postgres transaction; the polling publisher is the sole path to Kafka, and the consumer upserts on `event_id` so a resent event changes nothing.*

| ✕ Polling publisher | ✓ CDC tailer |
| --- | --- |
| An `outbox-poller` runs `SELECT ... FROM outbox WHERE published_at IS NULL ORDER BY id` on an interval, produces each row, then stamps it published. No new infrastructure; the cost is poll latency and query load. The name says it reads the table. | A `cdc-relay` (Debezium) tails the Postgres write-ahead log and emits the instant the `outbox` insert is durable. Lower latency, no polling load, but now you operate a connector. The name says it reads the change log. |

| Real world: The relay is at-least-once, so dedupe downstream |
| --- |
| Either publisher can crash between producing to Kafka and marking the row published, so the same event ships twice. Make that harmless: give each outbox row a stable `event_id` and have the recommendation consumer upsert on it, skipping repeats. At-least-once delivery plus an idempotent consumer is the standard contract; chasing exactly-once across the broker is not worth it. |

**Follow-up questions**

1. What keeps the `outbox` table from growing without bound?\
   *Expect a retention job that deletes rows safely behind the published watermark.*
2. `order_events` has several partitions, so how do you keep one order's events ordered?\
   *Key the Kafka message by order id so all of an order's events land on the same partition.*

</details>


<br>

---

## The Transactional Outbox Pattern

When an order commits, checkout has 2 jobs: save the order and tell shipping. The database and the broker are separate systems, and no transaction spans both. If checkout commits first and then publishes, a crash or broker outage between the 2 steps loses the event. Publishing first fails the other way: a rollback leaves shipping with an event for an order that doesn't exist.

The transactional outbox moves the second write into the first system. Checkout inserts the event as a row in an outbox table inside the order's own transaction, so both rows commit or neither does. A separate publisher reads committed outbox rows and sends them to the broker, retrying until it accepts. The animation runs 3 orders through both designs during a 2-second broker outage.

checkout writes the order to the database, then publishes the event to the broker0 s

service

checkout

WRITESdb, then broker

Postgres

orders

Kafka

order_events

shipping

shipping

DEDUPE ONid

shipping db

shipments

orders0 rows

order_id

status

records0 records

#

id

order_id

shipments0 rows

order_id

Three orders and a broker blip

1/14

In the dual-write variant, the order at second 3 arrives while the broker is down: its row commits but its publish fails, so shipping never hears of it. In the outbox variant, rows written during the outage wait in the table. Then the publisher crashes after sending 103 and before marking it, so its next run sends 103 again, and shipping dedupes on the outbox id to create 1 shipment. The outbox turns maybe-never into at-least-once; consumer dedupe makes it effectively-once.


<br>

### Designing the outbox table

![Schema diagram 1](images/pa_ingestion_patterns_advanced_diagram_1.png)

| Column | Why it is there |
| --- | --- |
| id | The dedupe key consumers store, and the polling publisher's read order. Never reused. |
| aggregate_id | The broker partition key, so each order's events land in 1 partition, in order. |
| event_type, schema_version | Consumers route and parse by declared type and version; versions change by expand-contract. |
| payload | The event as its data contract defines it, built inside the transaction. Internal columns stay out. |
| published_at | Polling publishers only: NULL until the broker acknowledges the row. |


<br>

### Polling publisher or CDC publisher

| ✓ Polling publisher | ✗ CDC publisher |
| --- | --- |
| • Queries the outbox every few seconds; delay is about the poll interval<br><br>• Adds query load on the primary at every poll, even an empty one<br><br>• Writes published_at per acknowledgement: 1 extra write per event<br><br>• Needs only SQL access | • Reads outbox inserts from the log; delay under a second<br><br>• Adds no query load: the log is written anyway<br><br>• Lets outbox rows be deleted soon after insert<br><br>• Needs a running log reader whose retention and failover you own |

**At peak, a payments service writes 3,000 outbox rows per second on a primary near 70% CPU. Fraud scoring needs each event within 1 second, and log-based CDC already runs for 2 other services. Which publisher fits?**

- **A.** A CDC publisher: no query load on a busy primary, sub-second delay, and the log reader is already operated here.
- **B.** Polling every 200 ms: frequent polls meet the 1-second target, and SQL access avoids operating a log reader.
- **C.** Polling every 5 seconds with 500 rows per batch: fewer queries keep the load on the primary low.
- **D.** CDC on the payments table itself, skipping the outbox, since the log already holds every change.

<details>
<summary>Solution</summary>

**A.** A CDC publisher: no query load on a busy primary, sub-second delay, and the log reader is already operated here.

It meets the 1-second target without adding polls or marking writes to a primary with little headroom.

**Why the others are wrong**

- **B.** It meets the delay, but adds 5 polls and 3,000 published_at writes a second to a busy primary.
- **C.** It drains 100 rows a second against 3,000 arriving, and the interval alone breaks the target.
- **D.** Every consumer then depends on the table's internal columns, so a rename breaks them all.

</details>

#### The gap a bookmark poller falls into

Sequence ids are assigned at insert, but rows become visible at commit. Transaction A takes id 207, transaction B takes 208, and B commits first. A poller that tracks id > last_id sends 208, moves its bookmark to 208, and never sees 207 when A commits a moment later. Polling on published_at IS NULL keeps a late-committing row visible until it is sent:

```sql
SELECT
  id,
  aggregate_id,
  event_type,
  schema_version,
  payload
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT 500
```

> **TIP**
>
> Per-order ordering survives late commits: an order's second event comes from a transaction that updates the same order row, so it waits for the first to commit and takes a higher id.

**This poller runs every 2 seconds, then stores the highest id it sent as the next bookmark. Every order has an outbox row, yet a few orders a month never reach consumers. Why?**

```sql
SELECT
    id,
    aggregate_id,
    payload
FROM outbox
WHERE id > 48211
ORDER BY id
LIMIT 500
```

- **A.** LIMIT 500 cuts each batch short, and rows past the limit are discarded instead of waiting for the next poll.
- **B.** A 2-second interval is shorter than some transactions, so the poller reads rows before they commit and loses them.
- **C.** A transaction holding a lower id committed after a higher id was sent, so the bookmark had already passed it.
- **D.** ORDER BY id ignores created_at, so rows inserted in the same millisecond come back shuffled and get skipped.

<details>
<summary>Solution</summary>

**C.** A transaction holding a lower id committed after a higher id was sent, so the bookmark had already passed it.

Ids are assigned at insert and rows appear at commit. Polling on published_at IS NULL catches late rows.

**Why the others are wrong**

- **A.** Rows past the limit have ids above the new bookmark, so the next poll picks them up.
- **B.** Uncommitted rows are invisible to the poller. The interval only decides when it looks.
- **D.** Ids are unique, so ordering by id is total. Timestamps never decide what gets skipped.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

The producer bursts to 50k events/sec during a flash sale, but the consumer drains only 5k/sec and crashes, losing orders that must not be lost. Buffer the spike with a durable queue between them, named with its depth and what happens to events past it. Replace the consumer with a stream processor that pulls from the queue at its own rate and lands orders within 15 minutes.

**Starting canvas** (already placed for you):

- Producer service (50k events/sec during spike) (source, API)
- Consumer (5k events/sec capacity; crashes on spike) (transform, Pandas)
- Iceberg raw_events landing (storage, Iceberg)

<details>
<summary><strong>Solution</strong></summary>

#### Backpressure: decide where the extra events wait

Backpressure is what a pipeline does when the producer outruns the consumer. At 50k events/sec in and 5k out, 45k/sec have nowhere to go, and with nothing between the 2 stages they pile up in the consumer's memory until it dies. Buffering the spike splits the problem in two: a durable queue holds the surplus, and a stream processor pulls from it continuously at the rate it can sustain. The spike turns into queue depth and lag instead of lost orders.

```mermaid
flowchart LR
    n0[("Producer service<br/>(API)")]
    n1[/"orders buffer (7d retention, producer slowed past depth)<br/>(queue)"/]
    n2["stream consumer (drains 5k per sec)<br/>(Flink)<br/>SLA: #60; 15min"]
    n3[("raw events landing<br/>(Iceberg)")]
    n0 --> n1
    n1 --> n2
    n2 --> n3
```

*The buffer sits inline and absorbs the 10x burst; the stream consumer pulls at its own 5k/sec pace, so the gap becomes lag it works off after the sale.*

| Pitfall: A bigger in-memory queue just relocates the crash |
| --- |
| An unbounded `list` or `asyncio.Queue` inside the consumer is the failure you were handed: memory grows until the host crashes and every in-flight order vanishes on restart. A durable queue keeps each message until the consumer acknowledges it, so a crash only delays those orders instead of losing them. |

| Key insight: The consumer pulls, it is never pushed |
| --- |
| A batch job that wakes up hourly would let the backlog grow for the whole hour and then hit it all at once. A stream processor with a `< 15min` freshness target reads continuously, so it drains the backlog at full capacity the moment the burst eases. Pulling is what makes it safe: the consumer never accepts more than it can process. |

| Real world: The queue's name states its bound |
| --- |
| `orders buffer (7d retention, producer slowed past depth)` tells on-call how much runway exists and what happens past it. Orders must not be lost, so past the bound the producer is slowed rather than the oldest events aged out; for debug telemetry, dropping the oldest would be the right call. |

**Follow-up questions**

1. The spike lasts 30 minutes at 50k/sec while the consumer drains 5k. How deep does the queue get, and how long until the lag is back under 15 minutes?\
   *Sizing depth from the drain deficit instead of by feel.*
2. If freshness had to stay under 1 minute during the sale, what would you change about the stream consumer?\
   *When buffering alone is not enough and the stream processor must scale out.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

A platform pages the HTTP APIs of 3,000 free, pro and enterprise accounts by incremental cursor, and one shared puller pool fetches every page. When the largest enterprise account surges, its pages fill the pool and every other tenant falls behind. Redesign it so pull work waits in a queue per lane, the enterprise tier gets its own lane, pullers back off and retry when an account's rate limit throttles them, and on-call can see pull lag per tier.

**Starting canvas** (already placed for you):

- customer account APIs (source, API)
- cursor scheduler (transform)
- shared API puller pool (transform)
- raw landing (storage, Iceberg)

<details>
<summary><strong>Solution</strong></summary>

#### One lane for the loud tier

Splitting one shared puller pool into lanes is **resource isolation**. Instead of every tenant competing for the same workers, each lane gets its own queue of pull work and its own pullers. An enterprise surge still happens, but it becomes a longer enterprise queue rather than a slower platform.

```mermaid
flowchart LR
    subgraph L0["schedule"]
        n0[("customer account APIs<br/>(API)")]
        n1["cursor scheduler<br/>(transform)"]
    end
    subgraph L1["buffer"]
        n2[/"enterprise pull queue<br/>(queue)"/]
        n3[/"free and pro pull queue<br/>(queue)"/]
    end
    subgraph L2["pull"]
        n4["enterprise pullers with retry backoff<br/>(transform)<br/>Backoff: exponential"]
        n5["free and pro pullers with retry backoff<br/>(transform)<br/>Backoff: exponential"]
    end
    subgraph L3["land and observe"]
        n6[("raw landing<br/>(Iceberg)")]
        n7(["per-tier lag monitoring<br/>(Grafana)"])
    end
    n1 --> n2
    n1 --> n3
    n2 --> n4
    n3 --> n5
    n0 --> n4
    n0 --> n5
    n4 --> n6
    n5 --> n6
    n2 --> n7
    n3 --> n7
```

*The scheduler drops each account's next cursor into its lane's queue. Each lane's pullers page the account APIs into shared raw landing, retrying throttled requests with exponential backoff, and queue lag feeds per-tier monitoring.*

The API is why the shared pool breaks. Each account is paged by cursor and capped by its own rate limit, so a surging account becomes thousands of pages that can only be fetched so fast. When the API throttles a request, the puller waits with exponential backoff and retries the same cursor, so no page is skipped and the account is not hammered. In a shared pool those slow, backing-off jobs hold workers while free and pro jobs wait behind them. With a queue per lane, the enterprise pullers work through that backlog at the account's pace, and free and pro cursors stay current.

| Pitfall: More pullers is not isolation |
| --- |
| Growing the shared pool only raises the ceiling. The next surge fills it again, and enterprise pages still crowd out everything queued behind them. Isolation means a separate queue and separate pullers, so the noisy tier can only use up its own capacity. |

| Real world: Lag per lane is what on-call reads |
| --- |
| Report 2 numbers for each queue: how many pull jobs are waiting, and how far that tier's cursors trail the newest event. On-call then sees the enterprise lane hours behind while free and pro sit near zero, and can scale the enterprise pullers without touching the other lanes. |

**Follow-up questions**

1. A single pro tenant surges inside the shared free and pro lane. How do you stop it from monopolizing that lane without a queue per tenant?\
   *Extends tier isolation to fair scheduling between tenants that share a lane.*
2. Your largest enterprise customer wants guaranteed capacity. When does a dedicated lane per tenant beat a lane per tier?\
   *Weighs stronger isolation against cost as the tenant count grows.*

</details>


<br>

---

## Backpressure at the Ingestion Layer

Backpressure travels upstream from a slow stage to a faster one until it reaches the pipeline's edge. Ingestion is that edge, and the next hop upstream is a system you do not run. When arrivals outpace what ingestion can drain, you have 3 possible responses. Which of them are open to you depends on the source.

- **Buffer** *Absorb now, drain later*\
  Hold the surge in durable storage. Costs freshness and disk, and fails when the surge outlasts the buffer.
- **Signal the producer** *Ask it to slow down*\
  Return 429 with Retry-After. For pull-based sources, stop pulling; for credit-based ones, stop granting credits. Works only if the producer holds data while it waits.
- **Drop selectively** *Discard by rule*\
  Sample, shed low-priority event types, or keep only the latest value per key. Works only where consumers agreed to the loss.


<br>

### Classify the source before choosing

| Source | Can it wait? | Can it replay? | Response |
| --- | --- | --- | --- |
| Partner order webhooks | Yes, retries for a limited window | Within that window | Buffer; return 429 near full |
| Mobile clickstream | No, the app discards what it can't send | No | Buffer, then sample under pressure |
| Internal payment events | Yes, rows wait in its outbox | Yes, from the outbox | Signal; the outbox is the buffer |
| Vehicle location pings | No | No | Keep the latest per vehicle, drop the rest |
| Log-based CDC | Yes, but the database holds its log meanwhile | Within log retention | Drain fast into your own log; slow down after it |


<br>

### Sizing the buffer you depend on

A worked surge: traffic normally runs at 2,000 events per second and workers drain 3,000. A promotion pushes arrivals to 12,000 per second for 15 minutes, with events averaging 1 KB. During the surge the backlog grows by the gap between arrivals and drain; afterwards it shrinks only by the spare drain capacity.

- **8.1 GB** — backlog when the surge ends
- **45 min** — peak freshness lag
- **135 min** — time to drain the backlog

Buffer capacity must cover the longest planned surge, with margin. The peak lag must fit the freshness SLA, and if it does not, a bigger buffer cannot help: the fix is more drain capacity or a drop rule for part of the stream. Recovery time depends on spare capacity, so doubling the workers' headroom halves it.


<br>

### When backpressure reaches the database

Slowing a CDC reader looks harmless because the source can wait, but the database waits by keeping every log segment the reader hasn't confirmed. A reader stalled for 6 hours on a primary writing 20 GB of log an hour pins 120 GB of disk, and a full disk stops the primary taking writes: ingestion has taken down checkout. So the CDC reader never absorbs backpressure: it copies changes into a durable log you own as fast as the database produces them, and every slowdown happens after that log. Track the log bytes retained for the reader, with a ceiling past which you drop it and re-snapshot in chunks.

![Pipeline diagram 2](images/pa_ingestion_patterns_advanced_diagram_2.png)

#### An admission rule that uses all 3

This handler fronts a shared ingestion endpoint. Below 70% fill it accepts everything. Between 70% and 90%, loss-tolerant events are sampled down to 1 in 10, and above 90% they are all dropped. Must-keep events are accepted until 95%, after which producers get 429 with a Retry-After header.

```sql
def admit(event, buffer, dropped):
    fill = buffer.size() / buffer.capacity
    if event.loss_tolerant:
        sampled = event.sample_key % 10 == 0
        if fill < 0.70 or (fill < 0.90 and sampled):
            buffer.put(event)
        else:
            dropped[event.event_type] += 1
        return 202, {}
    if fill < 0.95:
        buffer.put(event)
        return 202, {}
    return 429, {"Retry-After": "30"}
```

Dropped events still get 202, because a producer told to retry something you chose to discard only adds load. Every drop is counted by event type, since an uncounted drop shows up as a fake traffic decline.

**A fleet of 50,000 scooters sends GPS positions every 2 seconds with no retries. The only consumer is a live map of each scooter's current position. The shared buffer nears full during a surge. Which response fits?**

- **A.** Return 429 so scooters back off and resend their positions once the buffer drains.
- **B.** Let the buffer grow without a bound, so no position from any scooter is ever lost.
- **C.** Drop a random 90% of all events on the endpoint, from every source sharing it.
- **D.** Keep the latest position per scooter, drop superseded ones, and count the drops.

<details>
<summary>Solution</summary>

**D.** Keep the latest position per scooter, drop superseded ones, and count the drops.

A live map needs only the newest position, and an older one is worthless once a newer one arrives.

**Why the others are wrong**

- **A.** These devices never resend, so a 429 discards positions anyway and adds a round trip.
- **B.** Unbounded growth becomes a disk or memory outage later, and stale positions are worthless here.
- **C.** Random drops hit must-keep sources too. Dropping must be scoped to sources that tolerate it.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Redesign an order service that dual-writes to Postgres and Kafka so the handler commits each order and its event in one Postgres write to an outbox, and a change-data-capture connector drains that outbox into the Kafka order_events topic read by the warehouse, recommendation, and notification consumers. Route events the connector cannot deliver to a dead-letter queue, and add an alert that pages on-call when drift reappears.

**Starting canvas** (already placed for you):

- Order service handler (currently dual-writes to DB and Kafka) (source, API)
- Postgres orders database (storage, PostgreSQL)
- Kafka order_events topic (queue, Kafka)
- Downstream consumers (warehouse, recommendation, notification) (consumer, API)

<details>
<summary><strong>Solution</strong></summary>

#### One commit, then a log

A **transactional outbox** fixes dual-write drift by turning two writes into one. The handler inserts the order into `orders` and the event it owes into an `outbox` table inside the same Postgres transaction, so both rows commit or neither does. A change-data-capture connector then tails the write-ahead log and publishes each committed `outbox` row to `order_events`. The handler never talks to Kafka at all.

| Pitfall: No ordering of two writes is safe |
| --- |
| Commit then `produce()` loses the event when the process dies between them, which is where the silent drops came from. Publish then commit emits a phantom event for an order that rolled back. Swapping the order only changes which bug you get. A retry loop around `produce()` does not help either, because the crash can happen before the retry runs. |

```mermaid
flowchart LR
    n0[("Order service handler<br/>(API)")]
    n1[("Postgres orders + outbox (one transaction)<br/>(PostgreSQL)")]
    n2["CDC connector (Debezium, tails WAL)<br/>(transform)"]
    n3[/"Kafka order events topic<br/>(Kafka)"/]
    n4[/"Dead letter queue<br/>(Kafka)"/]
    n5(["Warehouse, recommendation, notification consumers<br/>(API)"])
    n6(["On-call drift alert<br/>(alerting)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n2 --> n4
    n3 --> n5
    n2 --> n6
    n4 --> n6
```

*The handler's only external write is the Postgres transaction; every hop after it replays a durable log.*

1. **Let the connector drain the outbox**

   The connector commits its log offset only after Kafka acks, so a crash re-reads from the last offset. That is at-least-once delivery: duplicates are possible, losses are not. Carry a generated `event_id` in each `outbox` row so consumers can dedupe a replay.

2. **Park the poison events and page on drift**

   An event that fails after a bounded retry budget goes to the dead letter queue instead of blocking the stream, where it waits to be replayed after a fix. The connector's lag and the queue's depth feed a paging alert, so drift pages on-call in minutes instead of surfacing in the next reconciliation.

| Real world: Reconciliation becomes a smoke alarm |
| --- |
| The same shape gets events out of any service-owned database without a distributed transaction: user signups feeding a CRM, payment state feeding a ledger. The nightly diff can stay, but it no longer carries the weight. |

**Follow-up questions**

1. The notification consumer sees the same `event_id` twice. Where does the dedupe live, and why does it matter more there than in the warehouse loader?\
   *Idempotency belongs at side-effecting consumers.*
2. What prunes `outbox` rows the connector has already published?\
   *An outbox needs a retention path or it grows without bound.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

Migrate a 3,000-account order platform off its dual write: each handler writes Postgres and publishes to Kafka separately, and a flash sale just overwhelmed ingestion. Write the order and an outbox row in one Postgres transaction, and add a CDC source node that publishes committed outbox rows to Kafka. Keep Kafka as the burst buffer, isolate tenants by tier, and add a Grafana dashboard showing per-tenant consumer lag. Give each event a stable id so every sink applies it idempotently. Route the warehouse, recommendation and notification consumers through the outbox-fed topics.

**Starting canvas** (already placed for you):

- Order service handler (currently dual-writes) (source, API)
- Multi-tenant ingest source (3,000 accounts) (source, API)
- Postgres orders database (storage, PostgreSQL)
- Kafka event topics (queue, Kafka)
- Iceberg multi_tenant_raw (storage, Iceberg)
- Three downstream consumers (warehouse, recommendation, notification) (consumer, API)

<details>
<summary><strong>Solution</strong></summary>

#### Why the dual write breaks, and what replaces it

Getting Postgres and Kafka to agree again comes down to the **transactional outbox pattern**. Instead of the order handler doing 2 independent operations, writing the order to Postgres and then publishing to Kafka, it writes the business row and an `outbox` row inside a single Postgres transaction. Because both live in the same database, one commit makes both durable or neither. The event is now a committed fact, and a separate reader ships it to the broker afterward.

| Pitfall: The dual write has no shared commit |
| --- |
| In the legacy handler the Postgres `INSERT` and the Kafka `publish()` are 2 separate calls with no atomic bracket around them. When the broker is down but the row commits, the order exists and no event ever fires. When Postgres rolls back after a successful publish, consumers act on an order that was never persisted. A single-system outage always leaves them disagreeing. |

```mermaid
flowchart LR
    n0[("Order service handler<br/>(API)")]
    n1[("Multi-tenant ingest (3,000 accounts)<br/>(API)")]
    n2[("Postgres orders + outbox<br/>(PostgreSQL)")]
    n3[("Outbox reader<br/>(CDC)")]
    n4[/"Kafka event topics (per tier)<br/>(Kafka)"/]
    n5[("Iceberg multi_tenant_raw<br/>(Iceberg)")]
    n6(["Warehouse<br/>(API)"])
    n7(["Recommendation<br/>(API)"])
    n8(["Notification<br/>(API)"])
    n9(["Per-tenant lag dashboard<br/>(Grafana)"])
    n0 --> n2
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n4 --> n6
    n4 --> n7
    n4 --> n8
    n4 --> n9
```

*Kafka sits downstream of the Postgres transaction boundary: nothing reaches the broker until the outbox row has committed.*

1. **Make the write atomic**

   The handler opens one transaction and writes both the order row and an `outbox` row carrying the event payload and a stable `event_id`. They commit together, so a broker outage can no longer split them. You replace a cross-system publish with a same-database `INSERT`, which Postgres already gives you atomically.

2. **Let a CDC reader carry committed rows to Kafka**

   The CDC node tails the `outbox` table through the write-ahead log and publishes only committed rows to the Kafka topics. It sees an event exactly when the transaction commits, never before. The handler no longer talks to Kafka at all, so its request path cannot fail on a broker hiccup.

3. **Absorb the burst and isolate the tenants**

   Keep Kafka between producers and consumers as the shock absorber: size retention to hold the flash-sale peak, so a slow consumer falls behind inside a bounded buffer instead of dropping events. Partition or route topics by tenant tier so one noisy account fills its own partitions instead of starving everyone. Then point a `Grafana` dashboard at per-tenant consumer lag so rising backlog on any tier is visible before it becomes an outage.

| Key insight: A stable event id turns redelivery into a no-op |
| --- |
| Kafka and CDC both deliver at least once, so every sink will occasionally see the same event twice. Because each `outbox` row carries a stable `event_id`, each consumer applies idempotently, keyed on that id: the warehouse upserts, the recommendation store dedupes, notifications skip an id already sent. A redelivered event converges to the same final state as a single delivery. |

**Follow-up questions**

1. The outbox table grows without bound. Where and how do you prune rows once CDC has published them?\
   *The outbox is a relay buffer, not a permanent log, and needs a cleanup job keyed on what has been published.*
2. What ordering guarantee does each consumer actually get, and what breaks if 2 orders for the same tenant land on different partitions?\
   *Partition-key choice: per-tenant ordering versus throughput, and why the key must match the isolation boundary.*
3. If the CDC reader crashes mid-batch and resumes, how do you keep it from skipping or reordering outbox rows?\
   *Idempotency extended to the relay itself: checkpointed log positions over the outbox.*

</details>


<br>

---

## Multi-Tenant Ingestion Fairness

A multi-tenant ingestion layer runs many customers' data through the same workers and queues and writes it to shared sinks. Sharing keeps hardware busy and cheap, and lets the largest tenant's worst day land on every tenant. Fairness means each tenant's freshness depends on its own load and plan, whatever its neighbors are doing.


<br>

### One resync, 300 tenants waiting

A shared queue serves 301 tenants. At 09:00, tenant acme requests a full resync, and 50M rows enter the queue. The other 300 tenants each add 2,000 rows every 15 minutes, and the workers process 200,000 rows per minute.

| Scheduler | A small tenant's 09:05 batch waits | acme's resync finishes after |
| --- | --- | --- |
| First in, first out | 250 minutes or more | About 250 minutes |
| Fair across tenants | 3 minutes at most | About 313 minutes |

Fairness costs acme about an hour and saves each small tenant more than 4 hours. The small tenants' rows use 3 minutes of every 15, and acme gets the rest, because a fair scheduler is work-conserving: capacity a tenant leaves unused goes to whoever has work.


<br>

### Deficit round robin across tenant queues

Fair scheduling needs a queue per tenant and a scheduler that hands out capacity in rounds. In deficit round robin, each tenant with work earns a quantum of rows per round, scaled by its weight, and spends that credit on its queued batches:

```sql
def schedule_round(queues, weights, deficit, quantum=2000):
    for tenant, queue in queues.items():
        if not queue:
            deficit[tenant] = 0
            continue
        earned = quantum * weights.get(tenant, 1)
        deficit[tenant] = deficit.get(tenant, 0) + earned
        while queue and queue[0].rows <= deficit[tenant]:
            batch = queue.popleft()
            deficit[tenant] -= batch.rows
            yield tenant, batch
```

Weights express plan tiers: a weight-4 tenant gets 4 times a weight-1 tenant's share while both have work. Resetting `deficit` for empty queues stops an idle tenant from banking credit and bursting later. A batch larger than its credit waits extra rounds, so large loads should arrive as keyset chunks.


<br>

### When sharing itself is the risk

A scheduler divides worker time and nothing else: it cannot stop an oversized file from crashing a shared worker, or a single tenant from filling the buffer. Each stronger level of isolation costs more capacity:

| Level | How it works | Protects against | Cost |
| --- | --- | --- | --- |
| Fair scheduling | Per-tenant queues sharing a worker pool by weight | Queue starvation | Lowest; a bad payload can still crash shared workers |
| Per-tenant quotas | A token bucket per tenant on rows or bytes per second at admission | A single tenant flooding the buffer | Tenants over quota wait or get 429 |
| Bulkheads | Separate worker pools per tier or for the largest tenants | Crashes and memory blowups spreading | Idle capacity in every pool |
| Dedicated cell | A full ingestion stack for a single tenant | Every shared resource | Highest; for tenants whose load would dominate any pool |


<br>

### Per-tenant observability

Averages hide the tenant in trouble. With 300 tenants at 2 minutes of lag and a single tenant at 6 hours, mean lag reads around 3.2 minutes against a 15-minute SLA, and the dashboard stays green. Track freshness and dead-letter rate per tenant. Compare each tenant's volume against its own history and alert on its schema changes, then report the worst tenant and the count past SLA:

```sql
SELECT
  tenant_id,
  MAX(lag_seconds) AS worst_lag_seconds,
  SUM(rows_ingested) AS rows_today,
  SUM(rows_dead_lettered) AS rows_dead_lettered
FROM ingest_batches
WHERE batch_date = DATE '2026-10-24'
GROUP BY tenant_id
HAVING MAX(lag_seconds) > 900
ORDER BY worst_lag_seconds DESC
```


<br>

### Carry tenant_id in every record's structure

Ingestion stamps `tenant_id` at admission and rejects records without it. From there it is a required envelope field, part of every table's primary key and a partition column in storage. Recovering the tenant later by joining to an accounts table fails when it matters most: when 2 tenants each have an order 1001, when an account moves between tenants, or when a departing tenant's rows must all be found and deleted.

![Schema diagram 3](images/pa_ingestion_patterns_advanced_diagram_3.png)

> **TIP**
>
> Partition the broker by tenant_id alone and the largest tenant lands in 1 hot partition. Key large tenants by tenant_id plus entity id instead: per-entity order holds, load spreads across partitions, and tenant_id still travels in every record.

**Tenant boreal uploads 40 GB JSON files that exhaust worker memory. Each crash fails every batch on that worker, whichever tenants they belong to. Fair scheduling is already in place. What's the smallest change that protects the other tenants?**

- **A.** Move boreal's files to a separate worker pool sized for them, so its crashes stay inside its own bulkhead.
- **B.** Raise boreal's scheduling weight so its files finish sooner and spend less time on shared workers.
- **C.** Give boreal a dedicated ingestion cell with its own queues, workers, sinks and buffer.
- **D.** Add a per-tenant quota on boreal's rows per second at admission.

<details>
<summary>Solution</summary>

**A.** Move boreal's files to a separate worker pool sized for them, so its crashes stay inside its own bulkhead.

A bulkhead contains the crash itself: other tenants keep their pool while boreal's workers fail.

**Why the others are wrong**

- **B.** Weight changes the share of worker time, and a 40 GB file still crashes a shared worker.
- **C.** It works, but duplicates the whole stack when only the worker pool is failing.
- **D.** A quota limits rate, and a single 40 GB file under quota still crashes a shared worker.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

A ride-hailing trip handler inserts each trip into PostgreSQL and publishes a trip event to Kafka, a dual write that leaves about 1 in 10,000 trips stored but unbilled, or billed but unstored. Redesign it for 2 million trips a day so the handler writes to one system only and both the trip database and billing are fed from that write, billing triggered by the trip event stream. Kafka redelivers events after restarts, so a replay must never store a trip twice.

<details>
<summary><strong>Solution</strong></summary>

#### One write, and everything else reacts to it

A **dual write** is two independent commits pretending to be one. The handler inserts the trip and publishes the event, and no transaction spans both systems. A broker outage leaves a stored trip that nobody bills; a failed commit leaves billing charging for a trip that does not exist. At 2 million trips a day, a 1 in 10,000 split is about 200 broken trips every day. The fix is architectural: the handler commits to exactly one system, and every other system reacts to that commit as an event.

```mermaid
flowchart LR
    n0[("trip_service<br/>(API)")]
    n1[/"trip_events<br/>(Kafka)"/]
    n2["trip_writer<br/>(transform)<br/>Idempotency: upsert"]
    n3[("trip_database<br/>(PostgreSQL)")]
    n4(["billing_service<br/>(API)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n1 --> n4
```

*The handler publishes once to `trip_events`; `trip_writer` upserts into `trip_database` and `billing_service` charges, each reacting to that one event.*

Here the single write is the publish. Once Kafka acknowledges it, two readers act independently: `trip_writer` writes the row into the database and `billing_service` charges the rider. If `trip_writer` is down, events wait in the topic and it catches up later. Nothing is lost, because nothing depends on a second commit inside the request.

| Trick: Redelivery is why the writer upserts |
| --- |
| Kafka delivers at least once, so after a restart `trip_writer` reprocesses events it may already have written. A plain `INSERT` turns each replay into a duplicate trip. Keying the write on `trip_id` and upserting lands the replay on the row that already exists, so catching up on an hour of events leaves the table exactly as it was. |

| Pitfall: Retrying the publish does not close the gap |
| --- |
| Wrapping the Kafka call in retries, or publishing before the insert instead of after, only changes which half can fail alone. As long as the handler itself touches two systems, two of the 4 outcomes still break the invariant. Only removing the second write removes them. |

| Key insight: The database now trails the event |
| --- |
| Because a consumer writes the trip row, a read in the same request may not find it yet. That is fine here: the handler only returns the trip id to the driver app, and the row appears moments later. |

**Follow-up questions**

1. A rider-receipts service now needs every completed trip. What changes?\
   *Whether a new reader subscribes to the existing stream instead of adding a write to the handler.*
2. Kafka redelivers a trip event after a consumer restart. What stops billing from charging twice?\
   *Idempotent consumption on the billing side, not just the database side.*
3. How would you notice that `trip_writer` has fallen hours behind `trip_events`?\
   *Monitoring consumer lag so a stalled writer is caught before trips go missing from reports.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

A payment service writes each captured payment to PostgreSQL, and a ledger loader reads payment events from Kafka into a finance warehouse. Use a transactional outbox: the payment and its event commit in one transaction, and a separate publisher drains unpublished outbox rows to Kafka. The publisher may crash after Kafka confirms but before marking a row published, so replays must leave each payment in the warehouse exactly once.

<details>
<summary><strong>Solution</strong></summary>

#### The outbox moves the duplicate, it does not remove it

A transactional outbox fixes atomicity, not delivery. Writing the payment and its outbox row in one PostgreSQL transaction means neither can exist without the other, but the publisher draining the outbox still talks to Kafka across a separate boundary. It sends, waits for the confirmation, then sets `published_at`. A crash between those 2 steps leaves the row unpublished, so the next run sends it again. The outbox delivers **at least once**, and an idempotent write downstream is what turns that into exactly once in the ledger.

```mermaid
flowchart LR
    n0[("payments_db_outbox<br/>(PostgreSQL)")]
    n1["outbox_publisher<br/>(transform)"]
    n2[/"payment_events<br/>(Kafka)"/]
    n3["ledger_loader<br/>(transform)<br/>Idempotency: upsert"]
    n4[("finance_warehouse<br/>(warehouse)")]
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
```

*One transaction writes the payment and its outbox row; the publisher drains unpublished rows to Kafka; the loader upserts on `event_id`.*

1. **Commit the payment and the event together**

   The payment service never calls Kafka. Inside the transaction that inserts the payment, it inserts an outbox row with a stable `event_id`, the `event_type` '`payment_captured`' and the payload. A commit or a rollback covers both rows at once.

2. **Let the publisher drain, and expect it to resend**

   The publisher selects rows where `published_at IS NULL`, which the partial index keeps cheap, publishes each to Kafka, and marks it only after the broker confirms. That order is correct, and it also guarantees a crash can resend an event Kafka already has.

3. **Make the warehouse write idempotent on the event ID**

   The loader writes each event into the ledger as an upsert keyed on the outbox `event_id`. A resent event hits the same key and rewrites the identical row, so a rerun leaves one ledger entry per payment. Key on the event, not the payment, so a later refund for the same payment is never mistaken for a duplicate.

| Pitfall: Marking before sending loses events instead |
| --- |
| Setting `published_at` before the Kafka send looks like it removes the duplicates. It does, by turning a crash into a payment the ledger never hears about. Keep the confirm-then-mark order and absorb the resend at the warehouse write. |

**Follow-up questions**

1. A fraud scorer starts reading the payment topic too. What does it need?\
   *Idempotency belongs to each reader: every consumer of an at-least-once topic needs its own guard on `event_id`.*
2. How do you keep the outbox table from growing without bound?\
   *Published rows are history; the partial index on `published_at IS NULL` keeps draining fast, and a cleanup of old published rows keeps the table small.*

</details>


<br>

---

## Dual-Write to Outbox + CDC

A marketplace's checkout service publishes order_placed to the broker inside its database transaction before commit. It logs and ignores publish errors so checkout never fails on them. Its 6 consumers are shipping, invoicing, search, fraud and email, plus warehouse ingestion. A 30-day reconciliation against 9.2M orders found both failure kinds from the outbox section:

- **4,140** — orders that never got an event
- **610** — events for orders that rolled back

The missing events cluster around broker restarts, when the publish failed and the commit went ahead. The phantom events come from constraint violations at commit, after the publish had gone out. Both counts form the baseline, and the new path's target is zero.


<br>

### Measure before you move

The reconciliation is a left join from orders to events, counted per hour so failures line up with incidents. count(DISTINCT ...) keeps duplicate deliveries from hiding a gap. The mirror query, from events to orders, counts phantoms.

```sql
SELECT
  DATE_TRUNC('hour', o.created_at) AS order_hour,
  COUNT(DISTINCT o.order_id) AS orders,
  COUNT(DISTINCT e.order_id) AS orders_with_event
FROM orders AS o
LEFT JOIN order_placed_events AS e
  ON e.order_id = o.order_id
WHERE o.created_at >= TIMESTAMP '2026-09-01 00:00:00'
GROUP BY DATE_TRUNC('hour', o.created_at)
ORDER BY order_hour
```


<br>

### The migration in 5 phases

| Phase | Change | Exit check |
| --- | --- | --- |
| 1. Expand | Add the outbox: checkout writes an order_placed row in the order's transaction, still publishing directly | Every new order has exactly 1 outbox row |
| 2. Shadow | A CDC publisher fills a new topic, order_events_v2, that no consumer reads yet | Reconciliation on v2 shows zero missing and zero phantom for 7 days |
| 3. Cut over | Move consumers to v2 in turn: search, warehouse, fraud, email, invoicing, shipping | Each consumer's output matches its v1 output across the overlap |
| 4. Contract | Delete the direct publish from checkout | v1 receives no new events |
| 5. Clean up | Retire the v1 topic once its retention passes; keep outbox rows 7 days | Nothing reads v1 |

Phases 1 and 2 change nothing consumers see, so the new path is proven before anyone depends on it. Consumers then move from cheapest mistake to costliest: a wrong search index rebuilds in minutes, while a duplicate shipment costs money.


<br>

### The overlap at cutover

Events on v1 carry no outbox id, so a consumer switching topics can't dedupe across the 2 streams by id. It switches on a business key instead: it notes the time of its last v1 event, reads v2 from 10 minutes before that, and upserts on `(order_id, event_type)`, unique because an order is placed once. Duplicates in the overlap collapse, and any event v1 lost in those 10 minutes arrives from v2.

**Shipping switches to v2. It starts reading v2 from 10 minutes before its last v1 event and dedupes on outbox id. What goes wrong?**

- **A.** Nothing goes wrong: outbox ids are unique, so every repeated order in the overlap is caught.
- **B.** Orders in the 10-minute overlap ship twice, because their v1 copies carry no outbox id to match.
- **C.** Orders in the overlap never ship, because the dedupe treats their v2 events as repeats.
- **D.** Events from v2 arrive out of order, so shipments are created before their orders exist.

<details>
<summary>Solution</summary>

**B.** Orders in the 10-minute overlap ship twice, because their v1 copies carry no outbox id to match.

Dedupe across the switch needs a key both streams share, here (order_id, event_type).

**Why the others are wrong**

- **A.** Ids are unique within v2 only, and the v1 copies carry no outbox id to match.
- **C.** Shipping has stored no outbox ids for those orders, so no v2 event looks like a repeat.
- **D.** v2 is keyed by order_id, so per-order ordering holds. The failure is duplicate shipments.

</details>

![Pipeline diagram 4](images/pa_ingestion_patterns_advanced_diagram_4.png)


<br>

### What the new path costs to run

> Costs to plan for
>
> - Log retention: a stalled reader pins log on the primary, so set a retained-bytes ceiling. A reader that crosses it gets dropped and re-snapshotted.
> - Outbox growth: around 9.2M rows a month, deleted after 7 days in off-peak keyset chunks.
> - Failover: a new primary may lack the reader's log position. Replay outbox rows by id range within the 7-day retention.
> - Payload ownership: checkout builds the event, so its schema falls under the data contract.
> - Duplicates: delivery is at least once, so every consumer stores processed outbox ids and skips repeats.

**Phase 2 has run 7 days. Reconciliation on v2 shows 12 orders missing, all committed within 2 minutes before a database failover. What should the team do?**

- **A.** Move on to phase 3: 12 misses is far below the dual-write baseline, so the new path already wins.
- **B.** Keep the direct publish permanently as a safety net running next to the outbox publisher.
- **C.** Switch to a polling publisher, since a log reader cannot survive a database failover.
- **D.** Hold phase 3, replay outbox rows by id range from before the failover, and make that replay part of every failover.

<details>
<summary>Solution</summary>

**D.** Hold phase 3, replay outbox rows by id range from before the failover, and make that replay part of every failover.

The outbox still holds the skipped rows. Replaying by id range closes the gap, and consumers dedupe on id.

**Why the others are wrong**

- **A.** The exit check is zero, and this cause will recur at every failover.
- **B.** That reintroduces the dual write, phantoms included, while the outbox already holds the 12 rows.
- **C.** Polling adds load and delay, and replay from the outbox fixes the failure where it happens.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

A multiplayer game emits match results, which must never be lost and feed a leaderboard that may lag during spikes, and frame-timing telemetry, which a performance dashboard only needs as a sample. On launch nights match results peak at 60k events/sec and telemetry at 240k/sec, 10 times the steady load, while the stream processor handles 40k/sec. Design the ingestion so each event type gets the backpressure strategy that fits it.

<details>
<summary><strong>Solution</strong></summary>

#### Two event types, two backpressure strategies

Backpressure stops being one decision the moment a stream carries data of different worth. Here the spike is 300k events/sec against a processor that drains 40k/sec, but the two event types answer the section's questions differently: a match result must not be lost and its lateness is tolerable, while a telemetry frame is worth little on its own. Slowing the producer is off the table too, because a client holding an unsent result loses it when the player quits.

```mermaid
flowchart LR
    n0[("game_clients<br/>(API)")]
    n1[/"match_result_queue<br/>(Kafka)"/]
    n2["telemetry_sampler<br/>(transform)"]
    n3["stream_processor<br/>(Flink)"]
    n4[("leaderboard<br/>(Redis)")]
    n5(["performance_dashboard<br/>(Grafana)"])
    n0 --> n1
    n0 --> n2
    n1 --> n3
    n2 --> n3
    n3 --> n4
    n3 --> n5
```

*Match results wait in `match_result_queue`; telemetry is cut down in `telemetry_sampler` before `stream_processor`.*

1. **Queue what must not be lost**

   Match results peak at 60k/sec against 40k/sec of drain, and a lost result is a wrong leaderboard. That is exactly the case a durable queue covers: `match_result_queue` takes the burst, `stream_processor` drains it at its own rate, and the price is lag the players already accept.

2. **Shed what is only worth a sample**

   Telemetry at 240k/sec could never be drained in time, and queueing it buys nothing because the dashboard reads percentiles. `telemetry_sampler` keeps a small slice, around 1 percent at peak, so the processor sees a few thousand frames a second and the estimate stays sound.

| Performance: The backlog clears in about 20 minutes |
| --- |
| With telemetry sampled, the processor spends about 37k/sec on match results, so the queue grows by roughly 23k/sec: around 40 million events over a 30-minute spike. When input falls back to 6k/sec and sampling stays on until the lag clears, about 34k/sec of headroom drains it in roughly 20 minutes. Queue storage is the cost, which is why only match results go in. |

| Pitfall: Buffering telemetry turns a spike into hours of lag |
| --- |
| Sending both streams through one queue feels safe, but the backlog then grows by about 260k/sec, close to 470 million events in 30 minutes. With only 10k/sec of steady-state headroom that takes over 13 hours to drain, and every match result sits behind frames nobody needed. |

**Follow-up questions**

1. What changes if the spike lasts 6 hours instead of 30 minutes?\
   *Whether queue retention and the resulting leaderboard lag still fit what players tolerate.*
2. What if the dashboard also needs an exact count of crash events from telemetry?\
   *How the worth of the data moves a subset of telemetry into the must-not-lose class.*

</details>


<br>

---

## Putting It All Together

Back at Shopify at noon on Black Friday, the sneaker drop tests every part of this lesson at once. The CDC reader that seeded the orders stream weeks earlier did it in watermark chunks on a replica, so the sale starts with a complete stream and no bloated tables. Each order commits with its outbox row, so a broker restart mid-drop costs a few seconds of delay instead of lost shipments. The surge fills the ingestion buffer, clickstream gets sampled, order events keep their reserved headroom, and the CDC reader keeps draining the log so the shared database never fills its disk. Deficit round robin keeps every other merchant's orders minutes fresh while the hot merchant's backlog drains, and the worst-tenant lag query is the number the on-duty engineer watches.

- Keyset chunking appears 3 times: in the safe extract, in the watermark snapshot, and in deleting old outbox rows, because every bulk operation on a live database has to be small transactions.
- The outbox is both a delivery mechanism and a backpressure buffer: payment events can be signaled to wait because their rows sit safely in the outbox.
- A stalled CDC reader turns ingestion backpressure into a disk problem on the primary, which is the same harm to the application that the first section guards against.
- Dedupe on a stable key is what makes at-least-once safe, from the outbox id at shipping to the (order_id, event_type) key at cutover.
- tenant_id in the outbox and in the partition key is what lets fair scheduling and per-tenant freshness work downstream without lookups.
- The dual-write migration is expand-contract applied to delivery: the outbox and v2 are added alongside the old path, proven by reconciliation, then the old path is removed.


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

A marketplace pulls orders from 940 sellers' paginated storefront APIs by incremental cursor, and on one shared worker pool the 40 large sellers (2 million rows per pull) starve the 900 small ones. Apply resource isolation per tier: a separate pull queue and worker pool for large and for small sellers, both landing in one shared warehouse. Small sellers' orders must land within 15 minutes, large sellers' within an hour. Retry rate-limited API calls with backoff.

<details>
<summary><strong>Solution</strong></summary>

#### Isolate the pulls by tier, share the landing table

This section has three layers of isolation, and resource isolation is the middle one. Tenants still share the platform, but each tier gets capacity that no other tier can take. Here the noisy neighbor is a large seller whose cursor walk pages through 2 million rows and holds a worker for most of an hour. On one shared pool, enough of those pulls fill every worker, and the 900 small sellers queue up behind them. With a separate queue and pool per tier, a large-seller backlog can only delay other large sellers.

```mermaid
flowchart LR
    n0[("seller_storefront_apis<br/>(API)")]
    n1[/"large_seller_pull_queue<br/>(RabbitMQ)"/]
    n2[/"small_seller_pull_queue<br/>(RabbitMQ)"/]
    n3["large_seller_workers_retry_with_backoff<br/>(transform)<br/>SLA: #60; 1h · Backoff: exponential"]
    n4["small_seller_workers_retry_with_backoff<br/>(transform)<br/>SLA: #60; 15min · Backoff: exponential"]
    n5[("shared_orders_warehouse<br/>(warehouse)")]
    n0 --> n1
    n0 --> n2
    n1 --> n3
    n2 --> n4
    n3 --> n5
    n4 --> n5
```

*Each tier has its own queue and worker pool. Both pools retry throttled calls with exponential backoff and land in the one `shared_orders_warehouse`, where sellers stay separated by seller id.*

| Pitfall: A bigger shared pool is still shared |
| --- |
| Adding workers to the single pool raises total throughput but not fairness. When a burst of large-seller pulls arrives, the new workers go to them too. Isolation is about who can claim capacity, not how much capacity exists. |

| Trick: Back off inside the tier, never across it |
| --- |
| A throttled call that waits out its backoff holds a worker only in its own pool. So a seller API rate-limiting a long pull in `large_seller_workers_retry_with_backoff` cannot stall a single pull in `small_seller_workers_retry_with_backoff`. |

| Key insight: Match the isolation layer to where the SLO breaks |
| --- |
| Storage stays logical: one warehouse filtered by seller id, because reads do not compete the way pulls do. The pull layer gets resource isolation because that is where the 15-minute SLO fails. Physical isolation, a dedicated stack per seller, is worth its cost only when one seller's SLO cannot be met inside its tier. |

**Follow-up questions**

1. A small seller grows to 500,000 rows per pull. How does it move between tiers without losing its cursor?\
   *Tier assignment as data that changes, and keeping incremental state independent of which pool runs the pull.*
2. Within the small tier, one seller's API is slow and every page takes 30 seconds. Is per-tier isolation still enough?\
   *When to cap concurrency per tenant inside a tier rather than adding another tier.*

</details>


<br>

### Exercise 2 of 2

**● TASK**

A ticketing platform's order service writes each order to PostgreSQL and its event to an outbox table in the same transaction. When a big show goes on sale, checkout bursts to 20,000 orders per minute for 10 minutes while the warehouse loader drains 5,000 per minute, and checkout must never be slowed to match. Design the ingestion: capture the outbox with CDC, absorb the burst without losing an order event, and load the warehouse so a redelivered event never creates a duplicate order row. Reporting dashboards read the warehouse within 2 hours.

<details>
<summary><strong>Solution</strong></summary>

#### Buffer the burst, never throttle the buyer

Once the outbox and CDC are in place, the open question is backpressure: for 10 minutes orders arrive 4 times faster than the loader writes, and something has to give. Of the 3 responses the lesson covered, 2 fail this workload. Slowing the producer means slowing checkout, and dropping events means losing paid orders, so what is left is a **durable buffer** that holds the surplus while the loader catches up at its own pace.

1. **Let the outbox be the only write**

   Checkout commits the order row and its outbox row in one PostgreSQL transaction and returns. Nothing in the request path waits on the broker or the warehouse, which is exactly what keeps checkout at full speed during the burst.

2. **Capture the outbox into a durable log**

   CDC reads the outbox from the database change log and publishes each event to Kafka. The log is the buffer: it accepts 20,000 events a minute as they arrive, keeps them on disk, and the loader's consumer offset simply falls behind. Size retention to the worst backlog with margin.

3. **Make the load an upsert on the order**

   CDC is at-least-once: a connector restart replays from its last checkpoint, so the loader sees some events twice. Setting `idempotencyStrategy` to 'upsert', keyed on the order id, turns a repeat into an overwrite of the same row instead of a second sale.

```mermaid
flowchart LR
    n0[("orders_postgres<br/>(PostgreSQL)")]
    n1[("outbox_cdc<br/>(CDC)")]
    n2[/"order_event_buffer<br/>(Kafka)"/]
    n3["warehouse_loader<br/>(Spark)<br/>Idempotency: upsert"]
    n4[("warehouse<br/>(warehouse)<br/>SLA: #60; 2h")]
    n5(["reporting_dashboard<br/>(Metabase)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
```

*Checkout writes the order and its outbox row; CDC streams the outbox into Kafka, which holds the burst; an upserting loader drains it into the warehouse the dashboards read.*

| Pitfall: Throttling checkout moves the queue onto customers |
| --- |
| Blocking the producer is the right backpressure answer when the producer is a batch job that can wait. Here the producer is a fan holding a card: added latency means abandoned purchases, and the backlog still exists, it just lives in browser tabs instead of a log. |

| Key insight: Dropping is only for events whose loss nobody can see |
| --- |
| If the stream also carried seat-map refreshes, shedding those under load would be fine: the next one supersedes them. The test is whether a lost event changes a number someone reconciles. A paid order always does. |

| Performance: The backlog is minutes of disk, not a scaling problem |
| --- |
| The surplus is 15,000 events a minute for 10 minutes, so the log peaks near 150,000 events. After the sale, checkout falls to about 500 a minute and the loader clears the backlog in roughly half an hour, well inside the 2-hour freshness the dashboards need. |

**Follow-up questions**

1. What changes if a confirmation email must go out within a minute, even mid-sale?\
   *Giving a second reader its own offset on the same log so it is not stuck behind the warehouse backlog, and keeping it idempotent.*
2. What happens if Kafka is unavailable for 20 minutes during an on-sale?\
   *Recognizing that the outbox table itself holds the events and CDC resumes from its position with nothing lost.*

</details>


<br>

---

## Key Takeaways

- Read production databases in short keyset chunks from a replica, throttled on replica lag and application latency, so no snapshot stays open for hours.
- Seed log-based CDC with watermark-bracketed chunks: changes the log shows between the markers override the chunk's copy, and no table lock is needed.
- The transactional outbox commits the event with the business change; the publisher delivers at least once, and consumers dedupe on the outbox id.
- Poll the outbox on published_at IS NULL, never on an id bookmark, and choose a CDC publisher when query load or delay rules polling out.
- Match backpressure to the source: buffer what can replay, signal what can wait, drop only what consumers agreed to lose, and never let a CDC reader stall.
- Fairness in shared ingestion comes from per-tenant queues and weighted scheduling. For stronger isolation, add quotas and bulkheads or split tenants into cells.
- Stamp tenant_id at admission, carry it in keys and partitions, and watch the worst tenant instead of the average.
- Migrate a dual write by expanding into an outbox, shadowing on a new topic, cutting consumers over on a business key, then removing the direct publish.
