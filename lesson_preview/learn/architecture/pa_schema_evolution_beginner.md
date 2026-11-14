Beginner·Pipeline Architecture · 25 min · 11 challenges

# Schema Drift: Beginner

This lesson follows launch week for tipping in the Uber rider app. The trip service starts sending a new `tip_usd` field on Monday, and nobody warns the warehouse team. That same night, riders who finished trips in subway tunnels upload them days late. By Wednesday, finance wants to know why tips are missing and why Monday's trip count keeps moving.


<br>

---

## When a Producer Adds a Column

### The field your loader never expected

A producer is the system that writes the data a pipeline reads. Here it is the trip service, which emits an event per completed trip, and the loader is the step that copies those events into a warehouse table called `trips`. The loader was built when every event carried 4 fields, `trip_id` and `city` plus `fare_usd` and `completed_at`. Those names and the type of value each holds are the schema the loader expects.

```sql
{
  "trip_id": "t_88213",
  "city": "Lisbon",
  "fare_usd": 14.2,
  "completed_at": "2026-03-02T23:50:00Z",
  "tip_usd": 2
}
```

Then the producer team ships tipping, and each event gains `tip_usd`. Their change is correct: the app needs the field and their tests pass. Nobody on that team knows your loader exists, and nothing in the event announces a new shape. This is schema drift: the structure of data changes over time, and each system reading it finds out when the new shape arrives.


<br>

### 3 reactions to an unknown field

When the loader meets a field its table has no column for, it does 1 of 3 things, whether its author chose on purpose or inherited a library default. Learn which one yours does before the next producer release.

```sql
def load_event(event, table_columns, mode):
    unknown = [name for name in event if name not in table_columns]
    if unknown and mode == "reject":
        raise ValueError(f"unknown fields: {unknown}")
    if unknown and mode == "evolve":
        for name in unknown:
            add_column(name, infer_type(event[name]))
            table_columns.append(name)
    row = {name: event.get(name) for name in table_columns}
    insert_row(row)
```

What each reaction costs

- **Reject** *Stop the load and fail the run*\
  No wrong data reaches the table, but every dashboard reading trips goes stale. Retrying changes nothing: this is a permanent failure.
- **Ignore** *Load the known fields, drop the rest*\
  The run succeeds and trips stays fresh, while every tip value is thrown away. Without a raw copy of the events, those values are gone.
- **Evolve** *Add a column and load everything*\
  Tips land from the first event. The risk is unreviewed growth: a typo such as tipUsd in a single app version becomes a second permanent column.

Here is `trips` after a batch of 3 events: an old event with no tip, then 2 new events tipping 2.00 and 0.00.

| Reaction | New rows in trips | tip_usd column | Who notices, and when |
| --- | --- | --- | --- |
| Reject | 0 of 3: the batch fails | Absent | On-call, at the failed run |
| Ignore | 3 of 3 | Absent, values lost | An analyst, weeks later, asking for tips |
| Evolve | 3 of 3 | null, 2.00, 0.00 | Whoever reviews new columns, if anyone does |

**A nightly loader keeps succeeding after a producer release, with row counts that match the source. A week later an analyst finds the new promo_code field nowhere in the warehouse. Which reaction is the loader using?**

- **A.** Reject: the loader failed on the unknown field, and someone reran it without the field.
- **B.** Ignore: it loaded the fields its table knows and dropped promo_code without failing.
- **C.** Evolve: it added a promo_code column, but the column stays empty for new rows.
- **D.** Reject: the warehouse refused the new column, so the loader quietly kept the old schema.

<details>
<summary>Solution</summary>

**B.** Ignore: it loaded the fields its table knows and dropped promo_code without failing.

The table stayed fresh and the counts matched while the field went missing, which is what ignore does.

**Why the others are wrong**

- **A.** A reject reaction fails the run. Every run here succeeded, so nothing was stopped or rerun.
- **C.** Evolve would have created a column holding the values. The analyst found the field nowhere.
- **D.** Refusing the field and loading anyway is ignore behavior, whatever it is called. Reject stops the run.

</details>

No reaction is right everywhere. A table feeding driver payouts should reject, because a stale number is safer than a silently incomplete one. A clickstream table with hundreds of loosely owned fields often ignores, with an alert naming each dropped field, because stopping it would page someone every week. A table where analysts want new fields fast can evolve, if someone reviews each new column.

> **TIP**
>
> Keep every raw event in the lake exactly as it arrived. Then reject and ignore both become recoverable: once the team decides what `tip_usd` means, you reload it from the raw copy instead of asking the producer to resend.

**Your team owns payouts, which decides what drivers are paid each week, and app_clicks, which collects UI events from 40 product teams that add fields often. Which pairing of reactions fits best?**

- **A.** Evolve for both, so no field from any producer is ever lost.
- **B.** Reject for both, so no table ever holds data nobody reviewed.
- **C.** Ignore for payouts, reject for app_clicks, so money keeps flowing.
- **D.** Reject for payouts, ignore with an alert for app_clicks.

<details>
<summary>Solution</summary>

**D.** Reject for payouts, ignore with an alert for app_clicks.

Payouts stop rather than run on an unreviewed shape. Clicks stay fresh, and the alert names each dropped field.

**Why the others are wrong**

- **A.** Evolve lets an unreviewed field into payout math, and grows a column for every typo from 40 teams.
- **B.** Rejecting on app_clicks stops the load whenever any of 40 teams ships a field, paging someone constantly.
- **C.** This is backwards. A dropped field in payouts can underpay drivers, and stopping is costly for the click table.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 1

**● TASK**

The checkout_completed event gained a field, discount_code_applied, and the loader on the canvas now fails every row because it writes into a strict fixed-column table. Of a loader's 3 reactions to an unexpected field (reject, silently drop, accept), choose accept: replace the loader and target with a destination that absorbs new fields through schema evolution, such as a lakehouse table. The checkout dashboard must read from that new destination.

**Starting canvas** (already placed for you):

- Checkout service (checkout_completed events; new field discount_code_applied) (source, API)
- Strict loader: SELECT INTO fixed columns (rejects rows with unexpected fields) (transform, SQL)
- Strict target table (no discount_code_applied column) (storage, PostgreSQL)
- Checkout dashboard (consumer, dashboard)


<br>

### Solution

#### Schema evolution, not schema enforcement

Schema evolution is a pipeline's ability to absorb a new field without anyone touching the loader first. The `SELECT INTO` loader had the opposite property: it was pinned to a fixed column set, so the moment `discount_code_applied` appeared on `checkout_completed`, every row carrying it fell outside the target's shape and the load failed. The fix is not to race the backend team with a matching column each time. **Land the whole payload in a table that already tolerates extra keys**, and let readers pick out the fields they care about.

```mermaid
flowchart LR
    n0[("Checkout service<br/>(API)")]
    n1["Loader appends full payload<br/>(Spark)"]
    n2[("Raw checkout table with schema evolution<br/>(Iceberg)")]
    n3(["Checkout dashboard<br/>(dashboard)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
```

*The strict loader and fixed-column target give way to a loader that keeps the whole payload and a lakehouse table whose schema grows when `discount_code_applied` appears; the dashboard reads from that table.*

| Pitfall: Reject is the loud failure; silent drop is the quiet one |
| --- |
| There are 3 reactions a loader can have to an unexpected field. The strict loader chose 'reject': loud, and you found out the same afternoon. The dangerous sibling is 'silent drop': a loader that quietly discards `discount_code_applied` runs green for months while the field never lands, until someone asks where the numbers went. 'Accept', keeping the whole payload, is the only reaction that neither crashes nor loses the field. |

| ✕ Fixed-column target | ✓ Schema-evolving landing |
| --- | --- |
| The target enumerates its columns, so the loader must know every field in advance. A producer change becomes a schema migration on your side, and any row arriving before you catch up fails the load. | A lakehouse table with schema evolution enabled (or a semi-structured `JSON` column in a warehouse) stores the whole payload. New keys ride along with no migration, and you promote them to typed columns when a consumer needs them. |

**Follow-up questions**

1. What happens the day the backend team changes `discount_code_applied` from a string to a nested object?\
   *Adding a column is the easy case; a retyped field is where schema evolution needs a decision about compatibility.*
2. A consumer wants `discount_code_applied` as a first-class column: how do you expose it without re-ingesting history?\
   *Because the raw payload was kept, the field can be surfaced from data already landed, so no backfill from the producer is needed.*


<br>

---

## Forward vs Backward Compatibility

### Writers and readers upgrade separately

A schema change splits the data into 2 versions: events written before it and events written after. It also splits the code that reads them, because the producer team and the data team ship on different days. For a while, every mix of old and new runs somewhere, and compatibility describes which mixes still work.

| Day | Producer writes | Loader code | Mix in play |
| --- | --- | --- | --- |
| Monday | v1 events, 4 fields | v1 | Old reader, old data |
| Tuesday | v2 events, adds `tip_usd` | v1 | Old reader, new data |
| Thursday | v2 events | v2, knows `tip_usd` | New reader, new data |
| Thursday, reprocessing last week | v1 events from the raw copy | v2 | New reader, old data |


<br>

### Backward and forward

✓Backward compatible

- New reader, old data
- The v2 loader reads v1 events
- Needed when new code reprocesses history
- Breaks when a new reader requires a field old events lack

✗Forward compatible

- Old reader, new data
- The v1 loader reads v2 events
- Needed in the gap before readers upgrade
- Breaks when an old reader needs a field new events dropped

Name the direction from the reader's point of view. Backward means a new reader looks back at data from its past. Forward means an old reader receives data from its future. A change that keeps both working is *fully compatible*.


<br>

### 2 rules in the reading code

Compatibility comes down to 2 rules in the reading code. If the reader skips fields it doesn't recognize, new fields cannot hurt it. If every field it expects falls back to a default like null or 0 when missing, events that lack the field cannot hurt it. With both rules in place, the verdicts below follow.

| Change | Backward? | Forward? | Why |
| --- | --- | --- | --- |
| Add `tip_usd` with default 0 | Yes | Yes | New reader fills 0 for old events; old reader skips the field |
| Add `tip_usd` as required, no default | No | Yes | New reader finds no value in old events; old reader skips the field |
| Remove `promo_code`, which had a default | Yes | Yes | New reader skips it in old events; old reader fills the default |
| Remove `city`, required by old readers | Yes | No | New reader skips it in old events; old reader finds it missing |
| Rename `city` to `city_name` | No | No | Each side looks for a name the other side's events lack |
| Change `fare_usd` from a number to text | No | No | Neither reader expects the other's value type |

**The trip service wants to add a required field vehicle_type with no default. Readers skip fields they do not recognize. How does the change classify?**

- **A.** Forward compatible only: old readers skip it, but new readers fail on old events that lack it.
- **B.** Backward compatible only: new readers read old events, but old readers fail on the extra field.
- **C.** Fully compatible: adding a field never breaks a reader in either direction of time.
- **D.** Neither direction: any change to a schema breaks old readers and new readers alike.

<details>
<summary>Solution</summary>

**A.** Forward compatible only: old readers skip it, but new readers fail on old events that lack it.

Old readers skip the unknown field, so forward holds. New readers find no vehicle_type in older events, so backward fails.

**Why the others are wrong**

- **B.** This reverses the directions. Old readers skip unknown fields, so an extra field cannot break them.
- **C.** Adding a field is fully compatible only with a default. Without one, new readers have nothing for older events.
- **D.** Many changes keep one or both directions working. Here old readers survive because they skip the field.

</details>

On Tuesday the v1 loader meets v2 events, so the change must be forward compatible or Tuesday's load breaks. On Thursday the v2 loader rereads last week's v1 events, so it must be backward compatible or the reprocessing breaks. A batch pipeline that ever reprocesses history needs both, which is why a new field with a default is the change data teams hope to see.

> **TIP**
>
> For any proposed change, check 2 things. If today's reader can read the new events, the change is forward compatible. If next week's reader can read last month's events, it is backward compatible. A no to either means the change needs a plan.

**On Monday the producer drops the optional field promo_code, whose default is null. The loader is upgraded on Friday. The next week, the team reruns the new loader over 30 days of raw events. Which direction was needed, and when?**

- **A.** Backward from Monday to Friday, then forward during the 30-day rerun.
- **B.** Forward only, because once the loader is upgraded old events stop mattering.
- **C.** Forward from Monday to Friday, then backward during the 30-day rerun.
- **D.** Backward only, because the field was removed rather than added.

<details>
<summary>Solution</summary>

**C.** Forward from Monday to Friday, then backward during the 30-day rerun.

Before Friday, an old reader met new events: forward. In the rerun, a new reader met old events: backward.

**Why the others are wrong**

- **A.** Reversed. From Monday to Friday the old loader reads new events, which is forward.
- **B.** The rerun feeds old events to the new loader. That mix is backward, and it matters whenever you reprocess.
- **D.** Whatever the change was, the direction depends on whether the reader or the data was newer on that day.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

The producer starts writing a new user_signup schema next Monday, but the consumer reading the Kafka topic stays on the old schema for two more weeks, so old consumer code must keep reading the new events. Add a schema registry node between the producer and the topic, named for the compatibility mode the topic must enforce, so an incompatible schema is rejected at registration before it can be published.

**Starting canvas** (already placed for you):

- Producer: user_signup events (shipping new schema next Monday) (source, API)
- Kafka user_signup topic (mixed v1 and v2 events in flight) (queue, Kafka)
- Consumer: signup analytics (still on old schema for two more weeks) (transform, Pandas)


<br>

### Solution

#### Forward compatibility: old code reading new data

The whole puzzle turns on one term the lesson named: **forward compatibility**. A schema is forward-compatible when data written with a *newer* version can still be read by a consumer running the *older* version. That is exactly the two-week gap here: the producer starts emitting the v2 `user_signup` event on Monday, but the analytics consumer stays on v1 until its own deploy lands. The old code has to read the new data. So the topic must enforce the 'FORWARD' mode, and the registry has to enforce it at the moment the producer tries to register v2, before a single new event reaches Kafka.

```mermaid
flowchart LR
    n0[("Producer: user_signup (v2 schema)<br/>(API)")]
    n1["Schema Registry (FORWARD compatibility)<br/>(schema registry)"]
    n2[/"Kafka user_signup topic<br/>(Kafka)"/]
    n3["Consumer: signup analytics (v1 schema)<br/>(Pandas)"]
    n0 --> n1
    n1 --> n2
    n2 --> n3
```

*The registry sits on the write path, between producer and topic, so v2 is validated before it can be published.*

| Trick: The gate goes before the topic, not after |
| --- |
| Placement is half the answer. If the registry sits after Kafka, the incompatible schema is already in the topic and the old consumer has already choked on it. Registration is a write-path check: the producer submits v2, the registry compares it against the last version under the topic's rule, and a schema that the v1 reader could not parse is rejected at registration. Nothing bad reaches the topic because the bad schema never gets an ID to publish under. |

| ✕ FORWARD (what this topic needs) | ✓ BACKWARD (the tempting wrong pick) |
| --- | --- |
| Old reader, new data. 'FORWARD' guarantees a consumer on the previous schema can read events written with the new one. The registry lets you add a field or drop an optional one, because a v1 reader safely ignores what it does not know about. This matches producer-ships-first, consumer-catches-up-later. | New reader, old data. 'BACKWARD' guarantees a consumer on the new schema can read events written with the old one, which is the consumer-ships-first ordering. Pick this here and the registry validates the wrong direction: it would happily accept a v2 that the still-running v1 analytics job cannot parse. |

| Pitfall: BACKWARD is the default, and it is wrong here |
| --- |
| Most registries ship with 'BACKWARD' as the default compatibility mode, so the easy mistake is to add the node and leave it unnamed or generic. The direction is set by *who deploys first*. Here the producer moves first while the old consumer keeps reading, so you need 'FORWARD'. Reach for 'BACKWARD' only when consumers upgrade ahead of producers. |

In production this is how two teams ship on independent schedules without a coordinated big-bang cutover. The topic owner sets the compatibility mode once, and from then on the registry is the contract: it refuses any schema change that would break a reader still on the old version, so a producer cannot accidentally strand a consumer mid-migration. The mode you choose encodes your rollout order, and 'FULL' (both directions at once) is what you set when you cannot guarantee which side deploys first.

**Follow-up questions**

1. The v2 change adds a new required field with no default. Does 'FORWARD' let the producer register it?\
   *Tests that forward-compatible additions must be readable by the old reader; a required field with no default breaks a v1 consumer, so it is rejected.*
2. Both teams may deploy in either order over the next month. Which mode covers you?\
   *Points at 'FULL' compatibility, which enforces both forward and backward at once.*


<br>

### Exercise 2 of 2

**● TASK**

The orders stream feeds a daily revenue dashboard through a nightly load into a warehouse. Next release, the producer team plans 3 changes: add an optional coupon field, drop the promo flag, and rename amount to order total. Design the path from the stream to the dashboard so the additive change flows straight through while a removal or rename is caught before it corrupts the load.

**Starting canvas** (already placed for you):

- orders events (source, Kafka)


<br>

### Solution

Every change a producer proposes falls into one of 3 buckets, and sorting them is most of the work of **schema evolution**. Adding an optional `coupon_code` is additive: no existing reader ever asked for it, so nothing breaks. Dropping `promo_flag` is destructive: any query that reads it fails or quietly goes NULL. Renaming `amount` to `order_total` is the one that fools people, because to every reader it is a drop plus an add.

```mermaid
flowchart LR
    n0[("orders events<br/>(Kafka)")]
    n1["schema registry check<br/>(Apicurio)"]
    n2["nightly batch load<br/>(Spark)"]
    n3[("revenue warehouse<br/>(warehouse)")]
    n4(["daily revenue dashboard<br/>(dashboard)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
```

*The registry check reads the stream before the nightly load, so a breaking change stops at ingest instead of in the warehouse.*

#### Where the checkpoint sits

The schema registry check reads straight off the stream, before the nightly load touches anything. Under backward compatibility it accepts a new schema version that only adds an optional field, so the coupon change flows through with no work on your side. It refuses a version that drops or relabels a field the load reads. The failure shows up at the checkpoint, not as a revenue number that went to zero overnight.

| Pitfall: A rename is a drop plus an add |
| --- |
| Renaming `amount` to `order_total` looks harmless in a pull request, but every reader still asks for `amount`. The load either fails or reads NULL revenue. Treat a rename as destructive until it is coordinated. |

| Key insight: Rename by expanding, then contracting |
| --- |
| The safe rename is coordinated. First the producer adds `order_total` alongside `amount` and writes both. Next, consumers switch to `order_total`. Only then is `amount` dropped. Each step on its own is either additive or a drop that no reader depends on anymore. |

**Follow-up questions**

1. The producer wants to change `amount` from an integer to a decimal. Is that additive, destructive, or coordinated?\
   *Applies the same classification to a type change.*
2. If the producer skipped straight to dropping `amount`, which step would the registry reject?\
   *Checks that the compatibility mode lines up with the expand-then-contract steps.*


<br>

---

## Adding Is Safe, Renaming Is Not

### 3 classes of schema change

The compatibility test judges a single change. A proposal also needs a verdict on its plan: can it ship today, or does it need a schedule agreed with other teams? Each proposal falls into 1 of 3 classes.

- **Additive** *Adds something readers can skip*\
  A new optional field with a default, or a new table. Readers keep working, so the producer ships without waiting for anyone.
- **Destructive** *Removes or reshapes something readers use*\
  Any single release that drops a field or changes its name or type. Some reader breaks the moment it ships, either loudly with an error or quietly with wrong numbers.
- **Coordinated** *A destructive goal reached in safe steps*\
  The same rename or type change, split so that no step removes anything a reader still uses. It takes weeks and needs every reader's owner to agree.


<br>

### A rename looks like a delete to every reader

A rename feels small to the team making it. To every reader it is 2 changes at once: a field they rely on disappears, and an unknown field appears. Suppose the trip service adopts a naming standard and renames `fare_usd` to `fare_amount_usd` in a release at 15:00 on March 10. The loader runs in ignore mode, so it skips the unknown name and stores null in `fare_usd`. Every load succeeds.

```sql
SELECT
  DATE(completed_at) AS trip_date,
  SUM(fare_usd) AS revenue_usd
FROM trips
WHERE DATE(completed_at) BETWEEN DATE '2026-03-08'
AND DATE '2026-03-11'
GROUP BY DATE(completed_at)
ORDER BY trip_date
```

| trip_date | revenue_usd |
| --- | --- |
| 2026-03-08 | 612,400 |
| 2026-03-09 | 598,900 |
| 2026-03-10 | 214,700 |
| 2026-03-11 | null |

March 10 holds only the fares from before 15:00, and March 11 sums nothing but nulls, which is null. Revenue appears to fall by almost two thirds and then vanish. No run fails, and a finance analyst is the first to notice. A reject loader would stop on March 10 instead. That is the better failure because it is louder, but readers break either way, so a single-step rename is destructive.

**Which of these proposals is coordinated?**

- **A.** Add an optional surge_multiplier field with a default of 1.0 in Thursday's release.
- **B.** Remove promo_code in Thursday's release, since the app no longer shows promo codes.
- **C.** Change completed_at from a timestamp to text in Thursday's release, keeping its name.
- **D.** Add rider_rating, copy values from rating, move readers over, then drop rating.

<details>
<summary>Solution</summary>

**D.** Add rider_rating, copy values from rating, move readers over, then drop rating.

A rename in safe steps. Each step keeps every current reader working, which makes it coordinated.

**Why the others are wrong**

- **A.** This is additive. A field with a default passes both directions and needs no schedule.
- **B.** Destructive as proposed. Any reader still selecting promo_code breaks or reads nulls on Thursday.
- **C.** A type change in place in one release is destructive. Readers doing date math break at once.

</details>


<br>

### The Safe Rename: Add, Copy, Switch, Drop

A coordinated rename reaches the same end state in 4 steps, and nothing a reader uses disappears until that reader has stopped using it.

| Step | Producer writes | Readers use | Why nothing breaks |
| --- | --- | --- | --- |
| 1. Add | Both `fare_usd` and `fare_amount_usd` | `fare_usd` | A new field passes both compatibility directions |
| 2. Copy | Both | `fare_usd` | Rows from before step 1 get `fare_amount_usd` filled from `fare_usd` |
| 3. Switch | Both | `fare_amount_usd`, a reader at a time | Both names hold the same values, so each reader moves on its own schedule |
| 4. Drop | `fare_amount_usd` only | `fare_amount_usd` | No reader is left on the old name |

```sql
SELECT
  COUNT(*) AS rows_missing_new_name
FROM trips
WHERE fare_amount_usd IS NULL
  AND fare_usd IS NOT NULL
```

Run the steps out of order and readers break. Drop before switch, and any reader still on `fare_usd` reads nulls: the March 11 dashboard again. Switch before the copy finishes, and a switched reader sees nulls for every row older than step 1. A type change uses the same steps under a new name: to store fares as whole cents, add `fare_cents`, copy, switch, then drop the old field.

✓Do

- Ship new fields with a default so they pass both directions
- Treat every rename as add, copy, switch, drop
- List every reader of a field before removing it

✗Don't

- Rename or retype a field in a single release
- Drop the old field because the producer stopped writing it
- Trust ignore mode during a rename: it hides the break

**A team renaming city to city_name finishes the add and copy steps, then removes city from the producer and the table the next morning. What happens?**

- **A.** Nothing breaks, because the copy step already put every value under the new name.
- **B.** Every reader that still selects city breaks, because the switch step was skipped.
- **C.** Only new events break, because old rows in the table still hold their city values.
- **D.** Readers break only if the loader is in reject mode, because ignore mode shields them.

<details>
<summary>Solution</summary>

**B.** Every reader that still selects city breaks, because the switch step was skipped.

Drop came before switch. Readers on the old name now fail or read nulls, depending on how they treat a missing field.

**Why the others are wrong**

- **A.** The copy moved the data, but readers still select city. Removing it before they switch breaks them.
- **C.** The column left the table too, so old rows lost it as well. Readers of city break for every date.
- **D.** Loader mode governs unknown incoming fields. A reader selecting a column that no longer exists breaks in every mode.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Mobile checkout events are queued offline and can reach the pipeline days late: one stamped Tuesday 09:14 arrived Thursday. The daily aggregation counts each event on the day it arrived, so Finance's daily revenue lands late checkouts in the wrong day. Rework the aggregation to count revenue on the day the checkout happened, and add a step that recomputes the recent days on every run, safe to rerun without double counting, so late arrivals correct their own day.

**Starting canvas** (already placed for you):

- Mobile SDK events (queued offline; arrival runs days behind when the action happened) (source, API)
- Daily aggregation (currently counts each event on the day it arrived; late events land in the wrong day) (transform, SQL)
- Snowflake daily_revenue (mart) (storage, Snowflake)
- Finance dashboard (consumer, Looker)


<br>

### Solution

#### Count on the day it happened, not the day it landed

Every event carries two clocks: **event time**, when the checkout happened (the Tuesday `09:14` the SDK stamped), and arrival time, when the row reached the warehouse. The old job grouped by arrival, so a phone that reconnected Thursday booked Tuesday's revenue into Thursday. Group `daily_revenue` by the event-time date instead, and a straggler knows its day no matter when it lands.

| Pitfall: `WHERE arrival_date = CURRENT_DATE` freezes late rows out |
| --- |
| The tempting shape appends only rows that arrived since the last run, grouped by arrival day. Cheap, and wrong: the Tuesday checkout that lands Thursday counts under Thursday, and Tuesday is never revisited. Once a day scrolls past the run window its total freezes with a hole in it. |

```mermaid
flowchart LR
    n0[("Mobile SDK events (event_time)<br/>(API)")]
    n1["Aggregate GROUP BY event_date<br/>(SQL)"]
    n2["Reprocess last N days per run<br/>(SQL)<br/>Idempotency: delete_write"]
    n3[("daily_revenue partitioned by event_date<br/>(warehouse)")]
    n4(["Finance dashboard<br/>(dashboard)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
```

*Group by the event-date column, then overwrite a trailing window of partitions every run.*

1. **Re-key the aggregation on `event_date`**

   Derive `event_date` from each event's own timestamp and make it the partition key of `daily_revenue`. `GROUP BY event_date` routes the Tuesday checkout to Tuesday's partition however late it arrives. This is what makes correctness possible at all.

2. **Recompute a trailing window, don't append**

   Each run, rebuild the last N days of partitions from the raw events rather than appending new rows, with N sized to your worst realistic arrival lag. Every day stays open across the window, so Thursday's straggler folds into Tuesday's total on the next run. Past N you accept stragglers as negligible: a deliberate cost trade.

| Trick: Overwrite the partition so reruns don't double-count |
| --- |
| The window heals history only if a rerun lands the same answer instead of stacking a second copy. Replace each day's partition wholesale (`DELETE`+`INSERT` or `INSERT OVERWRITE` keyed on `event_date`), never a blind `INSERT`. The job is then safe after a retry or backfill, because each partition is a pure function of that day's raw events. |

**Follow-up questions**

1. How would you choose N, and what happens to an event that arrives on day N+1?\
   *Ties N to the measured arrival-lag distribution and names the backfill fallback.*
2. If reprocessing 7 days per run gets too expensive, how would you rebuild only the partitions that received late rows?\
   *Pushes toward tracking touched `event_date` values over a blanket window.*


<br>

### Exercise 2 of 2

**● TASK**

The nightly daily_revenue job computes today only, so a mobile event that arrives 5 days late is never counted. Apply a seven-day rerun window: add an orchestrator that schedules the daily run and states the seven-day window in its name. Replace the append-only destination with a lakehouse table that overwrites each event_date partition, so rerunning a day replaces its row instead of duplicating it, and keep the dashboard reading from it.

**Starting canvas** (already placed for you):

- Mobile events source (raw events retained for last 30 days) (source, Custom)
- daily_revenue aggregation (today-only; freezes each day's number) (transform, SQL)
- Plain Snowflake daily_revenue: INSERT INTO append (no partition overwrite; reruns duplicate) (storage, Snowflake)
- Finance dashboard (consumer, Looker)


<br>

### Solution

#### Widening the window so late data lands

The fix you just wired is a **rerun window** (a backfill or lookback window): instead of computing only today, the pipeline recomputes a trailing span of days on every run, so an event that arrives late still gets folded into the day it belongs to. The old design froze each day's `daily_revenue` the night it ran, so a mobile event 5 days late landed nowhere. You added an orchestrator that schedules a daily run recomputing the last seven `event_date` partitions, which gives late events up to a week to show up and still be counted.

```mermaid
flowchart LR
    n0["Daily run (7-day rerun window)<br/>(Airflow)"]
    n1[("Mobile events (raw, 30-day retention)<br/>(API)")]
    n2["daily_revenue aggregation (last 7 days)<br/>(SQL)"]
    n3[("daily_revenue (partition overwrite by event_date)<br/>(Iceberg)")]
    n4(["Finance dashboard<br/>(dashboard)"])
    n0 --> n2
    n1 --> n2
    n2 --> n3
    n3 --> n4
```

*The orchestrator triggers a daily run scoped to the last seven `event_date` partitions; the lakehouse sink overwrites each partition so the rerun is idempotent.*

| Key insight: A wider window is worthless without an idempotent write |
| --- |
| Looking back seven days only helps if re-running a day produces the same row, not a second one. That is why the destination moves off a plain `INSERT INTO` append and onto a lakehouse table (Iceberg or Delta) that does partition overwrite by `event_date`: re-running 'March 3' replaces the 'March 3' partition wholesale instead of adding to it. Same input, same output, however many times the run fires. |

| Pitfall: Seven reruns, seven copies |
| --- |
| The tempting half-fix is to widen the window but keep the `INSERT INTO` append. Now every morning you re-insert seven days of rows on top of the seven you wrote yesterday, and `daily_revenue` inflates by roughly 7x. A longer lookback on a non-idempotent sink makes the numbers worse, not better. The partition overwrite is the load-bearing half of the pattern, not an optional nicety. |

| Real world: The window can't outrun your raw retention |
| --- |
| The mobile source keeps raw events for 30 days, so a seven-day window is safe: the data you recompute against is still there. If retention were shorter than the window, the rerun would recompute days whose source rows had already aged out and quietly undercount them. In practice you size the window against both how late events realistically arrive and how long upstream keeps the raw data, and the shorter of those 2 caps it. |

**Follow-up questions**

1. How would you move from a fixed seven-day window to one that only reruns the `event_date` partitions with newly-arrived late events?\
   *Tests whether you see the window length as a cost knob: a full seven-partition overwrite every night is wasted work if only one day actually changed.*


<br>

---

## What Late Data Means

### 2 clocks on every event

Every event has 2 timestamps, even if the pipeline stores only one. Event time is when the thing happened in the world: the moment the trip ended, recorded by the phone in `completed_at`. Processing time is when the pipeline received it: the loader stamps `loaded_at` on each row it writes into `trips`. Usually the 2 sit seconds apart, but sometimes they sit days apart.

| trip_id | completed_at (event time) | loaded_at (processing time) | Gap |
| --- | --- | --- | --- |
| t_88213 | 2026-03-02 23:50:00 | 2026-03-04 08:10:00 | 32 h 20 m |
| t_88214 | 2026-03-02 21:12:03 | 2026-03-02 21:12:07 | 4 s |
| t_88215 | 2026-03-02 18:40:00 | 2026-03-03 01:55:00 | 7 h 15 m |

Trip `t_88213` ended in a tunnel, and the phone held the event until it found a signal on the morning of March 4. Trip `t_88215` was sent on time, then sat in a buffer during a trip service outage. Gaps like these have a handful of causes:

- Phones offline in tunnels, on flights or in rural areas send events when they reconnect
- A producer outage holds events in a buffer, then releases hours of them at once
- A partner delivers Monday's file on Wednesday
- An upstream retry resends a failed batch long after its events happened


<br>

### An event is late once its period is computed

A gap matters once the pipeline has already answered a question about the event's period. The nightly job runs at 02:00 and counts the previous day's trips. At 02:00 on March 3 it published 41,200 trips for March 2. Trip `t_88215` arrived at 01:55 and made it in. Trip `t_88213` arrived more than 30 hours after that count went out. A late event is one whose event-time period was already computed when it arrived.

```sql
SELECT
  DATE_DIFF('hour', completed_at, loaded_at) AS hours_late,
  COUNT(*) AS trips
FROM trips
WHERE DATE(completed_at) = DATE '2026-03-02'
GROUP BY DATE_DIFF('hour', completed_at, loaded_at)
ORDER BY hours_late
```

**A row in trips shows completed_at 2026-03-05 22:10 and loaded_at 2026-03-06 09:30. Each day's count runs at 02:00 the next morning. Is this trip late, and for which day?**

- **A.** Late for March 5: that day was published at 02:00, 7.5 hours before the trip arrived.
- **B.** Late for March 6: it arrived on March 6, so March 6 is the day the trip belongs to.
- **C.** On time: it arrived within 12 hours of the trip, so no published day is affected.
- **D.** On time: late data only exists in streaming pipelines, and this pipeline runs nightly.

<details>
<summary>Solution</summary>

**A.** Late for March 5: that day was published at 02:00, 7.5 hours before the trip arrived.

Its event time falls on March 5, which was computed at 02:00 on March 6, before loaded_at.

**Why the others are wrong**

- **B.** March 6 is its processing date. The day a trip belongs to comes from event time: March 5.
- **C.** Lateness depends on whether its day was already computed. The size of the gap alone does not decide it.
- **D.** Batch pipelines get late events too, whenever a period is computed before all of its events arrive.

</details>


<br>

### 3 reactions to a late event

By the end of March 4, 180 more March 2 trips have arrived, all of them after that morning's 02:00 run. March 4 has 39,800 trips of its own. The pipeline can do 1 of 3 things with the 180.

| Reaction | What happens to the 180 | March 2 count | March 4 count | Cost |
| --- | --- | --- | --- | --- |
| Drop | Discarded as too late | 41,200 | 39,800 | March 2 undercounts by 180, forever |
| Update | Added to March 2, and the day recomputed | 41,380 | 39,800 | A published number moves after people saw it |
| Count on arrival | Added to the day they arrived | 41,200 | 39,980 | Both days are off; the 2-day total is right |

Count on arrival groups by processing time: a published day never moves and a month's total comes out right, while a Monday trip shows up as Wednesday's. Drop and update both group by event time and differ in whether a published day may change. Update is the one reaction that ends with every day correct, and the next section builds it for a batch pipeline.

> Which reaction fits which table
>
> - Finance close, where a filed number must never move: count on arrival, into the next open period
> - Daily trip counts feeding a demand forecast: update, since the model needs each day right
> - A live operations screen showing the last hour: drop, since a 2-day-old trip is noise there

**A payroll report is filed with a government agency each month, and a filed month can never be changed. Which reaction to late events fits it?**

- **A.** Update: recompute the filed month so it matches what really happened.
- **B.** Drop: discard late events so that the filed month stays fixed.
- **C.** Count on arrival: put late events into the next open month.
- **D.** Any of the 3: payroll is monthly, so lateness cancels out.

<details>
<summary>Solution</summary>

**C.** Count on arrival: put late events into the next open month.

The filed month never moves and nothing is lost: late events land in the next filing.

**Why the others are wrong**

- **A.** Update changes a published number, and here the filed number is not allowed to move.
- **B.** The month stays fixed, but real payroll events vanish from every future report.
- **D.** Lateness cancels out only under count on arrival. Drop loses events and update rewrites a filed month.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Apply all four beginner-tier fixes to this consumer-app event pipeline. Replace the strict loader and Snowflake target with a flexible-shape destination (a lakehouse table with schema_evolution, or VARIANT columns) that absorbs new producer fields and supports atomic partition overwrite by event_date. Rebucket the daily aggregation by event_time instead of processing_time, and add an orchestrator (Airflow, Dagster, or Prefect) whose name states a rerun window of at least seven days. Point the dashboard at the new destination.

**Starting canvas** (already placed for you):

- Mobile SDK source (producers ship new fields ungated; events arrive late) (source, Custom)
- Strict-schema loader (rejects rows with unexpected fields) (transform, Python)
- daily_revenue aggregation (buckets by processing_time; late events misrouted) (transform, SQL)
- Plain Snowflake daily_revenue: INSERT INTO append (no rerun, no partition overwrite) (storage, Snowflake)
- Finance dashboard (consumer, Looker)


<br>

### Solution

#### Four beginner fixes, wired into one line

This capstone assembles the four techniques the earlier sections taught, each aimed at one failure in the broken pipeline: **schema evolution** so producer additions absorb without a code change, event-time bucketing so late mobile events land in the day they happened, partition overwrite so a rerun replaces a day instead of stacking on it, and DAG orchestration with a rerun window so late data actually gets reprocessed. None of them is exotic; the lesson is that they only work together. A flexible loader that still buckets by `processing_time` fixes nothing, and an idempotent destination that never gets rerun freezes the same wrong number forever.

```mermaid
flowchart LR
    n0[("Mobile SDK source<br/>(Custom)")]
    n1["Flexible loader<br/>(schema_evolution / VARIANT)"]
    n2[("Landing (flexible shape)<br/>(Lakehouse / VARIANT cols)")]
    n3["daily_revenue aggregation<br/>(bucket by event_time)"]
    n4[("daily_revenue table<br/>(partition overwrite by event_date)")]
    n5["Orchestrator (7-day rerun window)<br/>(Airflow / Dagster)"]
    n6(["Finance dashboard<br/>(dashboard)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n6
    n5 --> n3
```

*The orchestrator drives the aggregation on a rolling 7-day window; each rerun overwrites its `event_date` partition, so the dashboard reads a self-healing number.*

1. **Make the destination shape-flexible so new fields absorb**

   Swap the strict loader plus plain warehouse for a flexible-shape landing: a lakehouse format with `schema_evolution` on, or VARIANT-typed columns in the warehouse. Now when a producer ships a new field ungated, the row lands instead of being rejected. This is the difference between a pipeline that pages you every time mobile ships an app update and one that quietly keeps the new field for whenever you need it.

2. **Bucket the aggregation by `event_time`, not `processing_time`**

   The old aggregation grouped rows by when they were processed, so a mobile event that arrived 2 days late counted toward the wrong day. Re-key the bucket on `event_time` (the moment the event happened on the device). This is what makes late data correctable at all: a row's day is now a fact about the event, fixed and known, not an accident of when the loader happened to see it.

3. **Write with partition overwrite by `event_date`**

   Replace the append-only `INSERT INTO` with a destination that overwrites the `event_date` partition. Appending means a rerun adds a second, duplicate row for the day; overwriting means a rerun replaces the day's number outright. That single property is what makes the whole job safe to run twice, which is the precondition for step 4 doing anything useful.

4. **Add an orchestrator that reruns a rolling seven-day window**

   An idempotent write is worthless if you only run each day once. Put a DAG on top whose schedule reprocesses the last seven days on every run, not just today. A late event that lands on day+3 gets swept up when its `event_date` partition is recomputed and overwritten. Seven days is the deliberate trade: it covers realistic mobile lateness without recomputing all of history nightly.

| Key insight: Idempotency and rerun are one mechanism |
| --- |
| The 2 easiest pieces to wire up wrong are the pair that only mean something together. `event_date` partition overwrite makes a rerun safe; the 7-day window makes reruns happen. Ship overwrite without the window and late data never gets reprocessed; ship the window over an append-only table and every rerun doubles the revenue. On the canvas, the dashboard must read from the final reconciled `daily_revenue` table, downstream of both, so it always shows the self-healed value. |

**Follow-up questions**

1. What breaks if you widen the rerun window from seven days to ninety?\
   *Tests whether they see the cost side of the window: correctness reach vs. nightly recompute volume.*
2. A VARIANT column absorbs new fields, but the finance dashboard needs `revenue` as a typed number. Where does that projection belong?\
   *Tests that schema flexibility at the landing does not excuse an untyped serving layer.*
3. How would you handle an event whose `event_time` is older than the rerun window when it finally arrives?\
   *Tests the boundary of late-data handling: the window is a bound, not a guarantee.*


<br>

### Exercise 2 of 2

**● TASK**

Thermostat firmware rolls out over six weeks, so old and new firmware publish readings to one Kafka topic side by side while the stream job that parses them upgrades on day one. Add a schema registry that checks every new firmware schema before it publishes, and name the stage for the compatibility mode that lets the job upgrade first. Land the parsed readings in a table fresh within 15 minutes for the operations dashboard.


<br>

### Solution

#### The reader moves first, so the check is backward

Backward compatibility means the new code reads the old data. In this rollout the new code is the parsing job, upgraded on day one, and the old data is every reading from thermostats still on last month's firmware, which keeps arriving for six weeks. So the registry enforces **backward compatibility**: a new firmware schema is accepted only if a reader built on it can still parse what older firmware writes. Forward compatibility answers the opposite question, whether old code reads new data, and here no old reader is left once day one passes.

1. **Ask which side upgrades first**

   The direction falls out of the deploy order. If the reader upgrades first, it will see old data, so you need backward. If the writer upgrades first, old readers will see new data, so you need forward. Here the job ships on day one and the firmware takes six weeks, so backward it is.

2. **Put the check in front of the topic**

   A compatibility rule only protects the job if it runs before a reading is published. The registry sits between the thermostats and the Kafka topic, so a firmware schema that fails the backward check is rejected at publish time instead of crashing the parser hours later.

```mermaid
flowchart LR
    n0[("thermostats<br/>(source)")]
    n1["backward_compatible_schema_registry<br/>(schema registry)"]
    n2[/"readings_topic<br/>(Kafka)"/]
    n3["parse_readings<br/>(Flink)"]
    n4[("thermostat_readings<br/>(warehouse)<br/>SLA: #60; 15min")]
    n5(["operations_dashboard<br/>(consumer)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
```

*Thermostats publish through a backward compatibility check into Kafka; the stream job parses both firmware versions into a table fresh within 15 minutes for the dashboard.*

| Pitfall: Forward mode lets the breaking schema through |
| --- |
| Picking forward because the firmware is the newer side is the common slip. Suppose new firmware adds a required `humidity` field with no default. A forward check passes it, since an old reader just ignores the extra field. But the job is already on the new schema, and every reading from old firmware lacks `humidity`, so the parser fails. A backward check rejects that schema; an optional field with a default passes both. |

| Real world: Neither direction means a lockstep deploy |
| --- |
| A change that is neither backward nor forward compatible forces every writer and reader to switch at the same instant. With two million thermostats updating over six weeks, that instant never exists, which is why teams pin the registry mode to their rollout order before the first schema ships. |

**Follow-up questions**

1. A billing job will keep its old code for three months while firmware rolls out. What mode does the registry need now?\
   *An old reader of new data adds forward compatibility on top of backward, so both directions must hold.*
2. What if the firmware shipped first and the parsing job upgraded a week later?\
   *Writer first means old code reads new data: the mode flips to forward.*


<br>

---

## Late Data: Rerun Last 7 Days

### Recompute the window every night

The update reaction needs a mechanism, and for a batch pipeline the simplest one is a wider rerun. Instead of computing only yesterday, the 02:00 job recomputes the last 7 days by event time. When `t_88213` lands in `trips` on March 4, the run at 02:00 on March 5 recomputes March 2 anyway, so the late trip reaches its correct day without anyone touching it.

| Run at 02:00 on | Days recomputed | March 2 count | What changed |
| --- | --- | --- | --- |
| March 3 | Feb 24 to Mar 2 | 41,200 | First count of March 2 |
| March 4 | Feb 25 to Mar 3 | 41,200 | Late trips not in yet |
| March 5 | Feb 26 to Mar 4 | 41,380 | All 180 late trips folded in |
| March 9 | Mar 2 to Mar 8 | 41,380 | Last run that touches March 2 |
| March 10 | Mar 3 to Mar 9 | 41,380 | March 2 is final |


<br>

### Replace Each Day, Never Append

The window works only if rerunning a day is idempotent. If the job appends, the March 4 run adds a second copy of March 2 on top of the first, and by March 9 the day has been inserted 7 times. The job must replace: delete the window's rows, then insert the recomputed ones. Run it once or 5 times and the table ends in the same state.

```sql
DELETE FROM daily_trip_counts
WHERE trip_date BETWEEN current_date - INTERVAL '7' DAY AND current_date - INTERVAL '1' DAY
```

```sql
INSERT INTO daily_trip_counts
SELECT
  date(completed_at) AS trip_date,
  city,
  count(*) AS trips
FROM trips
WHERE date(completed_at) BETWEEN current_date - INTERVAL '7' DAY AND current_date - INTERVAL '1' DAY
GROUP BY
  date(completed_at),
  city
```

Both the filter and the grouping use `completed_at`, the event time, because each day is rebuilt from every trip that belongs to it. If the insert fails after the delete, the retry repeats both statements and still ends correct.

**A team switches its nightly job to cover the last 7 days with INSERT INTO daily_trip_counts SELECT ... and no delete. What does the table hold for March 2 after the March 9 run?**

- **A.** The correct 41,380, since each run includes the late trips that arrived.
- **B.** Seven stacked copies of March 2's rows, so its total is about 7 times too high.
- **C.** Only the first count of 41,200, since a day cannot be inserted a second time.
- **D.** An empty March 2, since by March 9 the window has already moved past it.

<details>
<summary>Solution</summary>

**B.** Seven stacked copies of March 2's rows, so its total is about 7 times too high.

The runs on March 3 through March 9 each inserted the day again, piling up instead of replacing.

**Why the others are wrong**

- **A.** Each run adds rows without removing earlier ones. The late trips are there, and so are 6 earlier copies.
- **C.** A plain insert has no check for duplicates, so each of the 7 runs adds a full set of rows.
- **D.** March 2 is still inside the window on March 9, and an insert-only job never removes rows anyway.

</details>

![Pipeline diagram 1](images/pa_schema_evolution_beginner_diagram_1.png)


<br>

### Choose the window from measured lateness

The 7-day window came from the lateness query in the previous section. Run it over a month and record what share of each day's trips had arrived by each point after the day ended.

| Arrived within | Share of the day's trips, March average |
| --- | --- |
| 2 hours (the first run) | 99.52% |
| 1 day | 99.81% |
| 3 days | 99.95% |
| 7 days | 99.99% |
| 30 days | 99.998% |

Widening from 1 day to 7 catches nearly all of the remaining tail. Widening to 30 recovers a sliver more for more than 4 times the nightly work, and keeps 30 days provisional for readers instead of 7.

- **7x** — nightly work vs a 1-day job
- **0.01%** — trips arriving after day 7

Where the window stops helping

- Trips arriving after day 7 reach trips but never daily_trip_counts. A daily check on rows loaded today with a completed_at over 7 days old counts them.
- The rerun reads 7 days of history with today's code, so every schema change must stay backward compatible for at least that long.

**A partner's daily sales file arrives late in this pattern: 92% of a day's rows within 1 day, 99.9% within 3 days, 99.95% within 14 days. Rebuilding a day takes 5 minutes. Which window is the best starting choice?**

- **A.** One day, since 92% is close enough for a sales table.
- **B.** Fourteen days, since it catches the most rows of any option.
- **C.** Thirty days, since a wider window is always safer for late data.
- **D.** Three days, since it catches 99.9% for 15 minutes a night.

<details>
<summary>Solution</summary>

**D.** Three days, since it catches 99.9% for 15 minutes a night.

Past day 3, each extra day catches very little. A 3-day window captures the tail cheaply, and a check counts the rest.

**Why the others are wrong**

- **A.** Missing 8% of each day at publish is large for sales, and widening costs only minutes.
- **B.** It runs 70 minutes a night to gain 0.05% over a 3-day window, which buys almost nothing.
- **C.** Each extra day costs another rebuild and keeps more history provisional, for rows that almost never come.

</details>


<br>

### Capstone: drift and lateness together

**The trip service starts renaming fare_usd to fare_amount_usd on March 10 with add, copy, switch, drop. The nightly job rebuilds 7 days of revenue from raw events stored exactly as they arrived. On March 14 the team points the job at fare_amount_usd alone. What happens to the rebuilt days?**

- **A.** Trips from before the rename rebuild with null revenue, since their raw events carry only fare_usd.
- **B.** Nothing changes, since the copy step already gave every old row the new field name.
- **C.** The job fails, since a renamed field can never be read across a rerun window.
- **D.** Only March 14 is affected, since earlier days in the window are already final.

<details>
<summary>Solution</summary>

**A.** Trips from before the rename rebuild with null revenue, since their raw events carry only fare_usd.

The copy filled the table, and raw events stayed as they arrived. Reading coalesce(fare_amount_usd, fare_usd) until the March 18 run keeps the rebuild backward compatible.

**Why the others are wrong**

- **B.** The copy updated the warehouse table. Raw events from before the rename still carry only fare_usd.
- **C.** It can, with a fallback such as coalesce. A field missing from an event usually reads as null.
- **D.** Every day in the window is rebuilt each night, including the trips from March 7 to 10 that predate the rename.

</details>


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

Trip events reach a warehouse from a Kafka topic, and the producer team wants to rename `fare` to `fare_amount` while new optional fields keep arriving. Design the pipeline so a schema check between the topic and the warehouse lets additive changes through but rejects an uncoordinated rename or removal. During the switch producers publish both names, and the warehouse must keep one fare column.


<br>

### Solution

#### Let the additive change carry the rename

The asymmetry from this section does the work: adding `fare_amount` is safe, dropping `fare` is not. So the rename never ships as one change. The producer adds `fare_amount` beside `fare`, which every existing reader ignores, and removes `fare` only once nothing downstream asks for it. A schema check on the topic is what makes that order enforceable instead of a hope.

```mermaid
flowchart LR
    n0[("trip_events<br/>(Kafka)")]
    n1["schema_check<br/>(schema registry)"]
    n2["map_fare_names<br/>(SQL)"]
    n3[("trips_warehouse<br/>(warehouse)")]
    n0 --> n1
    n1 --> n2
    n2 --> n3
```

*`trip_events` passes `schema_check`, then `map_fare_names` folds both fare names into one column in `trips_warehouse`.*

1. **Put the check between the topic and the warehouse**

   Register the trip event schema and check every new version against it. An added optional field passes, because old readers never asked for it. A version missing `fare` fails, so a rename that skipped the dual-publish phase is stopped at the producer instead of surfacing in a broken report.

2. **Resolve both names into one column**

   While both names ship, `map_fare_names` takes `fare_amount` when it is present and falls back to `fare` for older events. The warehouse keeps its single fare column, and when the producer finally drops `fare`, this step is the only place that changes.

| Pitfall: A rename is a removal plus an addition |
| --- |
| Treating `fare` to `fare_amount` as one edit means every reader of `fare` sees it vanish at once: joins on it return nothing and sums quietly fall to zero. Splitting it into add, publish both, then remove turns one breaking change into safe steps. |

| Real world: Registries enforce the rule APIs already follow |
| --- |
| A REST API adds response fields without a version bump but needs a major version to remove one. A schema registry in a backward-compatible mode applies the same rule to events: additions register freely, removals are refused until readers are ready. |

**Follow-up questions**

1. What if the producer also wants `fare` to switch from integer cents to a decimal?\
   *Whether a retype is handled like a rename: add a new field, publish both, then drop the old one.*
2. How do you know when it is safe to remove `fare` from the schema for good?\
   *Confirming no reader still depends on the old field before the destructive step.*


<br>

### Exercise 2 of 2

**● TASK**

Smart electricity meters buffer readings offline and can upload them up to three days late, so land every raw reading with its event_time plus an ingestion_time stamped on arrival. Billing sums usage by ingestion day, because an issued invoice must never change. The grid-load dashboard sums usage by event hour, so a late reading counts in the hour it was measured. Design the pipeline that feeds both from the same landed readings.


<br>

### Solution

#### One reading, two clocks

Late data is the gap between `event_time`, when the meter measured, and `ingestion_time`, when the reading reached you. A reading taken Monday 18:00 that uploads Wednesday is two days late. Neither clock is wrong: each answers a different question, and which one you bucket by is a promise to the consumer reading the total.

1. **Stamp `ingestion_time` when the reading lands**

   The meter owns `event_time`; nobody records arrival unless the pipeline does. Landing raw readings with both values means every downstream job can choose its clock, and lateness itself becomes something you can measure.

2. **Choose the bucket per consumer**

   Billing buckets by arrival day: Monday's late reading joins Wednesday's total, so Monday's invoice stays exactly as issued. The dashboard buckets by event hour: the same reading lands in Monday 18:00, which changes that hour after the fact, and that is the correct answer for operators.

```mermaid
flowchart LR
    n0[("meter_readings<br/>(Kafka)")]
    n1[("raw_landing<br/>(object storage)")]
    n2["billing_by_ingestion_day<br/>(Spark)"]
    n3["load_by_event_hour<br/>(Spark)"]
    n4[("warehouse<br/>(warehouse)")]
    n5(["billing_invoices<br/>(API)"])
    n6(["grid_load_dashboard<br/>(Grafana)"])
    n0 --> n1
    n1 --> n2
    n1 --> n3
    n2 --> n4
    n3 --> n4
    n4 --> n5
    n4 --> n6
```

*Both aggregations read the same `raw_landing` readings: `billing_by_ingestion_day` groups on `ingestion_time`, `load_by_event_hour` groups on `event_time`.*

| Pitfall: One aggregate cannot keep both promises |
| --- |
| A single job grouped on `event_time` rewrites invoices that already went out; grouped on `ingestion_time` it moves Monday evening's peak onto Wednesday's chart. Dropping late readings keeps the math simple but undercharges every customer whose meter went offline. |

| Real world: Money closes by arrival, analysis by event time |
| --- |
| Ledgers and invoices usually close on arrival because a published figure must stay put, while capacity and behaviour analysis follows `event_time` because it asks what happened when. |

**Follow-up questions**

1. What changes if billing may add a correction line to next month's invoice for late usage?\
   *Whether billing could follow `event_time` once history is amended through a separate adjustment instead of edited.*
2. How would you report how much usage arrives more than a day late?\
   *Using `ingestion_time - event_time` on the landed readings to measure lateness itself.*


<br>

---

## Putting It All Together

Back at Uber's tipping launch, the trip service adds tip_usd with a default of 0. The loader runs in evolve mode with an alert naming each new column, and riders in tunnels upload trips days late. A nightly job rebuilds the last 7 days of daily_tips by completed_at, deleting and inserting each day. A month later, the team renames tip_usd to tip_amount_usd to match a naming standard.

- The default of 0 makes the launch additive for any reader that does not know tip_usd . In trips , evolve leaves pre-launch rows null. sum ( ) skips those nulls, and the rebuild reads them as coalesce ( tip_usd , 0 ) .
- Evolve mode lets tips land from the first event, and the alert gives a person the review that evolve skips.
- Late tunnel trips count toward their completed_at day because the window rebuilds by event time, which is the update reaction.
- Delete-then-insert makes each rebuilt day idempotent. A day rebuilt on 7 nightly passes, plus any retries, still never double counts.
- The later rename adds the new field, copies values into it, switches readers over and then drops the old one. The rebuild reads coalesce ( tip_amount_usd , tip_usd ) until its window starts after the add step.


<br>

---

## Pipeline Architecture

### Exercise 1 of 2

**● TASK**

A mobile game's SDK retries failed uploads, so session events can arrive up to seven days after their event time. Land the raw session events in object storage, then add a nightly batch job that recomputes the last seven days of daily active player counts by event time and overwrites those daily partitions in a warehouse, so running it twice gives the same counts. A dashboard reads the daily counts from the warehouse.


<br>

### Solution

#### Recompute a trailing week, overwrite what you recompute

A **rerun window** is the batch answer to late data: each night the job rebuilds today plus the previous seven event-time days from the raw events, so a session played last Tuesday that only uploads on Friday is counted in Tuesday's bucket on Friday night. Seven days matches this SDK's retry tail: almost every late upload falls inside it. The window is only safe because each recomputed day replaces what was there before.

```mermaid
flowchart LR
    n0[("mobile_game_sdk<br/>(API)")]
    n1[("raw_session_landing<br/>(object storage)")]
    n2["recompute_last_7_days<br/>(Spark)<br/>SLA: #60; 24h · Idempotency: delete_write"]
    n3[("daily_players_warehouse<br/>(warehouse)")]
    n4(["players_dashboard<br/>(Metabase)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
```

*Raw sessions land in `raw_session_landing`; each night `recompute_last_7_days` rebuilds 8 event-time days and overwrites them in `daily_players_warehouse`.*

| Pitfall: Appending makes every rerun count twice |
| --- |
| If the job inserts rows for each recomputed day, tonight's run adds Tuesday's players on top of the Tuesday rows written last night, and the number climbs every night for a week. Write with `delete_write` (or overwrite each day's partition) so recomputing a day replaces it, and a second run on the same night changes nothing. |

| Performance: 8 days of reading buys a week that corrects itself |
| --- |
| The job reads 8 days of raw events instead of one, so daily compute is roughly 8x. A 3-day window is cheaper but misses the slow retries; a 30-day window catches nearly everything at about 31x the cost. Size the window to where the late tail actually ends, not to zero misses. |

**Follow-up questions**

1. A new SDK release can hold uploads for 14 days. What changes, and what does it cost?\
   *Sizing the window to the late tail: each run reads about 15 days instead of 8.*
2. A session arrives 10 days late. What happens to it, and how could you catch it without widening the nightly window?\
   *What a fixed window misses, and a less frequent wider rerun as a cheaper second pass.*


<br>

### Exercise 2 of 2

**● TASK**

Retail stores upload point-of-sale events through an API whenever their connection returns, so a sale can arrive up to 14 days after its event time, and the vendor keeps adding fields to the event. Land every raw upload in a store that accepts added columns without failing the load. Each night, rebuild the daily sales table from that raw store over a rerun window sized to the lateness, overwriting each event-date partition so a rerun replaces the day instead of counting it twice. A store-sales dashboard reads the daily table.


<br>

### Solution

#### Let the rerun window correct history

Late data and schema drift break a batch pipeline in the same way: the day you computed last night is not the day that exists today. Both are fixed from the same place, a raw store that keeps every upload as it arrived, so any day can be recomputed from it. On top of that, the nightly job rebuilds a trailing window of event days instead of one, and writes each day by replacing its partition whole.

```mermaid
flowchart LR
    n0[("pos_upload_api<br/>(API)")]
    n1[("raw_upload_store<br/>(Delta Lake)")]
    n2["nightly_rebuild_last_14_days<br/>(Spark)<br/>SLA: #60; 24h · Backfill: partition_overwrite"]
    n3[("daily_sales_table<br/>(warehouse)")]
    n4(["store_sales_dashboard<br/>(Metabase)"])
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
```

*Uploads land raw in `raw_upload_store`; `nightly_rebuild_last_14_days` overwrites the last 14 event-date partitions of `daily_sales_table` every night.*

1. **Size the window to the slowest store**

   Stores can lag 14 days, twice the seven-day default from the lesson, so a seven-day rerun would freeze days 8 through 14 before their late sales arrive. Rerun the last 14 event days every night. Key the window on event date, not arrival date: a sale stamped the 3rd that lands on the 15th must correct the 3rd.

2. **Replace each partition, never append**

   Each night the window is recomputed from `raw_upload_store` and every event-date partition of `daily_sales_table` is overwritten whole. A day rebuilt fourteen nights in a row ends with the same total one run would give, plus any late sales that arrived since. That idempotent write is what lets history correct itself safely.

| Pitfall: Appending a rerun counts every sale twice |
| --- |
| Inserting the recomputed days on top of the rows already there doubles every sale the earlier run counted. A rerun window is only safe because the write replaces the partition instead of adding to it. |

| Key insight: Added fields belong in the raw layer first |
| --- |
| The vendor's new fields are additive, the safe kind of change. A raw store that accepts added columns keeps them without failing the load, and because the rebuild reads raw, the fields are already there the day the dashboard wants them. |

**Follow-up questions**

1. What if one store stays offline for 3 weeks?\
   *Whether a one-off rebuild of older partitions beats widening the nightly window for every run.*
2. What changes if the vendor renames `amount` to `sale_amount`?\
   *Contrasts a rename, which breaks readers, with the added fields the raw store absorbs.*


<br>

---

## Key Takeaways

- A loader that meets an unknown field either rejects it or lets it through, and letting it through means ignoring the field or evolving the table to hold it. Know which one yours does before the next producer release.
- Backward compatible means a new reader reads old data. Forward compatible means an old reader reads new data.
- Adding a field with a default is additive and safe in both directions. Removing a field in one release is destructive, and so is changing its type or its name.
- Rename in 4 steps. Add the new field and copy values into it, then switch readers over, and drop the old field only after every reader has switched.
- Event time is when something happened; processing time is when the pipeline saw it. A late event arrives after its period was computed.
- A late event can be dropped, used to update the old result, or counted on arrival.
- Rebuilding the last 7 days nightly with delete-then-insert folds late events into their correct day, and measured lateness sets the window.
