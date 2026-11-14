Advanced·Data Modeling · 25 min · 7 challenges

# Fact Table Selection: Advanced

You are 20 minutes into an onsite at Amazon when the interviewer draws a box labeled orders on the whiteboard and starts talking. An order is placed and picked, then goes through packing and shipping before it is delivered, usually within 4 days, sometimes not for 40. Finance wants daily revenue by product. Operations wants the median time from pack to ship, by warehouse. Leadership wants to know how many orders are sitting unshipped right now. Design the fact tables. Answering with a single table is the mistake most candidates make here. Each of those consumers wants a different grain, a different write pattern and a different row count, and the complete answer names which of the 4 fact table types serves each one and what each costs to keep current. This lesson builds that answer piece by piece, with the numbers the interviewer expects you to say out loud.


<br>

### What you will be able to do

Recognize a fact table selection question in any disguise and map the consumer's question to one of the 4 fact table types

State the grain of each type in one sentence and derive its row count, which follows events or process instances for 2 of the types and entities times periods for the other 2

Write the queries each type makes cheap, and name the query each type makes expensive or impossible

Explain the write pattern of each type, including in-place updates and late-arriving data, and what each costs to keep current

Defend a design that pairs a transaction table with a coverage table or with a snapshot of either kind derived from it, and name the future requirement that breaks it


<br>

---

## "Which Fact Table Would You Use?"

Every version of this question hides the same 2 decisions. The first is grain: what 1 row of the fact table means. The second is write pattern: a row is inserted once and left alone, or new rows land on a schedule even when nothing changed. The third possibility is that a row gets revisited and rewritten as a process moves forward. Combine those 2 decisions and you get exactly 4 fact table types, and the interviewer wants to hear the type named and the grain defended, with a figure for what it costs to keep.

| Type | 1 row means | Written when | Row updated later? | Serves the question |
| --- | --- | --- | --- | --- |
| Transaction | 1 event: an order line, a payment, a click | at the moment the event occurs | never | what happened, and how much |
| Periodic snapshot | 1 entity at the end of 1 period: an account per day | on a schedule, even if nothing changed | never | what the level was at a point in time |
| Accumulating snapshot | 1 instance of a process, 1 column per milestone | when the process starts | yes, each milestone fills a column | how long each step took, and what is stuck |
| Factless or coverage | 1 occurrence or eligibility with no numeric measure | when the condition holds | never | what was possible, and what did not happen |


<br>

### 4 questions that reveal the type

The disguise falls away when you interrogate the consumer rather than the source system. Ask these 4 questions out loud. The interviewer hears a method, and the answers land directly on a row of the table above.

- Does the consumer ask what happened, what the level is, how long a step took, or what did not happen?
- Is a row written once and never touched, or revisited as the business process advances?
- Is the measure additive across every dimension, including time, or does summing across days produce nonsense?
- Does the row count scale with events, with entities multiplied by periods, or with process instances?


<br>

### One business, 4 fact tables

Take a retailer with 200,000 SKUs across 40 warehouses and 2,000 stores. Stock movements are events, receipts and picks as well as transfers and adjustments: 50 million a day, each one an insert. The buyer asking how much of SKU 48812 is on hand this morning wants a level, and computing a level from events means summing every movement since the SKU was first stocked. The planner asking how long an inbound purchase order takes from issued to received wants a lifecycle: 1 row per purchase order with a date for each milestone. The marketing analyst asking which promoted products sold zero units wants coverage, the list of what was eligible, so that absence has something to join against. Each of the 4 questions needs its own grain and so its own fact table, all fed from the same operational system.

| Type | Rows scale with | Retailer example | Rows per year |
| --- | --- | --- | --- |
| Transaction | events | 50M inventory movements a day | 18.25B |
| Periodic snapshot | entities x periods | 200,000 SKUs x 40 warehouses x 365 days | 2.92B |
| Accumulating snapshot | process instances | 3M purchase orders a year | 3M |
| Coverage | eligible combinations | 5,000 promoted SKUs x 2,000 stores x 365 days | 3.65B |

Row counts separate the types as cleanly as grain does. The accumulating snapshot is 3 orders of magnitude smaller than its neighbours because a process instance is rare compared with the events inside it, and the coverage table is as large as the snapshot because eligibility, like a level, exists every day whether or not anything sold. Say the formula before the number and the interviewer knows you have sized one of these before.

> The same question in 5 disguises
>
> - Model a ride-hailing trip: transaction for fares and tips, accumulating snapshot for request to pickup to dropoff
> - Model a bank account: transaction for postings, daily snapshot for balances
> - Model support tickets: accumulating snapshot for opened to resolved, transaction for every status change
> - Model a school: factless fact for attendance, coverage table for who was registered
> - Model ad campaigns: transaction for impressions, coverage for which ads were live in which placement


<br>

### The cost of choosing wrong

A single transaction table can technically answer all 4 questions, which is why candidates reach for it, and why the follow-up hurts. The level question becomes a scan of all history on every query. The lifecycle question becomes a pivot of 5 event types into columns, repeated on every run. The absence question has no rows at all to join against. The strong answer names the second table and its grain, then gives the row count and the refresh cost, so the interviewer can see you weighed the duplication rather than defaulted to it.

> **TIP**
>
> State the grain in 1 sentence before you name a single column. 'One row per order line at the moment the order is placed' tells the interviewer the type, which measures will be additive, and the row-count formula, before the schema is drawn.


<br>

---

## Transaction Fact Tables

A transaction fact table records 1 row per event at the grain of the event itself, written when the event occurs and never touched again. The grain is the finest the source can offer: an order line rather than an order, a payment attempt rather than a payment, a single inventory movement rather than a day of movements. Anything coarser can be derived by aggregation, but nothing finer can be recovered once it's thrown away.

Modeling at 1 row per order pushes the line items into an array or a second table, and the question 'units of product 48812 sold in March' now needs an unnest or a join on every run. Modeling at 1 row per order line makes the order a degenerate dimension and answers the product question with a filter. The cost is row count: an order averaging 3.2 lines becomes 3.2 rows, and order-level totals need a GROUP BY on `order_id`.

![Schema diagram 1](images/dm_pattern_fact_table_selection_diagram_1.png)

The table has 4 foreign keys, 1 degenerate dimension (`order_id` has no attributes of its own, so it lives on the fact row), and 4 measures. Of the measures, `quantity` and `extended_amount` are fully additive, and so is `discount_amount`: sum any of them across any slice of any dimension, time included, and the total means something. `unit_price` is non-additive. The 3 sale rows below sum to 288.98, which prices nothing. Keep it on the row for reference, and compute an average price as `SUM(extended_amount) / SUM(quantity)`, never as a SUM or AVG of `unit_price`.

| order_date_key | product_key | customer_key | order_id | line | transaction_type | quantity | unit_price | extended_amount |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 20260301 | 48812 | 7730041 | O-2291 | 1 | sale | 2 | 19.99 | 39.98 |
| 20260301 | 11207 | 7730041 | O-2291 | 2 | sale | 1 | 249.00 | 249.00 |
| 20260301 | 48812 | 9102 | O-2292 | 1 | sale | 6 | 19.99 | 119.94 |
| 20260302 | 48812 | 7730041 | O-2291 | 1 | cancel | -2 | 19.99 | -39.98 |

The last row is how an immutable table handles a cancellation: a reversing event with negative quantity, inserted on the day the cancellation happened, carrying a transaction type that distinguishes it from a sale. Rewriting the original row would erase the fact that 2 units were sold on March 1 and unsold on March 2, and it would silently change every aggregate that already counted the sale.


<br>

### Queries a transaction table makes cheap

```sql
SELECT
  d.calendar_date,
  p.product_name,
  SUM(f.quantity) AS units,
  SUM(f.extended_amount) AS revenue
FROM fact_order_line AS f
JOIN dim_date AS d
  ON f.order_date_key = d.date_key
JOIN dim_product AS p
  ON f.product_key = p.product_key
WHERE d.calendar_date BETWEEN DATE '2026-03-01'
AND DATE '2026-03-31'
GROUP BY d.calendar_date, p.product_name
```

*The date filter prunes partitions when the table is partitioned by order date, which it should be, because that is the column nearly every query filters on.*

Every additive question costs 1 scan of the partitions in the date range. The table is sparse in time, meaning a day with no sales of a product has no row for it, and dense in dimensions, meaning every row carries every foreign key. That sparsity is why the row count tracks events and nothing else.

- **12M** — order lines inserted a day
- **4.38B** — rows after one year
- **0** — rows ever updated in place


<br>

### Levels and lifecycles get expensive

Ask this table how many units of product 48812 were on hand at the end of March 3 and the answer is the sum of every movement for that product from the first day it was stocked. Across a year of history that is a scan of hundreds of partitions for 1 number, and the dashboard showing on-hand for all 200,000 SKUs runs that scan for each of them. Partition pruning does nothing, because the answer depends on every partition before the date asked about. The lifecycle question hurts in a different way: placed-to-shipped time needs the shipped event matched back to the placed event, a self-join on `order_id` filtered by transaction type, for every order, on every run.

✓Cheap on a transaction table

- Sum of any measure over any dimension, including time
- Counts of events inside a date window
- Filters on any attribute as it stood at event time
- Reprocessing a day, because rows are immutable and partitioned by event date

✗Expensive or impossible

- A balance or level at a point in time
- Elapsed time between 2 events of the same entity
- Anything about events that didn't happen
- The current stage of a long-running process


<br>

### The decisions an interviewer probes

Follow-ups you should expect

- Late-arriving events are inserted into the partition of their event date with the load date stored on the row, so a reprocessed day is complete and auditable.
- Idempotency comes from a natural key, order_id plus order_line_number plus transaction_type, deduplicated at load, so a replayed file adds no rows.
- Grain is set at the finest event the business can act on, because a coarser grain throws away the question nobody has asked yet.
- Ratios and balances do not belong here. Store numerator and denominator as additive measures and divide at query time.

Treat the transaction table as the source the other measured fact types are derived from, and say so in the interview: the periodic snapshot is this table rolled forward, and the accumulating snapshot is this table pivoted. The coverage table is the exception. Its rows record what was eligible, so it's loaded from its own source and joined against this table to expose what did not happen. For promotions that source is the promotion calendar; elsewhere it might be a schedule or an eligibility list. That framing turns 4 tables into 1 design: 1 source of events, 2 tables derived from it, and 1 independent denominator.


<br>

---

## Periodic Snapshot Tables

A periodic snapshot fact table writes 1 row per entity per period on a schedule, even for an entity nothing happened to. The grain reads 'one product per warehouse per day' or 'one account per month end', and the measures are levels: quantity on hand, balance, headcount, open ticket count. The table exists to turn the level question from a sum over history into a lookup of a single period.

![Schema diagram 2](images/dm_pattern_fact_table_selection_diagram_2.png)

| snapshot_date_key | product_key | warehouse_key | quantity_on_hand | quantity_on_order | quantity_reserved | inventory_value |
| --- | --- | --- | --- | --- | --- | --- |
| 20260301 | 48812 | 7 | 348 | 500 | 12 | 6956.52 |
| 20260302 | 48812 | 7 | 340 | 500 | 12 | 6796.60 |
| 20260303 | 48812 | 7 | 340 | 500 | 10 | 6796.60 |
| 20260304 | 48812 | 7 | 840 | 0 | 10 | 16791.60 |

*A row exists for March 3 although no movement touched the SKU in warehouse 7 that day. That guaranteed row is what makes the level a lookup.*


<br>

### Semi-additive measures

`quantity_on_hand` sums correctly across products and across warehouses: the on-hand total for warehouse 7 is the sum over its SKUs. It does not sum across days. Adding the 4 rows above gives 1,868 units, a figure that describes nothing in the building. Across time you can take the last value or the average, or the minimum and maximum, and the interviewer will ask you to write whichever one finance wants.

```sql
WITH month_end AS (
  SELECT
    DATE_TRUNC('month', calendar_date) AS month_start,
    MAX(date_key) AS last_date_key
  FROM dim_date
  WHERE calendar_date BETWEEN DATE '2026-01-01'
  AND DATE '2026-03-31'
  GROUP BY DATE_TRUNC('month', calendar_date)
)

SELECT
  m.month_start,
  w.warehouse_name,
  SUM(f.inventory_value) AS month_end_value
FROM fact_inventory_daily AS f
JOIN month_end AS m
  ON f.snapshot_date_key = m.last_date_key
JOIN dim_warehouse AS w
  ON f.warehouse_key = w.warehouse_key
GROUP BY m.month_start, w.warehouse_name
```

The wrong version of this query filters the fact table to the quarter and sums `inventory_value` directly, which adds 90 days of levels together and reports an inventory 90 times too large. Candidates write it under time pressure. The fix is to constrain the date dimension to 1 period before any SUM touches a semi-additive column.


<br>

### Building the snapshot by rolling forward

The snapshot is derived from the transaction table 1 period at a time: yesterday's level plus today's net movement. Materializing that roll-forward nightly moves the cost of the level question from every query to 1 batch job.

```sql
SELECT
  20260304 AS snapshot_date_key,
  COALESCE(y.product_key, m.product_key) AS product_key,
  COALESCE(y.warehouse_key, m.warehouse_key) AS warehouse_key,
  COALESCE(y.quantity_on_hand, 0) + COALESCE(m.net_quantity, 0) AS quantity_on_hand
FROM (
  SELECT
    product_key,
    warehouse_key,
    quantity_on_hand
  FROM fact_inventory_daily
  WHERE snapshot_date_key = 20260303
) AS y
FULL OUTER JOIN (
  SELECT
    product_key,
    warehouse_key,
    SUM(quantity) AS net_quantity
  FROM fact_inventory_movement
  WHERE movement_date_key = 20260304
  GROUP BY product_key, warehouse_key
) AS m
  ON y.product_key = m.product_key
    AND y.warehouse_key = m.warehouse_key
```

A late-arriving movement dated March 2 that lands on March 5 makes every snapshot from March 2 onward wrong, so the job must detect late data and rebuild forward from the earliest affected day. And because each day is built from the previous one, a corrupt snapshot propagates. A full rebuild from the transaction table on a fixed cadence, monthly for instance, bounds how far an error can travel.


<br>

### Sizing the period

- **2.92B** — snapshot rows per year at daily grain
- **70B** — rows per year at hourly grain
- **96M** — rows per year at monthly grain

Row count is entities times periods, and it does not shrink on quiet days, so the period is the only size control you have. A retailer with 200,000 SKUs in 40 warehouses pays a fixed daily cost of 8 million rows regardless of sales volume. Hourly grain multiplies that by 24 for a question almost nobody asks. Monthly grain divides it by 30 and loses every intra-month stockout. The interview answer picks daily, states the row count, and names the consumer who would need something finer.

✓Reach for a snapshot when

- The question is a level at a point in time
- The level is the net of millions of events
- Dashboards read the level for every entity at once
- Finance or a regulator needs a reproducible period-end figure

✗Stay with transactions when

- Entities number in the hundreds of millions and the level is rarely read
- The period would have to be finer than the event rate
- The measure is already additive and events are sparse
- Levels are needed only for a handful of entities on demand

The future requirement that breaks a daily snapshot asks about the state between 2 snapshots. When operations starts asking what was on hand at 11am when the truck arrived, the daily table cannot say. A good answer offers a finer period for the few warehouses that need it, or the transaction table for that single question, and quotes the row-count cost of each.


<br>

---

## Accumulating Snapshot Tables

An accumulating snapshot fact table holds 1 row per instance of a process that has a defined start and end and a known sequence of milestones between them. An order moves from placement to pick to pack to ship to delivery. A mortgage application moves from submitted to underwritten to approved to funded. A support ticket moves from opened to assigned to resolved. The row is inserted when the instance starts, and each milestone writes its date into a column that was NULL until that night.

![Schema diagram 3](images/dm_pattern_fact_table_selection_diagram_3.png)

Five foreign keys point at the same date dimension, 1 per milestone. That is a role-playing dimension: 1 physical `dim_date` table joined under 5 aliases, so 'orders placed in March but delivered in April' is a filter on 2 of the 5 keys with no self-join anywhere.


<br>

### One row, rewritten as the order moves

| load night | order_id | order_date_key | picked_date_key | packed_date_key | shipped_date_key | delivered_date_key | order_to_ship_days | current_status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Mar 1 | O-2291 | 20260301 | NULL | NULL | NULL | NULL | NULL | placed |
| Mar 2 | O-2291 | 20260301 | 20260302 | 20260302 | NULL | NULL | NULL | packed |
| Mar 4 | O-2291 | 20260301 | 20260302 | 20260302 | 20260303 | 20260304 | 2 | delivered |

*The row count never grows past 1 per order. The columns fill in, and the lag is written on the night the shipped date arrives.*

This is the only fact type whose rows change after they're written, and the change is the whole point. The lag between milestones becomes a column subtraction stored once at update time rather than a self-join of 5 event types computed on every query. The price is that every assumption the transaction table gave you about immutability is gone: the row keeps changing for days or weeks after its first insert.

Measure the difference. The transaction table of status events holds 5 rows per order. The placed-to-shipped question needs the shipped row matched to the placed row for each order, a self-join over 2 of 5 event types across the whole date range, and then the median of the result. The accumulating row already holds `order_to_ship_days`, and the median is 1 pass over 1 column. For a month of orders the self-join reads 5 times the rows and repeats the match work on every run. The accumulating table did that match once, on the night the order shipped.


<br>

### Queries the accumulating row makes cheap

```sql
SELECT
  w.warehouse_name,
  DATE_TRUNC('month', ds.calendar_date) AS ship_month,
  COUNT(*) AS orders_shipped,
  APPROX_PERCENTILE(f.order_to_ship_days, 0.5) AS median_days_to_ship,
  APPROX_PERCENTILE(
    DATE_DIFF('day', dp.calendar_date, ds.calendar_date),
    0.5
  ) AS median_pack_to_ship_days,
  SUM(
    CASE
      WHEN f.order_to_ship_days <= 2 THEN 1
      ELSE 0
    END
  ) * 1.0 / COUNT(*) AS pct_within_2_days
FROM fact_order_fulfillment AS f
JOIN dim_warehouse AS w
  ON f.warehouse_key = w.warehouse_key
JOIN dim_date AS ds
  ON f.shipped_date_key = ds.date_key
JOIN dim_date AS dp
  ON f.packed_date_key = dp.date_key
GROUP BY w.warehouse_name, DATE_TRUNC('month', ds.calendar_date)
```

Leadership's whiteboard question, how many orders are sitting unshipped right now, is also cheap. Open instances are rows where a milestone column is still NULL, and their age is the gap between today and the order date.

```sql
SELECT
  w.warehouse_name,
  f.current_status,
  COUNT(*) AS unshipped_orders,
  MAX(DATE_DIFF('day', d.calendar_date, CURRENT_DATE)) AS oldest_age_days
FROM fact_order_fulfillment AS f
JOIN dim_date AS d
  ON f.order_date_key = d.date_key
JOIN dim_warehouse AS w
  ON f.warehouse_key = w.warehouse_key
WHERE f.shipped_date_key IS NULL
GROUP BY w.warehouse_name, f.current_status
ORDER BY unshipped_orders DESC
```


<br>

### The cost of updating in place

On storage that rewrites whole files, an in-place update means rewriting the partition that holds the row. Partition by `order_date_key` and the rewrite set stays bounded: an order placed on March 1 is touched by nightly loads until it is delivered, and after a close-out window its partition is never rewritten again. If the typical order closes in 4 days and the slowest 1 percent close within 45, the nightly job rewrites 45 partitions and leaves the other 320 of the year untouched. Lag columns are stored rather than computed so that the rewrite does the subtraction once.

- **3.75M** — fulfillment rows added a day
- **1.37B** — process rows after one year


<br>

### Repeating milestones break it

The type assumes a fixed sequence of milestones, each occurring at most once. A ticket reopened 3 times has 3 resolved dates and 1 column. A shipment split into 2 parcels has 2 delivered dates. Each fix is a compromise: keep the first or latest date plus a `reopen_count` measure, add a column per repeat up to a cap, or push the repeating milestone back into a transaction table of status changes and keep the accumulating row for the single-occurrence milestones only. Name the compromise and its blind spot, and the interviewer hears judgement rather than a memorized pattern.

✓Do

- Partition by the start-date key so nightly rewrites touch only partitions holding open instances
- Store lag columns at update time: a subtraction done once beats one done on every query
- Keep a current_status column so open-item dashboards filter without testing 5 NULL columns
- Keep the transaction table of status events alongside it as the source of truth

✗Don't

- Use it for a process whose milestones can repeat or loop back, or that has optional branches
- Leave the close-out window open-ended, because unbounded rewrites eat the nightly budget
- Store only timestamps: a date key per milestone is what joins to each dim_date role
- Treat a row as an event: 1 row is 1 order, so COUNT(\*) counts orders, never line items


<br>

---

## Factless Facts and Coverage Tables

A factless fact table has foreign keys and no numeric measure. The row itself is the fact: this student attended this class on this date, this product was on promotion in this store on this day, this job posting was visible to this candidate. There are 2 flavors, and they serve different questions. An event factless table records something that happened but carries nothing to measure. A coverage table records something that was possible, so that you can find what didn't happen.

![Schema diagram 4](images/dm_pattern_fact_table_selection_diagram_4.png)


<br>

### The question only coverage can answer

Which promoted products sold nothing on a day they were promoted? The sales fact has no rows for products that did not sell, so no filter on it can produce the list. The coverage table supplies the denominator: every product on promotion, in every store, on every day the promotion ran, written on the day the condition held. Absence becomes a LEFT JOIN followed by a NULL test.

```sql
SELECT
  c.coverage_date_key,
  s.store_name,
  p.product_name
FROM fact_promotion_coverage AS c
JOIN dim_store AS s
  ON c.store_key = s.store_key
JOIN dim_product AS p
  ON c.product_key = p.product_key
LEFT JOIN fact_store_sales AS f
  ON f.sale_date_key = c.coverage_date_key
    AND f.store_key = c.store_key
    AND f.product_key = c.product_key
WHERE c.coverage_date_key BETWEEN 20260301 AND 20260307
  AND f.product_key IS NULL
```

The same shape answers every absence question: customers eligible for an offer who did not redeem, employees scheduled who didn't clock in, pages served that received no click. In each case the coverage table holds what was true, the transaction table holds what happened, and the gap between them is the business question. The coverage table must sit at exactly the grain of the transaction table it joins against. Coverage at product-by-store-by-day joined to sales rolled up to product-by-store-by-week matches each weekly sales row to 7 coverage rows and reports 7 times the sales. Reverse the mismatch and the coverage side fans out: a weekly coverage row matches up to 7 daily sales rows, so counts of promoted combinations run up to 7 times too high and a day with no sale never surfaces as a NULL.


<br>

### Why a promotion flag on the product fails

The tempting shortcut is an `on_promotion` flag on `dim_product`. It fails on 2 axes at once: a promotion applies in some stores and not others, and it runs between a start date and an end date. Tracking that as a slowly changing attribute means a new product version for every store and date combination, which turns a 200,000-row dimension into millions of versions and still cannot say which store. The coverage table places the condition at the grain it varies on: product by store by day.

> Phrases that always mean coverage
>
> - Which of the eligible ones did not: redeem, sell, attend, respond, convert
> - What share of what we offered was taken up, where the offer is the denominator
> - Show me every combination that was live, including the ones with zero activity


<br>

### Sizing

Coverage rows scale with eligible combinations, and the combinations are dense in the way a snapshot is. A week-long promotion on 5,000 products across 2,000 stores is 70 million rows for 1 promotion. Running promotions every day of the year puts the table in the same size class as the daily inventory snapshot, which is the number to say out loud, because the interviewer wants to hear that you've seen the denominator cost more than the numerator.

- **3.65B** — coverage rows a year with daily promos
- **0** — numeric measures per row


<br>

### The attendance flavor

Event factless tables are the other flavor: `fact_class_attendance` with `student_key`, `class_key`, `date_key` and nothing else. The question is a count: how many students attended, what share of registered students attended, which classes fell under 50 percent attendance. COUNT(\*) is the measure. Some teams add a column named `attended` holding the constant 1 so that SUM works in every reporting tool. It costs 1 byte per row and spares the analyst a special case. The pair of tables, registration coverage and attendance events, has the same shape as promotion coverage and sales.

✓Event factless

- A row says something happened
- Written when the event occurs
- The measure is COUNT(\*)
- Sparse, like a transaction table
- Attendance, logins, impressions

✗Coverage

- A row says something was possible
- Written for every period the condition holds
- The measure is the join to what happened
- Dense, like a periodic snapshot
- Promotions, eligibility, schedules


<br>

### Putting the 4 together

The whiteboard question now has a complete answer with named parts. Daily revenue by product comes from the transaction table at order-line grain. Pack-to-ship time by warehouse comes from the accumulating snapshot, 1 row per order with a date key per milestone. Orders stuck unshipped is a NULL test on that same accumulating row. The moment someone asks which listed products were never ordered, that's a coverage table of listings joined against order lines. So 2 tables serve the 3 consumers, with the transaction table underneath feeding the accumulating snapshot, and a third table is ready for the absence question that always arrives.

> **TIP**
>
> When you present the design, give each table its grain sentence and its row-count formula with a number attached. Then say how it is written and which 1 question it cannot answer.


<br>

---

## Putting It All Together

A food delivery company asks you to model the order side of its business. Every tap, order line, courier assignment and payment is an event. Finance closes the month on gross order value by city. Operations tracks the time from order placed to restaurant accepted to courier pickup to doorstep, and wants a live list of orders past 45 minutes with no pickup. Marketing runs restaurant-level promotions in some zones and wants to know which promoted restaurants received zero orders during the promotion. Supply planning wants, for every zone and hour, how many couriers were online whether or not they took an order.

- Order lines and payments are a transaction fact at 1 row per order line, written at placement, with refunds as reversing rows, and finance's monthly close is a GROUP BY over it.
- The placed-accepted-pickup-doorstep lifecycle is an accumulating snapshot at 1 row per order, partitioned by order date and rewritten only inside a 1-day close-out window. Each of its 4 milestones carries a timestamp alongside its date key, and the 45-minute list filters on placed timestamps older than 45 minutes where the pickup timestamp is NULL.
- Promoted restaurants with zero orders need a coverage table at restaurant by zone by day, LEFT JOINed to the order transaction table at the same grain, with the NULL side as the answer.
- Couriers online per zone per hour is a periodic snapshot at hourly grain, because headcount is a level, it does not sum across hours, and a row must exist for zones where nobody ordered.
- The order transaction table feeds the accumulating lifecycle table, the courier snapshot is built from courier online and offline events, and the promotion coverage table is loaded from the promotion schedule. A complete design names those 3 sources and states that an hourly courier snapshot across 400 zones is 3.5 million rows a year, while the order lifecycle table is bounded by orders rather than events.


<br>

---

## Key Takeaways

- Fact table type is the product of 2 decisions: what 1 row means, and whether that row is rewritten as a process advances or only inserted, once or every period.
- Transaction tables are the source of truth: 1 row per event, immutable, fully additive, with rows that scale with event volume and a level question that costs a scan of all history.
- Periodic snapshots make the level question a lookup at the price of entities times periods rows per year, and their level measures never sum across time.
- Accumulating snapshots hold 1 row per process instance, rewritten as milestones arrive, so lags are a stored subtraction and stuck items are a NULL test, bounded by a close-out window on the start-date partition.
- Coverage tables supply the denominator for absence questions, sit at exactly the grain of the transaction table they join, and cost as many rows as a snapshot.
- A complete answer pairs tables and gives each one a grain and a row count, along with the future requirement it can't meet.
