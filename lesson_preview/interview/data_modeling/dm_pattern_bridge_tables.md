Advanced·Data Modeling · 25 min · 7 challenges

# Bridge Tables: Advanced

Priya is 40 minutes into an onsite at Spotify when the interviewer draws 2 boxes on the whiteboard: a streams fact and a track dimension beside it. "Many tracks credit more than 1 artist," she says. "The royalties team wants streams per artist, and finance insists the artist totals add back up to the company total. Model it." Priya knows the join that answers the first request and the double count it causes for the second. The question turns on where the many-to-many relationship lives, how each artist's share is decided, and what happens when a credit changes after the fact. This lesson works through the bridge table pattern the way that interview expects it: the fan-out it prevents, the structure of the bridge, the weighting factor that makes totals reconcile, the array column that competes with it, and the way to defend the choice out loud.


<br>

### What you will be able to do

Spot a multi-valued dimension in any domain and predict the fan-out a plain join produces

Design a bridge table, choose between a group key and a direct key, and state what 1 row of each table means

Assign decimal weights that sum to 1 per group, test them, and know which measures may never be allocated

Decide between a bridge and an array or map column from the queries, the tooling and the rate of change

Deliver the design in a 2-minute spoken answer and handle the follow-ups about history, testing and scale


<br>

---

## When One Row Maps to Many

### The grain holds until the second dimension arrives

Start with a fact table that behaves. `fact_streams` holds 1 row per track per country per day with 2 measures, `stream_count` and `revenue_usd`. `dim_track` holds 1 row per track. Every fact row finds exactly 1 track row, so a join between them neither adds nor removes rows, and a `SUM(stream_count)` returns the same figure with or without the join. That property, 1 fact row in and 1 row out, is what every report on the table depends on.

Then the royalties team asks for streams by artist. Track 501, Midnight Drive, credits 3 artists: Lena Ko, DJ Orbit and Marco Vell. A mapping table `track_artist` holds 3 rows for track 501. Join the fact to that mapping and each fact row for track 501 becomes 3 rows. On 1 October the track recorded 1,200,000 streams across all countries. After the join the same day sums to 3,600,000.

| track_key | artist | stream_count after the join |
| --- | --- | --- |
| 501 | Lena Ko | 1,200,000 |
| 501 | DJ Orbit | 1,200,000 |
| 501 | Marco Vell | 1,200,000 |
| Total |  | 3,600,000 |

The damage hides because half of the result is right. Each artist did take part in 1,200,000 streams, so a dashboard filtered to Lena Ko shows a true number. Clear the filter and the total for the day climbs from 48,000,000 to 61,500,000, because every multi-artist track in the catalog has multiplied itself. No row is null and no query fails. The report with the filter and the report without it disagree, and the person who notices is in finance, 3 weeks later, reconciling against the payout file.


<br>

### Recognizing the question in disguise

Interviewers rarely say the words bridge table. They describe a fact whose rows can each relate to several members of 1 dimension, then ask for a report by that dimension. The pattern surfaces in any industry:

- A hospital visit with several diagnoses, and a cost report per diagnosis
- A bank account with joint holders, and a balance report per customer
- An order with several promotions applied, and discount spend per promotion
- A support ticket with several tags, and resolution time per tag
- A sales opportunity with 3 reps on the deal, and quota credit per rep
- An insurance claim spanning several coverages, and loss per coverage

The tell is a phrase like per diagnosis, per holder or per rep where the fact row can carry more than 1 of that thing, often followed by a second phrase: the totals have to tie out. The first phrase creates the fan-out. The second phrase forbids it. Hearing both means the interviewer wants an allocation, and an allocation needs a weight. Hearing only the first means an involvement count may be enough, and saying which one you heard is the opening move of a strong answer.


<br>

### Three questions before drawing anything

Ask the interviewer, in this order

- **How many members per group?** *typical and maximum*\
  A track averages 1.4 credited artists with a maximum of 12. A visit averages 3 diagnoses with a maximum of 40. The maximum sets the worst-case fan-out and decides whether a nested column stays readable.
- **Must totals reconcile?** *allocation or involvement*\
  Royalty payouts must sum to the revenue pool. A report of how many streams involved an artist may exceed it. The answer decides whether a weight exists at all.
- **Does membership change?** *and does history matter*\
  An artist credit added after a dispute changes the split from that date. If past payouts must stay as they were, the relationship needs effective dates.

Each answer feeds a decision later in the lesson. The reconciliation requirement decides whether the bridge carries a weight and which reports must apply it. The change frequency decides whether the bridge needs effective dates. The maximum group size, together with how often membership changes, bears on whether a nested column on the parent row can stand in for the bridge. One more fact matters once the members are listed: whether the same member sets recur across parents, as they do across the tracks of an album. That answer decides between a shared group key and a key on each parent. Asking before drawing shows the interviewer that the schema follows from requirements rather than from habit, and it buys the numbers you will need to defend the design later.

> **TIP**
>
> Name the double count before proposing anything. Saying 'a plain join triples track 501's streams, so the totals will not tie out' tells the interviewer you see the failure the pattern exists to prevent, and it earns the time to explain the fix.


<br>

---

## The Bridge Table Structure

A bridge table stores the many-to-many relationship in a table of its own so that the fact keeps its grain and the dimension keeps its grain. The fact never mentions artists. The track dimension never repeats a track. Between them sits a table whose only job is to say which artists belong to which track, and in what proportion.

![Schema diagram 1](images/dm_pattern_bridge_tables_diagram_1.png)

Read 1 row of each. A `fact_streams` row is 1 track, 1 country, 1 day. A `dim_track` row is 1 track, pointing at 1 artist group. A `bridge_artist_group` row is 1 artist's membership in 1 group, with its share. A `dim_artist` row is 1 artist. The bridge's primary key is the pair `(artist_group_key, artist_key)`, which is why both columns carry the PK badge, and that pair is what stops a member appearing in the same group twice, which would otherwise double that member's share without anyone touching the weight column. Once the bridge keeps history, later in this section, `effective_from` joins that key.


<br>

### Group key or direct key

The bridge can hang off the track directly, keyed by `track_key`, or off a separate group key that the track points to. The direct form is shorter to explain. The group form pays off when the same set recurs. An album of 12 tracks credited to the same 3 artists needs 36 direct rows or 3 group rows plus 1 group key on each track. A hospital with 40,000,000 visits and a few hundred thousand distinct diagnosis combinations compresses even further. When every set is unique to its parent, the group key adds a lookup with nothing to share, and the direct form is the right call.

| artist_group_key | artist_key | artist_name | weight | is_primary |
| --- | --- | --- | --- | --- |
| 9001 | 11 | Lena Ko | 0.500000 | true |
| 9001 | 12 | DJ Orbit | 0.250000 | false |
| 9001 | 13 | Marco Vell | 0.250000 | false |

*Track 501 and the 11 other tracks on the same album all carry artist_group_key 9001, so the 3 rows above serve 12 tracks.*


<br>

### The join that returns the right total

```sql
SELECT
  a.artist_name,
  SUM(f.stream_count * b.weight) AS allocated_streams
FROM fact_streams AS f
JOIN dim_track AS t
  ON f.track_key = t.track_key
JOIN bridge_artist_group AS b
  ON t.artist_group_key = b.artist_group_key
JOIN dim_artist AS a
  ON b.artist_key = a.artist_key
WHERE f.date_key = 20261001
GROUP BY a.artist_name
```

The path is fact to track to bridge to artist, and the multiplication by `b.weight` happens inside the `SUM` so that each fact row contributes its full count once, split across members. For track 501 on 1 October the query returns 600,000 for Lena Ko and 300,000 each for DJ Orbit and Marco Vell, which add back to 1,200,000. Remove `b.weight` from the expression and the same query returns 1,200,000 for each artist, which is the involvement report, a separate and legitimate answer that the next section takes up.


<br>

### Where the group key belongs

Attach the group key to the row the set describes. Artists are a property of the track, so `dim_track` carries the key. Diagnoses are a property of the visit, and the visit is the fact row, so the group key sits on the fact table itself, with the bridge joined straight to it and no dimension in between. Reps on a deal are a property of the opportunity, which is usually the fact grain, so the same placement applies. The question to ask is which row would change if the set changed, and that row owns the key.


<br>

### When membership changes

A credit dispute adds a fourth artist, Sami Hart, to every track on the Midnight Drive album from 1 December, and the new contract pays Lena Ko 40 percent and each featured artist 20 percent. Overwriting the 3 bridge rows with 4 rewrites every past payout, because the October facts now join to the December split. The repair is 2 columns on the bridge, `effective_from` and `effective_to`, and a new version of the group's rows instead of an update. Group 9001 keeps its key, so `dim_track` never changes, and the primary key becomes `(artist_group_key, artist_key, effective_from)`, which lets Lena Ko hold 1 row per version. Every row of a version shares the same 2 dates, and the old version's `effective_to` equals the new version's `effective_from`.

| artist_group_key | artist_key | weight | effective_from | effective_to |
| --- | --- | --- | --- | --- |
| 9001 | 11 | 0.500000 | 20250601 | 20261201 |
| 9001 | 12 | 0.250000 | 20250601 | 20261201 |
| 9001 | 13 | 0.250000 | 20250601 | 20261201 |
| 9001 | 11 | 0.400000 | 20261201 | 99991231 |
| 9001 | 12 | 0.200000 | 20261201 | 99991231 |
| 9001 | 13 | 0.200000 | 20261201 | 99991231 |
| 9001 | 14 | 0.200000 | 20261201 | 99991231 |

The join treats each range as half-open: a fact row matches a version when `f.date_key >= b.effective_from` and `f.date_key < b.effective_to`. Streams on 30 November match only the old rows, streams on 1 December match only the new ones, and the current version stays open with an `effective_to` of 99991231. An inclusive `BETWEEN` would put 1 December inside both versions, so that day's streams for each album track would join to 7 rows with weights summing to 2, and a track with 1,200,000 streams that day would be allocated 2,400,000.

```sql
SELECT
  a.artist_name,
  SUM(f.stream_count * b.weight) AS allocated_streams
FROM fact_streams AS f
JOIN dim_track AS t
  ON f.track_key = t.track_key
JOIN bridge_artist_group AS b
  ON t.artist_group_key = b.artist_group_key
    AND f.date_key >= b.effective_from
    AND f.date_key < b.effective_to
JOIN dim_artist AS a
  ON b.artist_key = a.artist_key
WHERE f.date_key BETWEEN 20261125 AND 20261205
GROUP BY a.artist_name
```

Versioning the group works because the change covers every track that shares it. When a credit changes on 1 track of a shared group, that track moves to a new group key instead, which is a change to `dim_track` and needs the track dimension to keep its own history; versioning group 9001 there would hand Sami Hart a share of 11 tracks the artist never appeared on. Effective dating is the future requirement most bridge designs skip, and an interviewer raises it once the base design is on the board. Adding it later means backfilling dates for every existing row, so design for it on day 1 even if the first load writes a single open-ended range per group.

> The 3 structural decisions
>
> - Key placement: the group key lives on whichever row would change if the set changed
> - Group key versus direct key: group when sets recur across parents, direct when every set is unique
> - Effective dates: present from the first load, because a change without them rewrites history


<br>

---

## Weighting Factors and Allocation

The same bridge answers 2 different questions, and the interview turns on knowing which one is being asked. An involvement report counts a stream for every artist on the track and ignores the weight. An allocated report splits each stream across the artists by weight. The first can exceed the fact total and is honest about it. The second must equal the fact total, to the stream.

Involvement, no weight

| artist | streams |
| --- | --- |
| Lena Ko | 1,200,000 |
| DJ Orbit | 1,200,000 |
| Marco Vell | 1,200,000 |
| Total | 3,600,000 |

Allocated, weight applied

| artist | weight | streams |
| --- | --- | --- |
| Lena Ko | 0.50 | 600,000 |
| DJ Orbit | 0.25 | 300,000 |
| Marco Vell | 0.25 | 300,000 |
| Total | 1.00 | 1,200,000 |

Both tables are correct. The left answers how many streams involved each artist, the figure a marketing team wants for a reach report. The right answers how many streams each artist is credited with, the figure finance wants for payouts. A dashboard that shows the left table under a heading that implies the right one is the most common bridge table bug in production, and the reason every report title should carry the word involved or allocated.


<br>

### Where the weights come from

The default weight is an equal split, 1 divided by the member count, assigned by the pipeline when nothing better exists. A better source is the business rule that already governs the money: a contract that gives the primary artist 50 percent and splits the remainder among featured artists, or a sales plan that credits the account owner 60 percent and the overlay specialist 40 percent. When 2 measures split by different rules, the bridge carries 2 weight columns, `stream_weight` and `revenue_weight`, and the invariant below holds for each column on its own.


<br>

### The invariant and its test

Every group's weights sum to exactly 1, version by version: each dated version of a group sums to 1 on its own, and the versions of a group together sum to the number of versions. The query that enforces it runs as a quality gate after each bridge load and before any report reads the table, and a non-empty result blocks the publish.

```sql
SELECT
  artist_group_key,
  effective_from,
  SUM(weight) AS weight_sum,
  COUNT(*) AS member_count
FROM bridge_artist_group
GROUP BY artist_group_key, effective_from
HAVING SUM(weight) <> 1
```

A bridge with 2 weight columns runs the same gate with `HAVING SUM(stream_weight) <> 1 OR SUM(revenue_weight) <> 1`, so neither split can drift while the other passes. A second gate compares the allocated total to the raw fact total for the load date, and passing the first gate does not guarantee it. A track with no bridge row drops out of the inner join and loses 100 percent of its streams from the artist report while the fact table still counts them. An `artist_key` with no row in `dim_artist` drops out at the last join and takes its share with it. Date ranges that overlap allocate a day's streams twice, and ranges that leave a gap allocate them not at all, even when every version sums to 1. Logging the difference between the 2 totals, rather than a pass or fail alone, tells you on the first morning how many streams went missing and in which direction.


<br>

### Rounding

Equal splits with 3, 6 or 7 members do not divide evenly in decimal. Stored as `DECIMAL(9,6)`, a 3-way split gives each member 0.333333, and the 3 together fall 1 millionth short of a full group. With tracks averaging 1.4 artists, most of a day's 48,000,000 streams sit on single-artist groups with a weight of exactly 1, so the loss is small: if 10,000,000 of the day's streams sit on 3-way equal splits, about 10 streams vanish from the allocated total. Small is still a failure, because finance reconciles exactly and because the weight gate above flags every group that falls short before any report runs. The fix assigns the leftover millionth to the primary member, so the primary carries 1 millionth more than the others, the group sums to 1 exactly, and the gate passes. Floating point columns make the shortfall vary with summation order, which is why the weight column is a decimal and the gate uses an exact comparison.

- **0.999999** — 3-way group sum before adjustment
- **0.333334** — primary member's adjusted weight


<br>

### Which measures allocate

✓Allocate by weight

- stream_count
- revenue_usd
- discount_amount
- cost_usd
- Any measure that adds across rows

✗Never allocate

- Distinct listeners (half a listener has no meaning)
- Ratios such as completion rate
- Maximums and minimums
- Counts of distinct tracks or days

Additive measures split cleanly because a fraction of a stream added to other fractions still totals streams. A distinct count does not, so listeners per artist comes from the involvement join with `COUNT(DISTINCT listener_key)`, and the report states that a listener who heard 2 of an artist's tracks counts once for that artist and may also count for another. Ratios are computed last, from allocated numerators and allocated denominators, never allocated themselves. Allocating a completion rate of 0.8 by a 0.5 weight produces 0.4, a figure with no meaning that still looks like a percentage on a chart.


<br>

---

## Bridges vs Array and Map Columns

Columnar warehouses support nested columns, and that gives the interviewer a second design to put on the table: store the artists on the track row itself as `artist_keys ARRAY(BIGINT)` and the shares as `artist_weights MAP(BIGINT, DECIMAL(9,6))`. The many-to-many relationship still exists. It now lives inside 1 row of `dim_track` instead of in 3 rows of a bridge, and the query has to unpack it.

```sql
SELECT
  a.artist_name,
  SUM(f.stream_count * u.weight) AS allocated_streams
FROM fact_streams AS f
JOIN dim_track AS t
  ON f.track_key = t.track_key
CROSS JOIN UNNEST(t.artist_weights) AS u(artist_key, weight)
JOIN dim_artist AS a
  ON u.artist_key = a.artist_key
WHERE f.date_key = 20261001
GROUP BY a.artist_name
```

The `UNNEST` turns the map back into 3 rows at read time, which means the nested design is a bridge that the query engine rebuilds on every read. Everything the bridge did explicitly, the map does implicitly, including the fan-out, which is why the weight still sits inside the `SUM` and why a query that forgets the weight inflates the total in exactly the same way.


<br>

### What each design makes cheap

| Operation | Bridge table | Array and map columns |
| --- | --- | --- |
| Streams for tracks involving artist 12 | Join through the bridge, then filter | `contains(t.artist_keys, 12)` with no extra join |
| Artist attributes in the report, such as home country | Join `dim_artist` as a normal dimension | `UNNEST` first, then join |
| A credit changes and history is kept | Add effective dates to bridge rows | Rewrite the track row; history survives only if the dimension is versioned |
| Weight invariant check | `GROUP BY` with `HAVING` | `UNNEST` then `GROUP BY`, or validate at write time |
| Drag and drop in a BI tool | Modeled as a standard join path | Unsupported in most semantic layers |
| Storage for a 3-artist track | 3 bridge rows | 1 wider track row |
| Group of 40 members | 40 rows, query shape unchanged | 40-element array, still 1 row |

Group size moves the balance between the 2 designs. A track's maximum of 12 artists makes a readable array and a map small enough to inspect by eye. A visit with 40 diagnoses still fits in 1 row, but correcting 1 diagnosis rewrites the whole array, and every per-member question, from the weight check to a join on diagnosis attributes, runs through `UNNEST` first. As the maximum grows and members change more often, each of those costs lands on more rows, and the bridge's 1 row per member becomes the cheaper layout to maintain.


<br>

### The decision rule

Choose the array when the set is read together with its parent, filtered by member key rather than by member attributes, consumed by engineers writing SQL rather than by analysts in a BI tool, and changed rarely enough that rewriting the row is acceptable. Tags on a support ticket meet every one of those conditions. Choose the bridge when the member has a dimension of its own with attributes that reports group by, when the history of membership matters, when weights encode a business rule that someone audits, or when a BI tool needs to walk the relationship. Artists with payouts meet every one of those, and the audit alone would settle it.

The 2 designs also coexist. Keep the bridge as the system of record, with its effective dates and its tested weights, and materialize `artist_keys` onto `dim_track` from the bridge in the same nightly build. Reports and payouts read the bridge. Search filters such as which tracks involve artist 12 read the array. Because the array is derived, a mismatch between the 2 is a pipeline defect rather than a modeling question, and a check that `cardinality(t.artist_keys)` equals the bridge's member count for the track's group catches it before anyone reads a stale filter result.


<br>

### The derived array is a snapshot

```sql
SELECT
  artist_group_key,
  ARRAY_AGG(artist_key ORDER BY artist_key) AS artist_keys
FROM bridge_artist_group
WHERE effective_to = 99991231
GROUP BY artist_group_key
```

The filter on `effective_to` is a decision, and it has a consequence the interviewer may probe: the array holds today's members only. After 1 December, `contains(t.artist_keys, 14)` is true for all 12 album tracks, so a report that filters October streams with it counts them as involving Sami Hart, who was credited on none of them until December. A historical filter has to go through the bridge and its dates. Keeping the array current suits search and recommendation features that ask who is on the track now. A team that wants the array to answer as of a date has to version `dim_track` as well, which rebuilds the bridge's history inside a second table and doubles the places a credit change must land.

✓Do

- Keep 1 design as the source of truth and derive the other from it
- Put the weight inside the SUM in both designs
- Test weight sums after every load, before publishing
- State involved or allocated in every report title

✗Don't

- Store weights as DOUBLE and expect the sum to land on 1
- Overwrite bridge rows when a credit changes
- Put array columns in front of a BI tool that cannot unnest them
- Allocate distinct counts by weight


<br>

---

## Explaining Your Bridge Design

The interviewer grades the order of the reasoning as much as the schema. An answer that opens with a bridge table and works backward to the problem sounds memorized. An answer that opens with the grain, finds the fan-out, and arrives at the bridge because nothing else keeps the totals honest sounds like someone who has shipped one and been paged about it.

The answer in 6 moves

- **State the grain**\
  1 row per track per country per day, with stream_count and revenue_usd.
- **Show the fan-out**\
  A track with 3 artists triples its own streams in a plain join, and the daily total rises with it.
- **Place the relationship**\
  A bridge keyed by an artist group on the track, because albums reuse the same set.
- **Define the weights**\
  Contract shares when they exist, equal split otherwise, stored as decimals, summing to 1 per group.
- **Prove reconciliation**\
  Allocated total equals the fact total on every load, enforced by a quality gate.
- **Name what breaks it**\
  A credit change without effective dates rewrites every past payout.


<br>

### A spoken answer, in order

"The fact is 1 row per track per country per day. Each fact row relates to several artists, so the relationship is many-to-many, and a direct join to artist multiplies rows; a 3-artist track reports 3 times its streams and the day's total climbs with it. I would keep the fact at its grain and add a bridge: the track points at an artist group key, the bridge lists each artist in the group with a decimal weight, and the weights sum to 1 per group. Royalty reports multiply stream_count by the weight inside the SUM, so totals tie to the fact. Reach reports skip the weight and say so in the title. The weights come from the contract split, 50 percent to the primary and the rest equal, with an equal split as the fallback. A quality gate checks weight sums and compares the allocated total to the raw total on every load. The bridge carries effective dates from day 1 because credits change, and without dates a December credit change would rewrite October's payouts."

That answer runs under 2 minutes, touches every decision the pattern contains, and leaves the interviewer with follow-ups rather than gaps. Each follow-up below probes 1 of the 6 moves, so an answer that covered all 6 has already prepared its own defense.


<br>

### Follow-ups and what each one probes

| Follow-up | What it probes | The core of the answer |
| --- | --- | --- |
| A fourth artist is added next year. What changes? | Effective dating | Close the old rows with effective_to set to the change date, open 4 new rows from that date with re-normalized weights, join on a half-open range. |
| Why not add artist_key to the fact? | Grain discipline | The fact would hold 3 rows per track, country and day for a 3-artist track, every other report would triple, and the weight problem moves into the fact. |
| Contract shares are unknown for half the catalog. | Missing rules | Equal split with is_primary set, a flag marking the weight as estimated, and a reload when contracts arrive. |
| How do you know the numbers are right? | Testing | Weight sum gate and allocated-versus-raw gate on every load, with the difference logged. |
| Marketing wants unique listeners per artist. | Measure additivity | Involvement join with a distinct count, labeled as overlapping across artists. |
| Could this be an array column instead? | Tradeoff judgement | Yes for filters, no for payouts with history and audited weights; derive the array from the bridge. |


<br>

### The number to bring

Bring the size of the bridge, because it decides whether the group key is worth its indirection. A catalog of 100,000,000 tracks averaging 1.4 credited artists produces 140,000,000 direct bridge rows. If 30 percent of tracks share their artist set with another track on the same album, the group form drops below 100,000,000 rows. Those are illustrative figures, and saying so is part of the answer. The point is that you measured the fan-out before choosing the form, and that the bridge stays a fraction of the fact table either way, so the fact scan dominates the join cost and the bridge's shape is chosen for correctness and reuse rather than for speed.

> Capstone problems
>
> - A visit fact has 1 row per visit with total_cost, and visits carry 1 to 40 diagnoses. Design the bridge, decide where the group key lives, and state which report must reconcile.
> - A sales fact credits 2 or 3 reps per opportunity, with the owner at 60 percent. A rep changes territory mid-quarter. Show bridge rows before and after, and the join condition.
> - A ticketing team stores tags as an array. Analysts want resolution time per tag in a BI tool. Argue for or against a bridge, and name the quality check if you add one.

> **TIP**
>
> If you want to practice explaining the design, say the spoken answer aloud with a different domain each time: diagnoses, joint account holders, promotions. The structure stays fixed while the nouns change, and that transfer is what the interview tests.


<br>

---

## Putting It All Together

An online retailer records 1 row per order line with net_revenue and discount_amount, and a line can carry up to 5 promotions at once: a coupon, a loyalty discount, a bundle price and 2 vendor-funded offers. Marketing wants revenue influenced by each promotion for reach reporting, and finance must bill each vendor for its exact share of the discount, with the shares summing to the discount on the line.

- The order line fact keeps its grain; a promotion group key on the line points to a bridge of promotion memberships, because the set belongs to the line rather than to any dimension.
- Marketing's influenced revenue is the involvement join, allowed to exceed total revenue and labeled as such in the report title.
- Finance's vendor billing is the allocated join, with discount_weight set from each funding agreement and tested to sum to 1 per group before publishing.
- A vendor renegotiates its funding share in November, so the bridge rows carry effective dates and the join checks the order date against them.
- A promotion_ids array on the line serves the storefront's filter for orders with coupon X, derived nightly from the bridge and checked against its member count.


<br>

---

## Key Takeaways

- A join through a multi-valued relationship multiplies fact rows, and the inflated totals look correct under a filter.
- The bridge holds the relationship in its own table so the fact and the dimension both keep their grain.
- A decimal weight per member, summing to 1 per group, turns an involvement count into an allocation that reconciles.
- Distinct counts and ratios come from the involvement join; only additive measures take a weight.
- Arrays rebuild the bridge at read time and win on filters; bridges win on history, attributes and BI tools.
- Effective dates on the bridge are the requirement that breaks a design without them.
