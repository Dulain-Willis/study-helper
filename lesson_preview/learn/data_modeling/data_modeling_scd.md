Intermediate·Data Modeling · 17 min · 12 challenges

# Change Tracking

Spotify's analytics team runs a monthly report: streams per subscription plan. On March 15, Ines upgrades from Free to Premium, and the customer table records her new plan. The next morning, February's Free stream count is lower than it was yesterday, and February's Premium count is higher. No February data changed. How the team stores Ines's plan change decides which report they can trust.


<br>

---

## The History Problem

A streaming service keeps 2 tables. The `customers` table holds one row per customer: who they are and which plan they pay for. The `streams` table holds one row per song play, and each play carries the `customer_id` of the person who pressed play. Finance asks one question every month: how many streams came from Free customers and how many came from Premium customers.

customers, as loaded on March 1

| customer_id | name | plan | country |
| --- | --- | --- | --- |
| 1042 | Maya Chen | Free | US |
| 2210 | Jonas Berg | Premium | SE |

streams

| stream_id | customer_id | streamed_at |
| --- | --- | --- |
| 501 | 1042 | 2024-02-10 |
| 502 | 1042 | 2024-02-27 |
| 503 | 2210 | 2024-02-14 |
| 504 | 1042 | 2024-03-20 |

The report joins the 2 tables on `customer_id`, groups by month and plan, and counts. Run on March 1, it says February had 2 Free streams and 1 Premium stream, which matches what happened.

```sql
SELECT
  DATE_TRUNC('month', s.streamed_at) AS month,
  c.plan,
  COUNT(*) AS streams
FROM streams AS s
JOIN customers AS c
  ON c.customer_id = s.customer_id
GROUP BY DATE_TRUNC('month', s.streamed_at), c.plan
```

Ines upgrades on March 15. The nightly load reads the source system, sees `plan = 'Premium'` for customer 1042, and writes that value into her row. The `customers` table has the same 2 rows it had before, with one cell changed. On March 16 the report runs again.

| report run on | February Free streams | February Premium streams |
| --- | --- | --- |
| March 1 | 2 | 1 |
| March 16 | 0 | 3 |

The load never touched the `streams` table, and both of Ines's February plays still sit there with their February dates. The join found her row, read Premium, and assigned her February listening to a plan she did not have in February. The one fact that would have kept the report stable, that Ines was Free until March 15, existed in the database right up to the moment the load wrote over it.

Overwriting feels harmless because the current value is right. The damage shows up in every question about the past:

> Questions one overwritten cell destroys
>
> - How many Free customers upgraded in March? Every upgrader already reads as Premium, so the count is 0.
> - Did the February ad campaign reach Free listeners? The campaign's audience now reads as partly Premium.
> - Why does the February report differ from the copy finance saved on March 1? The database holds no reason.

Tables like `customers`, which describe a thing that other tables point at by id, are called dimensions. Their values change rarely compared with the events that reference them: a customer changes plan a few times a year and streams a few songs an hour. A dimension with that pattern is a slowly changing dimension. When one of its values changes, you can overwrite the value (Type 1), add a new row for each version (Type 2), or keep the previous value in a second column (Type 3). The rest of this lesson builds each one on Ines's upgrade and shows which reports survive it.

> **TIP**
>
> Before overwriting any column, ask one question: will a report ever group or filter by this column's value on a past date? If yes, the overwrite rewrites that report.


<br>

---

## Type 1: Overwrite Old Value

Type 1 keeps one row per customer and replaces old values with new ones. The table always describes the present, and nothing in it says what came before. The load in the previous section was Type 1 by default, which is how most teams end up with it: a daily job reads the source, matches rows by `customer_id`, and writes whatever the source says now.

Written as a transformation, a Type 1 refresh takes the current dimension, left joins today's changed rows, and prefers the new value wherever one exists. The result has the same number of rows as the input, and the pipeline writes it back as the new `customers` table. Engines that support updating rows in place reach the same state with an UPDATE statement.

```sql
SELECT
  c.customer_id,
  COALESCE(u.name, c.name) AS name,
  COALESCE(u.plan, c.plan) AS plan,
  COALESCE(u.country, c.country) AS country
FROM customers AS c
LEFT JOIN todays_changes AS u
  ON u.customer_id = c.customer_id
```

This query handles changes to existing customers only. A customer who signs up today appears in `todays_changes` with no match in `customers` and drops out of the LEFT JOIN, so new rows are appended separately, or the join becomes a FULL OUTER JOIN that takes `customer_id` from whichever side has it. `COALESCE` also cannot clear a value: a source that sets a column to NULL looks the same as a source that sent no change, so a cleared value needs an explicit flag on the change row.

| state | customer_id | name | plan | country |
| --- | --- | --- | --- | --- |
| before March 15 | 1042 | Maya Chen | Free | US |
| after March 15 | 1042 | Maya Chen | Premium | US |

Type 1 is the right choice when the old value was wrong, or when no report will ever care what it used to be. Suppose the source had Ines's name as 'Ines Moreu' and support corrected the typo. Keeping 'Ines Moreu' as a historical version would preserve a mistake, and a report grouped by name would show 2 people where there is 1. The correct name should apply to every stream she ever played, which is what the overwrite does.

The same logic covers attributes that are looked up rather than reported on: email address, phone number, a marketing opt-in flag. Analysts filter by them today and never ask what they were last quarter. Type 1 also costs the least. The table holds one row per customer, joins match on `customer_id` alone, and no query needs date logic to find the right version.

Write the decision down. A table comment that says 'name, email: Type 1; plan, country: Type 2' tells the next engineer which overwrites are intended, and tells an analyst why a filter on name sees one value per customer while a filter on plan may see several.


<br>

### Where Type 1 breaks

Type 1 breaks the moment a reported column changes. The `plan` column appears in a report every month, so overwriting it restates every month. `country` has the same problem for any revenue-by-market report. A second, quieter cost: once a value is overwritten, no backfill can recover it. If finance decides in June that they want February streams by the plan each customer held in February, the data to rebuild that report no longer exists anywhere in the warehouse.

✓Do

- Use Type 1 for corrections: the old value was wrong
- Use Type 1 for attributes no report groups or filters by
- Record which columns are Type 1 in the table's documentation
- Keep raw copies of source extracts so a decision can be revisited

✗Don't

- Overwrite a column that any report breaks down by
- Assume a column is harmless because it changes rarely
- Let a daily load overwrite every column without a per-column decision


<br>

---

## Type 2: Preserve Full History

Type 2 keeps every version of a customer as its own row. When Ines upgrades, her Free row stays where it is and gets an end date, and a new Premium row is added beside it. 3 columns carry the versioning: `valid_from`, the first day the row's values were true; `valid_to`, the first day they no longer held, so the row covers only dates before it, or NULL while they still hold; and `is_current`, a flag that marks the latest row so the most common query stays short.

| customer_version_id | customer_id | name | plan | country | valid_from | valid_to | is_current |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 7 | 1042 | Maya Chen | Free | US | 2022-06-01 | 2024-03-15 | false |
| 9 | 1042 | Maya Chen | Premium | US | 2024-03-15 | NULL | true |

Notice the first column. Once `customer_id` appears on 2 rows, it no longer picks out a single row, so each version gets its own number, `customer_version_id`. The date columns follow a half-open convention: a row is in effect from `valid_from` up to but excluding `valid_to`. March 15 belongs to the Premium row, and the 2 ranges touch without overlapping.


<br>

### Joining to the right version

The streams report now has to choose which of Ines's rows each play belongs to. The rule comes straight from the date columns: match the row whose range contains the stream date. The join condition carries that rule, and everything else about the query stays as it was.

```sql
SELECT
  DATE_TRUNC('month', s.streamed_at) AS month,
  d.plan,
  COUNT(*) AS streams
FROM streams AS s
JOIN customers AS d
  ON d.customer_id = s.customer_id
    AND s.streamed_at >= d.valid_from
    AND s.streamed_at < COALESCE(d.valid_to, DATE '9999-12-31')
GROUP BY DATE_TRUNC('month', s.streamed_at), d.plan
```

Trace Ines's 3 plays through it. Stream 501 on February 10 satisfies `2022-06-01 <= date < 2024-03-15`, so it matches version 7, plan Free. Stream 502 on February 27 matches the same row. Stream 504 on March 20 fails the first row's upper bound and matches version 9, plan Premium. February reads 2 Free and 1 Premium whenever the report runs, whether that is March 16, June 1 or any later day.


<br>

### Writing a change

A Type 2 refresh does 2 things to a changed customer: it closes the open row by setting `valid_to` to the change date, and it inserts a new open row. As a single transformation that rebuilds the table, that is every existing row, with the changed ones closed, stacked with UNION ALL on top of the new versions. The `COALESCE` closes a matched row, and the `is_current` expression turns the flag off for the same row. Each new version needs a fresh `customer_version_id`, which the load assigns before this step; here it arrives precomputed in `todays_changes` as `new_version_id`.

```sql
SELECT
  d.customer_version_id,
  d.customer_id,
  d.name,
  d.plan,
  d.country,
  d.valid_from,
  COALESCE(chg.changed_on, d.valid_to) AS valid_to,
  d.is_current
  AND chg.customer_id IS NULL AS is_current
FROM customers AS d
LEFT JOIN todays_changes AS chg
  ON chg.customer_id = d.customer_id
    AND d.is_current = TRUE

UNION ALL

SELECT
  chg.new_version_id,
  chg.customer_id,
  chg.name,
  chg.plan,
  chg.country,
  chg.changed_on,
  CAST(NULL AS DATE),
  TRUE
FROM todays_changes AS chg
```

With these columns in place, 2 more queries become short. The current state of every customer is `WHERE is_current = true`. The state on any past date, say the list of Premium customers on 2024-01-31, is the same range test used in the join with a literal date in place of the stream date. That second query is the one Type 1 can never answer, because Type 1 has no date to test.

> Type 2 pitfalls to check in review
>
> - Overlapping ranges: 2 open rows for one customer double-count every stream in the join.
> - Gaps between valid_to and the next valid_from: streams in the gap match no row and vanish from the report.
> - Fast-changing columns: a last_login_at column on a Type 2 table adds a row per customer per day.
> - Late changes: a plan change reported a week after it happened must split the open row at the real change date rather than today's date.

*The 'slowly' in the name is a limit on how often a column may change. Columns that change often belong in an events table rather than on a Type 2 dimension, where every change adds a row.*


<br>

---

## Type 3: Keep Previous Value

Type 3 keeps one row per customer, as Type 1 does, and adds columns that remember the value before the latest change: `previous_plan` and `plan_changed_on`. Ines's row after the upgrade holds both plans side by side, and Jonas, who has never changed plan, carries NULL in both new columns.

| customer_id | name | plan | previous_plan | plan_changed_on |
| --- | --- | --- | --- | --- |
| 1042 | Maya Chen | Premium | Free | 2024-03-15 |
| 2210 | Jonas Berg | Premium | NULL | NULL |

The design exists for one kind of question: comparing the current state with the one before it, across the whole table, with no date arithmetic. The product team wants an upgrade funnel for March: how many customers moved from Free to Premium, from Premium to Family, and so on. On a Type 3 table that is a group by on 2 columns with a filter on the change date.

```sql
SELECT
  previous_plan,
  plan,
  COUNT(*) AS customers
FROM customers
WHERE plan_changed_on >= DATE '2024-03-01'
  AND plan_changed_on < DATE '2024-04-01'
GROUP BY previous_plan, plan
```

The same shape answers 'show this quarter's revenue under the old sales territories and the new ones', the example textbooks introduce the pattern with, and 'which customers were in market X before the regional split'. In each case the report wants 2 columns next to each other on one row, which takes a self-matching query on Type 2 and has no answer at all on Type 1.

Type 3 also handles the moment when a company changes everything at once. If every customer's region changes on the same day because the market map was redrawn, Type 2 doubles the table overnight with 1 new row per customer, while Type 3 adds 1 column and lets every report run both ways until the old grouping is retired.


<br>

### What Type 3 forgets

Type 3 remembers exactly one step back. If Ines moves from Premium to Family on June 1, `plan` becomes Family, `previous_plan` becomes Premium, and Free leaves the table. A funnel run in July cannot see that she was ever Free. The pattern suits attributes that change once, or where only the most recent transition matters to anyone.

It also leaves the February report from the first section broken. The streams join still matches `customer_id` to one row, and that row says Premium, so Ines's February plays still count as Premium. Type 3 records that a change happened and what the prior value was. The row carries no date range, so the join has nothing to test a stream date against.

✓Type 3 answers

- How many customers moved from Free to Premium this month?
- Which customers changed country in Q1, and from where?
- Revenue by current region next to revenue by prior region

✗Type 3 cannot answer

- Which plan did Ines have on February 10?
- What were all 3 of her plans across the last 2 years?
- How do February streams split by the plan in force in February?


<br>

---

## Choosing Your Strategy

The choice is made one column at a time, and the input is the list of questions reports ask. For each column that can change, find out whether any report groups, filters or joins by its value as of a past date. Overwrite the column if the answer is never, and version it if the answer is yes. If the only historical question is 'what was it right before this', 2 extra columns cost less than versions.

| Question the report asks | Type | Why |
| --- | --- | --- |
| Current value only, or correct a mistake | 1 | One row, no date logic, the old value had no reporting use |
| Value as of the date of each event | 2 | Date ranges let each event find its own version |
| Current value beside the one before it | 3 | Old and new values sit in 2 columns on one row, no join on dates |
| Full change history for a customer | 2 | Every version is a row; Type 3 keeps 1 step |

Applied to the `customers` table: `name` is Type 1, because the only change it sees is a correction. `email` is Type 1. `plan` is Type 2, because the monthly streams report depends on the plan in force on the stream date. `country` is Type 2 for the same reason, with a `previous_country` column added if the markets team wants a one-step migration report without a date join. That puts 3 policies on the one table.


<br>

### Mixing types in one table

A Type 2 table can carry Type 1 columns, with one rule: a Type 1 change applies to every version of that customer. When Ines's name is corrected, both version 7 and version 9 get the new spelling, so a report grouped by name sees one person. A Type 1 change that touches only the current row splits one customer into 2 names and looks exactly like a Type 2 change nobody intended.

A second common hybrid adds the current value to every version. A `current_plan` column on all of Ines's rows lets one join produce February streams by the plan she had then and by the plan she has now, which is how teams reconcile the old report with the restated one. The cost is that every plan change updates every historical row for that customer.

- Row growth: Type 2 rows per customer equal versions, so estimate changes per year before choosing
- Query authors: every join to a Type 2 table needs the date range, so document the convention
- Corrections: decide in advance whether a bad value gets a new version or a rewrite of all versions
- Late changes: Type 2 loads need logic to split a row at a past date
- Storage: Type 3 adds 2 columns, Type 2 adds rows, Type 1 adds nothing

Change requests rarely arrive together. They come one at a time, months apart, against a table that already holds versions, so each one gets the same per-column test with the existing rows in mind. A column that changes often and that no report reads, such as a phone number, is overwritten across every version. Versioning it would add a row per change, and plan or tier reports would then scan versions that differ in nothing they group by.


<br>

---

## Putting It All Together

A subscription streaming service closes its quarter. Finance needs February streams by plan to match the figures it published on March 1, product wants the count of Free to Premium upgrades in March, and support has corrected 400 misspelled names since January.

- The plan column is Type 2, so February streams join to the plan in force in February and the published figures hold.
- The upgrade count comes from versions starting in March with plan Premium whose preceding version for the same customer had plan Free, or from a previous_plan column if the team added one.
- The 400 name fixes are Type 1 and are written to every version, so no customer appears under 2 spellings.
- Email and phone stay Type 1 on the same table, keeping the version count driven by plan changes alone.


<br>

---

## Key Takeaways

- An overwrite keeps the present and deletes the past: any report grouped by that column on a past date restates.
- Type 1 suits corrections and lookup attributes that no report breaks down by.
- Type 2 adds a row per version with valid_from and valid_to, and every join to it carries the date range.
- Type 3 keeps the current value and the one before it on a single row, for one-step comparisons.
- Decide one column at a time from the questions reports ask, and apply Type 1 fixes across all versions.
