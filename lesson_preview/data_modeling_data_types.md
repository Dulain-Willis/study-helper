Intermediate·Data Modeling · 22 min · 12 challenges

# Schema Types

A billing team closes the month, reconciles the revenue report against the bank statement, and finds the two disagree by $4,712.38. No transaction is missing. No single row is wrong on its own. The gap traces back to a column someone declared as FLOAT 3 years earlier. Each column in a schema carries a decision like that one, and this lesson walks through the ones that break in production.


<br>

---

## The FLOAT Money Bug

The subscription billing platform this lesson models has 3 tables that matter here: `customers`, `invoices` and `invoice_lines`. One row of `invoice_lines` is one priced line on one invoice: a quantity, a unit price and a line total. The first migration declared `unit_price` and `line_total` as FLOAT because the ORM mapped a Python float to that type by default. The revenue report summed `line_total` and matched the bank for 2 years, then drifted the month volume crossed 10 million lines.

| invoice_line_id | invoice_id | quantity | unit_price | line_total |
| --- | --- | --- | --- | --- |
| 8841001 | INV-30417 | 3 | 19.99 | 59.97 |
| 8841002 | INV-30417 | 1 | 0.10 | 0.10 |
| 8841003 | INV-30418 | 2 | 0.20 | 0.40 |


<br>

### Why 0.1 has no binary form

FLOAT and DOUBLE follow IEEE 754: a sign, an exponent and a binary fraction. A value is exact only when it is a sum of powers of 2, so 0.5 and 19.75 are exact while 0.1 repeats in binary the way 1/3 repeats in decimal. The nearest 64-bit double to 0.1 is 0.1000000000000000055511151231257827, and every operation carries that difference forward. Width depends on the platform: FLOAT is 32-bit single precision in MySQL, as is REAL in PostgreSQL, while PostgreSQL's bare FLOAT is a 64-bit double. The billing platform's columns were the 32-bit kind, good for about 7 significant digits, which is why the drift reached dollars.

```sql
SELECT
  CAST(0.1 AS FLOAT8) + CAST(0.2 AS FLOAT8) = CAST(0.3 AS FLOAT8) AS equal_float,
  CAST(0.1 AS FLOAT8) + CAST(0.2 AS FLOAT8) AS total_float,
  CAST(0.1 AS NUMERIC) + CAST(0.2 AS NUMERIC) = CAST(0.3 AS NUMERIC) AS equal_decimal
```

| equal_float | total_float | equal_decimal |
| --- | --- | --- |
| false | 0.30000000000000004 | true |

Two effects follow. The first is drift under aggregation: summing 0.1 across 1,000,000 rows returns 100000.00000133288 in DOUBLE and 100958.34 in 32-bit REAL, because each addition rounds and the rounding never cancels. The second is failed matching: a reconciliation join on `invoices.total = payments.amount` drops every pair computed along different arithmetic paths, and the dropped rows are the discrepancy nobody can explain.


<br>

### DECIMAL and integer minor units

✓DECIMAL(p, s)

- p total digits, s of them after the point: DECIMAL(12,2) tops out at 9,999,999,999.99
- Addition, subtraction and multiplication are exact; division still rounds to s digits
- Readable in every tool with no conversion, which is why reporting schemas prefer it
- Arithmetic runs in software, roughly 3 to 10 times slower than hardware floats

✗BIGINT minor units

- $19.99 stored as 1999; the currency's minor unit is the whole unit of the column
- Integer arithmetic is exact and hardware-fast, with no rounding anywhere
- Needs a currency column beside it, because JPY has no minor unit and KWD has 3 digits
- Division by 100 belongs to the presentation layer, and every consumer has to know that

The platform's `invoice_lines` moves to `unit_price DECIMAL(12,4)` and `line_total DECIMAL(12,2)`; 4 places on the unit price give a $0.0025 per-request metered rate a home. Tax rates get `DECIMAL(6,5)`, which holds 0.08250 exactly. The rounding rule for a line total becomes a written policy, since 3 units at $0.0025 is $0.0075 and somebody decides whether that invoices as $0.01 or $0.00. Stripe's API takes every amount as an integer in the currency's smallest unit, the minor-units design applied to a whole payments company.

The integer design has a ceiling worth knowing. A 32-bit INT holds 2,147,483,647, so a running `balance_cents INT` on a customer account overflows after $21.47 million of lifetime billing, which one enterprise contract can reach. BIGINT removes that concern for any business that exists.

- **$92.2 quadrillion** — ceiling of BIGINT cents

FLOAT remains the right type where approximation is the nature of the value: sensor readings, GPS coordinates, ML feature vectors, percentages computed for a chart. Measurement error there dwarfs representation error, and hardware speed matters when a query scans billions of rows. The rule per column is one question: will anyone ever reconcile this number against another system? If yes, the type is exact.

> **TIP**
>
> Migrating an inherited FLOAT money column: `CAST(line_total AS DECIMAL(12,2))` rounds on the cast itself, so record `SUM(line_total)` before and after, and keep the difference in the migration ticket. Finance will ask for that number.


<br>

---

## String Types & Platform Traps

The `customers` table holds `full_name`, `email`, `country_code`, `currency_code` and `billing_address`, all text. The 3 families are CHAR(n), fixed width and space-padded; VARCHAR(n), variable width with a declared ceiling; and TEXT, variable width with no declared ceiling. What each does when real data arrives depends on the platform more than on the SQL standard.


<br>

### CHAR earns its place only for codes

A CHAR(3) column stores 'US' as 'US ' and pads every shorter value to the declared width. PostgreSQL treats the trailing spaces as insignificant in comparisons but keeps them in the stored bytes, MySQL strips them on read, and a value exported to a CSV carries them into whichever system reads it next. That is acceptable only when the value is the same width by definition. ISO 3166-1 alpha-2 country codes are CHAR(2), ISO 4217 currency codes are CHAR(3), and a width that matches the standard rejects 'USA' in a country column before it reaches a join.


<br>

### What each platform does past the limit

| Platform | Insert longer than VARCHAR(n) | VARCHAR(n) versus TEXT |
| --- | --- | --- |
| PostgreSQL | Error: value too long for type character varying(n) | Identical storage and speed; the limit is validation only |
| MySQL 5.7+ | Error in strict mode; truncated with a warning in non-strict mode | Same InnoDB storage for long values; TEXT is indexable only by prefix and cannot take a literal DEFAULT before 8.0.13 |
| Snowflake | Error; the default VARCHAR ceiling is 16,777,216 characters | No storage penalty for a large declared ceiling |
| BigQuery | Error when declared STRING(L); plain STRING has no limit | STRING is the only type; L is a check, not a storage hint |
| Oracle | Error; VARCHAR2(100 BYTE) and VARCHAR2(100 CHAR) count differently | Default semantics are bytes unless NLS_LENGTH_SEMANTICS says CHAR |

The MySQL row is the one that loses data. Without STRICT_TRANS_TABLES in `sql_mode`, a 300-character address inserted into VARCHAR(255) becomes a 255-character address and a warning the driver never surfaces. Strict mode has been the default since 5.7, but a server configured in 2014 and upgraded in place can still lack it, so the first check on an inherited MySQL instance is `SELECT @@sql_mode`.


<br>

### Characters, bytes and the number 191

VARCHAR(n) counts characters everywhere except Oracle in byte mode, but indexes count bytes. Under MySQL's `utf8mb4` a character takes up to 4 bytes, and InnoDB's older COMPACT row format caps an index key at 767 bytes. Dividing 767 by 4 gives 191, which is why Laravel migrations defaulted string columns to 191 characters for years and why `email VARCHAR(255)` with a unique index fails to create on those tables. The DYNAMIC row format, default since MySQL 5.7.9, raises the cap to 3,072 bytes for new tables.




<br>

### Sizing against the data that will arrive

A length chosen from the first 100 rows of sample data is the common failure. The rule that survives: a code gets CHAR of the standard's width, text a human types gets a VARCHAR whose ceiling comes from a published limit or a generous round number, and free text that can grow gets TEXT. Email addresses have a ceiling of 254 characters under RFC 5321, so `email VARCHAR(254)`. Personal names have no documented maximum, and a 40-character limit rejects Hubert Blaine Wolfeschlegelsteinhausenbergerdorff Sr., a real 53-character name, so `full_name VARCHAR(255)`. Addresses run past 200 characters in Japan and Brazil, so `billing_address TEXT`.

Some values look numeric and are text. A US postal code 02134 loses its leading zero in an INT column. A phone number in E.164 form is a plus sign and up to 15 digits, and the plus sign is data. Account numbers, order references and tax ids are never summed, so each is a string sized to the issuing system's format. The test is arithmetic: a value never added, subtracted or compared as a magnitude is text.


<br>

---

## Temporal Types & DST

In the `subscriptions` table, `started_at` is an instant that happened, `next_renewal_at` is an appointment that will happen, and `trial_ends_at` is a calendar day. In `payment_events`, `occurred_at` records each charge attempt, another instant. A type that fits one kind of time corrupts the others.

The temporal types and what one value means

- DATE: a calendar day with no clock and no zone. 2026-11-01 is a different span of instants in Tokyo and in Denver.
- TIME: a wall-clock time with no day. 09:00 is an appointment pattern, and it names an instant only once a day and a zone are attached.
- TIMESTAMP (naive): a day and a clock reading with no zone. 2026-11-01 01:30:00 could be any of roughly 38 instants, one per UTC offset in use.
- TIMESTAMPTZ: an instant on the global timeline, converted to UTC on write. The same instant reads back in any zone the session asks for.
- INTERVAL: a duration. '1 month' is calendar arithmetic; '2592000 seconds' is 30 days elapsed, and the two disagree in every month that is not 30 days long.


<br>

### The hour that happens twice

On 2026-11-01 the United States leaves daylight saving time: at 02:00 Eastern Daylight Time the clocks return to 01:00 Eastern Standard Time, so every reading between 01:00 and 02:00 occurs twice. On 2026-03-08 the readings between 02:00 and 03:00 never occur at all. A naive TIMESTAMP `payment_events.occurred_at` filled by a server set to America/New_York records 2 charge attempts an hour apart as the same value.

| Wall clock in New York | Actual instant (UTC) | Minutes since 00:30 local |
| --- | --- | --- |
| 2026-11-01 00:30:00 | 2026-11-01 04:30:00Z | 0 |
| 2026-11-01 01:30:00 (first time) | 2026-11-01 05:30:00Z | 60 |
| 2026-11-01 01:30:00 (second time) | 2026-11-01 06:30:00Z | 120 |
| 2026-11-01 02:30:00 | 2026-11-01 07:30:00Z | 180 |

In a naive column the second and third rows are indistinguishable, and elapsed time between them computes as 0 minutes for a gap of 60. As TIMESTAMPTZ they hold 05:30Z and 06:30Z, and the subtraction is correct in every zone. The point reaches past DST: subtracting 2 instants gives elapsed time, while subtracting 2 wall-clock readings is right only within one zone on a day without a transition.

PostgreSQL's TIMESTAMPTZ stores the UTC instant and discards the offset it arrived with: '2026-11-01 01:30:00-05:00' reads back in the session's `TimeZone`, and the Eastern origin is gone. Where the originating zone is a business fact (which local day a charge belongs to on a statement), it gets its own column, `occurred_tz VARCHAR(40)` holding an IANA name such as America/New_York.


<br>

### Past instants versus future appointments

A charge that already happened is an instant and belongs in TIMESTAMPTZ. A renewal at 09:00 local on the first of the month is an appointment, and converting it to a UTC instant today bakes in today's timezone rules. Mexico abolished daylight saving time in October 2022, so a Mexico City renewal stored as a UTC instant in 2021 fires an hour early from 2023 on; Egypt reinstated DST in April 2023 and shifted pre-computed instants the other way. The schema that survives law changes stores `renewal_time TIME` plus `renewal_tz` holding the IANA name, and computes the instant when the scheduler enqueues the job.

Durations split the same way. A 14-day trial is elapsed time and stores as `trial_days INT`. A monthly plan is calendar time: in PostgreSQL `started_at + INTERVAL '1 month'` from January 31 lands on February 28, and another month lands on March 28, so a subscription begun on the 31st drifts to the 28th forever unless `billing_day_of_month INT` is stored separately. Store the rule the business stated, and derive each date from it at the point of use.

| Platform | Naive civil time | Instant | Note |
| --- | --- | --- | --- |
| PostgreSQL | TIMESTAMP | TIMESTAMPTZ | Instant type keeps UTC and drops the offset |
| MySQL | DATETIME | TIMESTAMP | TIMESTAMP range ends 2038-01-19 03:14:07 UTC |
| Snowflake | TIMESTAMP_NTZ | TIMESTAMP_TZ keeps offset; TIMESTAMP_LTZ renders in session zone | Bare TIMESTAMP is an alias for NTZ by default |
| BigQuery | DATETIME | TIMESTAMP | A literal with no zone is read as UTC |

> **TIP**
>
> A MySQL TIMESTAMP cannot hold a value past January 19, 2038, so a 15-year contract end date written today fails on insert. On MySQL, store instants in DATETIME, in UTC by convention, enforced in the application's connection settings.


<br>

---

## ENUM Traps

`subscriptions.status` launches with 3 values: trialing, active and cancelled. ENUM looks built to reject anything else, and it is, for a set that never changes; subscription statuses at any company have never stayed still for a year. The 3 storage designs for a constrained value differ in what the fourth value costs.


<br>

### What ENUM gives and what it takes

In PostgreSQL an ENUM is a named type: `CREATE TYPE subscription_status AS ENUM ('trialing','active','cancelled')`. A value takes 4 bytes, an invalid insert fails, and `ORDER BY status` follows declaration order, which puts 'trialing' first where an alphabetical sort puts it last. `ALTER TYPE ... ADD VALUE` could not run inside a transaction block before version 12, and from 12 the new value is unusable until that transaction commits; renaming a value arrived in version 10. Removing a value has no statement: the path is a new type, an `ALTER COLUMN ... TYPE ... USING` cast that rewrites the table, and a drop of the old type.

MySQL stores an ENUM as a 1-based integer index into the declared list. Appending a value is an in-place change while the storage width holds; inserting one mid-list renumbers rows and rewrites the table, which on a billion rows locks writes for hours. Sharper still: in non-strict mode an invalid value is stored as '' with index 0, so an application typo writes a row whose status is nothing and whose insert succeeded.

✓ENUM

- Values live in the schema; adding one is a DDL change deployed with a migration
- Compact storage and a rejected insert on a bad value
- No place for a display label, a sort weight or a retirement date
- Right for sets fixed by definition: day of week, card suit

✗Lookup table with a foreign key

- Values live in rows of subscription_statuses; adding one is an INSERT
- Carries label, sort_order and retired_at beside the code
- Costs one join, usually against a table of fewer than 50 rows
- Right for any set a product manager has opinions about

Between the two sits `status VARCHAR(20)` with a CHECK listing the allowed values. Changing the list is a drop and re-add of the constraint, and PostgreSQL's `NOT VALID` skips the full-table scan at add time, so the swap takes milliseconds at any table size. The CHECK gives ENUM's rejection without ENUM's rebuild cost, and lacks only the labels and ordering a lookup table carries.


<br>

### Booleans that hold 3 values

A nullable BOOLEAN holds true, false and NULL, and `WHERE is_active = false` skips the NULLs, so a report of inactive subscriptions undercounts by every row whose flag was never set; `is_active BOOLEAN NOT NULL DEFAULT false` removes the third state. Flag creep is the second failure: `is_active`, `is_paused` and `is_cancelled` have 8 combinations and 3 valid ones, and nothing stops a row being active and cancelled, so 3 mutually exclusive flags are one status column. Third, a boolean can hide a timestamp: `cancelled_at TIMESTAMPTZ NULL` records that and when, and `cancelled_at IS NOT NULL` replaces `is_cancelled`.


<br>

### UUIDs as identifiers

When ids are generated outside the database, by a mobile client or a service that cannot wait for a sequence, UUIDs replace BIGINT. PostgreSQL's native `uuid` is 16 bytes; the same value in CHAR(36) is 36 bytes compared as text, which inflates every index and foreign key that references it. The version matters as much. A random UUIDv4 lands on a random index page, so InnoDB's clustered index splits pages continuously and a PostgreSQL btree fills with half-empty leaves. UUIDv7, standardised in RFC 9562 in 2024, leads with a millisecond timestamp so inserts append at the rightmost page the way a sequence does, and PostgreSQL 18 ships `uuidv7()` built in.


<br>

---

## Type Review Framework

A column's type is the cheapest quality control a schema has, applied by the database to every write with no pipeline, test or reviewer, and the most expensive thing to change once the table is large. A type review before the first deploy asks every column the same questions.

> The per-column review
>
> - Money or anything reconciled against another system: DECIMAL with stated precision and scale, or BIGINT minor units beside a currency column. FLOAT is a finding.
> - Text: does the length come from a standard or a known format, and does this platform reject or truncate past it? Is it a code that wants CHAR?
> - Time: past instant (TIMESTAMPTZ), future appointment (local time plus IANA zone), calendar day (DATE) or duration? A naive TIMESTAMP needs a written source for its zone.
> - Constrained values: will the set grow or carry labels? Lookup table if so, CHECK if only validity matters, ENUM only for sets fixed by definition.
> - Booleans: NOT NULL with a default, no 2 flags that cannot both be true, and a nullable timestamp for a flag that records an event.
> - Keys: BIGINT from a sequence, or native uuid holding time-ordered values. INT primary keys and CHAR(36) ids are findings.


<br>

### A type shapes; a constraint rejects

The type decides what a stored value looks like, a weaker promise than it sounds. Inserting 19.995 into a PostgreSQL `NUMERIC(10,2)` column stores 20.00 with no error, because the engine rounds to the declared scale on write; MySQL in strict mode does the same and emits a note the driver discards. A negative amount fits DECIMAL(10,2), and so does a NULL. Rejection comes from constraints: `NOT NULL` for a fact that must exist, `CHECK (amount >= 0)` for a sign the business forbids, a foreign key for a code that must exist in a lookup table. A reviewed column names both its type and the constraints that make it a validator.

```sql
CREATE TABLE invoice_lines (
  invoice_line_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  invoice_id      BIGINT NOT NULL REFERENCES invoices(invoice_id),
  quantity        INTEGER NOT NULL CHECK (quantity > 0),
  unit_price      DECIMAL(12,4) NOT NULL CHECK (unit_price >= 0),
  line_total      DECIMAL(12,2) NOT NULL CHECK (line_total >= 0),
  currency_code   CHAR(3) NOT NULL REFERENCES currencies(code),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```


<br>

### What a wrong type costs to fix

| Platform | Metadata-only change | Rewrite or rebuild |
| --- | --- | --- |
| PostgreSQL | Widening VARCHAR(n), VARCHAR to TEXT | INT to BIGINT, FLOAT to NUMERIC: full rewrite under an exclusive lock, plus every index |
| MySQL | Appending an ENUM value, widening VARCHAR within the same length-prefix size | Most others copy the table; gh-ost or pt-online-schema-change avoid downtime |
| Snowflake | Widening VARCHAR, raising NUMBER precision | Scale or type family: add a column, backfill, swap names |
| BigQuery | INT64 to NUMERIC, NUMERIC to BIGNUMERIC or FLOAT64, lengthening STRING(L) | Narrowing or cross-family: CREATE OR REPLACE TABLE AS SELECT |

The rewrite column is where the review pays for itself. An `id INT` primary key on `payment_events` holds 2,147,483,647 values; at 1,000 inserts per second that is 24.9 days of continuous writes, which a growing platform reaches within a few years of normal traffic. The fix rewrites the table and then every table whose foreign key references it, in dependency order, each under its own lock. Declared BIGINT on the first day, the column costs 4 extra bytes per row and nothing else.

✓Do

- Declare money as DECIMAL or BIGINT minor units, and write down the rounding rule beside it
- Take string lengths from a standard or a known format, and check how the platform treats overflow
- Store past events as TIMESTAMPTZ and future appointments as local time plus an IANA zone name
- Model a growing value set as a lookup table with a foreign key
- Pair every type with the NOT NULL, CHECK and FK constraints that reject bad rows

✗Don't

- Let an ORM default pick FLOAT for a column anyone will reconcile
- Size a column from a sample and assume the longest value has been seen
- Subtract 2 naive timestamps and call the result elapsed time
- Add a third boolean flag to a row that already cannot keep 2 consistent
- Choose INT for a key or CHAR(36) for a UUID on a table that will grow


<br>

---

## Putting It All Together

The subscription billing platform expands from US-only to 40 countries. The product team asks for prices in local currencies, renewals at 09:00 in each customer's own zone, a paused status for customers on payment holidays, and client-generated ids for a new mobile signup flow. One schema review covers all 4 requests before any migration runs.

- Local currencies turn unit_price and line_total into DECIMAL columns beside a CHAR(3) currency_code with a foreign key, and the review catches that a 10-digit precision fails on rupiah and dong invoices.
- Renewals at 09:00 local are appointments, so subscriptions gains renewal_time TIME and renewal_tz holding an IANA name, while payment_events . occurred_at stays TIMESTAMPTZ because charges are instants that happened.
- With status modelled as a lookup table, paused is an INSERT into subscription_statuses with a label and sort order, and no table is rewritten.
- Client-generated ids arrive as UUIDv7 in a native uuid column, so the primary key index keeps appending at its right edge as signups scale.
- Every new column ships with NOT NULL, a CHECK or a foreign key, because the type decides what a value looks like and the constraint decides whether it is allowed in at all.


<br>

---

## Key Takeaways

- Binary floating point cannot hold 0.1, so any column that is reconciled against another system is DECIMAL or BIGINT minor units.
- String lengths come from standards and known formats; whether overflow errors or truncates depends on the platform and its sql_mode.
- Past events are TIMESTAMPTZ instants; future appointments are a local time plus an IANA zone name; durations are the rule, not the date.
- A value set that grows or carries labels is a lookup table; a nullable boolean has 3 states and a flag that records an event wants a timestamp.
- Types shape values and constraints reject them, and a wrong type on a large table costs a rewrite, so review every column before the first deploy.
