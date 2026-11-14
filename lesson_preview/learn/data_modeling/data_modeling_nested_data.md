Intermediate·Data Modeling · 15 min · 12 challenges

# Nested Data

An Airbnb listing page shows a host, a location and a checklist of amenities: wifi, kitchen, free parking, 23 items on one listing and 4 on the next. A guest filters on 'has a dishwasher' and expects results in under a second. Whether those amenities live inside the listing row or in a table of their own decides how that filter runs and how the search index is rebuilt.


<br>

---

## The Nesting Decision

A nested column holds a structure where a flat column holds a single value. An order row in a flat schema stores `customer_id`, `order_date` and `total`, 1 value per column. The same order in a nested schema can also carry `items`, a list of 3 records, each with a sku, a quantity and a price. The list sits inside the row, so reading the order reads its items without a join.

orders

| order_id | customer_id | order_date |
| --- | --- | --- |
| 1001 | 77 | 2026-09-14 |
| 1002 | 12 | 2026-09-14 |

order_items

| order_id | sku | qty | price |
| --- | --- | --- | --- |
| 1001 | MUG-350 | 2 | 9.00 |
| 1001 | TEE-M | 1 | 24.00 |
| 1001 | CAP-BLK | 1 | 15.00 |
| 1002 | TEE-M | 3 | 24.00 |

Order 1001 occupies 1 row in `orders` and 3 rows in `order_items`, linked by `order_id`. Splitting a repeating part into its own table like this, 1 row per item, is called normalizing. The embedded version keeps the same facts in 1 table:

| order_id | customer_id | order_date | items |
| --- | --- | --- | --- |
| 1001 | 77 | 2026-09-14 | [{MUG-350, 2, 9.00}, {TEE-M, 1, 24.00}, {CAP-BLK, 1, 15.00}] |
| 1002 | 12 | 2026-09-14 | [{TEE-M, 3, 24.00}] |

Both versions answer every question about these orders. They differ in what a row means and therefore in which queries are cheap. In the normalized version, a row of `order_items` means 1 item of 1 order, so counting how many times `TEE-M` sold is a filter and a sum over 1 table. In the embedded version, a row means 1 complete order, so loading an order for the checkout page is 1 row read, and counting `TEE-M` sales means opening every list.


<br>

### Five questions that decide it

- **Ownership** *Does the child exist without the parent?*\
  A line item without its order means nothing, which argues for embedding. A product referenced by 10,000 orders has a life of its own, so it stays in a table.
- **Read pattern** *Are parent and child read together?*\
  Checkout loads an order with its items every time. A nightly report reads items alone, millions of them, and wants them as rows.
- **Update pattern** *Are children changed on their own?*\
  Amenities change when a host edits the listing, as a set. A review is edited by its author with no change to the listing, and belongs in its own table.
- **Size bound** *Does the list have a ceiling?*\
  Amenities come from a fixed list of 120 options, so a listing holds at most 120. Reviews on a popular listing pass 2,000 and keep growing, and a row that grows forever is a row you cannot embed.
- **Sharing** *Is the same child attached to many parents?*\
  A host with 5 listings is 1 host. Embedding the host in each listing makes 5 copies, and copies drift apart the first time the host changes their phone number.

Run the Airbnb listing through those 5 questions. Amenities exist only for the listing, load with it, change with it, stay small and belong to 1 listing: embed. Reviews are written and edited on their own, grow without a ceiling and are read in bulk by the ratings pipeline: normalize. The host is shared across listings, so the listing stores a `host_id` and the host table holds the facts, with at most a display name copied in for the search card.

✓Embed when

- The child has no meaning outside this parent
- Parent and child are read together on the hot path
- The list has a small, known ceiling
- The child changes only when the parent changes

✗Normalize when

- The child is shared by many parents
- The child is queried in bulk across parents
- The list grows without bound
- The child is edited by a different process or team

> **TIP**
>
> Ask the sharing question first. A shared child that gets embedded produces copies, and every later bug about stale data traces back to that one decision. The other 4 questions tune performance; this one decides correctness.


<br>

---

## STRUCT: Embedded Objects

A struct is 1 column holding a fixed set of named, typed fields. The field names and their types live in the table schema, the same as top-level columns, so the engine knows before reading a single row that `shipping_address` has a `city` of type VARCHAR. In the SQL used here the type is spelled `ROW`; the idea is the one most people call a struct.

```sql
CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_date DATE,
  shipping_address ROW(
    street VARCHAR,
    city VARCHAR,
    postal_code VARCHAR,
    country VARCHAR
  )
)
```

| order_id | customer_id | order_date | shipping_address |
| --- | --- | --- | --- |
| 1001 | 77 | 2026-09-14 | {Torstrasse 1, Berlin, 10119, DE} |
| 1002 | 12 | 2026-09-14 | {Rua Augusta 20, Lisboa, 1100-053, PT} |
| 1003 | 91 | 2026-09-15 | NULL |

Order 1003 is a digital gift card and has no shipping address at all, so the whole struct is NULL. Reading a field uses dot notation, and a filter on a field works like a filter on any column:

```sql
SELECT
  order_id,
  shipping_address.city AS city
FROM orders
WHERE shipping_address.country = 'DE'
```


<br>

### Why a struct beats 4 prefixed columns

The same 4 facts could live in `shipping_street`, `shipping_city`, `shipping_postal_code` and `shipping_country`. The struct wins on 3 counts. The grouping is in the schema, so a reader sees 1 address rather than 4 columns that happen to share a prefix. Absence is 1 check: `shipping_address IS NULL` says the order has no address, where the prefixed version needs 4 checks and a rule about which columns may be missing on their own. And the shape is reusable: `billing_address` can be declared with the same `ROW(...)` and compared field by field with `shipping_address` in a single expression.

A separate `addresses` table loses here too. Every order has exactly 1 shipping address, it is read whenever the order is read, and it is a snapshot that never changes after shipment. A join would cost a lookup per order and buy nothing, since none of the 5 questions from the previous section point toward a table.


<br>

### Two kinds of NULL

```sql
SELECT
  order_id,
  shipping_address IS NULL AS no_address,
  shipping_address.city IS NULL AS no_city
FROM orders
```

| order_id | no_address | no_city |
| --- | --- | --- |
| 1001 | false | false |
| 1002 | false | false |
| 1003 | true | true |

A field read through a NULL struct is NULL, so order 1003 reports both. A pickup order could hold a struct with `country` set and `city` NULL, and would report `no_address = false` with `no_city = true`: a partial address, a different state from no address. The struct also separates no address from an address whose every field is NULL, where 4 flat columns, all NULL, could mean either one.


<br>

### Structs inside structs

Fields can themselves be structs. Adding `geo ROW(lat DOUBLE, lon DOUBLE)` inside `shipping_address` gives `shipping_address.geo.lat`, and the same NULL rule applies at every level. Building a value by hand uses `ROW(...)` with a cast to name the fields:

```sql
SELECT
  CAST(
    ROW('Torstrasse 1', 'Berlin', '10119', 'DE')
    AS ROW(street VARCHAR, city VARCHAR, postal_code VARCHAR, country VARCHAR)
  ) AS address
```

> Reach for a struct when
>
> - The fields always travel together and the set of fields is known in advance
> - The whole group can be absent as a unit
> - The same shape appears more than once, such as shipping and billing
> - The data is a snapshot: a copy of the facts at the time the row was written


<br>

---

## ARRAY: Ordered Collections

An array is 1 column holding an ordered list of values that share a type. The list can be empty, hold 1 value or hold 200, and each row decides its own length. Positions start at 1 in the SQL used here. Two shapes cover most uses: a list of scalars, such as a listing's amenities, and a list of structs, such as its photos, which is a child table folded into the parent row.

```sql
CREATE TABLE listings (
  listing_id BIGINT,
  title VARCHAR,
  amenities ARRAY(VARCHAR),
  photos ARRAY(ROW(url VARCHAR, caption VARCHAR))
)
```

| listing_id | title | amenities | photos |
| --- | --- | --- | --- |
| 501 | Loft near Alexanderplatz | [wifi, kitchen, dishwasher] | [{/p/501-1.jpg, Living room}, {/p/501-2.jpg, Kitchen}] |
| 502 | Studio in Alfama | [wifi] | [{/p/502-1.jpg, View}] |
| 503 | Cabin by the lake | [] | [] |

Reading an array in place covers 3 operations: how long it is, what sits at a position, and whether it contains a value.

```sql
SELECT
  listing_id,
  CARDINALITY(amenities) AS amenity_count,
  ELEMENT_AT(amenities, 1) AS first_amenity,
  CONTAINS(amenities, 'dishwasher') AS has_dishwasher
FROM listings
```

| listing_id | amenity_count | first_amenity | has_dishwasher |
| --- | --- | --- | --- |
| 501 | 3 | wifi | true |
| 502 | 1 | wifi | false |
| 503 | 0 | NULL | false |

> **TIP**
>
> `amenities[1]` and `element_at(amenities, 1)` return the same value when the element exists. On listing 503, whose array is empty, the subscript raises an error and the whole query fails, while `element_at` returns NULL. Use `element_at` on any array whose length you do not control.


<br>

### UNNEST: back to rows

The guest's dishwasher filter runs on the array in place, with `WHERE contains(amenities, 'dishwasher')`. A question across listings, such as which amenities are most common, wants 1 row per amenity instead. `UNNEST` produces exactly that, 1 output row per element, and the `CROSS JOIN` pairs each element with its parent listing:

```sql
SELECT
  l.listing_id,
  a.amenity
FROM listings AS l
CROSS JOIN UNNEST(l.amenities) AS a(amenity)
```

Input: 3 listings

| listing_id | amenities |
| --- | --- |
| 501 | [wifi, kitchen, dishwasher] |
| 502 | [wifi] |
| 503 | [] |

Output: 4 rows

| listing_id | amenity |
| --- | --- |
| 501 | wifi |
| 501 | kitchen |
| 501 | dishwasher |
| 502 | wifi |

Listing 503 contributes 0 rows and vanishes from the output, which is correct for counting amenities and a trap for counting listings. From the unnested rows, the most common amenities is an aggregate over those rows:

```sql
SELECT
  a.amenity,
  COUNT(*) AS listings_with_it
FROM listings AS l
CROSS JOIN UNNEST(l.amenities) AS a(amenity)
GROUP BY a.amenity
ORDER BY listings_with_it DESC
```


<br>

### Order is part of the data

The photos array is ordered because the host chose which picture comes first. Unnesting an array of structs spreads each struct's fields into columns, and `WITH ORDINALITY` adds the position so the order survives the trip to rows:

```sql
SELECT
  l.listing_id,
  p.url,
  p.pos
FROM listings AS l
CROSS JOIN UNNEST(l.photos) WITH ORDINALITY AS p(url, caption, pos)
```

Position 1 is the cover photo. Had the photos lived in a separate table, that fact would need a `sort_order` column, an `ORDER BY` on every read, and a uniqueness rule on (`listing_id`, `sort_order`) to keep 2 photos off position 1. The array gives all 3 by construction.


<br>

### What an array will not enforce

- Uniqueness: ARRAY['wifi', 'wifi'] is a valid value. Deduplicate on write with array_distinct.
- A ceiling: an embedded review list can reach 20,000 elements and make every read of that row expensive.
- Referential checks: an amenity is a string, and 'Wifi' and 'wifi' are 2 different values to contains.


<br>

---

## MAP: Dynamic Key-Value Pairs

A map is 1 column holding key-value pairs where each row chooses its own keys. All keys share a type and all values share a type, and the schema says nothing about which keys will appear. That makes it the shape for attributes that vary from record to record: a product catalog where a laptop has `ram_gb` and `screen_in`, a shirt has `size` and `color`, and a mug has `capacity_ml`. A struct would need a field for every attribute any product might have, hundreds of fields, nearly all NULL on every row.

```sql
CREATE TABLE products (
  product_id BIGINT,
  name VARCHAR,
  category VARCHAR,
  attributes MAP(VARCHAR, VARCHAR)
)
```

| product_id | name | category | attributes |
| --- | --- | --- | --- |
| 7 | Ultrabook 14 | laptop | {ram_gb: 16, screen_in: 14} |
| 8 | Linen shirt | apparel | {size: M, color: navy} |
| 9 | Stoneware mug | kitchen | {capacity_ml: 350, color: cream} |

Looking up a key uses `element_at`, which returns NULL when the key is absent, the same way it does for arrays. The subscript form `attributes['color']` raises an error on product 7, where no such key exists, and takes the query down with it.

```sql
SELECT
  product_id,
  ELEMENT_AT(attributes, 'color') AS color,
  CARDINALITY(attributes) AS attribute_count
FROM products
```

| product_id | color | attribute_count |
| --- | --- | --- |
| 7 | NULL | 2 |
| 8 | navy | 2 |
| 9 | cream | 2 |

Filtering on a key is the same call in a `WHERE` clause: `WHERE element_at(attributes, 'size') = 'M'` returns the shirt. Unnesting a map produces 1 row per pair, with the key and the value as 2 columns, which turns the question of which keys exist at all into an aggregate:

```sql
SELECT
  kv.attr_key,
  COUNT(*) AS products_with_key
FROM products AS p
CROSS JOIN UNNEST(p.attributes) AS kv(attr_key, attr_value)
GROUP BY kv.attr_key
ORDER BY products_with_key DESC
```

That query is the one to run every month on a map column. Keys that appear on most rows and show up in filters are candidates to move out of the map and into a column of their own, a point the next section makes concrete.


<br>

### What a map gives up

The flexibility costs 3 guarantees. First, type. Every value in `MAP(VARCHAR, VARCHAR)` is text, so `ram_gb` is stored as `'16'`, and `element_at(attributes, 'ram_gb') > '8'` compares 2 strings, where `'16'` sorts before `'8'` because `'1'` comes before `'8'`. The laptop fails the filter. Correct code casts every time: `CAST(element_at(attributes, 'ram_gb') AS INTEGER) > 8`. Second, the key set. Nothing stops 1 upstream team writing `color` and another writing `colour`, and both land in the table. Third, visibility. A reader of the schema learns that attributes exist and nothing about what they are, so documentation has to carry what the schema would otherwise say.

| Shape | Keys or fields | Values | Reach for it when |
| --- | --- | --- | --- |
| STRUCT | Fixed, named in the schema | Each field has its own type | Every row has the same fields |
| ARRAY | Positions 1, 2, 3, ... | All one type, order matters | A row holds a list of things |
| MAP | Vary per row, chosen at write time | All one type, no order | Attributes differ from row to row |

*A map whose values are numbers, dates and words all stored as text is a sign that the common fields want a struct, with the map kept for the rest.*


<br>

---

## Columnar Storage & Nesting

Row storage writes each row's values next to each other, so reading 1 column means reading every row in full. Columnar storage writes each column's values together, in chunks, with the minimum and maximum of each chunk recorded in its footer. A query that touches 2 of 40 columns reads 2 streams, and a filter such as `WHERE country = 'PT'` skips every chunk whose min and max rule it out. Most analytical tables are stored this way, and nested columns get the same treatment, with a twist.


<br>

### Nested columns are stored as leaves

The file format splits every nested column into its leaf fields and writes each leaf as its own stream, the same way it writes a top-level column. Next to each value it records how many levels of nesting are present for that row and whether the value starts a new list, which is enough to rebuild the row and enough to read 1 leaf on its own.

![Schema diagram 1](images/data_modeling_nested_data_diagram_1.png)

| Declared column | Leaf streams on disk |
| --- | --- |
| listing_id BIGINT | listing_id |
| location ROW(city, country, geo ROW(lat, lon)) | location.city, location.country, location.geo.lat, location.geo.lon |
| amenities ARRAY(VARCHAR) | amenities.element |
| photos ARRAY(ROW(url, caption)) | photos.url, photos.caption |
| attributes MAP(VARCHAR, VARCHAR) | attributes.key, attributes.value |

- **10** — leaf streams in this listings table
- **1** — streams read by a city filter
- **2** — streams read by a map key lookup

The consequences follow from the table. `WHERE location.city = 'Lisbon'` reads the `location.city` stream and nothing else, and its chunk statistics let the engine skip chunks that hold no Lisbon. Counting photos with `CROSS JOIN UNNEST(photos)` reads at most the `photos.url` stream, since the list markers beside it say where each listing's photos begin and end. Reading a field of a struct costs the same as reading a top-level column of the same type.




<br>

### Maps break the pattern

A map has only 2 leaf streams, keys and values, shared by every key that will ever appear. To evaluate `element_at(attributes, 'color')` the engine reads the entire key stream, scans each row's keys for a match, and pulls the matching position from the value stream. The chunk statistics on the value stream cover ram sizes, shirt sizes and colors mixed together, so a filter on one key skips nothing. A struct field has its own stream and its own statistics, which is the whole difference.

| Operation | On a struct field | On a map key |
| --- | --- | --- |
| Read 1 field | Reads 1 leaf stream | Reads the key stream and the value stream |
| Filter on 1 field | Skips chunks by min and max | Scans every row in every chunk |
| Add a field | Schema change, old rows read NULL | Write the key, no schema change |

This is why the monthly key-frequency query from the previous section matters. A key like `pet_policy` that sits on 80 percent of listings and appears in the search filter has outgrown the map. Promoting it to a top-level column gives it a stream and statistics, and the map keeps the long tail of rare keys where flexibility earns its cost.


<br>

### JSON text is a different thing

A VARCHAR column holding JSON looks nested and stores as 1 stream of text. To answer `json_extract_scalar(payload, '$.location.city')` the engine reads every byte of every payload and parses it row by row, work that grows with the size of the whole document rather than the field asked for. Typed nested columns hand the engine the leaves; JSON text makes it dig for them on each query. A raw landing table can hold JSON text, and the first transformation should cast it into declared structs, arrays and maps.

✓Do

- Declare structs for fields every row has, and read them by name
- Cast JSON text into typed nested columns in the first transformation
- Promote map keys that appear in filters to their own columns
- Cap embedded arrays, and move unbounded lists to a table

✗Don't

- Store numbers as text inside a MAP(VARCHAR, VARCHAR) and compare them
- Filter on a map key inside a hot query and expect chunk skipping
- Keep a 200-field struct where most fields are NULL on most rows
- Use array subscripts on arrays whose length you do not control


<br>

---

## Putting It All Together

Picture the listings table behind Airbnb's search. A location struct feeds the map view and the country filter, an amenities array answers 'has a dishwasher' with 1 contains call, and a host-defined attributes map holds the long tail while pet_policy, set on most listings and filtered constantly, lives in its own column. Reviews, unbounded and edited by guests, stay in a separate table joined only by the ratings pipeline.

- The ownership and size questions from section 0 put amenities in the row and reviews in a table.
- The location struct from section 1 reads 1 leaf stream per field, so the country filter skips chunks by min and max as section 4 describes.
- The amenities array from section 2 serves the filter in place and the nightly popularity count through UNNEST.
- The map from section 3 absorbs new host attributes without schema changes, and the key-frequency query decides which keys earn a column.


<br>

---

## Key Takeaways

- Embed a child when it has no meaning without its parent, loads with it, changes with it, stays small and belongs to it alone. A shared child goes in a table.
- A struct groups a fixed set of named fields into 1 column; the whole group can be NULL, and a field read through a NULL struct is NULL.
- An array stores an ordered list in a row; read it in place with contains and element_at , and unnest it when a question wants rows.
- A map stores keys that vary per row; every value shares 1 type, so numbers stored as text must be cast before comparison.
- Columnar files store each nested leaf as its own stream with its own statistics, except map values, which share 1 stream; promote hot map keys to columns.
