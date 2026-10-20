Q: When you hear these in an interview, what concept is being tested?

- "Why Parquet over CSV?"
- "Row-oriented vs columnar: explain the difference."
- "How does a columnar format speed up queries?"

A: Columnar vs row formats

---

Q: - "Why Parquet over CSV?"
- "Row-oriented vs columnar: explain the difference."
- "How does a columnar format speed up queries?"

A: Parquet stores data by column instead of by row. Analytical queries that only need 3 out of 100 columns read 97% less data. Same-type values grouped together compress 10x better. And row group statistics let the engine skip chunks without reading them. Done. Three benefits, one sentence each. That is the answer.

---

Q: For both row-oriented (CSV, Postgres) and columnar (Parquet, BigQuery) storage, give the pluses and minuses.

A:
**Row-oriented (CSV, Postgres)**
- All columns for each row together
- Fast for reading full records
- Used by transactional databases
- Must read all columns even for one

**Columnar (Parquet, BigQuery)**
- All values of each column together
- Fast for reading selected columns
- Used by analytical systems
- Excellent compression (same types)

---

Q: When asked about columnar vs row formats, a common follow-up is: "When would you NOT use Parquet?"

A: Transactional workloads (inserting single rows). Human inspection (CSV is readable). Very small files (Parquet overhead outweighs benefits under ~10 MB).

---

Q: When asked about columnar vs row formats, a common follow-up is: "How does compression work better in columnar?"

A: A column of country codes (US, US, US, CA, US) is just 2 unique values repeated. Compression algorithms love that. A row mixing strings, numbers, and dates compresses poorly.

---

Q: When asked about columnar vs row formats, a common follow-up is: "What is predicate pushdown?"

A: The engine reads column statistics (min/max per chunk) and skips chunks that cannot match the query filter. `WHERE revenue > 1000` skips any chunk where `max(revenue) < 1000`.

---

Q: What are the key takeaways when asked about columnar vs row formats?

A:
- The 10-second answer: columnar storage, 10x compression, predicate pushdown
- Row-oriented = transactions (Postgres). Columnar = analytics (BigQuery, Parquet).
- Know the exception: CSV for small human-readable exchanges, Avro for streaming

---

Q: When you hear these in an interview, what concept is being tested?

- "What compression do you use and why?"
- "Snappy vs Gzip: when do you use each?"
- "How does compression affect query performance?"

A: Compression and encoding

---

Q: - "What compression do you use and why?"
- "Snappy vs Gzip: when do you use each?"
- "How does compression affect query performance?"

A: Snappy for interactive queries because it decompresses fast. Gzip for archival because it compresses more. Zstd is the emerging winner: Gzip-level compression at nearly Snappy-level speed. That is the complete answer. They are testing whether you understand the speed-vs-ratio tradeoff, not whether you know the algorithms' internals.

---

Q: For each compression algorithm, give its speed, its compression ratio, and what to say about it in an interview.

A:
| Algorithm | Speed | Compression | Say this |
|---|---|---|---|
| Snappy | Very fast | Moderate (2-4x) | Default for analytics. Fast reads. |
| Gzip | Slow | High (5-8x) | Archival. Small files, slow queries. |
| Zstd | Fast | High (4-7x) | Best of both. Growing fast. |
| LZ4 | Fastest | Low (2-3x) | Ultra-low latency streaming. |

---

Q: When asked about compression, a common follow-up is: "Why not always use the highest compression?"

A: CPU cost. Gzip takes 5-10x more CPU to decompress than Snappy. On interactive queries, decompression time dominates. You trade smaller files for slower queries.

---

Q: When asked about compression, a common follow-up is: "Does compression affect write performance?"

A: Yes. Higher compression = slower writes. For streaming ingestion, use Snappy or LZ4 to keep write latency low.

---

Q: What are the key takeaways when asked about compression?

A:
- Say: 'Snappy for queries, Gzip for archives, Zstd for best of both.'
- The tradeoff is always speed vs ratio. Know which end your use case needs.
- You do not need to know algorithms internally. Just the tradeoff.

---

Q: When you hear these in an interview, what concept is being tested?

- "How do you partition this table?"
- "What is partition pruning?"
- "What happens if you over-partition?"

A: Partitioning strategies

---

Q: - "How do you partition this table?"
- "What is partition pruning?"
- "What happens if you over-partition?"

A: I partition by the column that appears most often in WHERE clauses, usually date. Partition pruning lets the engine skip all other date folders entirely. A query for one day reads 1/365th of the data. Then immediately add the pitfall: The risk is over-partitioning. Too many partitions create thousands of tiny files, which is actually slower than no partitioning at all because of per-file overhead.

---

Q: When asked about partitioning, a common follow-up is: "What is the small file problem?"

A: Over-partitioning creates thousands of tiny files. Opening 10,000 files of 1 MB each is far slower than 10 files of 1 GB, even though total data is the same. Per-file overhead dominates.

---

Q: When asked about partitioning, a common follow-up is: "How big should each partition be?"

A: Target 128 MB to 1 GB per partition. If partitions are smaller, coarsen the partition key (hourly to daily) or add compaction.

---

Q: When asked about partitioning, a common follow-up is: "Can you partition on two columns?"

A: Yes, but be careful. `date` + `region` creates (365 x number_of_regions) partitions. If that is too many, partition by date only and use Z-ordering or sorting on region.

---

Q: What are the key takeaways when asked about partitioning?

A:
- Say: 'Partition by the most common filter column, usually date. Pruning skips irrelevant partitions entirely.'
- Always mention the pitfall: over-partitioning creates the small file problem
- Rule of thumb: each partition should be 128 MB to 1 GB

---

Q: When you hear these in an interview, what concept is being tested?

- "Data lake vs warehouse: what is the difference?"
- "When would you use a lake instead of a warehouse?"
- "What is a lakehouse?"

A: Data lake architecture

---

Q: - "Data lake vs warehouse: what is the difference?"
- "When would you use a lake instead of a warehouse?"
- "What is a lakehouse?"

A: A lake stores raw data cheaply on object storage. A warehouse stores structured, query-optimized data in a purpose-built engine. A lakehouse puts a table format (Delta, Iceberg) on top of lake storage to get warehouse features without the warehouse cost. Then the critical insight: Most modern platforms use both. The lake is the cheap source of truth. The warehouse materializes the hot queries.

---

Q: For both a data lake and a data warehouse, give the pluses and minuses.

A:
**Data lake**
- Raw data on S3/GCS (cheap)
- Schema-on-read (flexible)
- Risk: becomes a data swamp
- Best for: raw archive, ML training

**Data warehouse**
- Structured data, optimized engine
- Schema-on-write (enforced)
- Risk: expensive at scale
- Best for: dashboards, SQL analytics

---

Q: When asked about lake vs warehouse, a common follow-up is: "What is the lakehouse and why is it popular?"

A: Lake cost + warehouse features. Add Delta Lake or Iceberg on top of S3 files and you get ACID transactions, schema enforcement, and time travel without paying for a separate warehouse.

---

Q: When asked about lake vs warehouse, a common follow-up is: "Would you use JUST a lake or JUST a warehouse?"

A: Almost never. Raw data lands in the lake (cheap, flexible). Query-ready data is served from the warehouse or lakehouse (fast, governed). Both have a role.

---

Q: When asked about lake vs warehouse, a common follow-up is: "What is a data swamp?"

A: A lake with no governance: no schema documentation, no ownership, no quality checks. Data goes in but nobody knows what is there or whether it is correct.

---

Q: What are the key takeaways when asked about lake vs warehouse?

A:
- Say: 'Lake for cheap raw storage, warehouse for fast queries, lakehouse for both via Delta/Iceberg.'
- The smart answer: 'Most platforms use both. Lake is the archive, warehouse serves the hot queries.'
- Always mention the swamp risk: a lake without governance is useless

---

Q: When you hear these in an interview, what concept is being tested?

- "What is Delta Lake?"
- "Delta vs Iceberg vs Hudi: what is the difference?"
- "How do you get ACID on object storage?"

A: Table formats

---

Q: - "What is Delta Lake?"
- "Delta vs Iceberg vs Hudi: what is the difference?"
- "How do you get ACID on object storage?"

A: Raw Parquet on S3 has no transaction guarantees. A failed write corrupts the table. Table formats like Delta Lake and Apache Iceberg add a metadata layer that provides ACID transactions, time travel, and schema enforcement on top of Parquet files. Then the cheat sheet: Delta if you use Databricks, Iceberg if you want engine-agnostic, Hudi if you need fast upserts.

---

Q: For each table format, name who created it and what to say about it in an interview.

A:
| Format | Created by | Say this |
|---|---|---|
| Delta Lake | Databricks | Tight Spark integration. Default if using Databricks. |
| Apache Iceberg | Netflix | Engine-agnostic. Best partition evolution. Growing fast. |
| Apache Hudi | Uber | Fastest upserts. Good for streaming into a lake. |

---

Q: When asked about table formats, a common follow-up is: "Which would you choose?"

A: Depends on the engine. If we are on Databricks, Delta. If we want flexibility across Spark, Trino, and Presto, Iceberg. I would not choose based on features alone; ecosystem fit matters more.

---

Q: When asked about table formats, a common follow-up is: "What is time travel?"

A: Query the table as it existed at a specific timestamp or version number. Useful for debugging, auditing, and reproducing ML training data.

---

Q: When asked about table formats, a common follow-up is: "Can you migrate between formats?"

A: Yes. In-place migration adds new metadata on top of existing files (fast, limited). Full rewrite reads and rewrites all data (slow, complete).

---

Q: What are the key takeaways when asked about table formats?

A:
- Say: 'Table formats add ACID, time travel, and schema enforcement to files on object storage.'
- Delta = Databricks, Iceberg = engine-agnostic, Hudi = upserts. Know the one-liner for each.
- The pro move: 'I would choose based on ecosystem fit, not feature comparison.'
