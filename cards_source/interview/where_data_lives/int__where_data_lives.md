Q: When you hear these in an interview, what concept is being tested?
- "How does Parquet compress data so well?"
- "What is dictionary encoding?"
- "RLE vs dictionary: when do you use each?"

A: Encoding types

---

Q:
- "How does Parquet compress data so well?"
- "What is dictionary encoding?"
- "RLE vs dictionary: when do you use each?"

A: Parquet applies encoding per column before compression. Dictionary encoding maps repeated values to small integers, so a column of 1 million country codes becomes 1 million tiny integers plus a 200-entry dictionary. RLE (run-length encoding) stores repeated consecutive values as (value, count). Delta encoding stores differences between sequential values, perfect for timestamps. The key insight: encoding converts data into a more compressible form BEFORE the compression algorithm runs.

---

Q: For each encoding, explain how it works and what it is best for.

A:
| Encoding | How it works | Best for |
|---|---|---|
| Dictionary | Map unique values to integers | Low-cardinality: country, status, type |
| RLE | Store (value, count) pairs | Sorted columns, boolean flags |
| Delta | Store differences between values | Timestamps, sequential IDs |
| Bit-packing | Use minimum bits per value | Small integers, enums |

---

Q: When asked about encoding types, a common follow-up is: "What happens if dictionary encoding is applied to a high-cardinality column?"

A: It gets WORSE. If a column has millions of unique values (like email addresses), the dictionary itself is huge, and the file is larger than plain encoding. Parquet falls back automatically when cardinality is too high.

---

Q: When asked about encoding types, a common follow-up is: "How do you choose the encoding?"

A: You usually do not. Parquet auto-selects based on column statistics. But knowing WHY dictionary encoding works shows you understand the mechanics, not just the tool.

---

Q: What are the key takeaways when asked about encoding types?

A:
- Say: 'Encoding converts data into a more compressible form before compression runs.'
- Dictionary = low-cardinality, RLE = repeated values, Delta = sequential values
- Know the gotcha: dictionary encoding HURTS on high-cardinality columns

---

Q: When you hear these in an interview, what concept is being tested?
- "Your queries are slow. What do you check first?"
- "How do you handle thousands of small files?"
- "What is file compaction?"

A: The small file problem

---

Q:
- "Your queries are slow. What do you check first?"
- "How do you handle thousands of small files?"
- "What is file compaction?"

A: Small file problem. Over-partitioning or high-frequency writes create thousands of tiny files. The per-file overhead of open/read/close dominates query time. I fix it with compaction: merge small files into 128 MB to 1 GB targets. Delta OPTIMIZE or Iceberg `rewrite_data_files`. This is the #1 practical storage question. Most candidates know partitioning but cannot diagnose why their partitioned table is still slow.

---

Q: When asked about the small file problem, a common follow-up is: "How often do you run compaction?"

A: Depends on write frequency. Daily for most batch pipelines. Hourly for high-throughput streaming. The tradeoff: compaction costs compute (you rewrite data that has not changed). Too frequent wastes money; too rare lets the problem grow.

---

Q: When asked about the small file problem, a common follow-up is: "How do you prevent the problem in the first place?"

A: Right-size your partitions. If hourly partitions are too small, use daily. Configure the writer to buffer and produce fewer, larger output files.

---

Q: When asked about the small file problem, a common follow-up is: "What about streaming into a lake?"

A: Streaming writes lots of tiny files by nature. Schedule compaction to run every 1-2 hours behind the streaming writer.

---

Q: What are the key takeaways when asked about the small file problem?

A:
- Say: 'Small files. Over-partitioning or streaming writes create thousands of tiny files. Compaction merges them.'
- This is the #1 'your queries are slow' diagnosis. Always check file count and sizes first.
- Target: 128 MB to 1 GB per file. Anything under 10 MB is a problem.

---

Q: When you hear these in an interview, what concept is being tested?
- "How does predicate pushdown work?"
- "Why does adding a WHERE clause make my query 10x faster?"
- "What are row group statistics?"

A: Predicate pushdown

---

Q:
- "How does predicate pushdown work?"
- "Why does adding a WHERE clause make my query 10x faster?"
- "What are row group statistics?"

A: Predicate pushdown pushes the WHERE clause to the storage layer. In Parquet, each row group stores min/max statistics per column. If the query asks for revenue > 1000 and a row group's max revenue is 500, the entire row group is skipped without reading it. Combined with partition pruning, this can skip 99%+ of the data. The key insight they are testing: pushdown works at TWO levels: partition pruning (skip folders) and row group pruning (skip chunks within files).

---

Q: When asked about predicate pushdown, a common follow-up is: "What makes pushdown ineffective?"

A: Randomly ordered data. If every row group's min is 0 and max is 10 million, no groups get skipped. Sorting data on the filter column makes pushdown dramatically more effective.

---

Q: When asked about predicate pushdown, a common follow-up is: "Is this the same as an index?"

A: Conceptually similar but lighter weight. Indexes are separate data structures that point to rows. Row group statistics are embedded in the file and work at chunk granularity, not row granularity.

---

Q: What are the key takeaways when asked about predicate pushdown?

A:
- Say: 'Pushdown works at two levels: partition pruning skips folders, row group statistics skip chunks within files.'
- Sorting data on filter columns makes pushdown dramatically more effective
- Combined, partition + row group pruning can skip 99%+ of data

---

Q: When you hear these in an interview, what concept is being tested?
- "How do you manage storage costs at scale?"
- "What is storage tiering?"
- "When do you archive old data?"

A: Storage tiering

---

Q:
- "How do you manage storage costs at scale?"
- "What is storage tiering?"
- "When do you archive old data?"

A: Not all data is accessed equally. I tier data by access pattern: hot (SSD, recent days/weeks, most expensive), warm (standard S3, recent months), cold (Glacier, historical years, cheapest). This alone can cut storage bills by 60-80% because the 80/20 rule applies: 80% of queries touch 20% of data.

---

Q: When asked about storage tiering, a common follow-up is: "An analyst needs data from 3 years ago. How long does it take?"

A: If it is in Glacier, 3-12 hours for retrieval. That is the tradeoff. Make sure retention policies are communicated to data consumers.

---

Q: When asked about storage tiering, a common follow-up is: "What triggers the tier transition?"

A: Time-based policies. Data older than 90 days moves to warm. Older than 1 year moves to cold. Some teams also tier based on query frequency.

---

Q: When asked about storage tiering, a common follow-up is: "What about compliance requirements for data retention?"

A: Compliance may require keeping raw data for 7+ years. Cold storage makes this affordable ($4/TB/month on Glacier vs $23/TB/month on standard S3).

---

Q: What are the key takeaways when asked about storage tiering?

A:
- Say: 'Tier by access pattern. Hot for recent, warm for months, cold for years. Cuts bills 60-80%.'
- The 80/20 rule: 80% of queries touch 20% of data. Tier the rest cheaply.
- Know the retrieval tradeoff: Glacier is 80% cheaper but takes hours to retrieve

---

Q: When you hear these in an interview, what concept is being tested?
- "What happens when a source adds a new column?"
- "How do you handle schema changes without breaking pipelines?"
- "Schema-on-read vs schema-on-write?"

A: Schema evolution

---

Q:
- "What happens when a source adds a new column?"
- "How do you handle schema changes without breaking pipelines?"
- "Schema-on-read vs schema-on-write?"

A: Schema changes are inevitable. My approach: adding a nullable column is always safe. Widening a type (int to long) is safe. Removing a column or narrowing a type is a breaking change that requires a migration plan with dual-write during the transition. Then mention table formats: Iceberg and Delta handle safe schema evolution natively. For breaking changes, I use a dual-write period where both old and new schemas are produced simultaneously.

---

Q: When asked about schema evolution, a common follow-up is: "A vendor renames a column without telling you. How do you handle it?"

A: The pipeline should validate schemas on ingestion. If the expected column is missing, alert and hold the data in Bronze (do not propagate the error downstream).

---

Q: When asked about schema evolution, a common follow-up is: "What is the difference between schema-on-read and schema-on-write?"

A: Schema-on-write (warehouse) rejects bad data at write time. Schema-on-read (lake) accepts anything and validates at query time. The tradeoff: early error detection vs flexibility.

---

Q: When asked about schema evolution, a common follow-up is: "How do you version schemas?"

A: Schema registry (Confluent for Kafka, Glue for AWS). Each schema version is stored with compatibility rules. Producers cannot break consumers.

---

Q: When it comes to schema evolution, what are some do's and don'ts?

A:
**Do**
- Adding a nullable column is always safe
- Widening a type (int to long) is safe
- Use schema registries for streaming data

**Don't**
- Remove a column without checking downstream consumers
- Narrow a type without verifying data range
- Rename a column without a migration period

---

Q: What are the key takeaways when asked about schema evolution?

A:
- Say: 'Additive changes are safe. Breaking changes need a dual-write migration period.'
- Table formats (Iceberg, Delta) handle safe evolution natively. Use them.
- The pro move: mention schema registries for streaming sources
