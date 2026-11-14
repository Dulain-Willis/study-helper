# Lesson index: start here

For data engineering architecture questions. Find the topic below, open that lesson's detail index, pick the section's line range, then read only those lines of the lesson.

1. Find the topic under **Topics** (best lesson first; the section name in parentheses is that lesson's own `##` title).
2. Open `lesson_preview/interview/architecture/index/<lesson>.md` for the lesson's sections and line ranges.
3. Read just that range of `lesson_preview/interview/architecture/<lesson>.md` (Read with offset and limit).

Two families: `pipeline_*` lessons teach fundamentals (beginner, intermediate, advanced); `pa_pattern_*` lessons go deep with numbers and interview-style answers. Use `pipeline_*` for definitions and `pa_pattern_*` for depth.

## Topics

### Delivery and correctness
- **Delivery guarantees** (at-most-once, at-least-once, exactly-once): `pa_pattern_delivery_semantics`; `pipeline_streaming_systems_intermediate` (Consumer Groups and Offsets); `pipeline_streaming_systems_beginner` (Event Platforms)
- **Idempotent writes and safe reruns** (partition replace, MERGE, version guards): `pa_pattern_idempotency`; `pipeline_making_it_repeatable_beginner` (Safe Retries); `pipeline_how_data_moves_advanced` (Idempotent Pipelines); `pa_pattern_make_it_reliable_intermediate` (Can You Re-run This Safely?)
- **Duplicates and deduplication** (dedup windows, idempotency keys, latest row per key): `pa_pattern_delivery_semantics` (At-Least-Once and Its Duplicates); `pa_pattern_make_it_reliable_intermediate` (How Do You Handle Duplicates?)
- **Dead-letter queues, retries, poison pills**: `pa_pattern_dead_letter_queue`; `pipeline_streaming_systems_intermediate` (DLQ Patterns); `pipeline_streaming_systems_beginner` (Dead Letter Queues)
- **Data quality checks, monitoring, rolling back a bad load**: `pa_pattern_make_it_reliable_intermediate`
- **Orchestration** (DAGs, sensors, trigger rules, dev/staging/prod, CI/CD for data): `pipeline_making_it_repeatable_beginner`

### Streaming, time, and throughput
- **Batch vs streaming vs micro-batch**: `pa_pattern_batch_or_stream_intermediate`; `pipeline_how_data_moves_beginner` (Batch vs Streaming); `pipeline_streaming_systems_beginner` (Micro-Batch vs True Streaming); `pipeline_streaming_systems_intermediate` (Spark Streaming vs Flink)
- **Lambda and Kappa, testing streaming jobs, checkpoints and restart**: `pa_pattern_batch_or_stream_intermediate`
- **Late data, watermarks, event time, windows, allowed lateness**: `pa_pattern_late_data_watermarks`; `pipeline_streaming_systems_intermediate` (Windowing and Watermarks); `pipeline_streaming_systems_beginner` (Late-Arriving Data)
- **Backpressure, consumer lag, partition counts, autoscaling, capacity sizing**: `pa_pattern_backpressure_scaling`; `pipeline_how_data_moves_advanced` (Backpressure); `pipeline_streaming_systems_intermediate` (Consumer Groups and Offsets)
- **Event logs** (topics, partitions, offsets, consumer groups, event-driven design): `pipeline_streaming_systems_beginner`; `pipeline_streaming_systems_intermediate`
- **Event sourcing and CQRS**: `pipeline_streaming_systems_intermediate` (Event Sourcing Patterns)
- **Cost of freshness** (what real-time costs vs batch): `pipeline_how_data_moves_advanced` (Cost of Freshness); `pa_pattern_batch_or_stream_intermediate`

### Ingestion and change capture
- **Change data capture** (log-based CDC, Debezium, LSN, polling limits, the dual-write problem, outbox): `pa_pattern_cdc_dual_writes`; `pipeline_keeping_data_fresh_beginner` (Change Data Capture); `pipeline_keeping_data_fresh_intermediate` (CDC Patterns)
- **Full vs incremental loads** (watermark columns, lookback windows, catching deletes): `pa_pattern_incremental_loading_intermediate`; `pipeline_keeping_data_fresh_beginner` (Full vs Incremental Loading); `pipeline_how_data_moves_intermediate` (Batch Mechanics)
- **Backfills**: `pa_pattern_incremental_loading_intermediate` (How Do You Backfill?); `pipeline_keeping_data_fresh_beginner` (Backfilling); `pipeline_keeping_data_fresh_intermediate` (Partition-Level Backfill)
- **Slowly changing dimensions and history tracking**: `pa_pattern_incremental_loading_intermediate` (How Do You Track History?); `pipeline_keeping_data_fresh_beginner` (Slowly Changing Dimensions); `pipeline_keeping_data_fresh_intermediate` (SCD in Pipelines)
- **MERGE and upsert cost, choosing overwrite vs MERGE vs rebuild**: `pipeline_keeping_data_fresh_intermediate` (Merge Strategies); `pa_pattern_idempotency` (DELETE-INSERT and MERGE)
- **File and API ingestion** (completeness markers, ledgers, pagination, rate limits, webhooks): `pipeline_how_data_moves_beginner`; `pipeline_how_data_moves_intermediate`; `pa_pattern_design_pipeline_intermediate` (Ingestion Patterns)

### Schemas
- **Schema evolution, compatibility, registries, data contracts, breaking changes**: `pa_pattern_schema_evolution`; `pipeline_keeping_data_fresh_beginner` (Schema Evolution); `pipeline_keeping_data_fresh_intermediate` (Schema Migration)

### Storage and file formats
- **Columnar formats, Parquet, encodings, compression**: `pipeline_where_data_lives_beginner` (Columnar vs Row, Compression); `pipeline_where_data_lives_intermediate` (Encoding Types); `pa_pattern_storage_question_intermediate` (Why Parquet?)
- **Partitioning and partition pruning**: `pipeline_where_data_lives_beginner` (Partitioning); `pa_pattern_storage_question_intermediate` (How Would You Partition?)
- **Small files and compaction**: `pa_pattern_small_files`; `pipeline_where_data_lives_intermediate` (The Small File Problem); `pipeline_distributed_compute_beginner` (Small File Problem)
- **Table formats** (Delta, Iceberg, snapshots, catalogs, deletes in a lake): `pa_pattern_storage_question_intermediate` (Delta or Iceberg?); `pipeline_where_data_lives_beginner` (Table Formats); `pipeline_where_data_lives_intermediate` (The Table Catalog, Deleting Records From a Data Lake)
- **Lake vs warehouse vs lakehouse**: `pa_pattern_storage_question_intermediate` (Data Lake or Warehouse?); `pipeline_where_data_lives_beginner` (Lake vs Warehouse)
- **Storage cost and tiering**: `pa_pattern_storage_question_intermediate` (How Much Will This Cost?); `pipeline_where_data_lives_intermediate` (Storage Tiering)

### Compute
- **Spark internals** (jobs, stages, shuffles, memory, skew, joins, cluster sizing): `pa_pattern_spark_deep_dive_intermediate`; `pipeline_distributed_compute_beginner`

### System design and operations at scale
- **Designing a pipeline end to end** (ingestion, storage, transformation, serving, reliability): `pa_pattern_design_pipeline_intermediate`
- **Platform-scale design** (sizing, governed ingestion, serving contracts, cost, guarantees by tier): `pa_pattern_design_pipeline_advanced`
- **Cross-region replication, data residency, moving sensitive data**: `pipeline_how_data_moves_advanced`

## When lessons overlap

- **Change capture vs delivery vs idempotency.** `pa_pattern_cdc_dual_writes` is about getting changes out of a source database (log, polling, outbox). `pa_pattern_delivery_semantics` is about what guarantee an event stream gives once events exist. `pa_pattern_idempotency` is about making writes safe to repeat. A question about duplicates after a crash usually belongs to the last two; a question about capturing deletes or avoiding dual writes belongs to the first.
- **Late data.** Concepts and clocks are in `pa_pattern_late_data_watermarks`; testing and recovery of streaming jobs are in `pa_pattern_batch_or_stream_intermediate`.
- **Small files** appear in four lessons. Depth and numbers are in `pa_pattern_small_files`; the others are short treatments inside broader lessons.
- **Backfills and incremental loads** are split by level: `pa_pattern_incremental_loading_intermediate` has the pitfalls and numbers; the `pipeline_keeping_data_fresh_*` lessons have the fundamentals.

## All lessons

- `pa_pattern_backpressure_scaling`: overload, lag, partition sizing, autoscaling, recovery time
- `pa_pattern_batch_or_stream_intermediate`: batch vs stream choice, Lambda/Kappa, testing, checkpoints
- `pa_pattern_cdc_dual_writes`: CDC methods, Debezium, polling limits, dual writes, outbox
- `pa_pattern_dead_letter_queue`: failing records, DLQ design, retries, poison pills, replay
- `pa_pattern_delivery_semantics`: at-most, at-least, exactly-once, dedup, idempotency keys
- `pa_pattern_design_pipeline_advanced`: platform sizing, governed ingestion, serving contracts, tiers
- `pa_pattern_design_pipeline_intermediate`: ingestion, storage, transformation, serving, reliability
- `pa_pattern_idempotency`: repeat-safe writes, MERGE, offsets, streaming vs batch, proofs
- `pa_pattern_incremental_loading_intermediate`: full vs incremental, deletes, filter columns, backfills, history
- `pa_pattern_late_data_watermarks`: event time, watermarks, allowed lateness, side outputs
- `pa_pattern_make_it_reliable_intermediate`: quality checks, safe reruns, duplicates, monitors, rollback
- `pa_pattern_schema_evolution`: compatibility, registries, contracts, breaking changes, table formats
- `pa_pattern_small_files`: diagnosis, cost, compaction, prevention, table-format compaction
- `pa_pattern_spark_deep_dive_intermediate`: architecture, slow jobs, joins, cluster sizing, skew
- `pa_pattern_storage_question_intermediate`: Parquet, partitioning, Delta vs Iceberg, lake vs warehouse, cost
- `pipeline_distributed_compute_beginner`: Spark execution, shuffles, memory, small files
- `pipeline_how_data_moves_advanced`: idempotent pipelines, backpressure, cross-region, sensitive data, freshness cost
- `pipeline_how_data_moves_beginner`: batch, streaming, file and API ingestion, batch vs streaming
- `pipeline_how_data_moves_intermediate`: high-water marks, stream guarantees, file formats, APIs, hybrid designs
- `pipeline_keeping_data_fresh_beginner`: full vs incremental, CDC, SCDs, schema evolution, backfills
- `pipeline_keeping_data_fresh_intermediate`: merge strategies, CDC, SCDs, schema migration, backfills
- `pipeline_making_it_repeatable_beginner`: DAGs, dependencies, safe retries, environments, CI/CD
- `pipeline_streaming_systems_beginner`: event platforms, event-driven design, late data, DLQs, micro-batch
- `pipeline_streaming_systems_intermediate`: consumer groups, event sourcing, windowing, DLQ patterns, Spark vs Flink
- `pipeline_where_data_lives_beginner`: columnar vs row, compression, partitioning, lake vs warehouse, table formats
- `pipeline_where_data_lives_intermediate`: encodings, small files, deletes, storage tiering, catalogs
