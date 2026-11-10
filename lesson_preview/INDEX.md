# Lesson index: start here

Pick the lesson below, open `lesson_preview/index/<lesson>.md` for its sections and line ranges, then read only that range of `lesson_preview/<lesson>.md`.

`pipeline_*` lessons teach fundamentals; `pa_pattern_*` lessons go deep with numbers and interview-style answers.

- `pa_pattern_backpressure_scaling`: overload, consumer lag, partition sizing, autoscaling, recovery time
- `pa_pattern_batch_or_stream_intermediate`: choosing batch vs stream, Lambda/Kappa, testing streaming jobs, checkpoints and restart
- `pa_pattern_cdc_dual_writes`: getting changes out of a source database (log CDC, polling, dual writes, outbox); for delivery guarantees see `pa_pattern_delivery_semantics`
- `pa_pattern_dead_letter_queue`: handling bad records, DLQ design, retries, poison pills, replay
- `pa_pattern_delivery_semantics`: at-most, at-least, exactly-once, duplicates and dedup, idempotency keys
- `pa_pattern_design_pipeline_advanced`: platform-scale design: sizing, governed ingestion, serving contracts, guarantees by tier
- `pa_pattern_design_pipeline_intermediate`: end-to-end pipeline design: ingestion, storage, transformation, serving, reliability
- `pa_pattern_idempotency`: making writes safe to repeat: partition replace, MERGE, offsets, streaming vs batch
- `pa_pattern_incremental_loading_intermediate`: full vs incremental loads, catching deletes, filter columns, backfills, history tracking
- `pa_pattern_late_data_watermarks`: event time, watermarks, allowed lateness, side outputs; for testing streaming jobs see `pa_pattern_batch_or_stream_intermediate`
- `pa_pattern_make_it_reliable_intermediate`: data quality checks, safe reruns, duplicates, monitoring, rolling back a bad load
- `pa_pattern_schema_evolution`: compatibility, schema registries, data contracts, breaking changes, table formats
- `pa_pattern_small_files`: diagnosing, costs, compaction and prevention of small files (the deep version; shorter treatments in three other lessons)
- `pa_pattern_spark_deep_dive_intermediate`: Spark architecture, slow jobs, joins, cluster sizing, data skew
- `pa_pattern_storage_question_intermediate`: Parquet, partitioning, Delta vs Iceberg, lake vs warehouse, storage cost
- `pipeline_distributed_compute_beginner`: Spark execution model, shuffles, memory, small files
- `pipeline_how_data_moves_advanced`: idempotent pipelines, backpressure, cross-region replication, sensitive data, cost of freshness
- `pipeline_how_data_moves_beginner`: batch, streaming, file and API ingestion, batch vs streaming
- `pipeline_how_data_moves_intermediate`: high-water marks, stream guarantees, file formats, API pagination, hybrid designs
- `pipeline_keeping_data_fresh_beginner`: full vs incremental loading, CDC, slowly changing dimensions, schema evolution, backfilling
- `pipeline_keeping_data_fresh_intermediate`: merge strategies, CDC, SCDs, schema migration, partition-level backfills
- `pipeline_making_it_repeatable_beginner`: DAG orchestration, dependencies, safe retries, dev/staging/prod, CI/CD for data
- `pipeline_streaming_systems_beginner`: event platforms, event-driven design, late data, dead-letter queues, micro-batch vs streaming
- `pipeline_streaming_systems_intermediate`: consumer groups and offsets, event sourcing, windowing, DLQ patterns, Spark Streaming vs Flink
- `pipeline_where_data_lives_beginner`: columnar vs row, compression, partitioning, lake vs warehouse, table formats
- `pipeline_where_data_lives_intermediate`: Parquet encodings, small files, deleting from a lake, storage tiering, catalogs
