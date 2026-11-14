# Index: pa_pattern_design_pipeline_intermediate.md
Source: `lesson_preview/interview/architecture/pa_pattern_design_pipeline_intermediate.md` (423 lines). Read just the line range you need (Read offset/limit).

## Ingestion Patterns (L28-116)

### Log-based CDC via a replication slot pins WAL until consumed (L35-43)

### Rate-limited API pulls need a high-water mark and cursor paging (L44-76)

### Deduplicate re-uploaded files by content checksum, not filename (L77-96)

### Choose micro-batch over streaming from the consumer's staleness tolerance (L97-116)

## Storage Architecture (L117-188)

### Immutable raw bronze layer lets silver and gold be rebuilt (L124-136)

### Columnar Parquet for reads, row-based Avro with schema for writes (L137-143)

### Iceberg-style table formats commit by atomic snapshot pointer swap (L144-152)

### Choose partition key from query filters, avoiding high-cardinality columns (L153-188)

## Transformation Strategy (L189-268)

### ELT over ETL: reprocess from raw storage, not re-extraction (L191-217)

### Idempotent incremental MERGE needs per-key dedup and an update guard (L218-251)

### Full refresh over a trailing window suits small late-arriving tables (L252-268)

## Serving the Consumer (L269-331)

### Serve high-frequency small aggregate reads from a row store (L282-302)

### Match revenue to payouts by capture date, not order date (L303-309)

### Serve full-history scans from silver before building feature stores (L310-316)

### Monitor each serving table's freshness with a max-timestamp query (L317-331)

## Reliability Under Failure (L332-402)

### Retries are safe only when the write is idempotent (L339-347)

### Recompute a trailing window to absorb late-arriving events (L348-354)

### Dead-letter paths for malformed records with a reject-rate alert (L355-361)

### Schema registry compatibility modes decide which upstream changes pass (L362-368)

### Monitors for freshness, row volume, and replication slot lag (L369-402)
