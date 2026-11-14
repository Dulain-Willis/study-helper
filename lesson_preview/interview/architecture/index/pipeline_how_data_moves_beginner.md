# Index: pipeline_how_data_moves_beginner.md
Source: `lesson_preview/interview/architecture/pipeline_how_data_moves_beginner.md` (508 lines). Read just the line range you need (Read offset/limit).

## Batch Processing (L27-106)

### Batch processing reads a bounded input on a schedule (L40-48)

### Half-open time windows define exact batch boundaries in UTC (L49-79)

### Batch job failure modes and the properties that prevent them (L80-97)

### Describe a batch pipeline: boundary, trigger, rerun behavior (L98-106)

## Stream Processing (L107-191)

### Streams move unbounded events from producer through broker to consumer (L114-133)

### Event-time windows need a grace period for late events (L134-149)

### Idempotent writes keyed by event_id make stream retries safe (L150-156)

### Justify streaming cost only when event value decays quickly (L157-182)

### Stream answer template: define, apply, handle failures, justify cost (L183-191)

## File Ingestion (L192-293)

### Detect a complete file by marker, manifest or stable size (L207-213)

### A load ledger tracks files by name for idempotent reloads (L214-241)

### Settle header, delimiter, encoding, date, null and number formats (L242-255)

### File load failure modes and the property that prevents each (L256-284)

### File load answer template: manifest, ledger, staging, named headers, alert (L285-293)

## API Ingestion (L294-408)

### Cursor pagination beats offset when the source data changes (L309-360)

### HTTP status codes map to retry, refresh, record or fail (L361-374)

### Incremental pulls with an updated_since overlap for clock skew (L375-381)

### API pull failures: resumable cursors, keyed upserts, token refresh (L382-399)

### API pull answer template: paging, retries, incremental, raw storage (L400-408)

## Batch vs Streaming (L409-486)

### Choose batch or streaming by freshness requirement and consumer (L416-446)

### Micro-batches every few minutes cover most near-real-time needs (L447-453)

### Answer a batch-or-streaming question in ordered steps (L454-473)

### Pick the cheapest design that meets a stated freshness need (L474-486)
