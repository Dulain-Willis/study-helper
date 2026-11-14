# Index: pipeline_streaming_systems_intermediate.md
Source: `lesson_preview/interview/architecture/pipeline_streaming_systems_intermediate.md` (478 lines). Read just the line range you need (Read offset/limit).

## Consumer Groups and Offsets (L27-121)

### Partition count caps consumer group parallelism (L29-42)
### Where the offset commit sits sets the delivery guarantee (L43-61)
### Rebalances replay records processed but not yet committed (L62-68)
### Alert on growing consumer lag, judged by drain rate (L69-75)
### Three ways to get an exactly-once effect from at-least-once delivery (L76-105)
### Use idempotency keys for effects outside any transaction (L106-121)

## Event Sourcing Patterns (L122-203)

### Event-sourced state is a fold over an append-only log (L124-151)
### Snapshot folded state periodically to bound replay length (L152-158)
### CQRS separates write models from read projections (L159-183)
### Event sourcing costs: versioning, erasure, replay time, ordering (L184-203)

## Windowing and Watermarks (L204-290)

### Choose tumbling, hopping, or session windows by question (L206-220)
### Window by event time, not processing time (L221-227)
### A watermark asserts that no earlier event time will arrive (L228-252)
### Choose a watermark bound from measured lateness percentiles (L253-264)
### Idle partitions stall the watermark unless a timeout excludes them (L265-290)

## DLQ Patterns (L291-381)

### Classify failures by whether retry would change the outcome (L293-308)
### Retry topics move failed records aside without blocking partitions (L309-319)
### DLQ records keep original bytes and failure metadata for redrive (L320-342)
### Redrive safely: dedupe, rate limit, and cap retries (L343-381)

## Spark Streaming vs Flink (L382-456)

### Micro-batch and continuous dataflow run jobs differently (L389-407)
### Micro-batch latency is trigger interval plus processing time (L408-418)
### Per-key timers and keyed state favor continuous dataflow (L419-425)
### Exactly-once and overload symptoms differ by engine model (L426-441)
### Choose a streaming engine from requirements, then state its cost (L442-456)
