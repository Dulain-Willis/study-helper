# Index: pa_pattern_idempotency.md
Source: `lesson_preview/interview/architecture/pa_pattern_idempotency.md` (597 lines). Read just the line range you need (Read offset/limit).

## "What If Your Pipeline Runs Twice?" (L27-140)

### Pipeline idempotency means repeated runs leave the target unchanged (L34-51)

### Five sources of repeated runs: retries, backfills, restarts, zombies, redelivery (L52-66)

### Crash after commit, before success marker, duplicates appended rows (L67-81)

### Nondeterministic functions in a transform make reruns write different rows (L82-140)

## DELETE-INSERT and MERGE (L141-285)

### Safe partition replace needs DELETE and INSERT in one transaction (L148-181)

### Version guard stops stale MERGE rows from overwriting newer data (L182-244)

### Choose partition replace or MERGE by where changes scatter (L245-285)

## Idempotent Reads With Offsets (L286-372)

### Pin each run's input to a recorded offset range (L293-308)

### Commit order sets crash cost: at-most-once, at-least-once, or exactly-once effect (L309-345)

### Timestamp watermarks skip rows that commit after a later stamp (L346-372)

## Idempotency in Streaming vs Batch (L373-467)

### Dedup state scales with rate times window length (L388-401)

### Checkpoint-bound sinks commit output with the checkpoint, trading latency (L402-408)

### Window aggregates need an upsert sink keyed by window start (L409-430)

### Side effects need receiver-side idempotency keys derived from events (L431-467)

## Proving Idempotency Out Loud (L468-574)

### Argue idempotency at each crash point, then fingerprint-test it (L488-496)

### Walk each crash point to show a retry converges (L497-508)

### Prove idempotency by fingerprinting a slice after identical reruns (L509-536)

### Exactly-once effect needs at-least-once delivery plus idempotent writes (L537-574)
