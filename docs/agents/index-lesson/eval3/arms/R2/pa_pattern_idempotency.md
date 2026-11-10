# Index: pa_pattern_idempotency.md
Source: `lesson_preview/pa_pattern_idempotency.md` (597 lines). Read just the line range you need (Read offset/limit).

## "What If Your Pipeline Runs Twice?" (L27-140)

### A testable definition of pipeline idempotency (L34-51)

### Sources of repeat executions in pipelines (L52-66)

### The three crash points of a pipeline run (L67-81)

### Nondeterminism inside a transform breaks reruns (L82-140)

## DELETE-INSERT and MERGE (L141-285)

### Partition replace needs matching DELETE and INSERT predicates (L148-181)

### MERGE is safe to rerun under three conditions (L182-244)

### Partition replace and MERGE differ in rewrite cost (L245-285)

## Idempotent Reads With Offsets (L286-372)

### Record a run's offset range before reading it (L293-308)

### Commit order determines what a crash costs (L309-345)

### Timestamp watermarks can skip late-committed rows (L346-372)

## Idempotency in Streaming vs Batch (L373-467)

### Dedup state size equals rate times window (L388-401)

### Checkpoint-bound sinks trade latency for exactly-once writes (L402-408)

### Window results need an upsert sink keyed by window (L409-430)

### Side effects need receiver-side idempotency keys (L431-467)

## Proving Idempotency Out Loud (L468-574)

### How the pieces of an idempotent design combine (L488-496)

### Each crash point must converge to one run's result (L497-508)

### A rerun fingerprint test proves idempotency (L509-536)

### Common follow-up challenges to idempotent pipeline designs (L537-574)
