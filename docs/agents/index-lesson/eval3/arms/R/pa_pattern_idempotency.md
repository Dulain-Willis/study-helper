# Index: pa_pattern_idempotency.md
Source: `lesson_preview/pa_pattern_idempotency.md` (597 lines). Read just the line range you need (Read offset/limit).

## "What If Your Pipeline Runs Twice?" (L27-140)

### Idempotent means a rerun leaves the same target state (L34-51)

### Causes of repeated pipeline executions (L52-66)

### Three points where a pipeline run can crash (L67-81)

### Nondeterministic transforms break safe reruns (L82-140)

## DELETE-INSERT and MERGE (L141-285)

### Partition replace needs DELETE and INSERT in one transaction (L148-181)

### MERGE reruns safely with unique source keys and version guard (L182-244)

### Partition replace versus MERGE: what each costs to rerun (L245-285)

## Idempotent Reads With Offsets (L286-372)

### Record a run's input offset range before reading it (L293-308)

### Commit order sets whether a crash loses or repeats records (L309-345)

### Timestamp watermarks skip late-committed rows (L346-372)

## Idempotency in Streaming vs Batch (L373-467)

### Dedup state grows with event rate times window length (L388-401)

### Checkpoint-bound sinks delay output until checkpoint completes (L402-408)

### Window results need an upsert sink keyed by window (L409-430)

### Side effects need receiver-side keys derived from events (L431-467)

## Proving Idempotency Out Loud (L468-574)

### Answering whether a pipeline design is idempotent (L488-496)

### Walking crash points to show a retry converges (L497-508)

### Rerun fingerprint test: compare target state after two runs (L509-536)

### Follow-up questions on backfill, exactly-once, overlap, and cost (L537-574)
