# Index: pa_pattern_idempotency.md
Source: `lesson_preview/pa_pattern_idempotency.md` (597 lines). Read just the line range you need (Read offset/limit).

## "What If Your Pipeline Runs Twice?" (L27-140)

### A definition you can test (L34-51)

### Where repeat executions come from (L52-66)

### The 3 moments a run can die (L67-81)

### Nondeterminism inside the transform (L82-140)

## DELETE-INSERT and MERGE (L141-285)

### Partition replace, written carefully (L148-181)

### MERGE repeats safely under 3 conditions (L182-244)

### What each choice costs (L245-285)

## Idempotent Reads With Offsets (L286-372)

### Record the range before reading it (L293-308)

### Commit order decides what a crash costs (L309-345)

### Timestamp watermarks leak rows (L346-372)

## Idempotency in Streaming vs Batch (L373-467)

### Dedup state has a size and an edge (L388-401)

### Checkpoint-bound sinks trade latency (L402-408)

### Window results need an upsert sink (L409-430)

### Side effects leave the transaction (L431-467)

## Proving Idempotency Out Loud (L468-574)

### A worked answer (L488-496)

### Walk every crash point (L497-508)

### The rerun test (L509-536)

### Follow-ups and what they probe (L537-574)
