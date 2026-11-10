# Index: pa_pattern_batch_or_stream_intermediate.md
Source: `lesson_preview/pa_pattern_batch_or_stream_intermediate.md` (490 lines). Read just the line range you need (Read offset/limit).

## When Would You Use Streaming? (L27-93)

### Four factors that decide between stream and batch (L34-61)

### Quantify the cost of running a stream continuously (L62-75)

### Applying the four factors to a dashboard refresh need (L76-93)

## Design for Both Batch and Stream (L94-204)

### Merging batch and speed views at the batch boundary (L103-160)

### Dual maintenance cost of Lambda's two code paths (L161-172)

### Converging batch and stream into one job definition (L173-204)

## How Do You Test a Streaming Job? (L205-324)

### Three test layers for streaming jobs: unit, replay, shadow (L212-223)

### Replay fixture covering duplicates, out-of-order and late events (L224-279)

### Scoring a shadow run against a bounded recompute (L280-324)

## Micro-batch or True Streaming? (L325-392)

### Where micro-batch trigger latency comes from (L332-367)

### Choose the engine by latency SLA first, then team skills (L368-392)

## How Do You Restart a Stream? (L393-467)

### How a stateful stream recovers from a crash (L402-422)

### Handling duplicate sink output after a restart (L423-450)

### Sizing checkpoint interval against replay and write load (L451-467)
