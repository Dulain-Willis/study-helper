# Index: pa_pattern_batch_or_stream_intermediate.md
Source: `lesson_preview/pa_pattern_batch_or_stream_intermediate.md` (490 lines). Read just the line range you need (Read offset/limit).

## When Would You Use Streaming? (L27-93)

### Four factors that decide between stream and batch (L34-61)

### Streaming costs compute that never stops (L62-75)

### How to score a workload on the 4 factors (L76-93)

## Design for Both Batch and Stream (L94-204)

### How a serving layer merges batch and speed views (L103-160)

### Lambda's dual maintenance duplicates business logic across two layers (L161-172)

### One job definition runs as both batch and stream (L173-204)

## How Do You Test a Streaming Job? (L205-324)

### Three test layers for a streaming job: unit, replay, shadow (L212-223)

### Replay fixtures must include duplicate and late events (L224-279)

### Shadow runs compare a live stream against a bounded recompute (L280-324)

## Micro-batch or True Streaming? (L325-392)

### Micro-batch latency floor equals the trigger plus the run (L332-367)

### Choose the engine by SLA first, then by team (L368-392)

## How Do You Restart a Stream? (L393-467)

### How a stream restart rebuilds state from a checkpoint (L402-422)

### Sinks must tolerate duplicate emissions after restart (L423-450)

### Checkpoint interval trades replay size against snapshot write load (L451-467)
