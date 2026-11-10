# Index: pa_pattern_batch_or_stream_intermediate.md
Source: `lesson_preview/pa_pattern_batch_or_stream_intermediate.md` (490 lines). Read just the line range you need (Read offset/limit).

## When Would You Use Streaming? (L27-93)

### Four factors for choosing stream or batch processing (L34-61)
### Estimating the cost of always-on streaming compute versus batch (L62-75)
### Applying the four factors to a stakeholder freshness request (L76-93)

## Design for Both Batch and Stream (L94-204)

### Merging batch and speed views at the batch boundary hour (L103-160)
### Lambda architecture duplicates logic across batch and speed layers (L161-172)
### Converging batch and stream into one replayable job definition (L173-204)

## How Do You Test a Streaming Job? (L205-324)

### Three test layers for a streaming job: unit, replay, shadow (L212-223)
### Building a replay fixture with duplicates, late and out-of-order events (L224-279)
### Scoring a shadow run against a bounded recompute (L280-324)

## Micro-batch or True Streaming? (L325-392)

### Micro-batch latency floor from trigger interval plus batch run time (L332-367)
### Choosing micro-batch or record-at-a-time from the SLA, then the team (L368-392)

## How Do You Restart a Stream? (L393-467)

### Recovery timeline after a crash: replayed events and catch-up latency (L402-422)
### Making a sink tolerate replayed emissions after a restart (L423-450)
### Choosing a checkpoint interval from replay size and store load (L451-467)
