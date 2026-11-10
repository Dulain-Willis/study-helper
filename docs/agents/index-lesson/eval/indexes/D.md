# Index: pa_pattern_batch_or_stream_intermediate.md
Source: `lesson_preview/pa_pattern_batch_or_stream_intermediate.md` (490 lines). Read just the line range you need (Read offset/limit).

## When Would You Use Streaming? (L27-93)

### Score a workload on four factors: latency, cost of lateness, source, team (L34-48)

### Mapping SLA and source shape to stream, micro-batch, or batch (L49-61)

### Streaming cost is always-on compute versus a short batch run (L62-75)

### Translate a vague 'real-time' request into an SLA number (L76-93)

## Design for Both Batch and Stream (L94-204)

### Lambda merge cuts the speed view at the batch boundary (L96-160)

### Lambda duplicates business logic in two codebases and drifts (L161-172)

### Kappa and converged design: one job, reprocess by log replay (L173-193)

### Convergence requires log retention and stream late-data handling (L194-204)

## How Do You Test a Streaming Job? (L205-324)

### Three test layers: unit, replay, and shadow run (L207-223)

### Replay fixture covers duplicate, out-of-order, and too-late events (L224-279)

### Shadow run diffed against a bounded recompute gates a release (L280-301)

### Streaming test do's and don'ts: watermark from fixture, no sleep() (L302-324)

## Micro-batch or True Streaming? (L325-392)

### Micro-batch latency equals trigger interval plus batch run time (L327-346)

### Engine comparison: latency floor, throughput, state, failure unit, skills (L347-367)

### Pick micro-batch or record-at-a-time from the SLA, then team skills (L368-392)

## How Do You Restart a Stream? (L393-467)

### Restart replays events after the last checkpoint without losing data (L395-422)

### Replayed emissions duplicate in the sink: upsert, transactions, or dedupe (L423-454)

### Template for answering how a stream restarts after failure (L455-467)
