# Index: pa_pattern_batch_or_stream_intermediate.md
Source: `lesson_preview/pa_pattern_batch_or_stream_intermediate.md` (490 lines). Read just the line range you need (Read offset/limit).

## When Would You Use Streaming? (L27-93)

### Score a workload on four factors: latency, cost of lateness, source, team (L34-46)

### Streaming pays off when the SLA is shorter than the fastest batch worst case (L47-48)

### Mapping SLA and source shape to stream, micro-batch, or batch (L49-61)

### Streaming cost is always-on compute versus a short batch run (L62-75)

### Translate a vague 'real-time' request into an SLA number (L76-86)

### Hybrid design: one source feeds a stream and a batch by SLA (L87-93)

## Design for Both Batch and Stream (L94-204)

### Lambda architecture: batch layer, speed layer, serving layer merge (L96-102)

### Lambda merge cuts the speed view at the batch boundary (L103-160)

### Lambda duplicates business logic in two codebases and drifts (L161-172)

### Kappa and converged design: one job, reprocess by log replay (L173-193)

### Convergence requires log retention and stream late-data handling (L194-204)

## How Do You Test a Streaming Job? (L205-324)

### Streaming tests must control event order, timing, and the clock (L207-211)

### Three test layers: unit, replay, and shadow run (L212-223)

### Replay fixture covers duplicate, out-of-order, and too-late events (L224-274)

### Watermark and lateness allowance decide when a window closes (L275-279)

### Shadow run diffed against a bounded recompute gates a release (L280-301)

### Streaming test do's and don'ts: watermark from fixture, no sleep() (L302-324)

## Micro-batch or True Streaming? (L325-392)

### How micro-batch and record-at-a-time engines process events (L327-331)

### Micro-batch latency equals trigger interval plus batch run time (L332-344)

### Shrinking the trigger trades lower latency for fixed per-cycle overhead (L345-346)

### Engine comparison: latency floor, throughput, state, failure unit, skills (L347-367)

### Pick micro-batch or record-at-a-time from the SLA, then team skills (L368-379)

### Micro-batch fails tight SLAs on p99 tail latency, not median (L380-385)

### Continuous mode offers sub-second micro-batch with weaker guarantees (L386-392)

## How Do You Restart a Stream? (L393-467)

### A checkpoint snapshots offsets, operator state, and sink position together (L395-401)

### Restart replays events after the last checkpoint without losing data (L402-422)

### Replayed emissions duplicate in the sink: upsert, transactions, or dedupe (L423-450)

### How to choose checkpoint interval from replay size and write load (L451-454)

### Template for answering how a stream restarts after failure (L455-467)
