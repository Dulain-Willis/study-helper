# Index: pa_pattern_batch_or_stream_intermediate.md
Source: `lesson_preview/pa_pattern_batch_or_stream_intermediate.md` (490 lines). Read just the line range you need (Read offset/limit).

## When streaming is worth its cost (L27-93)

### Streaming pays to shorten event-to-decision gap; SLA comes from consumer (L29-33)

### Four factors: consumer latency, cost of lateness, source shape, team ops (L34-46)

### Stream when SLA is under fastest affordable batch worst case (L47-61)

### Streaming cost is always-on compute; quote the SLA, not modernity (L62-75)

### Score a 'real-time' dashboard request; hybrid stream plus batch (L76-93)

## Designing for both batch and stream (L94-204)

### Lambda architecture: batch layer, speed layer, serving-layer merge (L96-102)

### Lambda serving merge: batch up to boundary hour, then speed (L103-160)

### Lambda dual-maintenance: same logic in two codebases drifts (L161-172)

### Kappa and converged design: one job, reprocess by replay (L173-193)

### Pitch convergence over Lambda; needs log retention, late-data handling (L194-204)

## Testing streaming jobs (L205-324)

### Replay testing fixes event order, gaps, and clock (L207-211)

### Three test layers: unit, replay, shadow run (L212-223)

### Replay fixture needs duplicate, out-of-order, and too-late events (L224-238)

### Watermark and lateness allowance decide window closing and late drops (L239-279)

### Shadow run vs bounded recompute: score diffs, release criterion (L280-301)

### Streaming test do's and don'ts; how to answer in interviews (L302-324)

## Micro-batch vs record-at-a-time engines (L325-392)

### Micro-batch plans per trigger; record-at-a-time processes each event (L327-331)

### Micro-batch latency is trigger plus run; tiny triggers waste overhead (L332-346)

### Micro-batch vs record-at-a-time: latency, throughput, state, failure (L347-367)

### Pick engine from SLA and p99 tail, then team's runtime (L368-392)

## Restarting a stream from checkpoints (L393-467)

### Checkpoint holds offsets, operator state, and sink position together (L395-401)

### Crash recovery replays since last checkpoint; costs latency, not correctness (L402-422)

### Replay duplicates sink output: upsert, transactional, or dedupe-on-read (L423-450)

### Checkpoint interval trades replay size against snapshot write load (L451-454)

### Restart answer in order: checkpoint, rewind, reprocess, tolerant sink (L455-467)
