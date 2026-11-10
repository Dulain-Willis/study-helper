# Index: pa_pattern_batch_or_stream_intermediate.md
Source: `lesson_preview/pa_pattern_batch_or_stream_intermediate.md` (490 lines). Read just the line range you need (Read offset/limit).

## When Would You Use Streaming? (L27-93)

### The 4 factors (L34-61)

### Put a number on the cost (L62-75)

### Practice: a request for real-time order counts (L76-93)

## Design for Both Batch and Stream (L94-204)

### The merge the serving layer runs (L103-160)

### The dual-maintenance tradeoff (L161-172)

### Convergence: one code path, 2 cadences (L173-204)

## How Do You Test a Streaming Job? (L205-324)

### 3 layers of tests (L212-223)

### A fixture that covers the cases that bite (L224-279)

### The shadow run, and the query that scores it (L280-324)

## Micro-batch or True Streaming? (L325-392)

### Where the latency comes from (L332-367)

### Match to the SLA first, then to the team (L368-392)

## How Do You Restart a Stream? (L393-467)

### A crash, second by second (L402-422)

### What the sink sees twice (L423-450)

### Choosing the checkpoint interval (L451-467)
