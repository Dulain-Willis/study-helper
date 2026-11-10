# Index: pa_pattern_late_data_watermarks.md
Source: `lesson_preview/pa_pattern_late_data_watermarks.md` (408 lines). Read just the line range you need (Read offset/limit).

## "What If Events Are Out of Order?" (L27-101)

### Recognize late data by event and arrival times that disagree (L29-42)

### Waiting for late events trades correctness against latency and state (L43-80)

### Event-time windows need a watermark and a late-event rule (L81-101)

## Event Time vs Processing Time (L102-174)

### Event, ingestion and processing time: three clocks on one record (L104-121)

### Set watermark bound as a percentile of measured event delay (L122-149)

### Clamp future event times against ingestion time (L150-174)

## Watermarks Estimate Completeness (L175-232)

### Watermark equals largest event time seen minus a fixed bound (L177-197)

### Idle inputs stall a minimum watermark until an idleness timeout (L198-216)

### Watermark bound sets latency and open-window state per key (L217-232)

## Allowed Lateness and Side Outputs (L233-320)

### Allowed lateness retains window state after watermark passes its end (L235-252)

### Idempotent upsert keyed by window makes accumulating firings retry-safe (L253-271)

### Distinct counts, percentiles and ratios cannot take late deltas (L272-278)

### Side output with batch recompute corrects events dropped past lateness (L279-320)

## Watermark Strategies in Production (L321-384)

### Bounded out-of-orderness, source-supplied and adaptive percentile watermarks (L323-335)

### Retry, replay, schema change and DST break watermark pipelines (L336-350)

### Four metrics that reveal a wrong watermark bound (L351-368)

### Structure late-data answers: clock, bound, watermark, lateness, sink, alerts (L369-384)
