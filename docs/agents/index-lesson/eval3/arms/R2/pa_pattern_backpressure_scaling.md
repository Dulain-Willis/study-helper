# Index: pa_pattern_backpressure_scaling.md
Source: `lesson_preview/pa_pattern_backpressure_scaling.md` (499 lines). Read just the line range you need (Read offset/limit).

## The Volume-Doubling Question (L27-106)

### Every load change reduces to arrival rate, capacity, and duration (L29-45)

### Find the first break with a per-stage capacity table (L46-64)

### Lag is the failure signal in pull-based pipelines (L65-106)

## Backpressure Mechanisms (L107-214)

### Four ways to handle excess load: block, buffer, shed, throttle (L114-127)

### Bound a consumer's in-memory buffer so overload becomes lag (L128-165)

### Decide per event class whether to shed or buffer overload (L166-214)

## Horizontal Scaling With Partitions (L215-289)

### How to size partition count from throughput target and headroom (L222-239)

### Changing partition count breaks per-key ordering (L240-264)

### Skew caps one partition's throughput at any partition count (L265-289)

## Autoscaling and Cost Tradeoffs (L290-366)

### Autoscale IO-bound consumers on lag, not CPU (L297-312)

### Estimate the reaction gap between a spike and added capacity (L313-325)

### Cap the maximum consumer count at the narrowest downstream quota (L326-332)

### Compare daily cost of static, autoscaled, and cached designs (L333-366)

## End-to-End Throughput Analysis (L367-476)

### Little's Law sizes concurrency and buffer memory at each hop (L374-389)

### Estimate backlog drain time from surplus over live input (L390-407)

### Estimate throughput when a dependency's latency rises (L408-414)

### Diagnose partition lag from per-partition metrics (L415-476)
