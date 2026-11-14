# Index: pa_pattern_backpressure_scaling.md
Source: `lesson_preview/interview/architecture/pa_pattern_backpressure_scaling.md` (499 lines). Read just the line range you need (Read offset/limit).

## The Volume-Doubling Question (L27-106)

### Load changes reduce to arrival rate, stage capacity, and duration (L29-45)

### Per-stage capacity table finds the first break and next ceiling (L46-64)

### Pull pipelines show overload as growing lag, not crashes (L65-106)

## Backpressure Mechanisms (L107-214)

### Four ways to handle excess load: block, buffer, shed, throttle (L114-127)

### Bounded consumer buffers prevent heap exhaustion and replay storms (L128-165)

### Shed or buffer per event class by value decay (L166-214)

## Horizontal Scaling With Partitions (L215-289)

### How to size partition count from throughput target and headroom (L222-239)

### Changing partition count breaks per-key ordering (L240-264)

### Hot keys cap a partition's throughput regardless of partition count (L265-289)

## Autoscaling and Cost Tradeoffs (L290-366)

### Autoscale IO-bound consumers on lag, not CPU (L297-312)

### Autoscaler reaction gap before new capacity arrives (L313-325)

### Cap autoscaling maximum at partition count and downstream quotas (L326-332)

### Cost tradeoffs: static, autoscaled, and cached provisioning (L333-366)

## End-to-End Throughput Analysis (L367-476)

### Little's Law for sizing concurrency and buffer memory (L374-389)

### Recovery time is backlog divided by surplus over live input (L390-407)

### Handling a degraded dependency: caching and shedding (L408-414)

### Diagnosing capacity shortage vs. hot keys from per-partition lag (L415-476)
