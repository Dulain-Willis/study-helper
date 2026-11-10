# Index: pa_pattern_backpressure_scaling.md
Source: `lesson_preview/pa_pattern_backpressure_scaling.md` (499 lines). Read just the line range you need (Read offset/limit).

## The Volume-Doubling Question (L27-106)

### Load-growth questions reduce to arrival rate, capacity, and duration (L29-45)

### Build a per-stage capacity table to find the first break (L46-64)

### How overload appears as lag in a pull-based pipeline (L65-106)

## Backpressure Mechanisms (L107-214)

### Where excess load waits: block, buffer, shed, or throttle (L114-127)

### Bound in-memory consumer buffers so overload becomes durable lag (L128-165)

### Choose each event class's backpressure mechanism before load arrives (L166-214)

## Horizontal Scaling With Partitions (L215-289)

### How to size partition count from throughput target and headroom (L222-239)

### Changing partition count breaks per-key ordering (L240-264)

### Hot keys cap throughput at one partition regardless of count (L265-289)

## Autoscaling and Cost Tradeoffs (L290-366)

### Autoscale on lag signals, not CPU, for IO-bound consumers (L297-312)

### Autoscaling delay from detection, startup, and rebalance (L313-325)

### Cap autoscaler maximum at the narrowest downstream quota (L326-332)

### Compare provisioning plans by daily instance-hour cost and risk (L333-366)

## End-to-End Throughput Analysis (L367-476)

### Little's Law: items in flight equal throughput times time inside (L374-389)

### Recovery time is backlog divided by surplus over live input (L390-407)

### Dependency latency cuts consumer throughput in proportion to call time (L408-414)

### Estimate per-partition drain time from lag and surplus (L415-476)
