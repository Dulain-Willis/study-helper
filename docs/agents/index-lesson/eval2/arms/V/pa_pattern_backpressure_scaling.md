# Index: pa_pattern_backpressure_scaling.md
Source: `lesson_preview/pa_pattern_backpressure_scaling.md` (499 lines). Read just the line range you need (Read offset/limit).

## The Volume-Doubling Question (L27-106)

### One question, many disguises (L29-45)

### The running example: trip events (L46-64)

### What breaking looks like in a pull pipeline (L65-106)

## Backpressure Mechanisms (L107-214)

### Where excess load can go (L114-127)

### The unbounded buffer inside a consumer (L128-165)

### Choosing a mechanism per event class (L166-214)

## Horizontal Scaling With Partitions (L215-289)

### Sizing the partition count (L222-239)

### Repartitioning breaks per-key ordering (L240-264)

### Skew caps one partition at any count (L265-289)

## Autoscaling and Cost Tradeoffs (L290-366)

### Scale on lag, not CPU (L297-312)

### The reaction gap (L313-325)

### Set the maximum from downstream quotas (L326-332)

### What each plan costs per day (L333-366)

## End-to-End Throughput Analysis (L367-476)

### Little's Law at each hop (L374-389)

### Recovery is a separate calculation (L390-407)

### A degraded dependency (L408-414)

### Measuring it in production (L415-476)
