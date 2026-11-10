# Index: pa_pattern_spark_deep_dive_intermediate.md
Source: `lesson_preview/pa_pattern_spark_deep_dive_intermediate.md` (365 lines). Read just the line range you need (Read offset/limit).

## Explain Spark Architecture (L27-81)

### Actions make jobs, shuffles make stages, partitions make tasks (L38-61)

### Shuffle bytes and fetch count set stage boundary cost (L62-81)

## This Job Is Slow (L82-148)

### Find slowest stage, then read its shuffle and spill bytes (L89-104)

### Max over median task duration reveals skew and sizing problems (L105-118)

### Map Spark UI symptoms to causes and first fixes (L119-148)

## Optimize This Join (L149-200)

### Compare network bytes of broadcast and sort-merge joins (L164-180)

### Bucket tables on the join key to skip repeated shuffles (L181-200)

## How Do You Size the Cluster? (L201-258)

### Size executor cores and heap from node cores and memory (L208-220)

### How unified memory and concurrent tasks set spill threshold (L221-227)

### Count waves from slot count and round partitions to slots (L228-258)

## What About Data Skew? (L259-343)

### Confirm skew from max-to-median shuffle read and hot key counts (L266-290)

### Adaptive skew join splits oversized partitions in sort-merge joins (L291-297)

### Salt hot keys and explode the small side to match (L298-320)

### Broadcast hot keys and sort-merge the rest of the join (L321-343)
