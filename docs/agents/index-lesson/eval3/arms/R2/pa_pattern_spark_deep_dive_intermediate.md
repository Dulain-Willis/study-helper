# Index: pa_pattern_spark_deep_dive_intermediate.md
Source: `lesson_preview/pa_pattern_spark_deep_dive_intermediate.md` (365 lines). Read just the line range you need (Read offset/limit).

## Explain Spark Architecture (L27-81)

### How one action becomes a job of stages and tasks (L38-61)

### Why the shuffle sets the cost of a Spark job (L62-81)

## This Job Is Slow (L82-148)

### How to find the cause of a slow Spark job (L89-104)

### Compare max and median task metrics to spot skew (L105-118)

### Map Spark UI symptoms to their likely causes (L119-148)

## Optimize This Join (L149-200)

### Choose broadcast or sort-merge by the size of each side (L164-180)

### Bucketing by join key removes the shuffle from repeated joins (L181-200)

## How Do You Size the Cluster? (L201-258)

### How to choose executor cores and heap per node (L208-220)

### How much execution memory each task gets (L221-227)

### How to count partitions and waves against task slots (L228-258)

## What About Data Skew? (L259-343)

### Confirm data skew from task sizes and key counts (L266-290)

### Adaptive query execution splits skewed sort-merge join partitions (L291-297)

### Salting a skewed key spreads it across partitions (L298-320)

### Handle hot keys with a separate broadcast join (L321-343)
