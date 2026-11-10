# Index: pa_pattern_spark_deep_dive_intermediate.md
Source: `lesson_preview/pa_pattern_spark_deep_dive_intermediate.md` (365 lines). Read just the line range you need (Read offset/limit).

## Explain Spark Architecture (L27-81)

### How one action becomes jobs, stages and tasks (L38-61)

### Why the shuffle sets the cost of a Spark job (L62-81)

## This Job Is Slow (L82-148)

### Read the stage list to find the slow stage (L89-104)

### Read the task summary to spot skew or sizing (L105-118)

### Map each Spark UI symptom to its likely cause (L119-148)

## Optimize This Join (L149-200)

### How to choose a join strategy from table sizes (L164-180)

### Bucketing: pay the join shuffle once at write time (L181-200)

## How Do You Size the Cluster? (L201-258)

### How to set executor count, cores and heap per node (L208-220)

### How executor heap sets memory available per task (L221-227)

### Sizing partition count and waves against a deadline (L228-258)

## What About Data Skew? (L259-343)

### How to confirm skew from task metrics and key counts (L266-290)

### Fixing skew with adaptive skew join in Spark 3 (L291-297)

### Fixing skew by salting the join key (L298-320)

### Fixing skew by isolating hot keys in a broadcast join (L321-343)
