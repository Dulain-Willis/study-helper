# Index: pa_pattern_spark_deep_dive_intermediate.md
Source: `lesson_preview/pa_pattern_spark_deep_dive_intermediate.md` (365 lines). Read just the line range you need (Read offset/limit).

## Explain Spark Architecture (L27-81)

### From one action to thousands of tasks (L38-61)

### Why the shuffle is the unit of cost (L62-81)

## This Job Is Slow (L82-148)

### Start at the stage list (L89-104)

### Then the task summary for the slow stage (L105-118)

### Symptom to cause (L119-148)

## Optimize This Join (L149-200)

### Work it on 3 tables (L164-180)

### Bucketing: pay the shuffle once at write time (L181-200)

## How Do You Size the Cluster? (L201-258)

### Executor shape (L208-220)

### Memory per task (L221-227)

### Partitions and waves (L228-258)

## What About Data Skew? (L259-343)

### Confirm it from 2 places (L266-290)

### Fix 1: adaptive skew join (L291-297)

### Fix 2: salt the key (L298-320)

### Fix 3: isolate the hot keys (L321-343)
