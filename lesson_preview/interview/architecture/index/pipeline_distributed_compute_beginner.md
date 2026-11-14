# Index: pipeline_distributed_compute_beginner.md
Source: `lesson_preview/interview/architecture/pipeline_distributed_compute_beginner.md` (467 lines). Read just the line range you need (Read offset/limit).

## Spark Execution Model (L27-105)

### Driver, cluster manager, and executor roles in Spark (L34-49)

### Actions create jobs, shuffles split stages, partitions become tasks (L50-70)

### Failed tasks rerun from lineage, so side effects repeat (L71-105)

## Distributed Primitives (L106-200)

### Lazy evaluation: transformations build a plan, actions run it (L113-130)

### Narrow versus wide operations: which ones force a shuffle (L131-150)

### Count stages by finding each join and key regrouping (L151-173)

### Retry cost differs: narrow reruns one partition, wide reruns upstream (L174-200)

## Shuffle Operations (L201-283)

### Shuffle steps: hash and write map output, fetch, merge (L208-227)

### Broadcast a small table to replace a shuffle join (L228-265)

### Redundant shuffles: double grouping, global sort, repartition, distinct (L266-283)

## Memory Management (L284-357)

### Executor memory regions: reserved, execution, storage, user, overhead (L291-305)

### Matching memory symptoms to the region that ran out (L306-325)

### Splitting one skewed key across tasks with salting (L326-332)

### Read spill and largest-task input before changing memory settings (L333-357)

## Small File Problem (L358-444)

### Output files equal writing tasks times partition values (L365-371)

### Why small files slow readers: opens, footers, and read tasks (L372-387)

### Write-time file count: coalesce after scans, repartition after big shuffles (L388-422)

### Compact small files atomically, for streams and late data (L423-444)
