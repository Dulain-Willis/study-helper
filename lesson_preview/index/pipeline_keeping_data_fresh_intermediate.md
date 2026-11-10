# Index: pipeline_keeping_data_fresh_intermediate.md
Source: `lesson_preview/pipeline_keeping_data_fresh_intermediate.md` (546 lines). Read just the line range you need (Read offset/limit).

## Merge Strategies (L27-123)

### Write cost follows the files and partitions a batch touches (L41-56)

### Guard MERGE against duplicate keys and replayed older versions (L57-107)

### Choose overwrite, MERGE, append-dedupe, or full rebuild by change spread (L108-123)

## CDC Patterns (L124-202)

### CDC event fields: op, row images, LSN, commit time (L133-161)

### Start CDC streaming at the position read before the snapshot (L162-170)

### Lag splits by stage, and the slowest stage owns it (L171-202)

## SCD in Pipelines (L203-352)

### Detect change by hashing tracked columns with NULL-safe separators (L212-229)

### SCD Type 2: expire and insert in one MERGE (L230-301)

### Hash natural key and valid_from so surrogate keys survive rebuilds (L302-324)

### Join facts to the dimension version with half-open validity intervals (L325-352)

## Schema Migration (L353-443)

### Classify schema changes as backward or forward compatible (L360-379)

### Rename a column via expand, migrate, contract steps (L380-405)

### Expose a view as a stable contract over changing tables (L406-443)

## Partition-Level Backfill (L444-522)

### A partition task is safe when parameterized, isolated, and atomic (L451-457)

### Validate staged partitions before the atomic swap (L458-474)

### Dedicated slot pool keeps backfill from starving nightly loads (L475-489)

### Backfill ordering, pinned code version, and downstream reruns (L490-522)
