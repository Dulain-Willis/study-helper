# Index: pipeline_where_data_lives_beginner.md
Source: `lesson_preview/interview/architecture/pipeline_where_data_lives_beginner.md` (533 lines). Read just the line range you need (Read offset/limit).

## Columnar vs Row (L27-146)

### Why Parquet questions test knowledge of on-disk byte layout (L29-35)
### Row layout versus columnar layout in file byte order (L36-75)
### Columnar layout enables compression, row-group skipping, and embedded schema (L76-96)
### Columnar formats cost batched writes and whole-file rewrites on edit (L97-121)
### Classifying workloads as row or columnar layout (L122-135)
### Columnar format tradeoff: column pruning versus batched write cost (L136-146)

## Compression (L147-231)

### Codec choice trades compression ratio against decompression speed (L154-169)
### Why same-type column values compress better than interleaved rows (L170-176)
### Unsplittable compressed files confine a read to one worker (L177-202)
### Choose codec from read frequency and parallel reader count (L203-218)
### Diagnosing serial reads from one unsplittable input file (L219-231)

## Partitioning (L232-344)

### How partition pruning skips directories that a filter rules out (L239-272)
### Choosing a partition column: filter use, value count, fixed values (L273-279)
### Too many partitions cause the small file problem (L280-292)
### Filters on a non-partition column skip no directories (L293-324)
### Late-arriving events force rewrites of old date partitions (L325-331)
### Judging whether a second partition column adds value (L332-344)

## Lake vs Warehouse (L345-408)

### Plain data lake failures: partial reads, duplicates, schema drift (L361-376)
### Lakehouse adds transactional commits via metadata over lake files (L377-393)
### Choosing lake, warehouse, or lakehouse from engines and writes (L394-408)

## Table Formats (L409-511)

### Table format defines a table as its metadata's file list (L416-431)
### Table metadata enables atomic commits, time travel, and planning (L432-463)
### Table format upkeep: snapshot expiry, orphan cleanup, compaction (L464-487)
### Choosing a table format by engine support and write pattern (L488-498)
### Why files copied outside a commit stay invisible to readers (L499-511)
