# Index: pipeline_where_data_lives_intermediate.md
Source: `lesson_preview/pipeline_where_data_lives_intermediate.md` (488 lines). Read just the line range you need (Read offset/limit).

## Encoding Types (L27-130)

### Parquet encodings: when each wins and where it loses (L34-45)
### Estimate column size from distinct values and row sort order (L46-54)
### Parquet dictionary overflow falls back to PLAIN encoding (L55-91)
### Sort order decides dictionary runs and delta size before compression (L92-112)
### Answer Parquet probes on footers, dictionary caps, and sorting (L113-130)

## The Small File Problem (L131-234)

### Small files come from streaming commits, shuffle fan-out, and over-partitioning (L138-151)
### Per-file open latency makes small files dominate scan time (L152-178)
### Diagnose small files from per-partition file count and size (L179-205)
### Small file fixes: write-time repartitioning versus compaction (L206-234)

## Deleting Records From a Data Lake (L235-305)

### Estimate files rewritten by a delete from row layout (L242-262)
### Copy-on-write versus merge-on-read for row deletes (L263-283)
### Deleted rows persist in old snapshots until expiry removes files (L284-292)
### Batch deletes into one commit to share file rewrites (L293-305)

## Storage Tiering (L306-385)

### Compare storage tiers by storage, retrieval, latency, minimum stay (L313-325)
### Price a tiered lake by age range, size, and tier (L326-342)
### Break-even read rate where a colder storage tier stops saving (L343-351)
### Tiering failure modes: compaction, transition fees, archived reads, erasure (L352-385)

## The Table Catalog (L386-464)

### Catalog holds a per-table metadata pointer, not the file list (L393-412)
### Compare-and-swap on the metadata pointer makes concurrent commits safe (L413-430)
### Catalog failures: outage, split catalogs, pointer rollback, missing files (L431-464)
