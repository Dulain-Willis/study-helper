# Index: pa_pattern_small_files.md
Source: `lesson_preview/pa_pattern_small_files.md` (440 lines). Read just the line range you need (Read offset/limit).

## "Why Is My Query So Slow?" (L25-101)

### Diagnose small files from file count and median size (L45-72)

### Scope requirements before designing a table layout (L73-101)

## Why Small Files Kill Performance (L102-174)

### Each file adds fixed request and metadata costs regardless of size (L109-124)

### Request wait, not bytes, dominates scan time for small files (L125-148)

### Packing small files into splits leaves request latency unchanged (L149-155)

### Hidden costs of small files beyond query latency (L156-174)

## Compaction Strategies (L175-274)

### Bin-packing versus sorting when compacting small files (L182-206)

### Select compaction candidates from file metadata, not timestamps (L207-231)

### How compaction cadence sets bytes rewritten per byte ingested (L232-245)

### Size compaction runs and target file size (L246-274)

## Preventing Small Files at the Source (L275-338)

### Shuffle rows by partition before writing to limit files (L293-299)

### How backfills multiply the files-per-commit bound (L300-306)

### Over-partitioning sets a file-count floor that compaction cannot remove (L307-316)

### Separate freshness from layout using a raw landing tier (L317-338)

## Compaction in Table Formats (L339-416)

### Directory swap versus table-format commit for compaction (L350-370)

### Which concurrent operations conflict with a compaction rewrite (L371-386)

### Manifest metadata grows per commit and needs compaction too (L387-395)

### Storage is reclaimed only after snapshots expire (L396-416)
