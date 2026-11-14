# Index: pa_pattern_small_files.md
Source: `lesson_preview/interview/architecture/pa_pattern_small_files.md` (440 lines). Read just the line range you need (Read offset/limit).

## "Why Is My Query So Slow?" (L25-101)

### Diagnose small files per partition from count and median size (L45-72)

### Requirements that determine a small-file fix design (L73-101)

## Why Small Files Kill Performance (L102-174)

### Each file adds a fixed cost regardless of its bytes (L109-124)

### Request wait, not data movement, dominates scan time (L125-148)

### Packing small files into splits leaves request latency unchanged (L149-155)

### Hidden small-file costs beyond read latency: compression, pruning, metadata (L156-174)

## Compaction Strategies (L175-274)

### Bin-pack by default, sort only when one key is filtered (L182-206)

### Select compaction candidates from file metadata, not wall-clock time (L207-231)

### Compaction cadence trades write amplification against small-file exposure (L232-245)

### Bounding compaction run size and choosing target file size (L246-274)

## Preventing Small Files at the Source (L275-338)

### Shuffle by partition before writing to limit files per commit (L293-299)

### Backfill file count multiplies tasks by partitions touched (L300-306)

### Over-partitioning sets a file floor compaction cannot lift (L307-316)

### Split freshness from layout with a raw ingestion tier (L317-338)

## Compaction in Table Formats (L339-416)

### Directory compaction versus table-format commit: visibility and retry (L350-370)

### Which concurrent operations conflict with a compaction rewrite (L371-386)

### Manifests grow per commit, so compact metadata like data (L387-395)

### Storage is reclaimed only after snapshot expiry and orphan cleanup (L396-416)
