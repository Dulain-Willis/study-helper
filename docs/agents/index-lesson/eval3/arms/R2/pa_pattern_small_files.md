# Index: pa_pattern_small_files.md
Source: `lesson_preview/pa_pattern_small_files.md` (440 lines). Read just the line range you need (Read offset/limit).

## "Why Is My Query So Slow?" (L25-101)

### Diagnose small files by file count and median size (L45-72)

### Requirements that shape a small-file layout design (L73-101)

## Why Small Files Kill Performance (L102-174)

### Every file carries a fixed price regardless of its bytes (L109-124)

### Comparing layouts by request wait and listing time (L125-148)

### Split packing does not remove per-file request latency (L149-155)

### Hidden costs of small files beyond query latency (L156-174)

## Compaction Strategies (L175-274)

### Bin-packing versus sorting for compacted output (L182-206)

### Select compaction candidates from file metadata, not age (L207-231)

### Compaction cadence trades write amplification against small-file exposure (L232-245)

### Sizing compaction run input and target file size (L246-274)

## Preventing Small Files at the Source (L275-338)

### Distribute rows by partition key before the write (L293-299)

### Backfills multiply the writer file-count bound (L300-306)

### Over-partitioning sets a floor that compaction cannot lift (L307-316)

### Separate freshness from layout with a raw tier (L317-338)

## Compaction in Table Formats (L339-416)

### Directory swap versus table-format commit for compaction (L350-370)

### Optimistic concurrency conflicts that abort compaction rewrites (L371-386)

### Manifest metadata needs compaction as well as data files (L387-395)

### Storage drops only after snapshots expire (L396-416)
