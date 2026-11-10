# Index: pa_pattern_small_files.md
Source: `lesson_preview/pa_pattern_small_files.md` (440 lines). Read just the line range you need (Read offset/limit).

## "Why Is My Query So Slow?" (L25-101)

### Diagnose with file counts first (L45-72)

### Scope the answer before giving it (L73-101)

## Why Small Files Kill Performance (L102-174)

### Every file carries a fixed price (L109-124)

### One day of trip_events in 3 layouts (L125-148)

### Why split packing does not rescue you (L149-155)

### Costs that never show up in a latency chart (L156-174)

## Compaction Strategies (L175-274)

### Bin-packing or sorting the output (L182-206)

### Select files from metadata, not from the clock (L207-231)

### Cadence and write amplification (L232-245)

### Sizing the job and the target (L246-274)

## Preventing Small Files at the Source (L275-338)

### Distribute rows before the write (L293-299)

### Backfills multiply the same bound (L300-306)

### Over-partitioning sets a floor compaction cannot lift (L307-316)

### Split freshness from layout with a raw tier (L317-338)

## Compaction in Table Formats (L339-416)

### Directory swap versus commit (L350-370)

### Optimistic concurrency and the compaction loop (L371-386)

### Metadata needs compaction too (L387-395)

### Storage drops only after snapshots expire (L396-416)
