# Index: pa_pattern_incremental_loading_intermediate.md
Source: `lesson_preview/pa_pattern_incremental_loading_intermediate.md` (562 lines). Read just the line range you need (Read offset/limit).

## Full Refresh or Incremental? (L27-114)

### A full refresh is correct by construction and stateless (L34-44)

### Incremental loading obliges the pipeline to hold its own state (L45-58)

### Choosing incremental depends on change ratio and window fit (L59-75)

### An insert-only incremental filter duplicates updated rows (L76-114)

## How Do You Catch Deletes? (L115-206)

### Hard deletes at the source never reach a timestamp filter (L122-148)

### Soft-delete flags with a trigger-set timestamp expose deletes (L149-155)

### Key reconciliation finds deletes by anti-joining the key snapshot (L156-173)

### Database change logs capture deletes within seconds (L174-206)

## Which Column Do You Filter On? (L207-298)

### Uncommitted rows can carry timestamps below the watermark (L222-239)

### Clock skew, silent source fixes, and local time corrupt updated_at (L240-250)

### Lookback overlap absorbs late commits, then dedupe on the key (L251-285)

### Lookback size comes from longest transaction plus clock skew (L286-298)

## How Do You Backfill? (L299-398)

### A partition replace write is idempotent and serves backfills (L306-352)

### A task's run date comes from the scheduler, not current_date (L353-361)

### Backfills replay a raw landing layer with tombstones (L362-368)

### Each task writes only its own watermark row (L369-377)

### Late changes can rewrite partitions of any age (L378-398)

## How Do You Track History? (L399-540)

### SCD Type 2 versions meet with no gap or overlap (L408-427)

### Detect SCD changes by hashing coalesced tracked columns (L428-465)

### Derive validity intervals from the version log with LEAD (L466-495)

### Resolve surrogate keys with a range join at fact load (L496-513)

### Late SCD changes break in-place edits, not an append-only log (L514-540)
