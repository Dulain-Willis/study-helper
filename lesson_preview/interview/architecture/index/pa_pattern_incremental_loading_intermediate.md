# Index: pa_pattern_incremental_loading_intermediate.md
Source: `lesson_preview/interview/architecture/pa_pattern_incremental_loading_intermediate.md` (562 lines). Read just the line range you need (Read offset/limit).

## Full Refresh or Incremental? (L27-114)

### Full refresh stays correct but cost scales with table size (L34-44)

### Incremental loads push deletes, change columns, history onto the pipeline (L45-58)

### Choosing refresh or incremental from size, change ratio, and window (L59-75)

### Incremental filter on updated_at duplicates updated rows (L76-114)

## How Do You Catch Deletes? (L115-206)

### A timestamp filter never sees a hard-deleted row (L122-148)

### Soft-delete flags reach incremental loads only with a trigger-bumped updated_at (L149-155)

### Reconcile keys daily to remove deletes, guarded by row-count check (L156-173)

### Change data capture from database log catches deletes in seconds (L174-206)

## Which Column Do You Filter On? (L207-298)

### Commit delay can leave updated rows behind the watermark forever (L222-239)

### Clock skew, silent backfills, and local time break timestamp filters (L240-250)

### Overlap lookback window, then dedupe by key keeping newest version (L251-285)

### Size lookback window from longest transaction plus clock skew, doubled (L286-298)

## How Do You Backfill? (L299-398)

### Partition replace makes writes idempotent and backfills reuse nightly code (L306-352)

### Inject the partition date from the scheduler, never from current_date (L353-361)

### Backfill from raw landing layer with tombstones, not the source (L362-368)

### Keep backfill tasks from writing the incremental watermark (L369-377)

### Late-arriving updates land in old partitions and must be replaced (L378-398)

## How Do You Track History? (L399-540)

### SCD Type 2 intervals meet with no gap or overlap (L408-427)

### Detect dimension changes by hashing tracked columns after coalescing nulls (L428-465)

### Derive valid intervals with LEAD over an append-only version log (L466-495)

### Resolve surrogate keys at fact load with a range join (L496-513)

### Late SCD changes need a version log, not in-place edits (L514-540)
