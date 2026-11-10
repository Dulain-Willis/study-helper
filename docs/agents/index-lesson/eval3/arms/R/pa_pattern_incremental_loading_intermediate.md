# Index: pa_pattern_incremental_loading_intermediate.md
Source: `lesson_preview/pa_pattern_incremental_loading_intermediate.md` (562 lines). Read just the line range you need (Read offset/limit).

## Full Refresh or Incremental? (L27-114)

### A full refresh is correct by construction, with no state (L34-44)

### Incremental loads must own delete, change, replay and history state (L45-58)

### Full refresh versus incremental depends on size, change rate and window (L59-75)

### Insert-only incremental filters double-count updated rows (L76-114)

## How Do You Catch Deletes? (L115-206)

### Hard deletes missed by a timestamp filter inflate aggregates (L122-148)

### Soft deletes make deletes visible to a timestamp filter (L149-155)

### Reconcile keys to catch hard deletes, with a count guard (L156-173)

### Read the database change log to capture deletes in seconds (L174-206)

## Which Column Do You Filter On? (L207-298)

### Timestamps are written before commit, so watermarks skip rows (L222-239)

### Clock skew, silent source fixes and local time corrupt timestamps (L240-250)

### Overlap the watermark window, then dedupe on the key (L251-285)

### Size the overlap window from the longest open transaction (L286-298)

## How Do You Backfill? (L299-398)

### A partition replace makes a load idempotent and re-runnable (L306-352)

### Pass the run date as a parameter, never current_date (L353-361)

### Backfill from a raw landing layer with tombstones for deletes (L362-368)

### Backfills must never write the incremental watermark (L369-377)

### Late updates rewrite old partitions, so replace every staged one (L378-398)

## How Do You Track History? (L399-540)

### Type 2 versions close exactly when the next begins (L408-427)

### Detect SCD changes by hashing tracked columns after coalescing nulls (L428-465)

### Derive validity intervals from an append-only version log with LEAD (L466-495)

### Resolve surrogate keys at fact load with a range join (L496-513)

### Late changes corrupt in-place SCD edits, not a version log (L514-540)
