# Index: pa_pattern_incremental_loading_intermediate.md
Source: `lesson_preview/pa_pattern_incremental_loading_intermediate.md` (562 lines). Read just the line range you need (Read offset/limit).

## Full Refresh or Incremental? (L27-114)

### What a full refresh buys you (L34-44)

### What incremental costs (L45-58)

### The 3 numbers that decide it (L59-75)

### The first crack (L76-114)

## How Do You Catch Deletes? (L115-206)

### Worked example: 2,000 orders a day vanish (L122-148)

### Option 1: soft deletes at the source (L149-155)

### Option 2: reconcile the keys (L156-173)

### Option 3: read the change log (L174-206)

## Which Column Do You Filter On? (L207-298)

### Lie 1: the row that committed after the watermark (L222-239)

### Lies 2 to 4 (L240-250)

### The fix: overlap, then dedupe (L251-285)

### Choosing the window (L286-298)

## How Do You Backfill? (L299-398)

### Property 1: the write replaces a whole partition (L306-352)

### Property 2: the date is a parameter (L353-361)

### Property 3: backfill from the raw layer you kept (L362-368)

### Property 4: the watermark stays where it is (L369-377)

### Late data lands in old partitions (L378-398)

## How Do You Track History? (L399-540)

### Worked example: 1 customer, 1 move (L408-427)

### Step 1: detect the changes (L428-465)

### Step 2: derive the intervals (L466-495)

### Step 3: resolve the key at fact load (L496-513)

### Where it breaks, and why the version log saves it (L514-540)
