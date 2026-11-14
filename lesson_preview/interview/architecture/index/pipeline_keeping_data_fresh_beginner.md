# Index: pipeline_keeping_data_fresh_beginner.md
Source: `lesson_preview/interview/architecture/pipeline_keeping_data_fresh_beginner.md` (488 lines). Read just the line range you need (Read offset/limit).

## Full vs Incremental Loading (L27-118)

### Full loads keep no state, so retries and fixes self-heal (L34-40)
### Watermark-based incremental loads read changes and upsert by key (L41-66)
### Incremental load failure modes: late commits, retries, hard deletes (L67-80)
### Choose full or incremental load by size and change rate (L81-118)

## Change Data Capture (L119-201)

### Timestamp queries miss hard deletes, intermediate states, and add load (L126-136)
### CDC change events carry operation, row values, and log position (L137-153)
### CDC pipeline parts: log reader, queue, and apply step (L154-162)
### CDC replays need idempotent apply via log positions and tombstones (L163-201)

## Slowly Changing Dimensions (L202-287)

### Type 2 dimensions keep history as versioned rows (L209-228)
### Join versioned dimensions by natural key and half-open date range (L229-269)
### Versioned dimension loads fail on reruns, late changes, new columns (L270-287)

## Schema Evolution (L288-376)

### Classify schema changes as safe, breaking, or silently breaking (L295-311)
### Positional inserts and star selects break when columns change (L312-341)
### Ship a breaking rename in three releases without outage (L342-376)

## Backfilling (L377-465)

### Scope damage from a bug to affected partitions and tables (L384-390)
### Overwrite partitions rather than append to make backfills idempotent (L391-426)
### Cap backfill concurrency and lower its priority below daily loads (L427-437)
### Order backfills upstream first, then by state carried forward (L438-465)
