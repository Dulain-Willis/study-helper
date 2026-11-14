# Index: pa_pattern_make_it_reliable_intermediate.md
Source: `lesson_preview/interview/architecture/pa_pattern_make_it_reliable_intermediate.md` (570 lines). Read just the line range you need (Read offset/limit).

## How Do You Ensure Data Quality? (L27-132)

### Key, count and invariant failures should block publish, not warn (L42-99)
### Weigh runtime and on-call cost of blocking data quality checks (L100-132)

## Can You Re-run This Safely? (L133-215)

### Make a load idempotent by overwriting its partition (L140-164)
### Make row-level updates idempotent with MERGE on a stable key (L165-198)
### How to prove load idempotency by comparing two run snapshots (L199-215)

## How Do You Handle Duplicates? (L216-349)

### Keep latest version per key with ROW_NUMBER and a tie-breaker (L223-263)
### Resolve cross-system duplicates by source priority (L264-324)
### Deduplicate once per pipeline, not in every downstream model (L325-349)

## How Do You Know It's Broken? (L350-469)

### Freshness monitor checks newest partition date against the deadline (L357-377)
### Volume monitors compare to a same-weekday median baseline (L378-408)
### Schema drift monitor compares live columns to expected types (L409-425)
### Define monitors as reviewed config with thresholds and actions (L426-469)

## How Do You Roll Back a Bad Load? (L470-546)

### Write, audit, publish: separate published name from physical data (L477-501)
### Options to correct data already published, ranked by restore speed (L502-521)
### Re-trigger downstream tables in dependency order after a rollback (L522-546)
