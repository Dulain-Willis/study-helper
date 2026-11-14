# Index: pipeline_making_it_repeatable_beginner.md
Source: `lesson_preview/interview/architecture/pipeline_making_it_repeatable_beginner.md` (603 lines). Read just the line range you need (Read offset/limit).

## DAG Orchestration (L27-133)

### Why DAG dependencies replace cron schedules that guess task duration (L29-39)

### Orchestrator duties: scheduling, dependencies, state, retries, backfill (L40-57)

### Fixed logical date keeps retries and backfills on same data (L58-72)

### Task states: queued, running, success, failed, upstream_failed, skipped (L73-88)

### Topological sort runs DAG tasks in waves of parallel work (L89-133)

## Task Dependencies (L134-220)

### Sensors wait on dependencies owned outside the DAG (L136-150)

### Wait for a readiness marker written last, not partition existence (L151-171)

### Trigger rules: all_success, all_done, one_failed, none_failed (L172-187)

### The critical path, the longest dependent chain, sets finish time (L188-220)

## Safe Retries (L221-365)

### Idempotent tasks: rerunning for one date gives the same result (L223-229)

### Appending INSERT on retry duplicates rows in the target table (L230-267)

### Replace the partition each run so retries never duplicate rows (L268-292)

### Take partition date from the run, not the wall clock (L293-311)

### Classify errors: retry transient ones with backoff and jitter (L312-330)

### Deduplicate external side effects with deterministic idempotency keys (L331-365)

## Dev/Staging/Prod (L366-448)

### Promotion path and write access across dev, staging, prod (L368-382)

### Parameterize table names and schedules per environment via config (L383-409)

### Lower-environment data: read-only prod, sample, masked copy, or synthetic (L410-423)

### Hardcoded tables, shared alerts, or write credentials break isolation (L424-448)

## CI/CD for Data (L449-578)

### Continuous deployment path: PR checks, staging data tests, prod (L456-462)

### Pre-merge CI: lint, DAG validation, unit tests, fixtures, schema diff (L463-475)

### Unit-test transforms on hand-written rows, including failure cases (L476-503)

### Data tests return violating rows and gate promotion to prod (L504-528)

### Ship backfills with logic changes to fix already-written partitions (L529-535)

### Rolling back code does not repair partitions already written (L536-578)
