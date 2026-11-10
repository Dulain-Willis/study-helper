# Index: pa_pattern_dead_letter_queue.md
Source: `lesson_preview/pa_pattern_dead_letter_queue.md` (406 lines). Read just the line range you need (Read offset/limit).

## "How Do You Handle Bad Data?" (L27-68)

### Three ways to handle unprocessable records: fail, drop, divert (L42-68)

## DLQ Architecture: Divert, Store, Alert (L69-131)

### Fields a dead letter envelope needs to replay a record (L78-114)

### Alert on rate against input, not raw count (L115-131)

## Retry Strategies and Poison Pills (L132-205)

### Retry in-place or through retry streams, by partition stall cost (L148-192)

### How a retry cap stops a poison pill record (L193-205)

## DLQ Monitoring and Reprocessing (L206-300)

### Replay dead letters through a new consumer version with dedupe (L254-300)

## DLQ as a Data Quality Signal (L301-382)

### Dead letter path or metric drop, by stream loss tolerance (L331-369)

### Structuring a dead letter design: classify, divert, envelope, retry, monitor (L370-382)
