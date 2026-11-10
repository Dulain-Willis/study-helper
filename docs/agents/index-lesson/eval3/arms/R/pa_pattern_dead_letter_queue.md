# Index: pa_pattern_dead_letter_queue.md
Source: `lesson_preview/pa_pattern_dead_letter_queue.md` (406 lines). Read just the line range you need (Read offset/limit).

## "How Do You Handle Bad Data?" (L27-68)

### Three ways to handle a bad record and their costs (L42-68)

## DLQ Architecture: Divert, Store, Alert (L69-131)

### The envelope decides what you can replay (L78-114)

### Alert on rate, never on count (L115-131)

## Retry Strategies and Poison Pills (L132-205)

### In-place retry versus retry streams (L148-192)

### A poison pill retries forever without a cap (L193-205)

## DLQ Monitoring and Reprocessing (L206-300)

### Replay through a new consumer version, never the failed one (L254-300)

## DLQ as a Data Quality Signal (L301-382)

### Which streams deserve the full dead letter path (L331-369)

### Dead letter design: classify, divert, envelope, retry cap, monitor, replay (L370-382)
