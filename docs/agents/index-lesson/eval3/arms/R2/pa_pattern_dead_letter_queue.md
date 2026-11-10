# Index: pa_pattern_dead_letter_queue.md
Source: `lesson_preview/pa_pattern_dead_letter_queue.md` (406 lines). Read just the line range you need (Read offset/limit).

## "How Do You Handle Bad Data?" (L27-68)

### Three ways to handle an unprocessable record: fail, drop, divert (L42-68)

## DLQ Architecture: Divert, Store, Alert (L69-131)

### Dead letter envelopes keep original bytes and source position (L78-114)

### Alert on dead letter rate against input, never on count (L115-131)

## Retry Strategies and Poison Pills (L132-205)

### Where retry waiting lives: in-place versus retry streams (L148-192)

### Retry caps stop a poison pill from looping forever (L193-205)

## DLQ Monitoring and Reprocessing (L206-300)

### Replay after a fix runs through a new consumer version (L254-300)

## DLQ as a Data Quality Signal (L301-382)

### Three tests decide which streams deserve a dead letter path (L331-369)

### Components of a complete dead letter design (L370-382)
