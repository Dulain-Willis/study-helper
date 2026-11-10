# Index: pa_pattern_cdc_dual_writes.md
Source: `lesson_preview/pa_pattern_cdc_dual_writes.md` (520 lines). Read just the line range you need (Read offset/limit).

## "How Do You Extract Source Data?" (L27-108)

### Who records a change, and what that record guarantees (L34-49)

### Capture methods compared: snapshot, polling, log, dual write, outbox (L50-62)

### Sizing a source by rows, change rate, deletes and freshness (L63-69)

### Choosing a capture method by deletes, transitions, ordering, access, load (L70-108)

## Log-Based CDC With Debezium (L109-218)

### Change events carry row images, LSN and transaction id (L116-147)

### Initial snapshot and chunked backfill with low and high watermarks (L148-158)

### At-least-once delivery: redelivered events need dedupe by highest LSN (L159-167)

### How log retention and schema changes break a log capture (L168-218)

## Query-Based CDC and Its Limits (L219-305)

### What timestamp polling misses: deletes, intermediate states, late commits (L239-268)

### Fixes that make timestamp polling survivable, and what each costs (L269-289)

### When timestamp polling is the right capture method (L290-305)

## The Dual-Write Problem (L306-396)

### How a dual write drifts when one write fails (L327-344)

### Concurrent dual writes can reorder even when no call fails (L345-360)

### Replace dual writes with CDC or a transactional outbox (L361-396)

## CDC Pipeline End-to-End (L397-497)

### CDC apply failures: replays, partial batches, backfills, schema changes (L406-420)

### Current state is the highest-LSN event per key, unless deleted (L421-452)

### Reconciling a copy with its source by counts and checksums (L453-467)

### Five-step answer to a CDC design prompt, with monotonic apply (L468-497)
