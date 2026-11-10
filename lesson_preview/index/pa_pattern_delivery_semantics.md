# Index: pa_pattern_delivery_semantics.md
Source: `lesson_preview/pa_pattern_delivery_semantics.md` (490 lines). Read just the line range you need (Read offset/limit).

## "What Is Your Delivery Guarantee?" (L27-98)

### Offset commit order relative to the effect sets the guarantee (L34-51)

### End-to-end delivery guarantee is capped by its weakest link (L52-68)

### Symptoms of duplicates and loss point to the commit position (L69-98)

## At-Least-Once and Its Duplicates (L99-212)

### Sources of duplicates: producer retry, consumer replay, rebalance, rerun (L106-121)

### Idempotency keys must be minted at origin, not ingestion (L122-144)

### Anti-join plus in-batch dedup makes table writes replay-safe (L145-182)

### Dedup window must span the longest replay gap between copies (L183-194)

### Failures that survive at-least-once: increments, payload hashes, sends, late copies (L195-212)

## At-Most-Once: Fire and Forget (L213-291)

### At-most-once fits where a lost record barely moves the result (L220-242)

### Bound accepted loss and reconcile source against sink counts (L243-262)

### Accidental at-most-once happens whenever progress outruns effects (L263-291)

## Exactly-Once and Its Cost (L292-381)

### One transaction for output and offset closes the gap (L301-323)

### Two-phase commit ties sink transactions to checkpoints (L324-330)

### Exactly-once costs visibility latency, coordination, and timeout risk (L331-345)

### Idempotent sinks trade checkpoint waits for keys and upserts (L346-366)

### Exactly-once stops at external calls, so extend idempotency keys outward (L367-381)

## Semantics for a Business Need (L382-467)

### Assign each consumer a guarantee from duplicate and loss costs (L396-413)

### Complete design answer covers guarantee, mechanism, failures, sizing, gaps (L414-429)

### Keyed writes absorb retries, late events, and additive schema changes (L430-467)

**Flagged**
- "What Is Your Delivery Guarantee?"
- "Semantics for a Business Need"
