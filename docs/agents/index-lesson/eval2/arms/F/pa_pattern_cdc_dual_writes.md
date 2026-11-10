# Index: pa_pattern_cdc_dual_writes.md
Source: `lesson_preview/pa_pattern_cdc_dual_writes.md` (521 lines). Read just the line range you need (Read offset/limit).

## How Do You Extract Source Data? (L27-108)

### The change record's source decides what a copy can know (L27-33)
### Extraction questions reduce to who records row changes (L34-49)
### Comparing five ways to capture database changes (L50-62)
### Judge capture methods by table size, change rate and freshness (L63-69)
### Five properties that decide the capture method (L70-79)
### When to choose log-based CDC over polling or snapshots (L80-93)
### Capture method when the source offers only SQL access (L94-99)
### Open an extraction answer with consumer guarantees, not tool names (L100-108)

## Log-Based CDC With Debezium (L109-218)

### The transaction log already records every committed change (L109-115)
### Change event fields: op, row images, LSN and txid (L116-147)
### Initial snapshot then log, with the log copy winning (L148-151)
### Chunked snapshots with watermarks avoid one long read (L152-158)
### Connector redelivery after a crash, deduped by highest LSN (L159-162)
### Event order holds only within one partition key (L163-167)
### Log retention: reader-pinned versus time-based rotation (L168-189)
### Rolling out schema changes to a captured table safely (L190-207)
### Recover a log gap by re-snapshot and key anti-join (L208-218)

## Query-Based CDC and Its Limits (L219-305)

### Watermark polling on updated_at and its three assumptions (L219-238)
### Hard deletes that polling can never return (L239-244)
### Intermediate states missed between polls (L245-251)
### Rows committed late, behind the watermark, never load (L252-268)
### Cost of stacking fixes onto a polling design (L269-280)
### Sequence version columns do not fix commit-order visibility (L281-289)
### When polling beats log-based capture (L290-305)

## The Dual-Write Problem (L306-396)

### Dual write: two independent writes with no shared commit (L306-326)
### Four ways two independent writes disagree (L327-335)
### Rare failures at volume become daily drift, patched by reconciliation (L336-344)
### Concurrent writes can reorder with no failures at all (L345-360)
### Three ways to derive the second write from the first (L361-370)
### Transactional outbox: commit the event with the state change (L371-385)
### Idempotency keys for external calls outside a transaction (L386-396)

## CDC Pipeline End-to-End (L397-497)

### Full CDC pipeline: connector, schema gate, change log, apply step (L397-405)
### Failure stress cases and the property that prevents each (L406-420)
### Highest LSN per key wins, and deletes remove the row (L421-452)
### Reconciling a copy against the source by count and checksum (L453-467)
### Five-step structure for answering any CDC design prompt (L468-482)
### Backfills need monotonic apply to avoid overwriting newer state (L483-488)
### Answer what-if CDC questions with the property first (L489-497)
