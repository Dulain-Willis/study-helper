# Index: pa_pattern_cdc_dual_writes.md
Source: `lesson_preview/pa_pattern_cdc_dual_writes.md` (520 lines). Read just the line range you need (Read offset/limit).

## "How Do You Extract Source Data?" (L27-108)

### Name the change-record guarantee before choosing a capture mechanism (L34-49)
### Compare change capture methods: snapshot, polling, log, dual write, outbox (L50-62)
### Estimate row volume, change rate, and freshness before choosing capture (L63-69)
### Deletes, transitions, ordering, access, and load pick the capture method (L70-108)

## Log-Based CDC With Debezium (L109-218)

### Fields of change events: op, row images, LSN, txid (L116-147)
### Chunked snapshots with log watermarks avoid long table-wide read holds (L148-158)
### At-least-once delivery means consumers dedupe by highest LSN per key (L159-167)
### Renames and retypes break CDC consumers, additive changes do not (L168-218)

## Query-Based CDC and Its Limits (L219-305)

### Polling cannot see deletes, intermediate states, or late-committing rows (L239-268)
### Fixes for polling blind spots each add a recurring cost (L269-289)
### Polling fits append-only, small, or SQL-only sources (L290-305)

## The Dual-Write Problem (L306-396)

### Partial failures in a dual write leave two systems disagreeing (L327-344)
### Concurrent writes reorder in the second system without any failure (L345-360)
### Derive the second write via CDC or a transactional outbox (L361-396)

## CDC Pipeline End-to-End (L397-497)

### CDC stages need rules for duplicates, disorder, and changed shapes (L406-420)
### Highest LSN per key defines current state, deletes removing keys (L421-452)
### Reconcile a copy with its source using counts and checksums (L453-467)
### CDC design stages: guarantees, capture, transport, apply, failure budget (L468-497)
