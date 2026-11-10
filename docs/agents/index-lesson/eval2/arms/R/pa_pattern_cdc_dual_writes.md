# Index: pa_pattern_cdc_dual_writes.md
Source: `lesson_preview/pa_pattern_cdc_dual_writes.md` (520 lines). Read just the line range you need (Read offset/limit).

## "How Do You Extract Source Data?" (L27-108)

### Extraction questions turn on who records each change (L34-49)

### Five methods for capturing changes from a database (L50-62)

### Stating volume, freshness and delete needs before choosing a method (L63-69)

### Choosing log CDC, polling or snapshots from source properties (L70-108)

## Log-Based CDC With Debezium (L109-218)

### Fields of a log change event: row images, LSN and txid (L116-147)

### Bootstrapping CDC with an initial snapshot and chunked watermarks (L148-158)

### Log CDC redelivers events, so consumers dedupe by LSN (L159-167)

### Failure modes of log CDC: retention and schema change (L168-218)

## Query-Based CDC and Its Limits (L219-305)

### Polling misses hard deletes, intermediate states, and late commits (L239-268)

### Costs of making a timestamp poll survivable (L269-289)

### When polling fits: append-only, small, SQL-only, or daily-freshness sources (L290-305)

## The Dual-Write Problem (L306-396)

### Ways a dual write's two writes can diverge (L327-344)

### Concurrent dual writes reorder, and retries cannot fix it (L345-360)

### Derive the second write from one source of truth (L361-396)

## CDC Pipeline End-to-End (L397-497)

### Failure cases a CDC pipeline must survive and their guards (L406-420)

### Current-state table: highest LSN per key, deletes excluded (L421-452)

### Reconcile a copy against source with counts and checksums (L453-467)

### Five-step template for answering a CDC design question (L468-497)
