# Index: pa_pattern_cdc_dual_writes.md
Source: `lesson_preview/pa_pattern_cdc_dual_writes.md` (520 lines). Read just the line range you need (Read offset/limit).

## "How Do You Extract Source Data?" (L27-108)

### The same question in 4 disguises (L34-49)

### Five ways changes leave a database (L50-62)

### The running example (L63-69)

### The properties that pick the method (L70-108)

## Log-Based CDC With Debezium (L109-218)

### What one change event carries (L116-147)

### Snapshot first, then the log (L148-158)

### Delivery is at least once (L159-167)

### Where the log itself bites (L168-218)

## Query-Based CDC and Its Limits (L219-305)

### Three things a poll cannot see (L239-268)

### What a survivable poll costs (L269-289)

### When polling is the right call (L290-305)

## The Dual-Write Problem (L306-396)

### Where the 2 writes come apart (L327-344)

### The race that retries cannot fix (L345-360)

### Make one write the source of truth (L361-396)

## CDC Pipeline End-to-End (L397-497)

### Where each box breaks (L406-420)

### Applying changes by LSN (L421-452)

### Proving the copy matches (L453-467)

### Shaping the answer in the room (L468-497)
