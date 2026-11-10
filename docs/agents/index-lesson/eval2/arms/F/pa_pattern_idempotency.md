# Index: pa_pattern_idempotency.md
Source: `lesson_preview/pa_pattern_idempotency.md` (597 lines). Read just the line range you need (Read offset/limit).

## "What If Your Pipeline Runs Twice?" (L27-140)

### A rerun must leave the target as one run would (L27-33)
### Idempotency means repeated runs equal one run for a slice (L34-37)
### Four separate properties make a run safe to repeat (L38-51)
### Retries, backfills, restarts, and zombie workers all repeat work (L52-66)
### How a plain INSERT fails at each crash point (L67-81)
### Make a transform deterministic: no UUIDs, clocks, or unordered ties (L82-121)
### Which write statements survive a retry, and why (L122-140)

## DELETE-INSERT and MERGE (L141-285)

### A retry needs an identity: slice or row key (L143-147)
### Replace a slice by DELETE and INSERT in one transaction (L148-170)
### Split DELETE and INSERT leaves readers an empty partition (L171-174)
### DELETE predicate must cover every row the INSERT writes (L175-181)
### Safe MERGE: one source row per key, version guard, tombstones (L182-230)
### Version guard stops an older replay overwriting newer state (L231-244)
### Replace rewrites one partition while MERGE rewrites every touched file (L245-253)
### Compare replace and MERGE on identity, cost, late data, deletes (L254-269)
### Choose replace when a slice determines its partitions, else MERGE (L270-285)

## Idempotent Reads With Offsets (L286-372)

### A retry must re-request the same bounded input range (L288-292)
### Record each run's offset range in a ledger before reading (L293-308)
### Commit order sets at-most-once, at-least-once, or exactly-once effect (L309-318)
### Fence offset commits in the same transaction as the write (L319-345)
### Timestamp watermarks skip rows that commit after the watermark passes (L346-361)
### Watermark extracts with >= and INSERT corrupt the target (L362-372)

## Idempotency in Streaming vs Batch (L373-467)

### Batch and streaming replay differ in unit, cause, and mechanism (L375-387)
### Dedup state size is rate times window times key size (L388-394)
### Pair a short dedup window with a nightly batch reconcile (L395-401)
### Checkpoint-bound sinks make output visible only after a checkpoint (L402-408)
### Upsert window results by group and window start (L409-430)
### Side effects need receiver-side dedup keyed on event fields (L431-434)
### Derive side-effect keys from event fields, not from each attempt (L435-456)
### Which effects repeat after a restart, and what fixes each (L457-467)

## Proving Idempotency Out Loud (L468-574)

### Six steps to prove a design idempotent (L470-487)
### Worked answer: partition replace plus version-guarded MERGE (L488-496)
### Walk each crash point to see what a retry finds (L497-508)
### Prove idempotency with a rerun fingerprint test (L509-536)
### Common follow-ups: backfills, exactly-once, overlapping runs (L537-574)
