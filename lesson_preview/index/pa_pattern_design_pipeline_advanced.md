# Index: pa_pattern_design_pipeline_advanced.md
Source: `lesson_preview/pa_pattern_design_pipeline_advanced.md` (461 lines). Read just the line range you need (Read offset/limit).

## Scoping and Sizing the Platform (L30-118)

### Shared platforms design in onboarding, schema governance and cost attribution (L42-62)

### Back-of-envelope sizing: events, bytes, compression and storage price (L63-82)

### Pick batch, micro-batch or stream from the latency requirement (L83-99)

### Layers of a data platform, from ingestion to observability (L100-118)

## Governed Self-Service Ingestion (L119-196)

### Classify schema changes as auto-approve, review or reject (L128-164)

### Ordering, backfill and failure differ by ingestion source type (L165-177)

### Choose at-least-once with idempotent merge unless duplicates move money (L178-196)

## Transformation Economics (L197-286)

### ELT by default, with three exceptions that require ETL (L199-219)

### Engine choice follows reprocess cost times change frequency (L220-240)

### Medallion breaks when fan-out turns gold into a namespace (L241-256)

### Shuffle cost: broadcast small sides, bucket join keys, size partitions (L257-286)

## Serving Contracts Across Teams (L287-360)

### Freshness tiers set the regime, so tighter tiers cost more (L316-328)

### Isolate compute per team with shared storage and query tags (L329-337)

### Deprecate by dual-publishing, measuring query history, then dropping (L338-360)

## Operational Guarantees by Tier (L361-439)

### Price exactly-once, idempotent merge, watermark and backfill retention (L376-403)

### Backfill corrections need overwrite or update-matched merge, not insert-only (L404-410)

### Route alerts by tier, and schema breaks page the source (L411-439)
