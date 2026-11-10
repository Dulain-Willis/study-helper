# Index: pipeline_how_data_moves_intermediate.md
Source: `lesson_preview/pipeline_how_data_moves_intermediate.md` (411 lines). Read just the line range you need (Read offset/limit).

## Batch Mechanics (L27-112)

### High-water mark failure modes and the lookback that fixes them (L56-112)

## Stream Guarantees (L113-183)

### Offset commit timing before or after processing decides delivery guarantee (L120-144)

### Size deduplication window from consumer outage and producer retry horizon (L145-183)

## File Format Depth (L184-247)

### Partition pruning, column projection and row group statistics skip reads (L191-218)

### Schema evolution: additive changes safe, renames silently read as null (L219-247)

## API Patterns (L248-320)

### Offset pagination repeats or skips records when data changes mid-pull (L260-302)

### Pair webhooks with a scheduled reconciliation poll (L303-320)

## Hybrid Architectures (L321-389)

### Fixing history with two paths versus replaying one log (L328-347)

### Key the serving boundary on the latest batch date (L348-389)
