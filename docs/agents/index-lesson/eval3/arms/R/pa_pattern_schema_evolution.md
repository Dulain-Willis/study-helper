# Index: pa_pattern_schema_evolution.md
Source: `lesson_preview/pa_pattern_schema_evolution.md` (551 lines). Read just the line range you need (Read offset/limit).

## "What If a Source Adds a Column?" (L27-116)

### Tracing a new source field through every pipeline hop (L29-39)

### Loud failures versus silent failures in schema changes (L40-81)

### Additive schema changes can still break field meaning (L82-96)

### Each pipeline layer needs its own schema-change policy (L97-116)

## Forward and Backward Compatibility (L117-227)

### Writer and reader schemas define backward and forward compatibility (L119-139)

### Compatibility verdicts for common schema changes (L140-160)

### Field defaults can hide missing values from readers (L161-206)

### Transitive compatibility checks against all prior versions (L207-227)

## Schema Registries and Contracts (L228-332)

### Schema registries enforce shape, not field meaning (L230-254)

### Data contracts add field meaning, units, and ownership (L255-298)

### Halting versus quarantining bad events at ingestion (L299-332)

## Handling Breaking Changes (L333-439)

### Expand-migrate-contract for breaking schema changes (L335-352)

### Database views as compatibility shims for legacy readers (L353-383)

### Upcasters convert replayed and late data to the latest version (L384-439)

## Schema Evolution in Table Formats (L440-529)

### Column binding: positional, name-matched, and ID-based (L442-465)

### Reusing a dropped column name in table formats (L466-488)

### Changing partition schemes without rewriting old files (L489-507)

### Keeping stream and lake table schemas in agreement (L508-529)
