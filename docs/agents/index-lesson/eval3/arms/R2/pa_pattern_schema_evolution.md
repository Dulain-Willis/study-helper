# Index: pa_pattern_schema_evolution.md
Source: `lesson_preview/pa_pattern_schema_evolution.md` (551 lines). Read just the line range you need (Read offset/limit).

## "What If a Source Adds a Column?" (L27-116)

### Trace a new field through every hop to its consumers (L29-39)

### Loud and silent failures a schema change can cause (L40-81)

### Additive schema changes can still break field meaning (L82-96)

### Each layer needs its own policy for unknown fields (L97-116)

## Forward and Backward Compatibility (L117-227)

### Writer schema versus reader schema in compatibility checks (L119-139)

### Compatibility verdicts for common schema changes (L140-160)

### A default value can hide a missing required field (L161-206)

### Transitive checks compare a new schema to every prior version (L207-227)

## Schema Registries and Contracts (L228-332)

### A schema registry enforces shape at registration time (L230-254)

### A data contract adds units, meaning and ownership (L255-298)

### Halting versus quarantining bad events at ingestion (L299-332)

## Handling Breaking Changes (L333-439)

### Expand, migrate, contract for breaking schema changes (L335-352)

### A view can rebuild a retired field for old readers (L353-383)

### Upcasters convert old event versions to the latest schema (L384-439)

## Schema Evolution in Table Formats (L440-529)

### Three ways a file format can bind columns (L442-465)

### Reusing a dropped column name can resurrect old values (L466-488)

### Partition schemes can change without rewriting old files (L489-507)

### Sink jobs must agree on schema with stream and table (L508-529)
