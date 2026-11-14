# Index: pa_pattern_schema_evolution.md
Source: `lesson_preview/interview/architecture/pa_pattern_schema_evolution.md` (551 lines). Read just the line range you need (Read offset/limit).

## "What If a Source Adds a Column?" (L27-116)

### Trace a new field hop by hop, naming each binding (L29-39)

### Schema changes fail silently where loads and exports still succeed (L40-81)

### A compatible column addition can still change a field's meaning (L82-96)

### Choose unknown-field handling per layer by what each promises (L97-116)

## Forward and Backward Compatibility (L117-227)

### Compatibility compares the writer's schema with the reader's (L119-139)

### Compatibility verdicts for adding, removing, renaming, and retyping fields (L140-160)

### Null defaults keep compatibility while leaving missing values visible (L161-206)

### Transitive checks compare a new schema against every version (L207-227)

## Schema Registries and Contracts (L228-332)

### A schema registry checks shape at registration, not meaning (L230-254)

### Data contracts add units, owners, and change policy to schemas (L255-298)

### Quarantine bad events per record and alarm on failure rate (L299-332)

## Handling Breaking Changes (L333-439)

### Expand-migrate-contract ships breaking changes in phases ending on evidence (L335-352)

### Rebuild a retired field in a view for old readers (L353-383)

### Upcasters turn each old event version into the latest shape (L384-439)

## Schema Evolution in Table Formats (L440-529)

### Binding by position, name, or field ID sets change cost (L442-465)

### Field IDs stop a reused column name exposing old values (L466-488)

### Partition scheme can change per file without a rewrite (L489-507)

### Sink jobs auto-evolve raw tables and migrate staging and curated (L508-529)
