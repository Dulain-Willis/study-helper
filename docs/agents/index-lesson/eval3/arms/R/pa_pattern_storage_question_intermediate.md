# Index: pa_pattern_storage_question_intermediate.md
Source: `lesson_preview/pa_pattern_storage_question_intermediate.md` (470 lines). Read just the line range you need (Read offset/limit).

## Why Parquet? (L27-101)

### Three mechanisms that make Parquet read fewer bytes (L29-47)

### How columnar reads and row group skipping shrink bytes scanned (L48-76)

### Dictionary and run-length encoding shrink columns before compression (L77-83)

### Workloads that Parquet's immutable file layout handles poorly (L84-101)

## How Would You Partition? (L102-207)

### A partition is a directory, pruned before any file is opened (L104-125)

### Size partition count from key cardinality and target partition size (L126-154)

### Over-partitioning costs listing time, small files and lost pruning (L155-163)

### High-cardinality keys belong in sorting or bucketing, not partitioning (L164-207)

## Delta or Iceberg? (L208-306)

### Directory listings cannot provide atomic commits or consistent schema (L210-218)

### How Delta Lake and Iceberg record each commit as metadata (L219-276)

### Delta Lake and Iceberg differ on time travel and schema evolution (L277-291)

### Choosing Delta Lake or Iceberg by engines, partitions and renames (L292-306)

## Data Lake or Warehouse? (L307-363)

### Warehouse, lake and lakehouse differ by who owns the files (L309-325)

### Layering raw, curated and serving data by consumer (L326-336)

### A lakehouse pays off when one copy serves all consumers (L337-343)

### Questions that decide between warehouse, lake and lakehouse (L344-363)

## How Much Will This Cost? (L364-448)

### A storage estimate has four lines: storage, requests, retrieval, compute (L366-380)

### How lifecycle tiers change the storage bill over time (L381-410)

### Compute cost multiplies bytes scanned by query volume (L411-421)

### Retained table versions count as storage you pay for (L422-448)
