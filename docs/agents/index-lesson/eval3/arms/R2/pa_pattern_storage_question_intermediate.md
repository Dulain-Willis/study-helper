# Index: pa_pattern_storage_question_intermediate.md
Source: `lesson_preview/pa_pattern_storage_question_intermediate.md` (470 lines). Read just the line range you need (Read offset/limit).

## Why Parquet? (L27-101)

### Three mechanisms that make Parquet read fewer bytes (L29-47)

### Estimating bytes read by a narrow query on Parquet (L48-76)

### Column encoding shrinks data before compression runs (L77-83)

### Workloads that Parquet's file layout handles poorly (L84-101)

## How Would You Partition? (L102-207)

### A partition is a directory pruned before any file opens (L104-125)

### Choosing partition keys by cardinality, partition size and skew (L126-154)

### Over-partitioning costs listing time, small files and lost statistics (L155-163)

### When high-cardinality keys belong in sorting or bucketing, not partitioning (L164-207)

## Delta or Iceberg? (L208-306)

### Why a directory listing cannot give atomic commits (L210-218)

### How Delta Lake and Iceberg write their transaction logs (L219-276)

### Delta Lake and Iceberg on commits, time travel and schema (L277-291)

### Choosing a table format from engines, partitioning and renames (L292-306)

## Data Lake or Warehouse? (L307-363)

### Warehouse, lake and lakehouse differ by file ownership (L309-325)

### Matching storage layers to each workload's consumers (L326-336)

### When a lakehouse fits one table serving many consumers (L337-343)

### Questions that decide between lake, warehouse and lakehouse (L344-363)

## How Much Will This Cost? (L364-448)

### Four cost lines in a storage and compute estimate (L366-380)

### Modeling a year of storage across lifecycle tiers (L381-410)

### Compute cost per query scales with bytes scanned and volume (L411-421)

### Request costs and retained table versions in storage bills (L422-448)
