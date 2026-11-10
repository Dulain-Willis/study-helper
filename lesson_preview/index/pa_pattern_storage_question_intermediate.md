# Index: pa_pattern_storage_question_intermediate.md
Source: `lesson_preview/pa_pattern_storage_question_intermediate.md` (470 lines). Read just the line range you need (Read offset/limit).

## Why Parquet? (L27-101)

### Parquet cuts bytes read via columnar layout, encoding, footer statistics (L29-47)

### Estimate bytes read from column chunks after row group skipping (L48-76)

### Dictionary, run-length and delta encoding shrink columns before compression (L77-83)

### Point lookups, updates and small appends suit columnar files poorly (L84-101)

## How Would You Partition? (L102-207)

### Partitions are directories, pruned before any file is opened (L104-125)

### Score partition key candidates by partition count, size and skew (L126-154)

### Over-partitioning slows listing, multiplies small files and weakens statistics (L155-163)

### Sort or bucket high-cardinality keys instead of partitioning them (L164-207)

## Delta or Iceberg? (L208-306)

### Directory listings give no atomic commits or consistent schema (L210-218)

### Atomic table commits: new log file or swapped metadata pointer (L219-276)

### Delta vs Iceberg on commits, time travel, schema and partitions (L277-291)

### Choose table format by engine count, partition changes, and renames (L292-306)

## Data Lake or Warehouse? (L307-363)

### Warehouse, lake and lakehouse differ in who can read files (L309-325)

### Raw data to a lake, curated tables to a warehouse (L326-336)

### Lakehouse fits when one copy serves both engines and auditors (L337-343)

### Questions that decide lake, warehouse or lakehouse placement (L344-363)

## How Much Will This Cost? (L364-448)

### Storage cost has four lines: tier storage, requests, retrieval, compute (L366-380)

### Lifecycle tier transitions follow the longest query window (L381-410)

### Scan-priced compute multiplies format and partition choices by query volume (L411-421)

### Small files and retained snapshots add cost beyond storage rates (L422-448)
