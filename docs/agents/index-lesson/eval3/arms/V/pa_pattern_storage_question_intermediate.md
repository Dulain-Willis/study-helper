# Index: pa_pattern_storage_question_intermediate.md
Source: `lesson_preview/pa_pattern_storage_question_intermediate.md` (470 lines). Read just the line range you need (Read offset/limit).

## Why Parquet? (L27-101)

### Three mechanisms, one file layout (L29-47)

### Worked example: 1 billion rows, 3 columns (L48-76)

### Encoding before compression (L77-83)

### Where Parquet loses (L84-101)

## How Would You Partition? (L102-207)

### What a partition physically is (L104-125)

### Cardinality analysis (L126-154)

### The over-partition trap (L155-163)

### High-cardinality keys: sort or bucket, never partition (L164-207)

## Delta or Iceberg? (L208-306)

### The problem a directory cannot solve (L210-218)

### How each format writes the log (L219-276)

### Three axes, compared (L277-291)

### Making the choice in the room (L292-306)

## Data Lake or Warehouse? (L307-363)

### Three shapes, separated by who owns the files (L309-325)

### A scenario with 2 workloads (L326-336)

### Where the lakehouse pays off (L337-343)

### The questions that decide it (L344-363)

## How Much Will This Cost? (L364-448)

### The lines on the bill (L366-380)

### Worked model: 1 year of the events table (L381-410)

### Compute: where the format decision gets multiplied (L411-421)

### Two smaller lines that bite (L422-448)
