# Index: pipeline_how_data_moves_advanced.md
Source: `lesson_preview/interview/architecture/pipeline_how_data_moves_advanced.md` (416 lines). Read just the line range you need (Read offset/limit).

## Idempotent Pipelines (L27-128)

### Diagnose where a rerun changes output before proposing deduplication (L29-35)
### Write patterns that make a rerun change its output (L36-50)
### Partition overwrite makes a batch rerun produce identical output (L51-77)
### Deduplicate replayed stream events by id within a bounded window (L78-128)

## Backpressure (L129-189)

### Turn a producer-consumer rate gap into lag and fill time (L131-140)
### Four ways to absorb backpressure: buffer, slow, speed up, drop (L141-154)
### Partition count caps consumer parallelism in a partitioned log (L155-161)
### Bound every buffer and define what happens when it fills (L162-170)
### Alert on lag growth rate rather than lag level (L171-189)

## Cross-Region Data Replication (L190-258)

### Cross-region replication has 3 costs: latency, egress, consistency (L192-198)
### Cross-region round-trip latency is bounded by the speed of light (L199-212)
### Estimate cross-region egress as per-gigabyte rate times monthly bytes (L213-233)
### Single-writer per key removes conflicts from async cross-region replication (L234-240)
### Compute aggregates in-region and union them for data residency (L241-258)

## Moving Sensitive Data Safely (L259-329)

### List every place personal data leaks while it moves (L261-267)
### Encrypt data at rest with KMS-held keys, not only TLS (L268-274)
### Choose masking, keyed hashing, or tokenization by which joins survive (L275-300)
### Pseudonymize identifiers at ingest and restrict raw values (L301-329)

## Cost of Freshness (L330-394)

### Match data freshness to the decision cadence that consumes it (L332-338)
### Streaming costs more than batch when capacity sits idle (L339-355)
### Micro-batch writes create small files that need compaction (L356-362)
### Split bulk batch work from the urgent streaming signal (L363-377)
### Freshness SLA needs a number, an owner, and late-data rule (L378-394)
