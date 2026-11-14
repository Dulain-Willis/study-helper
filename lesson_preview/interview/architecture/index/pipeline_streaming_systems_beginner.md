# Index: pipeline_streaming_systems_beginner.md
Source: `lesson_preview/interview/architecture/pipeline_streaming_systems_beginner.md` (511 lines). Read just the line range you need (Read offset/limit).

## Event Platforms (L27-122)

### Event log terms: topic, partition, offset, consumer group, retention, broker (L36-51)

### Hashing the key modulo partition count fixes per-key ordering (L52-84)

### Each consumer group commits its own offset per partition (L85-107)

### Committing after writing gives at-least-once delivery, requiring idempotent writes (L108-122)

## Event-Driven Architecture (L123-219)

### Name events as past-tense facts, not commands to receivers (L148-167)

### Event-driven failure modes: duplicates, schema breaks, topic ordering, consumer lag (L168-181)

### Use events for fan-out, calls when the caller must wait (L182-199)

### Pick event or call from delay tolerance and answer need (L200-219)

## Late-Arriving Data (L220-302)

### Watermark equals max event time seen minus allowed lateness (L227-233)

### Trace how a watermark closes windows and marks late events (L234-259)

### Late event handling options: drop, side table, re-emit, batch recompute (L260-302)

## Dead Letter Queues (L303-391)

### Classify errors as transient for retry or permanent for dead-letter (L312-349)

### Dead-letter record fields: original payload, source offset, error class (L350-364)

### Dead-letter queues need an owner, arrival alerts, and idempotent replay (L365-391)

## Micro-Batch vs True Streaming (L392-487)

### Choose micro-batch or streaming from the required freshness in seconds (L438-446)

### Trigger interval plus processing time must fit the freshness budget (L447-460)

### Idempotent writes make micro-batch reruns and checkpoint replays harmless (L461-487)
