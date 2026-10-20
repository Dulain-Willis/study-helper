Q: When you hear these in an interview, what concept is being tested?

"How do you guarantee exactly-once processing?"
"Your consumer group rebalances every 30 seconds. Why?"
"What happens when you add a consumer to the group?"

A: Consumer groups and offsets

---
Q:
- "How do you guarantee exactly-once processing?"
- "Your consumer group rebalances every 30 seconds. Why?"
- "What happens when you add a consumer to the group?"
A: Each consumer in a group tracks its position in the partition using offsets. After processing a batch of events, the consumer commits its offset. If the consumer crashes, it restarts from the last committed offset. This gives at-least-once delivery by default. For exactly-once, I use the transactional pattern: commit the offset and write the output in the same transaction, so either both happen or neither does. This is the answer that shows you understand the offset commit lifecycle and why exactly-once requires transactional writes.

---

Q: For each offset strategy, describe how it works and the guarantee it gives.

A:
| Offset strategy | How it works | Guarantee |
|---|---|---|
| Auto-commit | Offset committed on timer (every 5s) | At-most-once: may lose events on crash |
| Manual commit after processing | Commit after successful processing | At-least-once: may duplicate on crash |
| Transactional (commit + write atomically) | Offset and output in same transaction | Exactly-once: no loss, no duplicates |

---

Q: When asked about consumer groups and offsets, a common follow-up is: "What causes frequent rebalancing?"

A: Three common causes: (1) Consumer processing takes longer than `max.poll.interval.ms`, so the broker thinks the consumer is dead. Increase the interval or reduce batch size. (2) Unstable consumers joining and leaving. (3) Partition count changed, triggering reassignment.

---

Q: When asked about consumer groups and offsets, a common follow-up is: "What if you need to reprocess from the beginning?"

A: Reset the consumer group offset to the earliest position. Kafka retains events for the configured retention period (default 7 days), so you can replay anything within that window.

---

Q: When asked about consumer groups and offsets, a common follow-up is: "How do you handle exactly-once with a non-transactional sink?"

A: Idempotent writes. Use a unique event ID as a dedup key. The sink handles duplicates by ignoring events it has already seen.

---

Q: What are the key takeaways when asked about consumer groups and offsets?

A: Say: 'At-least-once by default. Exactly-once requires transactional offset + write, or idempotent sinks.'

Frequent rebalancing usually means `max.poll.interval.ms` is too short for the processing time

Reset offsets to replay. Kafka retention defines the replay window.

---

Q: When you hear these in an interview, what concept is being tested?

"What is event sourcing and when do you use it?"
"What is CQRS?"
"How do you rebuild state from events?"

A: Event sourcing patterns

---
Q:
- "What is event sourcing and when do you use it?"
- "What is CQRS?"
- "How do you rebuild state from events?"
A: Event sourcing stores every state change as an immutable event. The current state is derived by replaying events from the beginning. I pair it with CQRS: Command Query Responsibility Segregation, where writes go to the event log and reads come from a materialized view that is built by processing the event stream. This separates the write model from the read model, allowing each to be optimized independently. This is the answer that shows you understand event sourcing as an architectural pattern, not just a buzzword.

---

Q: When asked about event sourcing patterns, a common follow-up is: "When does event sourcing not make sense?"

A: When the event history has no business value. A shopping cart does not need event sourcing: just store the current cart. An audit log for financial transactions does: regulators require the full history.

---

Q: When asked about event sourcing patterns, a common follow-up is: "How do you handle event schema evolution?"

A: Version your events. Event v1 and v2 can coexist in the same log. The projection handles both versions with an upcaster that translates old formats to new.

---

Q: When asked about event sourcing patterns, a common follow-up is: "What about the event log growing forever?"

A: Snapshotting. Periodically write the current state as a snapshot event. On replay, start from the latest snapshot instead of event zero. This bounds replay time.

---

Q: What are the key takeaways when asked about event sourcing patterns?

A: Say: 'Event sourcing: immutable event log as the source of truth. CQRS: separate write model from read model.'

Use event sourcing when the history has business value (audit, compliance, analytics)

Snapshots prevent unbounded replay time

---

Q: When you hear these in an interview, what concept is being tested?

"Design a streaming aggregation with late data handling."
"What are tumbling vs sliding vs session windows?"
"How do you set the right watermark duration?"

A: Windowing and watermarks

---
Q:
- "Design a streaming aggregation with late data handling."
- "What are tumbling vs sliding vs session windows?"
- "How do you set the right watermark duration?"
A: I choose the window type based on the use case. Tumbling windows are fixed, non-overlapping intervals: count clicks per hour. Sliding windows overlap: count clicks in the last hour, updated every 5 minutes. Session windows are activity-based: group events that are close together in time, with a gap timeout. I set the watermark based on observed lateness: if 99th percentile lateness is 5 minutes, I set the watermark to 10 minutes to catch stragglers with margin. This is the answer that shows you can match window type to use case and set watermarks from data.

---

Q: For each window type, describe its behavior and give a use case.

A:
| Window type | Behavior | Use case |
|---|---|---|
| Tumbling | Fixed size, no overlap (e.g., every hour) | Hourly revenue, daily active users |
| Sliding | Fixed size, overlaps (e.g., 1 hour window, 5 min slide) | Rolling averages, trending topics |
| Session | Dynamic size, grouped by activity gaps | User sessions, conversation threads |

---

Q: Write the code to implement a tumbling window with a watermark in Spark Structured Streaming.

A:
```
# Spark Structured Streaming: tumbling window with watermark
from pyspark.sql.functions import window, col
events \
  .withWatermark("event_time", "10 minutes") \
  .groupBy(
    window(col("event_time"), "1 hour"),
    col("page_id")
  ) \
  .count()
```

---

Q: When asked about windowing and watermarks, a common follow-up is: "Your watermark is 10 minutes but some events arrive 2 hours late. What do you do?"

A: Accept that those events will not be in the streaming output. Route them to a DLQ and run a separate batch correction job that updates the affected windows daily. Do not extend the watermark to 2 hours: it would hold too much state in memory.

---

Q: When asked about windowing and watermarks, a common follow-up is: "How do session windows handle very long sessions?"

A: Set a session timeout (e.g., 30 minutes of inactivity). If a user is active for 4 hours, it is one long session. Set a maximum session duration to cap memory usage: force-close sessions after 24 hours.

---

Q: When asked about windowing and watermarks, a common follow-up is: "What is the cost of a longer watermark?"

A: More state in memory. Each open window and its partial aggregation must be held in the state store until the watermark passes. A 10-minute watermark holds 10 minutes of windows; a 2-hour watermark holds 2 hours of windows.

---

Q: What are the key takeaways when asked about windowing and watermarks?

A: Say: 'Tumbling for fixed intervals, sliding for rolling averages, session for user activity. Watermark from observed p99 lateness.'

Longer watermarks hold more state in memory. Do not over-extend for rare late events.

Extremely late events: DLQ + batch correction. Do not compromise streaming latency.

---

Q: When you hear these in an interview, what concept is being tested?

"Design a DLQ reprocessing workflow."
"How do you distinguish transient errors from permanent errors?"
"Your DLQ has 500K events. Walk me through remediation."

A: DLQ patterns

---
Q:
- "Design a DLQ reprocessing workflow."
- "How do you distinguish transient errors from permanent errors?"
- "Your DLQ has 500K events. Walk me through remediation."
A: I structure DLQ events with metadata: the original event, the error message, the retry count, and the timestamp. For reprocessing, I classify errors first. Transient errors (timeout, rate limit) go back to the main topic after a delay. Permanent errors (schema mismatch, invalid data) require a code fix before replay. I never blindly replay the entire DLQ: that just reproduces the same errors and wastes compute. This is the answer that shows you have actually dealt with a DLQ in production.

---

Q: For each error type, give examples and the action to take.

A:
| Error type | Examples | Action |
|---|---|---|
| Transient | Timeout, rate limit, network blip | Retry after backoff, then replay to main topic |
| Permanent (data) | Invalid JSON, missing required field | Fix data manually or with a transform, then replay |
| Permanent (code) | Bug in processing logic | Fix code, deploy, then replay affected events |
| Poison pill | Event that crashes the consumer | Isolate immediately, investigate separately |

---

Q: When asked about DLQ patterns, a common follow-up is: "How do you detect a poison pill event?"

A: A poison pill crashes the consumer, which restarts and crashes again on the same event. Detect it by tracking the event offset across restarts. If the same offset fails 3 times, route it to DLQ and skip. Without this pattern, one bad event blocks the entire partition.

---

Q: When asked about DLQ patterns, a common follow-up is: "How do you prioritize DLQ remediation?"

A: By business impact. Events affecting revenue (orders, payments) get fixed first. Events affecting analytics (page views, clicks) can wait. Track DLQ events by topic and error type to prioritize.

---

Q: When asked about DLQ patterns, a common follow-up is: "What is the DLQ schema?"

A: Original event payload, original topic, partition, offset, error message, exception stack trace, retry count, first failure timestamp, last failure timestamp. This gives you everything needed to diagnose and replay.

---

Q: What are the key takeaways when asked about DLQ patterns?

A: Say: 'Classify errors first. Transient: retry. Permanent: fix then replay. Never blindly replay the entire DLQ.'

Poison pill detection: if the same offset fails 3 times, skip to DLQ immediately

DLQ events include full metadata: original event, error, retry count, timestamps

---

Q: When you hear these in an interview, what concept is being tested?

"Why did you choose Spark Streaming over Flink?"
"What is the latency floor for Spark Structured Streaming?"
"How does Flink handle state differently than Spark?"

A: Spark Streaming vs Flink

---
Q:
- "Why did you choose Spark Streaming over Flink?"
- "What is the latency floor for Spark Structured Streaming?"
- "How does Flink handle state differently than Spark?"
A: Spark Structured Streaming is micro-batch: it collects events for a trigger interval (e.g., 10 seconds), processes them as a batch, then starts the next interval. Flink processes each event as it arrives with continuous operator pipelines. The practical differences: Spark has a 100ms latency floor and simpler state management. Flink has sub-10ms latency, more powerful windowing (session windows, event-time processing), and built-in savepoints for state migration. I choose Spark when the team already runs Spark for batch and latency requirements are relaxed. I choose Flink when latency is critical or the streaming logic requires complex stateful processing. This is the answer that shows you can make a technology choice with concrete tradeoff reasoning.

---

Q: For each dimension, compare Spark Structured Streaming and Apache Flink.

A:
| Dimension | Spark Structured Streaming | Apache Flink |
|---|---|---|
| Processing model | Micro-batch | True per-event streaming |
| Latency | 100ms+ (trigger interval) | Single-digit milliseconds |
| State management | State store backed by HDFS/S3 | RocksDB state backend, local |
| Exactly-once | Checkpoint + idempotent sink | Checkpoint barriers + 2-phase commit |
| Session windows | Not natively supported | First-class support |
| Batch + streaming | Same API for both | Separate DataStream and Table APIs |
| Operational complexity | Familiar if team uses Spark | New deployment, monitoring, tuning |

---

Q: When asked about Spark Streaming vs Flink, a common follow-up is: "What are Flink savepoints and why do they matter?"

A: Savepoints are user-triggered snapshots of the entire application state. You can stop a Flink job, modify the code, and restart from the savepoint without losing state. Spark does not have an equivalent: restarting from a checkpoint requires the same code.

---

Q: When asked about Spark Streaming vs Flink, a common follow-up is: "How does Spark's trigger interval affect throughput?"

A: Shorter intervals (1 second) give lower latency but higher scheduling overhead. Longer intervals (30 seconds) give higher throughput but higher latency. The sweet spot depends on event volume: high-volume streams benefit from longer intervals.

---

Q: When asked about Spark Streaming vs Flink, a common follow-up is: "Can you migrate from Spark Streaming to Flink?"

A: Yes, but it requires rewriting the processing logic. The state is not portable between them. Plan a parallel-run period where both systems process the same events and you compare outputs.

---

Q: What are the key takeaways when asked about Spark Streaming vs Flink?

A: Say: 'Spark for micro-batch (100ms+, simpler ops). Flink for true streaming (sub-10ms, complex state).'

Flink savepoints allow code changes without state loss. Spark checkpoints do not.

Choose based on latency needs AND team expertise. Flink's power comes with operational complexity.
