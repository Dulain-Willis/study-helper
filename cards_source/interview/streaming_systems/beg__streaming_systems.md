Q: When you hear these in an interview, what concept is being tested?

"Tell me about Kafka."
"How does an event streaming platform work?"
"What is the difference between Kafka and a message queue?"

A: Event platforms

---
Q:
- "Tell me about Kafka."
- "How does an event streaming platform work?"
- "What is the difference between Kafka and a message queue?"
A: Kafka is a distributed event streaming platform. Producers write events to topics. Each topic is split into partitions for parallel processing. Consumers read from partitions using consumer groups, where each partition is assigned to exactly one consumer in the group. The key difference from a traditional message queue: Kafka retains events after they are read, so multiple consumers can independently replay the same data. That is the answer. Topics, partitions, consumer groups, and retention. Four concepts in four sentences.

---

Q: For each Kafka term, define what it is and give a one-liner for interviews.

A:
| Term | What it is | One-liner for interviews |
|---|---|---|
| Topic | A named stream of events | Like a table, but append-only |
| Partition | A shard of a topic | Events with the same key go to the same partition |
| Producer | Writes events to a topic | Your application or data source |
| Consumer | Reads events from a topic | Your pipeline or processing job |
| Consumer Group | A set of consumers sharing the work | Each partition is read by exactly one consumer in the group |
| Offset | Position in the partition | Like a bookmark: tracks what you have read |

---

Q: When asked about event platforms, a common follow-up is: "What is the difference between Kafka and RabbitMQ?"

A: Kafka retains events and supports replay. RabbitMQ deletes events after delivery. Use Kafka when multiple consumers need the same data or when you need to reprocess.

---

Q: When asked about event platforms, a common follow-up is: "How do you choose the number of partitions?"

A: Partitions determine maximum parallelism. If you have 10 consumers, you need at least 10 partitions. But too many partitions increase metadata overhead. Start with the number of consumers and scale up as needed.

---

Q: When asked about event platforms, a common follow-up is: "What happens if a consumer crashes?"

A: The consumer group rebalances. The crashed consumer's partitions are reassigned to other consumers in the group. When the consumer comes back, it resumes from the last committed offset.

---

Q: What are the key takeaways when asked about event platforms?

A: Say: 'Kafka is a distributed event platform. Producers write to topics, consumers read from partitions via consumer groups.'

The key difference from message queues: Kafka retains events after consumption. Multiple consumers can replay.

Partitions = parallelism. At least one partition per consumer.

---

Q: When you hear these in an interview, what concept is being tested?

"What is event-driven architecture?"
"How is event streaming different from request-response?"
"When would you choose an event-driven approach?"

A: Event-driven architecture

---
Q:
- "What is event-driven architecture?"
- "How is event streaming different from request-response?"
- "When would you choose an event-driven approach?"
A: In event-driven architecture, services communicate by publishing events instead of calling each other directly. When an order is placed, the order service publishes an event. The inventory service, the notification service, and the analytics pipeline each consume that event independently. No service needs to know about the others. This decouples teams and systems. That is the answer. Publish, not call. Independent consumers. Decoupled teams.

---

Q: Request-response vs. event-driven: how does each one communicate, and how do they compare on timing, coupling, and scale?

A:
| | Communication | Timing | Coupling | Scale |
|---|---|---|---|---|
| Request-response | Service A calls Service B directly | Synchronous: A waits for B's response | Tight coupling: A breaks if B is down | Simple for 2-3 services |
| Event-driven | Service A publishes an event | Asynchronous: A does not wait | Loose coupling: A does not know about B | Scales to many consumers |

---

Q: When asked about event-driven architecture, a common follow-up is: "What is the downside of event-driven?"

A: Debugging is harder. When something goes wrong, you cannot trace a single request through a call stack. Instead you trace events across multiple systems. Distributed tracing tools (Jaeger, Zipkin) help.

---

Q: When asked about event-driven architecture, a common follow-up is: "What is event sourcing?"

A: Storing every state change as an immutable event instead of overwriting the current state. The current state is derived by replaying all events. Example: a bank account is a sequence of deposits and withdrawals, not a single balance number.

---

Q: When asked about event-driven architecture, a common follow-up is: "When would you NOT use event-driven?"

A: When you need a synchronous response: user login, payment processing, real-time API calls. If the caller needs an answer right now, request-response is the right pattern.

---

Q: What are the key takeaways when asked about event-driven architecture?

A: Say: 'Event-driven: services publish events instead of calling each other. Consumers are independent and decoupled.'

The tradeoff: loose coupling and scalability vs harder debugging

Event sourcing stores every change. The current state is derived by replay.

---

Q: When you hear these in an interview, what concept is being tested?

"What happens when events arrive late?"
"How do you handle out-of-order data?"
"What is a watermark?"

A: Late-arriving data

---
Q:
- "What happens when events arrive late?"
- "How do you handle out-of-order data?"
- "What is a watermark?"
A: Late data arrives after the window it belongs to has already been processed. A click that happened at 11:58 PM might arrive at 12:03 AM, after the hourly window closed. I handle this with watermarks: a threshold that says how late I am willing to wait. If my watermark is 10 minutes, I keep the window open for 10 extra minutes to accept late events. Events that arrive after the watermark are either dropped or sent to a dead letter queue for reprocessing. That is the answer. Late data is inevitable. Watermarks define how long you wait. After that, dead letter queue.

---

Q: When asked about late-arriving data, a common follow-up is: "How do you choose the watermark duration?"

A: Based on observed lateness. If 99% of events arrive within 5 minutes, a 10-minute watermark catches nearly everything. Longer watermarks mean more correct results but higher latency and memory usage.

---

Q: When asked about late-arriving data, a common follow-up is: "What is the difference between event time and processing time?"

A: Event time is when the event actually happened (the click timestamp). Processing time is when your system received it. Always use event time for aggregations, or your hourly counts will be wrong.

---

Q: When asked about late-arriving data, a common follow-up is: "What if late data is critical and cannot be dropped?"

A: Send it to a dead letter queue and run a separate batch job to backfill the affected windows. This gives you the speed of streaming with the correctness of batch.

---

Q: What are the key takeaways when asked about late-arriving data?

A: Say: 'Watermarks define how late I wait. Events past the watermark go to a dead letter queue.'

Always aggregate on event time, not processing time

The watermark tradeoff: longer = more correct, but higher latency and memory

---

Q: When you hear these in an interview, what concept is being tested?

"What happens to records that fail processing?"
"How do you handle poison messages?"
"What is a dead letter queue?"

A: Dead letter queues

---
Q:
- "What happens to records that fail processing?"
- "How do you handle poison messages?"
- "What is a dead letter queue?"
A: A dead letter queue (DLQ) is where events go when they cannot be processed. Instead of crashing the pipeline or blocking the stream, the bad event is moved to a separate topic for investigation. This keeps the main pipeline flowing. I monitor DLQ depth as a health metric: if it grows, something is systematically wrong. I reprocess DLQ events after fixing the root cause. That is the answer. DLQ = safety valve. Monitor depth. Fix root cause, then replay.

---

Q: When asked about dead letter queues, a common follow-up is: "What causes events to land in the DLQ?"

A: Schema mismatch (unexpected field types), malformed JSON, business rule violations (negative amounts), or downstream system unavailability.

---

Q: When asked about dead letter queues, a common follow-up is: "How many retries before sending to DLQ?"

A: Typically 3 retries with exponential backoff. If the event fails all 3, it goes to DLQ. Transient errors (timeout, rate limit) are worth retrying. Permanent errors (bad schema) should go to DLQ immediately.

---

Q: When asked about dead letter queues, a common follow-up is: "What if the DLQ itself fills up?"

A: Alert immediately. A growing DLQ means the root cause is not transient. Pause investigation of individual events and focus on the systemic issue first.

---

Q: What are the key takeaways when asked about dead letter queues?

A: Say: 'Dead letter queue: failed events go to a separate topic instead of blocking the pipeline. Monitor depth, fix root cause, replay.'

3 retries with backoff, then DLQ. Permanent errors skip retries.

DLQ depth is a health metric. Growing depth = systemic problem.

---

Q: When you hear these in an interview, what concept is being tested?

"What is the difference between Spark Streaming and Flink?"
"Is Spark Structured Streaming real streaming?"
"When is micro-batch good enough?"

A: Micro-batch vs true streaming

---
Q:
- "What is the difference between Spark Streaming and Flink?"
- "Is Spark Structured Streaming real streaming?"
- "When is micro-batch good enough?"
A: Micro-batch processes events in small time windows, typically every few seconds. Spark Structured Streaming uses this model. True streaming processes each event as it arrives with no batching delay. Flink uses this model. The practical difference is latency: micro-batch has a floor around 100 milliseconds. True streaming can process in single-digit milliseconds. For most use cases, micro-batch is good enough and simpler to operate. That is the answer. Micro-batch = small windows, 100ms floor, simpler. True streaming = per-event, sub-10ms, more complex.

---

Q: Micro-batch (Spark) vs true streaming (Flink): how does each process events, what latency does it give, and what are the engine and operational tradeoffs?

A:
| | Processing | Latency | Engine | Operations |
|---|---|---|---|---|
| Micro-batch (Spark) | Processes in small time windows (1-10 seconds) | Latency floor: ~100 milliseconds | Uses the same Spark engine as batch | Easier to operate if your team knows Spark |
| True streaming (Flink) | Processes each event individually | Single-digit milliseconds | Built-in exactly-once guarantees | Steeper learning curve, but more powerful for streaming |

---

Q: When asked about micro-batch vs true streaming, a common follow-up is: "When does latency actually matter?"

A: Fraud detection (block the transaction before it completes), live bidding (ad auction in 50ms), driver matching (Uber needs real-time location). For dashboards, alerting, and analytics, 100ms micro-batch is fine.

---

Q: When asked about micro-batch vs true streaming, a common follow-up is: "Can Spark do true streaming?"

A: No. Spark Structured Streaming is always micro-batch under the hood, even with `trigger='continuous'` (which is experimental and limited). If you need true per-event streaming, use Flink.

---

Q: When asked about micro-batch vs true streaming, a common follow-up is: "Why not always use Flink?"

A: Operational complexity. If your team already runs Spark for batch, adding Flink means learning a new framework, new deployment, new monitoring. Use Spark Streaming for most cases and Flink only when latency requirements demand it.

---

Q: What are the key takeaways when asked about micro-batch vs true streaming?

A: Say: 'Micro-batch (Spark) for most cases: 100ms latency, simpler ops. True streaming (Flink) when sub-10ms matters.'

Spark Streaming is micro-batch under the hood. It is not true per-event streaming.

Choose based on latency needs AND team expertise. Flink is powerful but has a learning curve.
