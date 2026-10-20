Q: When you hear these in an interview, what concept is being tested?
- Explain how Spark works.
- What is the difference between a driver and an executor?
- What happens when you submit a Spark job?

A: Spark execution model

---

Q: Explain how Spark works.
- What is the difference between a driver and an executor?
- What happens when you submit a Spark job?

A: Spark splits work across a cluster. The driver is the coordinator — it plans the work, divides it into tasks, and sends those tasks to executors. Executors are the workers — each one processes a partition of the data in parallel. The key insight is that Spark is lazy: it builds a plan (the DAG) but does not execute anything until you call an action like `.count()` or `.write()`. Driver plans, executors execute. Nothing happens until an action triggers it.

---

Q: For each term, define what it is and give a one-liner for interviews.

A:
| Term | What it is | One-liner for interviews |
|---|---|---|
| Driver | The coordinator process | Plans the DAG and assigns tasks to executors |
| Executor | A worker process on a cluster node | Processes one or more partitions in parallel |
| Partition | A chunk of data | Each partition is processed by one task on one executor |
| Task | A unit of work | One task processes one partition through one stage |
| Stage | A group of tasks with no shuffle between them | Stage boundaries are created by shuffle operations |
| DAG | Directed acyclic graph of operations | Spark's execution plan; built lazily, executed on action |

---

Q: When asked about the Spark execution model, a common follow-up is: what triggers Spark to actually run?

A: An action. Transformations like `.filter()` and `.join()` are lazy and just build the DAG. Actions like `.count()`, `.collect()`, and `.write()` trigger execution.

---

Q: When asked about the Spark execution model, a common follow-up is: what happens if an executor fails?

A: The driver reassigns the failed tasks to other executors. If the data partition was lost, Spark recomputes it from the source using the DAG lineage.

---

Q: When asked about the Spark execution model, a common follow-up is: what is the DAG?

A: A directed acyclic graph of all transformations. Spark uses it to optimize the execution plan before running anything. Think of it as a recipe that Spark reads before cooking.

---

Q: What are the key takeaways when asked about the Spark execution model?

A:
- Say: driver plans, executors execute. Each partition is processed by one task in parallel.
- Spark is lazy: transformations build the DAG, actions trigger execution.
- Know the vocabulary: driver, executor, partition, task, stage, DAG.

---

Q: When you hear these in an interview, what concept is being tested?
- What is a transformation versus an action?
- What does lazy evaluation mean in Spark?
- What is a narrow versus wide transformation?

A: Distributed primitives

---

Q: What is a transformation versus an action?
- What does lazy evaluation mean in Spark?
- What is a narrow versus wide transformation?

A: A transformation defines a new dataset from an existing one without executing anything; an action triggers execution and returns a result. Narrow transformations like `filter` and `map` process each partition independently. Wide transformations like `groupBy` and `join` require data to move between executors, which creates a shuffle. Narrow = no data movement, wide = shuffle. The distinction is the foundation of Spark performance.

---

Q: For narrow and wide transformations: give 4 example functions for each, describe how data moves between executors, whether a shuffle is created, and how fast each is.

A:
| | Examples | Data movement | Shuffle | Speed |
|---|---|---|---|---|
| Narrow | `filter()`, `map()`, `select()`, `withColumn()` | No data moves between executors | None | Fast: no network I/O |
| Wide | `groupBy()`, `join()`, `repartition()`, `distinct()` | Data must move between executors | Creates a shuffle (network + disk I/O) | Slow: the #1 performance bottleneck |

---

Q: When asked about distributed primitives, a common follow-up is: why does it matter if a transformation is narrow or wide?

A: Wide transformations create stage boundaries. Data must be serialized, sent over the network, and deserialized. This is the most expensive operation in Spark.

---

Q: When asked about distributed primitives, a common follow-up is: what is the Catalyst optimizer?

A: Spark's query planner. It rearranges transformations for efficiency: pushes filters before joins, chooses join strategies, and eliminates unnecessary columns. You write the logic. Catalyst optimizes the execution.

---

Q: When asked about distributed primitives, a common follow-up is: name an action.

A: `.count()`, `.collect()`, `.show()`, `.write()`. These trigger the DAG to execute. Until an action is called, Spark has done nothing.

---

Q: What are the key takeaways when asked about distributed primitives?

A:
- Say: narrow transformations need no data movement, wide transformations cause shuffles. Minimize wide operations.
- Transformations are lazy (build the DAG). Actions trigger execution.
- The Catalyst optimizer rearranges your code for performance. Trust it, but know what it does.

---

Q: When you hear these in an interview, what concept is being tested?
- What is a shuffle in Spark?
- Why is this join slow?
- Why do you avoid shuffles?

A: Shuffle operations

---

Q: What is a shuffle in Spark?
- Why is this join slow?
- How do you avoid shuffles?

A: A shuffle redistributes data across executors. It happens when Spark needs to group or join data by a key and the matching rows are spread across different partitions. Shuffles are expensive because every executor must write its data to disk and send it over the network, and every receiving executor must read and merge it. The number one way to avoid unnecessary shuffles is broadcast joins: if one side of the join is small enough to fit in memory, broadcast it to all executors so no shuffle is needed. Shuffle = redistribute = expensive. Broadcast = avoid the shuffle.

---

Q: When asked about shuffle operations, a common follow-up is: when is a broadcast join not possible?

A: When both sides of the join are too large to fit in executor memory. The default broadcast threshold in Spark is 10MB. You can increase it, but broadcasting a 1GB table wastes memory on every executor.

---

Q: When asked about shuffle operations, a common follow-up is: how do you know if a shuffle is happening?

A: Check the Spark UI. Stage boundaries in the DAG visualization indicate shuffles. The "shuffle read" and "shuffle write" metrics tell you how much data moved.

---

Q: When asked about shuffle operations, a common follow-up is: what causes the most shuffles?

A: `groupBy`, `join`, `distinct`, `repartition`, and window functions with `PARTITION BY`. Anytime Spark needs all rows with the same key on the same executor.

---

Q: What are the key takeaways when asked about shuffle operations?

A:
- Say: shuffles redistribute data across executors. They're the most expensive operation in Spark.
- Broadcast join: send the small table to all executors. No shuffle needed.
- Check the Spark UI for shuffle read/write metrics to find bottlenecks.

---

Q: When you hear these in an interview, what concept is being tested?
- Your Spark job is running out of memory. What do you check?
- How does Spark use memory?
- What does spill-to-disk mean?

A: Memory management

---

Q: Your Spark job is running out of memory. What do you check?
- How does Spark use memory?
- What does spill-to-disk mean?

A: Each executor gets a fixed amount of memory, split between storage (caching data) and execution (shuffles, joins, sorts). When execution memory runs out, Spark spills data to disk, which is much slower. When the disk fills up too, the job fails with an out-of-memory error. The fix depends on the cause: too few partitions means each one is too large, so repartition to create smaller chunks. Too much data cached means storage is crowding out execution, so unpersist unused caches. Memory splits into storage and execution. Spill to disk is the warning sign.

---

Q: For each symptom related to memory management issues, what is the likely cause and fix?

A:
| Symptom | Likely cause | Fix |
|---|---|---|
| Spill to disk | Partitions too large | Increase partition count with `repartition()` |
| OOM on executor | Single partition too large (data skew) | Salting or isolating the large key |
| OOM on driver | Calling `.collect()` on large dataset | Never `collect()` large data. Use `.write()` instead. |
| Slow joins | Not enough memory for hash table | Broadcast the smaller table or increase executor memory |

---

Q: When asked about memory management, a common follow-up is: how do you decide how much memory to give each executor?

A: Start with the cluster default (usually 4-8GB). If you see spill-to-disk, increase memory or increase the number of partitions to make each one smaller. The 5-core rule: 5 cores per executor, each processing a partition.

---

Q: When asked about memory management, a common follow-up is: what is the difference between `persist()` and `cache()`?

A: `cache()` stores data in memory only. `persist()` lets you choose the storage level: memory-only, memory-and-disk, or disk-only. Use `persist(MEMORY_AND_DISK)` for safety.

---

Q: When asked about memory management, a common follow-up is: when should you cache data?

A: Only when the same dataset is used in multiple actions. Caching a dataset used once wastes memory. Caching a dataset used in 5 different joins saves 4 re-computations.

---

Q: What are the key takeaways when asked about memory management?

A:
- Say: executor memory splits between storage and execution. Spill to disk means partitions are too large.
- OOM on driver = you called `.collect()` on too much data. Never collect large datasets.
- Cache only when the same data is reused. Caching once-used data wastes memory.

---

Q: When you hear these in an interview, this is the concept being tested:
- "Your data lake has millions of small files. What is the impact?"
- "What is the optimal file size for Parquet?"
- "coalesce vs repartition: what is the difference?"

A: Small files problem

---

Q: "Your data lake has millions of small files. What is the impact?"
- "What is the optimal file size for Parquet?"
- "coalesce vs repartition: what is the difference?"

A: Too many small files kill read performance. Each file requires a separate metadata lookup, a separate file open, and a separate read request. Thousands of 1KB files are far slower to read than one 128MB file with the same data. The target file size is 128MB to 256MB. To fix small files, I use `coalesce()` to reduce the number of output partitions before writing, or run a compaction job that rewrites small files into larger ones. Target size, the problem, and two fixes.

---

Q: Compare `coalesce()` and `repartition()`: how each handles shuffling, partition count, cost, and when to use each.

A:
| | `coalesce()` | `repartition()` |
|---|---|---|
| Shuffle | Reduces partitions without a shuffle | Redistributes data evenly with a shuffle |
| Partition count | Can only reduce, never increase | Can increase or decrease partition count |
| Merging | Merges partitions on the same executor | More expensive due to the shuffle |
| Use when | Reducing output files after filter | Fixing skewed partitions or increasing parallelism |

---

Q: When asked about the small files problem, a common follow-up is: what causes small files?

A: Over-partitioning (too many partition columns), streaming jobs writing one file per micro-batch, or a Spark job with too many output partitions.

---

Q: When asked about the small files problem, a common follow-up is: how do you fix small files that already exist?

A: Run a compaction job: read the small files, coalesce, and write back. Delta Lake and Iceberg have built-in OPTIMIZE commands that do this automatically.

---

Q: When asked about the small files problem, a common follow-up is: is there a too-large file problem?

A: Yes. Files over 1GB are hard to process in parallel because each file is processed by one task. Aim for 128-256MB so each task finishes quickly.

---

Q: What are the key takeaways when asked about the small files problem?

A:
- Say: target 128-256MB per file. Use `coalesce()` to reduce output files. Run compaction for existing small files.
- `coalesce()` is free (no shuffle). `repartition()` is expensive (full shuffle). Pick accordingly.
- Delta Lake OPTIMIZE and Iceberg compaction handle small files automatically.
