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
