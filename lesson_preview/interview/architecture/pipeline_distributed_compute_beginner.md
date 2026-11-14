Beginner·Pipeline Architecture · 20 min · 6 challenges

# Distributed Compute: Beginner

Ines is in the third round of a data engineering loop at Netflix. The interviewer shares a nightly job that rolls up a day of viewing events into watch time for each member and title in each country, and asks a single question: 'This used to finish in 40 minutes. Now it takes 3 hours and sometimes dies with an out-of-memory error, and the team reading its output says their queries crawl. Walk me through how it runs and where you would look.' Ines knows the DataFrame calls by heart. The interviewer wants the machinery under them: which process plans the work, how a folder of input files turns into tasks, which step moves rows across the network, why a single task can exhaust its memory, and why the output now holds 50,000 tiny files. This lesson builds that picture around the same job, with its real numbers.


<br>

### What you will be able to do

Describe how the driver works with the cluster manager to get executors, and how an action becomes a job made of stages, each made of tasks.

Classify any operation as narrow or wide and predict where the stage boundaries of a query fall.

Size a shuffle in bytes and decide when a broadcast join removes it.

Match a memory symptom (spill, executor OOM, driver OOM, container kill, skew) to the region of memory that ran out.

Explain why a job writes small files, and pick the fix: coalesce or repartition at write time, or compaction afterward.


<br>

---

## Spark Execution Model

Every question in this family opens with a version of the same request: describe what happens after the job is submitted. A complete answer names 3 kinds of process and 4 units of work, then walks a real job through them. The interviewer is checking whether you can reason about a failure from the architecture alone, before anyone opens a log, because in an incident the architecture tells you which log to open first.


<br>

### The 3 processes and what each one owns

Every application runs these

- **Driver** *1 per application*\
  Runs your program, turns DataFrame code into an optimized plan, cuts the plan into stages and tasks, hands tasks to executors and tracks which ones finished. It holds no table data unless the code pulls results back to it.
- **Cluster manager** *Grants resources*\
  Decides which machines host executors and how many cores and how much memory each gets. The driver requests executors at startup, or repeatedly as load changes when dynamic allocation is on, then sends tasks to them directly.
- **Executors** *Many per application*\
  Long-lived processes on worker machines. Each owns a fixed number of cores, runs 1 task per core at a time, keeps shuffle files on its local disk, and caches data when asked to.

![Pipeline diagram 1](images/pipeline_distributed_compute_beginner_diagram_1.png)


<br>

### From an action to 10,000 tasks

Nothing runs until the code asks for a result: writing a table, counting rows, collecting rows to the driver. Each such request is an action, and each action becomes 1 job. The driver cuts that job into stages wherever rows must regroup by key across machines, and runs each stage as a set of tasks, 1 per partition. A partition is the slice of data that a single task processes from start to finish; for a file scan it is usually a file, or a 128 MB piece of a larger one.

| Unit | Created by | In the nightly job |
| --- | --- | --- |
| Application | Submitting the program | 1 |
| Job | Each action; here, the final write | 1 |
| Stage | Each point where rows regroup by key | 2: scan, then aggregate |
| Task | Each partition of a stage | 10,000 in stage 1, 200 in stage 2 |

The nightly job reads 1.28 TB of viewing events stored as 10,000 files of 128 MB, so stage 1 has 10,000 tasks. The cluster runs 20 executors with 4 cores each, which makes 80 task slots. Tasks queue for those slots and run in waves, and each scan task here takes about 15 seconds.

- **125** — waves of tasks in stage 1
- **31 min** — stage 1 wall-clock time

Start any slow-job question with this arithmetic, because more executors cut wall-clock time only while a stage has more tasks than slots. Stage 2 has 200 tasks on 80 slots: 3 waves, and the third keeps only 40 slots busy. Tripling the cluster to 240 slots finishes stage 2 in a single wave with 40 slots idle. For stage 2, every executor added beyond that costs money and saves no time, though stage 1's 10,000 tasks would still speed up.


<br>

### What happens when a task fails

The driver records how every partition was derived from its inputs, a chain called lineage, and that record is what makes failures recoverable. When a task fails, the driver schedules it again, possibly on another executor, up to spark.task.maxFailures attempts, 4 by default, before it fails the whole job. Losing an executor costs more: its local shuffle files vanish with it, so tasks in the next stage that try to fetch them report a fetch failure, and the driver reruns the upstream tasks that produced the lost files.


> Where a rerun can hurt you
>
> - A rerun task writes its output again. The write path must commit each task's files exactly once, or retries leave duplicate files.
> - Speculative execution starts a second copy of a straggling task; only 1 copy's output may be committed.
> - Side effects inside a task, such as posting each row to an API, happen again on every attempt.
> - A dead driver ends the application. Executors are replaceable; the driver holds the plan and every task's state, so it has no stand-in.

✓Claim heard in interviews

- The code runs line by line as written
- More executors always make a job faster
- A failed task fails the job
- The driver processes the rows

✗What the system does

- Nothing runs until an action, and then the whole plan is optimized together
- Only while a stage has more tasks than slots
- The task gets up to 4 attempts first
- The driver plans and schedules; executors read and transform the rows

> **TIP**
>
> Order the answer the way the system runs: the driver builds the plan, the cluster manager grants executors, the action becomes a job, and shuffles cut it into stages. Each partition becomes a task, and the tasks run in waves on the available slots. Close with 1 failure and how the driver recovers from it.


<br>

---

## Distributed Primitives

This family of questions arrives as a code snippet and a prompt: how many stages will this run, and why? The answer comes down to 2 distinctions. Transformations versus actions decides when work happens. Narrow versus wide decides whether rows move between machines, and moving rows accounts for nearly all of the cost.


<br>

### Transformations describe, actions execute

filter is a transformation, as are select and groupBy: each returns a new DataFrame and computes nothing, and the driver appends a step to the plan. Actions like write demand a result, and so do count and collect; only then does the driver optimize the full plan and launch a job. Deferring the work pays off because the optimizer sees every step together: a filter written after a join can move before it, and columns no later step reads are never loaded from disk.

```sql
events = spark.read.load("/lake/viewing_events/dt=2026-10-22/")
watched = (
    events
    .filter(events.event_type == "play")
    .select("profile_id", "title_id", "country", "minutes")
)
daily = watched.groupBy("profile_id", "title_id", "country").sum("minutes")
daily.write.partitionBy("country").mode("overwrite").save("/lake/daily_watch_time/dt=2026-10-22/")
```


<br>

### Narrow or wide: does a row have to move?

A transformation is narrow when each input partition feeds at most 1 output partition. filter on partition 37 needs nothing but partition 37, so the task that read it applies the filter where the rows already sit, and a chain of narrow steps runs inside that task without pausing. A transformation is wide when the rows of an input partition scatter to many output partitions. Summing minutes for each profile and title within a country needs every row for that combination in the same place, and those rows are spread across all 10,000 input files, so they must be regrouped by key. That regrouping is a shuffle, and every shuffle ends a stage and begins the next.

| Operation | Narrow or wide | Why |
| --- | --- | --- |
| `filter`, `select`, `withColumn` | Narrow | Each row is handled in the partition it already sits in |
| `union` | Narrow | Input partitions pass through side by side |
| `coalesce(n)` | Narrow | Whole input partitions merge into fewer outputs without scattering |
| `groupBy` then `agg` | Wide | Rows sharing a key must meet in a single partition |
| `join`, both sides large | Wide | Matching keys from both tables must land together |
| `distinct` | Wide | Duplicates can sit in any partition |
| `orderBy` | Wide | A global order spans every partition |
| `repartition(n)` | Wide | Every input partition scatters rows to every output |

In SQL the rule reads off the clauses. WHERE and the expressions in the SELECT list are narrow. JOIN, GROUP BY, DISTINCT and ORDER BY are wide, unless the planner broadcasts the small side of a join, which the next section covers. A window function with PARTITION BY is wide too.


<br>

### Predict the stages

> Try it before reading on
>
> This query runs as 1 job, and the join moves both tables across the network. Count its stages and name the clause that causes each boundary.

```sql
SELECT
  e.country,
  t.genre,
  SUM(e.minutes) AS minutes_watched
FROM viewing_events AS e
JOIN titles AS t
  ON e.title_id = t.title_id
WHERE e.event_type = 'play'
GROUP BY e.country, t.genre
```

The query runs in 4 stages. Stage 1 scans viewing_events, applies the WHERE filter inside the scan tasks, and writes rows out keyed by title_id. Stage 2 does the same for titles, and the 2 scans run at the same time because neither needs the other. Stage 3 joins the 2 shuffled outputs and starts summing, but the join key is title_id while the grouping key is country and genre, so the partial sums shuffle again. Stage 4 finishes the aggregate. Notice that the filter added no stage and no network traffic: it ran before any row moved.


<br>

### Why the distinction matters on retry

Lineage makes narrow failures cheap. If the stage 1 task for partition 37 dies, the rerun reads the same input file and repeats the same filter: 1 file, about 15 seconds of work. A wide step has no such clean edge. One partition of the aggregate depends on shuffle files written by all 10,000 scan tasks, and if an executor holding some of those files is gone, those scan tasks run again before the aggregate can retry.

✓A chain of narrow steps

- Runs inside a single task, with no disk write or network hop between steps
- A failure reruns 1 partition from its source file
- Partition count stays whatever the input produced

✗A wide step

- Ends a stage, and the next stage waits for every upstream task
- A lost executor can force upstream tasks to rerun
- Partition count resets to the shuffle setting, 200 by default

*Each action is its own job. Calling count and then write on the same DataFrame scans the input twice, unless the DataFrame is cached before the first action.*

> **TIP**
>
> When asked to speed up a job, count the wide steps first. Removing 1 shuffle usually saves more than any tuning of the narrow steps around it.


<br>

---

## Shuffle Operations

Most slow-job questions end at a shuffle, because a shuffle holds rows in memory while it writes them to disk and sends them across the network, and no other step does all of that at once. A strong answer explains the mechanics, sizes the cost from the numbers in the job at hand, and points at the shuffle that can be removed.


<br>

### What a shuffle does, step by step

| Step | Side | What happens |
| --- | --- | --- |
| 1 | Map | Each task hashes the grouping key of every row and assigns the row to 1 of 200 target partitions. |
| 2 | Map | It sorts rows by target partition and writes 1 shuffle file to the executor's local disk, plus an index of where each partition starts. |
| 3 | Map | It reports the file's location to the driver. The next stage starts only after every map task has reported. |
| 4 | Reduce | Each of the 200 reduce tasks fetches its slice from every map output over the network. |
| 5 | Reduce | It deserializes and merges the slices, then aggregates or joins, spilling to disk if the rows exceed its memory. |

Every shuffled byte is serialized and written to local disk, then sent over the network, where the reading task loads it back and deserializes it. The cost scales with bytes, and the bytes can be estimated. The nightly job filters to plays and keeps 4 columns, which shrinks 1.28 TB of input to 640 GB that must be regrouped by profile and title within each country. Spark also sums minutes inside each map task before writing, which shrinks the data when a task holds many rows for the same key; a profile rarely repeats the same title many times inside a single 128 MB file, so here that step saves little.

- **2,000,000** — shuffle blocks fetched in stage 2
- **3.2 GB** — input per reduce task

Step 3 creates a barrier. The 200 reduce tasks wait for the slowest of the 10,000 map tasks, so a single straggling scan task delays everything after it. Shuffle files also outlive the tasks that wrote them on purpose: they stay on local disk so a failed reduce task can fetch them again without rerunning the map side.


<br>

### Broadcast joins remove the big shuffle

The genre report from the previous section joins the filtered events to titles, and a shuffle join moves both tables so that equal title_id values meet in the same task. When a side is small, a cheaper plan exists. The driver collects the small table and ships a full copy to every executor, and each scan task joins its slice of the big table against that local copy as it reads. The big table never leaves the task that read it, so the join becomes a narrow step inside the scan stage.

Shuffle join

| Step | Data moved |
| --- | --- |
| Shuffle filtered events by title_id | 640 GB |
| Shuffle titles by title_id | 40 MB |
| Stages for the join | 3 |

Broadcast join

| Step | Data moved |
| --- | --- |
| Collect titles to the driver | 40 MB |
| Copy titles to 20 executors | 800 MB |
| Stages for the join | 1 |

Spark picks a broadcast join on its own when it estimates a side to be below spark.sql.autoBroadcastJoinThreshold, 10 MB by default. The titles table is 40 MB, so the planner falls back to a sort-merge join, the default shuffle join, unless you raise the threshold or mark the table with a broadcast hint. The trade is lopsided here: 20 copies of 40 MB put 800 MB on the network, against 640 GB for the shuffle.

```sql
from pyspark.sql.functions import broadcast

enriched = watched.join(broadcast(titles), "title_id")
```

> When a broadcast backfires
>
> - The driver builds the broadcast table in its own memory first, so a table larger than the driver heap kills the application.
> - Every executor holds a full copy: 2 GB broadcast to 20 executors occupies 40 GB of cluster memory.
> - Size estimates come from files on disk, and compressed columnar files can grow several times once loaded as rows.
> - Tables grow. A hard-coded hint keeps broadcasting a dimension long after it has outgrown the executors.


<br>

### Other shuffles worth removing

- **Grouping twice on the same key.** Compute both results in a single aggregate, or the 640 GB moves twice.
- orderBy on a result that readers sort again anyway: a global sort is a full range shuffle.
- repartition called out of habit: every call is a full shuffle, so keep only the ones whose layout a later step uses.
- distinct before a GROUP BY on the same columns: the grouping already removes the duplicates.

*Adaptive query execution, on by default since version 3.2, measures shuffle outputs at runtime. It can switch a sort-merge join to a broadcast join once a side proves small, and it merges small shuffle partitions. Both happen after shuffle files were written, so a broadcast planned up front still saves the most work.*

> **TIP**
>
> State shuffles in bytes. 'This join shuffles 640 GB; broadcasting the 40 MB side moves 800 MB instead' gives the interviewer a number to check, and it shows you sized the problem before proposing a fix.


<br>

---

## Memory Management

Memory questions arrive as symptoms: a stage that got slow, an executor that keeps dying, an error message pasted into the chat. The skill under test is matching each symptom to the region of memory that ran out, and that takes a picture of how an executor divides the memory it was given.


<br>

### How a 16 GB executor divides its memory

| Region | Size on a 16 GB executor | What it holds |
| --- | --- | --- |
| Reserved | 300 MB | Fixed memory for the engine's own internals |
| Execution | Shares 9.4 GB with storage | Shuffle buffers, plus the sort and hash structures behind joins and aggregates |
| Storage | Shares 9.4 GB with execution | Cached DataFrames and broadcast tables |
| User | 6.3 GB | Objects your own code creates, such as data structures inside a UDF |
| Overhead, outside the heap | 1.6 GB | Network buffers, native memory and Python worker processes |

The split comes from 2 settings. spark.memory.fraction, 0.6 by default, gives 60% of the heap left after the reserved 300 MB to a unified region, roughly 9.4 GB here, which execution and storage share. Execution can evict cached blocks to grow, down to the half that spark.memory.storageFraction protects, while cached blocks can never evict running execution memory. Inside execution, each running task may take at most an equal share, so with 4 tasks busy on 4 cores, each task gets roughly 2.4 GB before it has to spill.


<br>

### Read the symptom, then the region

| Symptom | Region that ran out | Usual cause | First fix |
| --- | --- | --- | --- |
| Stage slows; stage metrics show spill (memory) and spill (disk) | Execution share per task | Shuffle partitions larger than the 2.4 GB per-task share | Raise `spark.sql.shuffle.partitions` |
| Executor lost with `OutOfMemoryError: Java heap space` | Executor heap | A skewed key, or a broadcast too large for every executor | Find the hot key; drop the broadcast |
| Container killed by the cluster manager for exceeding memory limits | Overhead outside the heap | Python UDF workers or native buffers | Raise `spark.executor.memoryOverhead`, leaving the heap as it is |
| Driver fails with `OutOfMemoryError` | Driver heap | `collect()` on a large result, or building a big broadcast | Write results to storage instead |
| Long pauses and high GC time | Executor heap | Millions of short-lived objects, or too much cached data | Cache less; prefer built-in functions over UDFs |

Spill is the slow but safe outcome. When the hash table a task builds for the aggregate reaches its 2.4 GB share, the task sorts what it holds and writes it to local disk. With that memory freed it keeps reading, and at the end it merges the spilled runs with what remains in memory. With 200 shuffle partitions, every reduce task in the nightly job receives 3.2 GB, so every one of them spills, and each spilled byte is written and read back at least once more. The stage metrics report spill (memory), the deserialized size of what was evicted, and spill (disk), its compressed size on disk, usually far smaller.

More partitions is the fix, and the target size follows from the share. Divide the shuffle bytes by a target of a few hundred megabytes and set spark.sql.shuffle.partitions to the result. Deserialized rows plus hash table structures can take several times their serialized size, so a partition well under the share leaves the headroom it needs.

- **1,000** — shuffle partitions after the fix
- **640 MB** — input per reduce task after the fix


<br>

### Skew: when more partitions stop helping

More partitions divide the keys among more tasks, yet every row for a given key still lands in a single task. Suppose the job also joins events to a large profiles table on profile_id, and a test profile driven by an automated player logs 30 GB of play events in a day. At 200 partitions or 1,000, the task that receives that profile_id gets all 30 GB, spills for its whole run or dies with a heap error, while the other 999 tasks finish in seconds. The stage view gives it away: the maximum task input sits far above the median. If the key is junk, filter it out before the shuffle. If it is real, add a random suffix from 0 to 9 to that key on the events side, copy the matching profile row 10 times with each suffix, and join on the salted key. Adaptive execution can split oversized join partitions on its own; salting is the manual version of the same idea.


<br>

### Fix the region that ran out

✓Do

- Read spill and the largest task's input in the stage view before changing any setting
- Size shuffle partitions from shuffle bytes divided by a target of a few hundred MB
- Raise memory overhead when the cluster manager kills containers for exceeding limits
- Compare the largest task to the median when 1 executor keeps dying

✗Don't

- Double executor memory as a first move; it hides skew and doubles the bill
- Call collect() on a result whose size you have not checked
- Cache a DataFrame that is read only once; it takes memory execution could use
- Broadcast a table without checking its size once loaded as rows

> **TIP**
>
> Use the exact vocabulary: spill, executor OOM, driver OOM, container killed for overhead, skew. Each names a different region of memory, and naming the right one tells the interviewer you diagnosed the failure from evidence.


<br>

---

## Small File Problem

After the partition fix, the nightly job finishes in 40 minutes again. A week later the team that reads its output complains that their queries got slower, and a listing of the output folder for a single day shows 50,000 files averaging 42 KB. The interview version asks why that happens and how to fix it, and each fix has a different price.


<br>

### Where the files come from

Every task writes its own files, 1 for each output partition value among the rows it holds. The job writes with partitionBy("country"), and the shuffle hashed rows on the full grouping key, profile and title as well as country, so each of the 1,000 reduce tasks holds rows from all 50 countries. The file count is the product: 1,000 tasks times 50 countries is 50,000 files for 2 GB of output. Before the fix, 200 tasks wrote 10,000 files of about 200 KB, already too small for readers; the memory fix multiplied the count by 5, and that is how the interviewer's 50,000 files arrive.


<br>

### What small files cost readers

- Every file costs a listing entry, an open request and a footer read before its first row comes back.
- Readers plan work per file or per packed group of files, so 50,000 files mean many short read tasks.
- Columnar files keep min and max statistics in each footer, so tiny files hand the reader thousands of footers to check.
- Compression and encoding find fewer repeated values in 42 KB of rows than in 128 MB.
- Any catalog or metadata service that tracks individual files slows down as the count reaches millions.

Spark's file reader limits the damage by packing small files into shared read tasks. It fills each task up to spark.sql.files.maxPartitionBytes, 128 MB by default, but charges every file an extra spark.sql.files.openCostInBytes, 4 MB, to account for the cost of opening it. Each 42 KB file therefore counts as about 4 MB, and about 32 of them fill a task.

- **1,563** — read tasks for 50,000 small files
- **200 GB** — size the planner charges for 2 GB


<br>

### Fix 1: control the file count at write time

Take an output with no partition column first. The 1,000 aggregate tasks would write 1,000 files of about 2 MB, and the instinct is to reduce the task count right before the write. The 2 methods for that look alike in code and behave very differently on the cluster.

✓coalesce(16)

- Narrow: merges whole partitions in place, with no shuffle
- Adds no stage boundary, so the aggregate stage itself shrinks to 16 tasks
- Each of those 16 tasks then reads 40 GB of shuffle data and spills heavily
- Fits when the step before the write is a scan or filter of modest size

✗repartition(16)

- Wide: adds a shuffle of the 2 GB result
- The aggregate keeps its 1,000 tasks; only the final write runs as 16
- Rows spread evenly, giving 16 files of roughly 128 MB
- Fits after a large shuffle, which is this job's case

With partitionBy("country") the file count is tasks times countries per task, so the fix is to make each task hold fewer countries. Repartitioning on country sends every row for a country to the same task, so each country folder receives 1 file: 50 files for the day, averaging 41 MB. The biggest country then lands on a single task, and the maxRecordsPerFile option caps rows per file so that task writes several files instead of a giant one.

```sql
(
    daily
    .repartition("country")
    .write
    .partitionBy("country")
    .option("maxRecordsPerFile", 5000000)
    .mode("overwrite")
    .save("/lake/daily_watch_time/dt=2026-10-22/")
)
```


<br>

### Fix 2: compact after the fact

Write-time control fails when files arrive through many small writes. The viewing events table that feeds this job is filled by a stream that commits every minute, 1,440 commits a day, and each commit adds files. Late events add more: a play watched offline and uploaded 3 days later lands in a date partition that was finished long ago. The tidy 128 MB input files from the first section exist because a compaction job rereads each partition's small files and rewrites them as a few large ones.

![Pipeline diagram 2](images/pipeline_distributed_compute_beginner_diagram_2.png)

> Where compaction breaks
>
> - Retry: a job that deletes old files before committing new ones loses data if it dies midway. Write the new files, then swap in 1 atomic step.
> - Concurrent readers: a query listing the folder during the rewrite can see old and new files together and double count rows.
> - Late data: compacting only yesterday misses old partitions that late events reopened; track which partitions changed since the last run.
> - Schema change: after a column is added, the older files lack it, so the rewrite must read both versions and write a single target schema.

> **TIP**
>
> Answer in this order: name the product of writing tasks and partition values, fix the write with repartition rather than coalesce after a big shuffle, then add atomic compaction for streams and late data.


<br>

---

## Putting It All Together

A streaming service runs a nightly job that rolls 1.28 TB of viewing events into daily watch time per profile, title and country, partitioned by country, plus a genre report that joins those events to a 40 MB titles table. Over a few months the job slowed from 40 minutes to 3 hours, executors began to die, and the dashboards reading its output got slower every week.

- The execution model sets the baseline: 10,000 scan tasks run in 125 waves on 80 slots, then 200 tasks aggregate.
- Narrow versus wide locates the cost: the filter and projection ride inside the scan, and the groupBy is the shuffle that matters.
- The shuffle explains the memory failure: 640 GB over 200 partitions is 3.2 GB per task against a 2.4 GB share, so every task spills.
- Broadcasting the 40 MB titles table replaces the genre report's 640 GB join shuffle with 800 MB of copies.
- The memory fix creates the file problem: 1,000 tasks times 50 countries writes 50,000 files of 42 KB.
- Repartitioning on country before the write cuts that to 50 files, and atomic compaction absorbs the stream's minute commits and late events.


<br>

---

## Key Takeaways

- The driver plans and schedules while executors run 1 task per core; every action is a job, every shuffle a stage boundary, every partition a task.
- Narrow steps run in place and retry from a single partition; a wide step shuffles at a stage boundary, and a retry there can force upstream tasks to rerun.
- State a shuffle's cost in bytes, and broadcast the small side of a join when a full copy fits comfortably on every executor.
- Map each memory symptom to its region: spill to the per-task execution share, container kills to overhead, driver OOM to collect or broadcast, a single huge task to skew.
- Output files equal writing tasks times partition values per task; control that at write time, and compact atomically for streams and late data.
