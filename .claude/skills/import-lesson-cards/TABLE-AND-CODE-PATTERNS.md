---
name: table-and-code-patterns
description: Shapes a lesson-page table or code block can take and the card to draft for each.
---

# Table and code patterns

Reached from `SKILL.md` step 2.3, only when the current section has a table,
comparison grid, or code block between "What They Want to Hear" and "The
Curveball Follow-ups". Three shapes show up: a real table, a **broken pair**,
or a code block.

## Real data table

A plain table with a header row and several labeled rows — e.g. "The
Vocabulary to Use" (Term / What It Is / One-Liner for Interviews), or a
symptom/cause/fix table (Symptom / Likely Cause / Fix).

Write **one question that names every column**, built from the header row,
and answer it with the table reproduced as markdown.

> Source columns: `TERM | WHAT IT IS | ONE-LINER FOR INTERVIEWS`

```
Q: For each term, define what it is and give a one-liner for interviews.

A:
| Term | What it is | One-liner for interviews |
|---|---|---|
| Driver | The coordinator process | Plans the DAG and assigns tasks to executors |
...
```

The question phrasing mirrors the header row's own words — "for each
[column 1], give [column 2] and [column 3]" — so it reads like something a
person would actually ask, not a literal column dump. The rule is the same
regardless of what the columns are — a second worked example, with entirely
different columns, to make that explicit:

> Source columns: `WINDOW TYPE | BEHAVIOR | USE CASE`

```
Q: For each window type, describe its behavior and give a use case.

A:
| Window type | Behavior | Use case |
|---|---|---|
| Tumbling | Fixed size, no overlap (e.g., every hour) | Hourly revenue, daily active users |
| Sliding | Fixed size, overlaps (e.g., 1 hour window, 5 min slide) | Rolling averages, trending topics |
| Session | Dynamic size, grouped by activity gaps | User sessions, conversation threads |
```

## Broken-pair comparison (the missing-header bug)

Some sections render as two side-by-side bullet lists instead of a table —
e.g. "NARROW TRANSFORMATIONS" vs. a second list with **no visible label**, or
"COALESCE()" vs. a second list with no visible label. This is a rendering
bug on the source site, not a sign the right side has no content: the right
list's bullets are real, paired one-for-one with the left list's bullets —
only its header text failed to render.

Reconstruct the missing header from what the paired content is obviously
contrasting with the labeled side, then build one question that asks for
both sides across every row of contrast (not a column dump — name the actual
dimensions: how each handles X, Y, Z):

> Left side labeled `NARROW TRANSFORMATIONS`: `filter(), map(), select(),
> withColumn()` / each partition processed independently / no data moves
> between executors / fast: no network I/O
>
> Right side unlabeled: `groupBy(), join(), repartition(), distinct()` /
> data must move between executors / creates a shuffle / slow: the #1
> bottleneck
>
> → unlabeled side is clearly **WIDE TRANSFORMATIONS** (the paired opposite
> of narrow on every row)

```
Q: What is a narrow vs. wide transformation? Give examples of each and
describe data movement, cost, and speed.

A:
| | Examples | Data movement | Speed |
|---|---|---|---|
| Narrow | `filter()`, `map()`, `select()`, `withColumn()` | No data moves between executors | Fast: no network I/O |
| Wide | `groupBy()`, `join()`, `repartition()`, `distinct()` | Data must move between executors | Slow: the #1 performance bottleneck |
```

If you can't confidently infer the missing header from the paired content,
say so when you propose the draft rather than guessing silently — flag it as
a reconstructed label for the user to confirm, the same way you'd flag any
other ambiguity before writing.

## Code block

A code snippet sitting in the same slot as a table (after "What They Want to
Hear", before "The Curveball Follow-ups") demonstrates the concept the
section just described. Card it in that same position: front asks for the
implementation, back is the snippet verbatim in a fenced code block with no
language tag (the app renders plain code blocks, not syntax highlighting).

Always open the question with "Write the code to..." so it reads
unmistakably as a coding prompt, not a conceptual one. Name the topic from
the snippet's own leading comment when it has one; otherwise describe what
the snippet demonstrates.

> Snippet comment: `# Spark Structured Streaming: tumbling window with watermark`

~~~
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
~~~
