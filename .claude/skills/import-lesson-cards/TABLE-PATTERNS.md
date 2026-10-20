---
name: table-patterns
description: Shapes a lesson-page table can take and the card to draft for each.
---

# Table patterns

Reached from `SKILL.md` step 2.3, only when the current section has a table
or comparison grid. Two shapes show up; tell them apart by whether the page
gave you a real table or a **broken pair**.

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
person would actually ask, not a literal column dump.

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
