---
name: index-lesson
description: Write the finding-aid index for one lesson in lesson_preview/, headers and line ranges only.
disable-model-invocation: true
---

# Index a lesson

Write `lesson_preview/index/<lesson-stem>.md` for one lesson file. An agent answering data engineering questions reads this index, picks a section, then reads only that line range of the lesson. The index finds; the lesson teaches. Lessons stay untouched.

Worked example: `lesson_preview/index/pa_pattern_backpressure_scaling.md`.

## Steps

### 1. Read the lesson

Read the whole file. Done when every `##` and `###` heading has been seen.

### 2. Copy the lesson's structure

List the lesson's `##` and `###` headings in order, each with its line range. Skip boilerplate ("What you will be able to do"), recaps ("Key Takeaways"), and wrap-up scenarios ("Putting It All Together"). A `###` that sits before the first `##` is boilerplate and is skipped; a `####` stays inside its parent `###` range and gets no entry.

Ranges:
- A `##` range spans its whole section, children included, and ends the line before the next `##` heading (or before the first skipped heading after it).
- A `###` range starts at its heading and ends the line before the next heading of the same or higher level, trailing `<br>` and `---` included.

Done when the list holds every kept heading once, with ranges that leave no gap between sibling `###` entries.

### 3. Rewrite each `###` header

Replace the text of every `###` using the Header rules. Keep every `##` title, every range, and the entry order exactly as copied.

Done when `python3 scripts/validate_lesson_index.py <lesson-stem>` prints `OK` (it checks the Source line count, the `###` count, verbatim `##` titles, ranges, the ten-word limit and semicolons), and every header passes the **standalone** check: read with no lesson in sight, it names a general concept an agent could search for, with no "the example", no pronoun pointing back at the lesson, and no number taken from the lesson's scenario.

### 4. Write the file

```
# Index: <lesson-file>.md
Source: `lesson_preview/<lesson-file>.md` (<N> lines). Read just the line range you need (Read offset/limit).

## <Lesson ## title, verbatim> (L<start>-<end>)

### <Rewritten header> (L<start>-<end>)
```

Write each range as `(L27-114)`: one `L`, a hyphen, and a bare end number. `<N>` is the number `wc -l` prints for the lesson. List each `###` directly under its `##`, flush left. Entries are a header and a range.

### 5. Add the lesson to the top-level index

Add one line to `lesson_preview/INDEX.md`, in alphabetical position among the existing lines:

```
- `<lesson-stem>`: <what the lesson covers, as five or six topics separated by commas>
```

Name the lesson's own topics. When the lesson overlaps another lesson, end the line with a pointer such as "; for X see `<other-stem>`". Done when the validator prints `OK` (it checks that the line exists).

### 6. Report flags

End the reply with a **Flagged** list: each `##` title that is vague on its own (a bare category or a question that names no concept or decision), quoted. Leave it verbatim in the file, punctuation and quote marks included; the human decides.

## Header rules

**Main idea.** A section holding several ideas takes the header of the idea that fills most of its range; when two ideas tie, take the one that comes first.

**Claim or topic label.** Write a claim when the section's point finishes the sentence "This section shows that ...": "Changing partition count breaks per-key ordering". Write a topic label when it compares or lists options: "Four ways to handle excess load: block, buffer, shed, throttle".

**One idea per header.** A semicolon means two ideas; keep the dominant one.

**Name the knowledge.** The lesson's own structure ("Rephrasing of the load question") and interview coaching ("how to answer in interviews") describe the lesson. State the concept a data engineer would apply. Answer templates are knowledge and qualify.

**General, free of the scenario.** Test: if the lesson's example changed, would the header stay true? "Size partition count at 70% utilization" fails (70% is the example's choice); "How to size partition count from throughput target and headroom" passes.

**Search words stay.** Terms an agent would grep for (partition, lag, shed, autoscale, Little's Law) appear in the header.

**Ten words or fewer.** A hyphenated term counts as one word. Write full phrases, and drop articles only when the header would otherwise pass ten words.

## Examples from `pa_pattern_backpressure_scaling.md`

| Lesson heading | Index header | Why |
| --- | --- | --- |
| One question, many disguises | Load changes reduce to arrival rate, stage capacity, and duration | Claim naming the knowledge, not the lesson's framing |
| The running example: trip events | Per-stage capacity table finds the first break and next ceiling | Names the technique the example demonstrates |
| Scale on lag, not CPU | Autoscale IO-bound consumers on lag, not CPU | Claim with its condition |
| Sizing the partition count | How to size partition count from throughput target and headroom | Passes the scenario test; drops the example's 70% |
| Measuring it in production | Diagnosing capacity shortage vs. hot keys from per-partition lag | Main idea of a range that also holds a model answer |
