---
name: import-lesson-cards
description: Convert a pasted datadriven.io lesson page into flashcard source. Use when the user pastes a lesson page containing "Interview Trigger Phrases" / "What They Want to Hear" / "The Curveball Follow-ups" / "KEY TAKEAWAYS" sections, or says "import this lesson" / "turn this lesson into cards".
---

# Import lesson cards

Datadriven.io lesson pages repeat one layout per concept **section**. This
skill turns that layout into card source, gets it approved, then writes it
both to `cards_source/` and into the app via the `study-helper` MCP tools.
Never call `add_card` before the user has approved the drafted markdown —
that's the one hard gate in this flow.

## Card source convention

One pair per card:

```
Q: <question>

A: <answer>

---
```

Blank line between Q and A, `---` on its own line between cards, no `---`
after the last card. When a front is a bullet list, `Q:` sits alone on its
line and every bullet takes a `- ` below it:

```
Q:
- "Why Parquet over CSV?"
- "Row-oriented vs columnar: explain the difference."
```

## Steps

### 1. Find the sections

A section is any heading immediately followed by an "Interview Trigger
Phrases" block. Identify a section's parts by **position**, not by heading
text: the prose block right after the trigger phrases is the **mechanism
block**, whatever it is titled ("What They Want to Hear", "The 10-Second
Answer", anything else). Everything else on the page is noise — skip it
without asking:

- The intro paragraph and "What you will be able to do" bullets
- "Daily Life / Interviews" tab labels
- "What to Whiteboard" and any diagram/architecture-widget text (Source →
  Executor 1/2/3 labels, etc.) — these are visual, not Q&A content
- "Fun fact" headers with no body
- Every "PIPELINE ARCHITECTURE / TASK ... Your work / Solution / Submit"
  challenge block
- The page-bottom "PUTTING IT ALL TOGETHER" recap and its aggregate "KEY
  TAKEAWAYS" (this duplicates the per-section takeaways you already draft —
  do not also card it)
- The "Lesson Sections" index and metadata block (Category/Difficulty/
  Duration/Challenges/Topics covered) near the bottom — SEO duplicate of the
  header
- Nav/footer chrome (Home/Learn/Help/Contact/Privacy/Terms/Security, ←
  Prev/Next pagination, NOTES/ASK AI)

Section done: you have the ordered list of section headings (e.g. "Spark
Execution Model", "Distributed Primitives") with their four live blocks —
Interview Trigger Phrases, the mechanism block, The Curveball Follow-ups,
KEY TAKEAWAYS — and, when present, one comparison/data table or code block
between the mechanism block and "The Curveball Follow-ups".

### 2. Draft cards per section, in page order

Each section yields the same fixed run of cards. Follow it exactly — the
duplication in steps 1 and 2 below is intentional, not a bug to fix:

1. **Trigger card.** Front: "When you hear these in an interview, what
   concept is being tested?" plus the Interview Trigger Phrases bullets
   verbatim (quotes included). Back: the section heading, lightly cased to
   read as a label (e.g. "Spark Execution Model" → "Spark execution model").

2. **Mechanism card.** Front: the *same* trigger-phrase bullets, this time
   with no intro sentence — just the bullets. Back: the mechanism block's
   paragraph, word for word, dropping only the outer quote marks.

3. **Table or code card(s)**, if the section has a table, comparison grid,
   or code block between the mechanism block and "The Curveball
   Follow-ups". See `TABLE-AND-CODE-PATTERNS.md` for how to turn each shape
   into a card — load it now if this section has one.

4. **One follow-up card per Curveball bullet.** Front: "When asked about
   [section topic, lowercased], a common follow-up is: [the quoted
   question]?" Back: the answer text verbatim, with inline code like
   `.collect()` or `groupBy` wrapped in backticks.

5. **Takeaways card.** Front: "What are the key takeaways when asked about
   [section topic, lowercased]?" Back: the KEY TAKEAWAYS bullets verbatim —
   prefix the first bullet with "Say:" only if the source text already
   starts that bullet with "Say:".

Carry transcription fixes a human dictation pass would also make: expand
`.filter()`-style calls in backticks, keep code terms exact, fix nothing else
— the source text is already clean prose, so no ASR cleanup is needed here.

Once every section is drafted, concatenate all of it into one piece of card
source in the convention above, in page order.

### 3. Show and get approval

Post the full drafted markdown in a code block in chat. If any card's answer
is itself a fenced code block (see the code-block pattern in step 2.3), wrap
the whole draft in a longer fence — ` ~~~~ ` or 4+ backticks — so the card's
own 3-backtick fence can't prematurely close the outer one and corrupt
everything after it.

Treat this as a draft, not a done deal — stop and wait for the user to
confirm or edit it. Apply any requested edits and re-show before moving on.
Do not proceed to step 4 on an implicit go-ahead; get an explicit one.

### 4. Find or create the destination set

Derive the folder from the page title, which is `<Topic>: <Difficulty>`
(e.g. "Distributed Compute: Beginner"):

- slug = snake_case(Topic) → `distributed_compute`
- prefix = first 3 letters of Difficulty, lowercased, + `__` → `beg__`,
  `int__`, `adv__`
- archive path = `cards_source/interview/<slug>/<prefix><slug>.md`
- set name = slug (e.g. `distributed_compute`)

Use `list_groups` / `list_sets` to find an existing `interview` group/`slug`
set; if neither exists, create the group with `create_group` then the set
with `create_set`.

### 5. Write the archive file

Save the approved markdown to the archive path above — this is the
human-readable record, independent of the app. If a file already exists
there, append to it rather than overwriting.

### 6. Push to the app

Call `add_card` once per pair with the approved `set_id`, `front` =
question, `back` = answer.

### 7. Report

State how many cards were added, to which set, and the archive path, so the
user can go check the UI.
