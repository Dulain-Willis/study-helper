---
name: import-lesson-cards
description: Convert a pasted datadriven.io lesson page into flashcard source. Use when the user pastes a lesson page containing "Interview Trigger Phrases" / "What They Want to Hear" / "The Curveball Follow-ups" / "KEY TAKEAWAYS" sections, or says "import this lesson" / "turn this lesson into cards".
---

# Import lesson cards

Datadriven.io lesson pages repeat one layout per concept **section**. This
skill turns that layout into card source using the same Q/A convention as
`draft-cards`, then hands off to it for approval and write-out.

## 1. Find the sections

A section is any heading immediately followed by an "Interview Trigger
Phrases" block. Everything else on the page is noise — skip it without
asking:

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
Interview Trigger Phrases, What They Want to Hear, The Curveball Follow-ups,
KEY TAKEAWAYS — and, when present, one comparison/data table between "What
They Want to Hear" and "The Curveball Follow-ups".

## 2. Draft cards per section, in page order

Each section yields the same fixed run of cards. Follow it exactly — the
duplication in steps 1 and 2 below is intentional, not a bug to fix:

1. **Trigger card.** Front: "When you hear these in an interview, what
   concept is being tested?" plus the Interview Trigger Phrases bullets
   verbatim (quotes included). Back: the section heading, lightly cased to
   read as a label (e.g. "Spark Execution Model" → "Spark execution model").

2. **Mechanism card.** Front: the *same* trigger-phrase bullets, this time
   with no intro sentence — just the bullets. Back: the "What They Want to
   Hear" paragraph, word for word, dropping only the outer quote marks.

3. **Table card(s)**, if the section has a table or comparison grid between
   "What They Want to Hear" and "The Curveball Follow-ups". See
   `TABLE-PATTERNS.md` for how to turn each shape into a question + markdown
   table — load it now if this section has one.

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

## 3. Hand off

Once every section is drafted, you have one long piece of card source in the
standard

```
Q: <question>

A: <answer>

---
```

format. From here, **follow the `draft-cards` skill starting at its step 2**
(show the full draft in chat, get explicit approval, find/create the
group/set, write the archive file, optionally push via MCP) — do not
duplicate that logic here.

For the archive path, derive the folder from the page title, which is
`<Topic>: <Difficulty>` (e.g. "Distributed Compute: Beginner"):

- slug = snake_case(Topic) → `distributed_compute`
- prefix = first 3 letters of Difficulty, lowercased, + `__` → `beg__`,
  `int__`, `adv__`
- path = `cards_source/interview/<slug>/<prefix><slug>.md`

If a file already exists at that path, append — don't overwrite — matching
`draft-cards`' own rule.
