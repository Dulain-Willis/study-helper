---
name: draft-cards
description: Turn a rambled dump of flashcard content into Q/A card source, show it for approval, then push it into study-helper via MCP. Use when the user rambles study material meant to become flashcards, says "draft cards" / "make flashcards" / "turn this into cards", or pastes raw Q&A content for the study-helper app.
---

# Draft cards

Convert ramble into reviewable card source, get approval, then write it both to
`cards_source/` and into the app via the `study-helper` MCP tools. Never call
`add_card` before the user has approved the drafted markdown — that's the one
hard gate in this flow.

## Card source convention

One pair per card:

```
Q: <question>

A: <answer>

---
```

Blank line between Q and A, `---` on its own line between cards, no `---` after
the last card. Split any distinct fact into its own card rather than folding
several facts into one answer — a ramble covering five facts is five cards, not
one.

## Steps

1. **Draft.** Read the user's ramble and turn it into Q/A pairs in the format
   above, preserving their phrasing and voice rather than smoothing it into
   generic textbook prose. Don't invent facts the ramble didn't contain.

2. **Show and get approval.** Post the full drafted markdown in a code block in
   chat. Treat this as a draft, not a done deal — stop and wait for the user to
   confirm or edit it. Apply any requested edits and re-show before moving on.
   Do not proceed to step 3 on an implicit go-ahead; get an explicit one.

3. **Find or create the destination set.** Ask which set this belongs to if
   it's not obvious from context. Use `list_groups` / `list_sets` to find an
   existing group/set by name; if neither exists, create the group with
   `create_group` then the set with `create_set`.

4. **Write the archive file.** Save the approved markdown to
   `cards_source/<group>/<set>.md` (append to the existing file if one is
   already there for this set, otherwise create it) — this is the
   human-readable record, independent of the app.

5. **Push to the app.** Call `add_card` once per pair with the approved
   `set_id`, `front` = question, `back` = answer.

6. **Report.** State how many cards were added and to which set, so the user
   can go check the UI.
