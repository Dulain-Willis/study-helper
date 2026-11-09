---
name: lesson-to-md
description: Convert a datadriven.io lesson to Markdown with tools/lesson_to_md.py, or add a rule when the converter prints "no rule for block type". Use when the user names a lesson slug or URL, or pastes that warning.
---

# Lesson to Markdown

`tools/lesson_to_md.py <slug> --screenshots` writes `lesson_preview/<slug>.md` and 2x diagram screenshots to `lesson_preview/images/`. The user runs it and pushes the result. You hand over the command, then verify by structure so lesson text stays out of the chat.

## Steps

### 1. Confirm the slug

Slugs look like `pipeline_<topic>_<level>`, with level beginner, intermediate or advanced.

```
curl -s -o /dev/null -w "%{http_code}" https://datadriven.io/learn/<slug>
```

Done when the status is 200.

### 2. Hand over the command

One `!` line: run the converter for each slug, then commit and push the files you changed.

```
! cd ~/projects/study-helper && for l in <slugs>; do .venv/bin/python tools/lesson_to_md.py $l --screenshots || break; done && git add tools/lesson_to_md.py lesson_preview <other changed paths> && git commit -m "<message>" && git push origin main
```

`<slugs>` is the new lesson alone, or every lesson the change affects: each lesson that printed a warning, or every lesson in `lesson_preview/` when the change touches shared parsing (indentation, escaping, spacing).

### 3. Read the run's output

The user pastes it back. Each `warning: no rule for block type 'x'` goes to **A block type we haven't seen**. A clean run goes to step 4.

### 4. Verify by counts

Run every check against every regenerated file. The two heading equations held on every lesson checked so far; a mismatch points at a new block type.

- `##` count = the page's `section_header` blocks + 2 (the synthesis and key takeaways headers).
- `###` count = the page's `h3` blocks + 1 ("What you will be able to do").
- Every `![...](images/...)` resolves to a file. `not captured` 0, escaped underscores `\_` 0, lone `//` lines 0.
- Python lessons: indented lines inside fences, above 0.
- `info_list` rows: each `li` with 2 or more text spans appears as a two-line bullet (`- **label** *tag*\` then the description).

Done when every check reports a number and each failure names its lesson and count.

### 5. Report

Say what passed, then what only the user can judge: GitHub rendering, spacing, whether each image sits beside the right text.

## A block type we haven't seen

The converter falls back to generic conversion, so output exists but may be flattened.

1. Save the page (`curl -s -o <scratchpad>/page.html <url>`) and print the block's HTML skeleton: styles and classes stripped, text cut to about 20 characters.
2. Classify it:
   - **Real tags** (`ul`, `li`, `h4`, `p`, `table`): markdownify already renders it. Add the type to `GENERIC_BLOCKS`.
   - **Text present, structure lost** (divs with canvas or SVG bullets, stat cards, boxes, spans fused together): write a rule in `tools/lesson_to_md.py` that rewrites the block into plain tags before markdownify, and add the type to `CUSTOM_BLOCKS`.
   - **Visual** (boxes drawn from divs and SVG, no readable text structure): add it to `DIAGRAM_LABELS` so it gets a screenshot.
3. Test with a snippet of your own that mirrors the skeleton, using made-up text. Then run the real block through `parse` and the new rule, printing counts only.
4. Hand over the step 2 command for every lesson that printed the warning.

Done when the snippet produces the intended Markdown and no regenerated lesson prints the warning.

Before writing a rule, read [BLOCK-TYPES.md](BLOCK-TYPES.md): the HTML shape and handling of every type solved so far. A new type often resembles one of them.
