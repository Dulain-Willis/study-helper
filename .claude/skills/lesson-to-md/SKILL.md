---
name: lesson-to-md
description: Convert a datadriven.io lesson into a local Markdown preview with tools/lesson_to_md.py, and extend the converter when it prints "warning: no rule for block type". Use when the user names a datadriven lesson slug or URL to convert, asks for a lesson as Markdown or with its diagrams, or pastes a "no rule for block type" warning.
---

# Lesson to Markdown

`tools/lesson_to_md.py` turns a lesson page into `lesson_preview/<slug>.md`, plus 2x diagram screenshots in `lesson_preview/images/`. Your work is the converter and the checks. The user runs the converter on lessons and pushes the result, so hand them the command and verify by structure.

## Steps

### 1. Confirm the slug

Slugs look like `pipeline_<topic>_<level>`. Confirm the page exists:

```
curl -s -o /dev/null -w "%{http_code}" https://datadriven.io/learn/<slug>
```

Done when the status is 200. On 404, try neighbouring spellings of the topic and level.

### 2. Hand over the command

Give the user one `!` line that runs every lesson to refresh, then commits and pushes:

```
! cd ~/projects/study-helper && .venv/bin/python tools/lesson_to_md.py <slug> --screenshots && git add tools/lesson_to_md.py lesson_preview && git commit -m "<message>" && git push origin main
```

Loop over all slugs already in `lesson_preview/` when the change touches shared parsing (indentation, escaping, spacing). A new lesson alone needs only its own slug.

### 3. Read the run's output

The user pastes the output back. Every `warning: no rule for block type 'x'` line goes to **A block type we haven't seen** below. A clean run goes to step 4.

### 4. Verify by counts

Check the file by structure, so lesson text stays out of the chat:

- Heading counts: `##` sections and `###` mini headers against the page's block counts.
- Every `![...](images/...)` points to a file that exists; `not captured` appears 0 times.
- Escaped underscores (`\_`) 0; stray lone `//` lines 0.
- Indented code lines inside fences, where the lesson has Python.

Done when every check has a number beside it. Name each failure with its count.

### 5. Report

Say what passed, then what only the user can judge: GitHub rendering, spacing, whether each image sits beside the right text.

## A block type we haven't seen

The converter prints the warning and falls back to generic conversion, so output exists but may be flattened.

1. Save the page: `curl -s -o <scratchpad>/page.html <url>`. Print the block's HTML skeleton: styles and classes stripped, text cut to about 20 characters.
2. Classify it:
   - **Real tags** (`ul`, `li`, `h4`, `p`, `table`): markdownify already renders it. Add the type to `GENERIC_BLOCKS`.
   - **Text present, structure lost** (divs with canvas or SVG bullets, stat cards, boxes): write a rule in `tools/lesson_to_md.py` that rewrites the block into plain tags before markdownify, and add the type to `CUSTOM_BLOCKS`.
   - **Visual** (boxes drawn from divs and SVG, no readable text structure): add it to `DIAGRAM_LABELS` so it gets a screenshot.
3. Test with a snippet of your own that mirrors the skeleton, using made-up text. Then run the real block through `parse` and the new rule, printing counts only.
4. Hand the user the regenerate command from step 2.

Done when the snippet produces the intended Markdown and the lesson that raised the warning no longer prints it.

Past block types, their handling, and the cause behind each gotcha: [BLOCK-TYPES.md](BLOCK-TYPES.md). Read it before writing a rule, since a new type often resembles one already solved.
