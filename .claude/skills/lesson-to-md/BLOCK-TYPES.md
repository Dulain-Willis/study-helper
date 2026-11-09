# Block types and gotchas

The site builds a lesson from blocks, each tagged `data-note-block-type="<type>"`. The converter keys its rules on that tag. The code is the source of truth for what is handled; this file holds the reasons the code cannot show.

## Block types seen so far

| Type | Handling | Why |
|---|---|---|
| `h3`, `h4`, `body_text`, `rich_text`, `note_text`, `static_table`, `side_by_side_tables`, `comparison`, `info_list`, `rich_info_list`, `best_practices`, `learning_objectives`, `lesson_title`, `section_header` | generic markdownify | Real HTML tags; markdownify renders them as they stand |
| `static_code` | rule: code fence | Code arrives as a table, one row per line, code in the last cell |
| `big_number` | rule: bullets pairing figure and caption | Stat cards are two divs with no list semantics |
| `tip`, `callout`, `fun_fact` | rule: blockquote | The box is a styled div; `tip` already prints its own TIP label, so the rule bolds it |
| `key_takeaways`, `synthesis` | rule: `##` heading plus bullets | Bullets are drawn on a canvas, so the HTML holds plain divs and no `li` |
| `pipeline_diagram`, `schema_diagram` | screenshot | HTML boxes layered with SVG arrows; no text structure survives conversion |

## Gotchas and their causes

- **Indentation vanished from code.** The site indents with a whitespace-only `<span>` and CSS `white-space: pre`. BeautifulSoup collapses whitespace-only strings to one space outside `<pre>`. `parse()` keeps whitespace inside `td`.
- **Italics.** The site uses an inline `font-style: italic` on the element, never `<em>`. The converter follows the style, so any lesson using it is handled.
- **`fare\_amount`.** markdownify escapes underscores by default. `escape_underscores=False` keeps column names readable.
- **Diagram images overwrote each other.** Images were once one shared filename. They are now `<slug>_diagram_<n>.png`, numbered in page order across all diagram kinds.
- **Blurry images.** Screenshots render at `device_scale_factor=2`, the same layout with 4x the pixels.
- **Star ratings (Daily Life, Interviews).** Drawn on a canvas by JavaScript, so the page source holds no values. Omitted.
- **The interactive pipeline-architecture exercise.** A live drag-and-drop widget; a screenshot shows an empty canvas. Omitted.
- **`//` beside headings.** A decorative span. The converter drops it outside table cells.
- **Label glued to description** (`**Label**Body`). Appeared only in synthetic tests. Real lessons separate them with whitespace, so no rule exists.

## Spacing

Blank lines collapse in Markdown, so breaks are explicit: `<br>` plus `---` above each `##`, `<br>` above each `###`, and nothing below a `##`. Lines inside code fences are left alone.

## Repo facts

- `lesson_preview/` is tracked and committed on purpose. The user has said DataDriven's people gave permission for this copy.
- Tooling lives in `.venv/` (markdownify, beautifulsoup4, playwright). Playwright's Chromium sits in `~/.cache/ms-playwright`.
- `tools/__pycache__/*.pyc` is tracked and shows as modified after every run. Leave it out of commits.
