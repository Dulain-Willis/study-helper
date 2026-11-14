# Block types

The site builds a lesson from blocks tagged `data-note-block-type="<type>"`, and the converter keys its rules on that tag. Types in `GENERIC_BLOCKS` have real HTML tags and need no rule. The reason behind each rule sits in the comment beside it in `tools/lesson_to_md.py`; this file records what each block's HTML looks like, so a new type can be matched to a solved one.

## Types with a rule

| Type | HTML shape | Handling |
|---|---|---|
| `info_list` | Rows of 1 to 3 sibling spans (label, gray tag, description) with no whitespace between them. The label looks bold through styling, with no `<strong>`, so a search for `**` finds nothing wrong; count spans per `li` instead. | Rows with 2 or more spans become `**label** *tag*` plus the description on its own line. One-span rows stay plain. |
| `rich_info_list` | Like `info_list`, but the label is a real `<strong>`. | None needed. |
| `icon_list` | Rows of a decorative icon plus a sentence in nested divs, with no list tags. | One bullet per row. |
| `static_code` | A table, one row per line, code in the last cell. | Code fence. |
| `big_number` | Stat cards, two divs each: figure, then caption. | Bullets pairing figure and caption. |
| `tip`, `callout`, `fun_fact` | A styled div. `tip` prints its own TIP label. | Blockquote; `tip` bolds its label. |
| `key_takeaways`, `synthesis` | Plain divs with canvas-drawn bullets and no `li`. | `##` heading plus bullets. |
| `pipeline_diagram`, `schema_diagram` | HTML boxes layered with SVG, no text structure. | 2x screenshot. |

## Left out on purpose

- **Star ratings (Daily Life, Interviews):** drawn on a canvas by JavaScript, so the page source holds no values.
- **`multiple_choice`:** the rendered options are buttons with no correct-answer marker, but the page's Next.js payload (`self.__next_f.push` script strings) holds `{"type":"multiple_choice","question","options":[{"text","is_correct","explanation"}]}`. `embedded_questions` reads it, matching each block to its JSON by question text, and `multiple_choice_markdown` writes the question, lettered options, then a closed `<details><summary>Solution</summary>` with the correct option, its explanation, and "Why the others are wrong" (one explanation per wrong option).
- **`multiple_choice` JSON quirks:** key order varies (a question with a code snippet has `"code"`, `"language"` before `"type"`), so `embedded_questions` walks back from each `"type":"multiple_choice"` marker to the enclosing object. Inline code in a question renders without its quote marks, so `question_key` drops quotes, backticks and whitespace before matching.
- **`full_width`:** a wrapper around a real table (pa_what_is_a_pipeline_advanced); in `GENERIC_BLOCKS`.
- **`pipeline_animation`:** an animated pipeline widget (retry storms, ingestion flows). Its data is in the page JSON (`nodes`, `edges`, `clock`, `variants`). Not handled yet: it prints "no rule" and falls back to generic conversion, so the animation is lost. Seen in 8 learn/architecture lessons.
- **`schema_story` (data_modeling lessons):** interactive quiz with no answers in the page, so only a dangling question would remain. `SKIPPED_BLOCKS` drops it. Not yet checked for embedded JSON the way `multiple_choice` was.
- **Airflow exercises (tools/airflow):** all left out by the `INCLUDE_AIRFLOW_EXERCISES = False` switch in `tools/lesson_to_md.py`; the converter prints `note: N Airflow exercise(s) left out (INCLUDE_AIRFLOW_EXERCISES is False)` and runs no exercise browser pass. Why: the exercises are badly made. The grid/run-naming forms are buggy (a widget's sentence is sometimes drawn outside its frame, e.g. the 9 UTC / 3 November `signups_daily` one), and Run/Submit need a login. `airflow_animation` blocks are kept (lesson text refers to "the animation below"). To turn exercises back on, set the constant to True: the converter still has the repair-the-DAG conversion (`config.form == "dag_repair"`, Mermaid after-fix graphs, a `What changes` diff); other forms stay left out.
- **dbt and Kafka exercises (tools/dbt, tools/kafka):** also left out, via `SKIPPED_TOOL_EXERCISES = {"dbt", "kafka"}` in `tools/lesson_to_md.py` (matched on the challenges endpoint's `challenge_type`); the converter prints `note: N dbt|kafka exercise(s) left out`. They are not copies of Airflow's: dbt forms include compile_it, build_outcome, select_the_nodes, two_run_proof, model_it, catch_the_defect, draw_the_lineage, snapshot_timeline; Kafka forms include read_the_tape, assign_the_group, lag_board, crash_point, connector_config, log_cleanup, schema_rollout, delivery_trace, route_the_keys, durability_dial, replica_trace. No conversion code exists for them. The `placed_challenge` stub is dropped and `dbt_animation` / `kafka_animation` blocks are kept as GIFs. Lesson titles still say "N challenges", and a few sentences in the text point at the removed exercises (kafka_the_log_advanced has two "the drill below" paragraphs).
- **The interactive pipeline-architecture exercise:** a live drag-and-drop widget; a screenshot shows an empty canvas.

## Repo facts

- `lesson_preview/` is tracked and committed on purpose; per the user, DataDriven's people approved this copy.
- Python tooling lives in `.venv/`; Playwright's Chromium sits in `~/.cache/ms-playwright`.
- `tools/__pycache__/*.pyc` is tracked and changes on every run, so `git add` files by path.
