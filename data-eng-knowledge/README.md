# data-eng-knowledge

A small MCP server that lets an agent look up data engineering best practice in your own lessons instead of answering from training data.

Tools: `list_lessons` (top-level map), `get_lesson_index` (one lesson's sections and line ranges), `read_lesson` (a line range), `search` (keyword search over section headers), `grep_lessons` (regex over the lesson text).

## Run

The environment is local to this folder (`.venv`). Nothing is installed globally.

    uv venv .venv && uv pip install --python .venv/bin/python "mcp>=1.2"    # one time, already done here
    .venv/bin/python server.py                                              # stdio server

Content comes from `DATA_ENG_KNOWLEDGE_DIR` (default `~/projects/study-helper/lesson_preview`). It needs `INDEX.md`, the lesson `.md` files, and `index/<lesson>.md`.

## Register with Claude Code

    claude mcp add --scope user data-eng-knowledge -- ~/projects/data-eng-knowledge/.venv/bin/python ~/projects/data-eng-knowledge/server.py

Use `--scope project` instead to enable it for one project only.

## Adding knowledge

1. Put the new lesson `.md` in the content folder.
2. Run the `index-lesson` skill on it (writes `index/<lesson>.md` and the `INDEX.md` line).
3. `python3 scripts/validate_lesson_index.py <lesson>` in the study-helper repo must print OK.
