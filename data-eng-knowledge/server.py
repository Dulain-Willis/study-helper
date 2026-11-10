"""Data engineering knowledge MCP server.

Serves a folder of lessons plus hand-written indexes so an agent can look up best practice
instead of answering from training data. Layout of the content folder (KNOWLEDGE_DIR):

    INDEX.md                      one line per lesson (start here)
    <lesson>.md                   the lessons
    index/<lesson>.md             per-lesson sections with line ranges, e.g. "### Header (L120-160)"

Set DATA_ENG_KNOWLEDGE_DIR to point at a different folder.
"""
import math
import os
import re
from pathlib import Path

from mcp.server.mcpserver import MCPServer

KNOWLEDGE_DIR = Path(
    os.environ.get("DATA_ENG_KNOWLEDGE_DIR", "~/projects/study-helper/lesson_preview")
).expanduser()
MAX_READ_LINES = 400

INSTRUCTIONS = """\
This server holds the user's own data engineering lessons. Treat them as the source of truth for \
data engineering best practice: answer from them, not from general knowledge.

Workflow:
1. search(query) finds sections by keyword across every lesson, or list_lessons() shows the map.
2. get_lesson_index(lesson) shows one lesson's sections with line ranges.
3. read_lesson(lesson, start, end) reads just that range.

Cite the lesson and line range behind each claim. If no lesson covers the question, say so \
explicitly before falling back to general knowledge."""

mcp = MCPServer("data-eng-knowledge", instructions=INSTRUCTIONS)

STOP = set(
    "a an and are as at be but by can do does for from how i if in into is it its my of on or our "
    "so that the their then there this to we what when where which who why will with without you your "
    "should would could about each per vs".split()
)


def _lessons() -> list[str]:
    return sorted(p.stem for p in KNOWLEDGE_DIR.glob("*.md") if p.name != "INDEX.md")


def _check(lesson: str) -> str | None:
    """Return an error message if the lesson name is unknown, else None."""
    lesson = lesson.removesuffix(".md")
    if lesson in _lessons():
        return None
    near = [n for n in _lessons() if lesson.lower() in n.lower()][:5]
    hint = f" Did you mean: {', '.join(near)}?" if near else " Call list_lessons() to see the names."
    return f"Unknown lesson {lesson!r}.{hint}"


def _tokens(text: str) -> list[str]:
    out = []
    for w in re.findall(r"[a-z0-9]+", text.lower()):
        if w in STOP or len(w) < 2:
            continue
        for suffix in ("ing", "ed", "es", "s"):
            if len(w) > len(suffix) + 3 and w.endswith(suffix):
                w = w[: -len(suffix)]
                break
        out.append(w)
    return out


def _entries() -> list[dict]:
    entries = []
    for lesson in _lessons():
        path = KNOWLEDGE_DIR / "index" / f"{lesson}.md"
        if not path.exists():
            continue
        section = ""
        for line in path.read_text().splitlines():
            m = re.match(r"^(##|###) (.*) \(L(\d+)-(\d+)\)$", line)
            if not m:
                continue
            if m.group(1) == "##":
                section = m.group(2).strip('"')
            else:
                entries.append(dict(lesson=lesson, section=section, header=m.group(2),
                                    start=int(m.group(3)), end=int(m.group(4))))
    return entries


@mcp.tool()
def list_lessons() -> str:
    """The top-level map: every lesson with a one-line description. Start here to browse."""
    return (KNOWLEDGE_DIR / "INDEX.md").read_text()


@mcp.tool()
def get_lesson_index(lesson: str) -> str:
    """One lesson's sections with line ranges. Use after list_lessons() or search()."""
    if err := _check(lesson):
        return err
    return (KNOWLEDGE_DIR / "index" / f"{lesson.removesuffix('.md')}.md").read_text()


@mcp.tool()
def read_lesson(lesson: str, start: int, end: int) -> str:
    """Read lines start..end (1-indexed, inclusive) of a lesson, with line numbers. Max 400 lines."""
    if err := _check(lesson):
        return err
    name = lesson.removesuffix(".md")
    lines = (KNOWLEDGE_DIR / f"{name}.md").read_text().splitlines()
    start = max(1, start)
    end = min(len(lines), end, start + MAX_READ_LINES - 1)
    if start > len(lines):
        return f"{name} has only {len(lines)} lines."
    body = "\n".join(f"{i}: {lines[i - 1]}" for i in range(start, end + 1))
    return f"{name} L{start}-{end} of {len(lines)}\n\n{body}"


@mcp.tool()
def search(query: str, limit: int = 8) -> str:
    """Keyword search over the section headers of every lesson. Returns the best matches as
    'lesson | lesson section | header | L<start>-<end>', ready for read_lesson()."""
    entries = _entries()
    q = _tokens(query)
    if not q or not entries:
        return "No query terms, or no indexes found."
    docs = [set(_tokens(e["header"])) | set(_tokens(e["section"])) for e in entries]
    n = len(docs)
    idf = {t: math.log(1 + n / (1 + sum(t in d for d in docs))) for t in set(q)}
    scored = []
    for e, d in zip(entries, docs):
        head = set(_tokens(e["header"]))
        score = sum(idf[t] * (1.0 if t in head else 0.5) for t in set(q) if t in d)
        if score > 0:
            scored.append((score, e))
    scored.sort(key=lambda x: -x[0])
    if not scored:
        return "No matches. Try other keywords, or call list_lessons()."
    return "\n".join(
        f"{e['lesson']} | {e['section']} | {e['header']} | L{e['start']}-{e['end']}"
        for _, e in scored[:limit]
    )


@mcp.tool()
def grep_lessons(pattern: str, limit: int = 20) -> str:
    """Case-insensitive regex search over the full text of every lesson. Returns
    'lesson:line: text' matches, ready for read_lesson(). Use when search() misses because the
    section headers use different words than you do."""
    try:
        rx = re.compile(pattern, re.IGNORECASE)
    except re.error as e:
        return f"Bad regex: {e}"
    hits = []
    for lesson in _lessons():
        for i, line in enumerate((KNOWLEDGE_DIR / f"{lesson}.md").read_text().splitlines(), 1):
            if rx.search(line):
                hits.append(f"{lesson}:{i}: {line.strip()[:160]}")
    if not hits:
        return "No matches."
    more = f"\n... {len(hits) - limit} more matches; narrow the pattern." if len(hits) > limit else ""
    return "\n".join(hits[:limit]) + more


if __name__ == "__main__":
    mcp.run()
