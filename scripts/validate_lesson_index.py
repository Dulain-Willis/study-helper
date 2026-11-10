#!/usr/bin/env python3
"""Check lesson_preview/index/<stem>.md against its lesson. Usage: validate_lesson_index.py <stem> [<stem> ...]"""
import re, sys, pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent / "lesson_preview"
SKIP = {"What you will be able to do", "Putting It All Together", "Key Takeaways"}


def check(stem):
    stem = stem.removesuffix(".md")
    lesson = (ROOT / f"{stem}.md").read_text().splitlines()
    idx = (ROOT / "index" / f"{stem}.md").read_text().splitlines()
    problems = []
    hs = [(len(m.group(1)), m.group(2), i) for i, l in enumerate(lesson, 1) if (m := re.match(r"^(##|###) (.*\S)", l))]
    exp = []
    for k, (lvl, t, s) in enumerate(hs):
        end = len(lesson)
        for l2, _, s2 in hs[k + 1:]:
            if l2 <= lvl:
                end = s2 - 1
                break
        if t not in SKIP:
            exp.append((lvl, t, s, end))
    got = []
    for l in idx:
        m = re.match(r"^(##|###) (.*) \(L(\d+)-(\d+)\)$", l)
        if m:
            got.append((len(m.group(1)), m.group(2), int(m.group(3)), int(m.group(4))))
    top = ROOT / "INDEX.md"
    if top.exists() and not re.search(rf"^- `{re.escape(stem)}`:", top.read_text(), re.M):
        problems.append(f"lesson_preview/INDEX.md has no line starting with - `{stem}`:")
    src = next((l for l in idx if l.startswith("Source:")), "")
    if f"({len(lesson)} lines)" not in src:
        problems.append(f"Source line should say ({len(lesson)} lines): {src[:90]!r}")
    if len(exp) != len(got):
        problems.append(f"entry count: lesson has {len(exp)}, index has {len(got)}")
    for (el, et, es, ee), (gl, gt, gs, ge) in zip(exp, got):
        if el != gl:
            problems.append(f"level mismatch at {gt!r}")
        if (es, ee) != (gs, ge):
            problems.append(f"range for {gt!r}: expected L{es}-{ee}, got L{gs}-{ge}")
        if el == 2 and et != gt:
            problems.append(f"## title not verbatim: {gt!r} (lesson: {et!r})")
        if el == 3:
            if len(gt.split()) > 10:
                problems.append(f"over 10 words ({len(gt.split())}): {gt!r}")
            if ";" in gt:
                problems.append(f"semicolon in header: {gt!r}")
    return problems


if __name__ == "__main__":
    bad = 0
    for s in sys.argv[1:]:
        p = check(s)
        print(("OK   " if not p else "FAIL ") + s)
        for x in p:
            print("   -", x)
        bad += bool(p)
    sys.exit(1 if bad else 0)
