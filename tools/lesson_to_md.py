#!/usr/bin/env python3
"""Convert a datadriven.io lesson page into a local Markdown file.

Usage: python3 tools/lesson_to_md.py <lesson-url-or-slug> [out_dir]

For personal study use. Stdlib only. Output defaults to ./lesson_preview/.
"""
import os
import re
import sys
import urllib.request
from html.parser import HTMLParser

BASE = "https://datadriven.io/learn/"
SKIP_TAGS = {"script", "style", "nav", "head", "footer"}


class Converter(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []
        self.buf = ""
        self.skip = 0
        self.svg = 0
        self.table = None
        self.row = None
        self.cell = None
        self.lvl = 0
        self.in_li = 0

    def flush(self):
        text = re.sub(r"\s+", " ", self.buf).strip()
        if text:
            if self.lvl:
                text = "#" * self.lvl + " " + text
            elif self.in_li:
                text = "- " + text
            self.out.append(text)
        self.buf = ""
        self.lvl = 0

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag in SKIP_TAGS:
            self.skip += 1
            return
        if tag == "svg":
            self.svg += 1
            return
        if self.svg or self.skip:
            return
        if tag == "img":
            src = a.get("src", "")
            # decorative icons and logos carry no content
            if "data_icons" not in src and "logo" not in src and a.get("alt"):
                self.buf += f" ![{a['alt']}]({src}) "
            return
        if tag in ("h1", "h2", "h3", "h4"):
            self.flush()
            self.lvl = int(tag[1])
        elif tag == "li":
            if self.cell is None:
                self.flush()
            self.in_li = 1
        elif tag in ("p", "div", "section", "ul", "ol", "pre"):
            if self.cell is None:
                # inside a list item, a nested block is a label/body split
                if self.in_li and self.buf.strip():
                    self.buf += " — "
                else:
                    self.flush()
        elif tag == "span" and self.in_li and self.buf.strip() and self.cell is None:
            self.buf += " — "
        elif tag == "table":
            self.flush()
            self.table = []
        elif tag == "tr":
            self.row = []
        elif tag in ("td", "th"):
            self.cell = ""
        elif tag == "code" and self.cell is None:
            self.buf += "`"

    def handle_endtag(self, tag):
        if tag in SKIP_TAGS:
            self.skip -= 1
            return
        if tag == "svg":
            self.svg -= 1
            return
        if self.svg or self.skip:
            return
        if tag in ("td", "th"):
            self.row.append(re.sub(r"\s+", " ", self.cell or "").strip())
            self.cell = None
        elif tag == "tr":
            self.table.append(self.row)
            self.row = None
        elif tag == "table":
            self.emit_table()
        elif tag in ("h1", "h2", "h3", "h4", "p", "div", "section", "li", "pre"):
            if self.cell is None:
                self.flush()
                if tag == "li":
                    self.in_li = 0
        elif tag == "code" and self.cell is None:
            self.buf += "`"

    def handle_data(self, data):
        if self.skip or self.svg:
            return
        if self.cell is not None:
            self.cell += data
        else:
            self.buf += data

    def emit_table(self):
        rows, self.table = self.table, None
        if not rows:
            return
        width = max(len(r) for r in rows)
        rows = [r + [""] * (width - len(r)) for r in rows]
        # SQL/code blocks arrive as a table whose first column is empty
        if width == 2 and all(r[0] == "" for r in rows):
            self.out.append("```sql\n" + "\n".join(r[1] for r in rows) + "\n```")
            return
        lines = ["| " + " | ".join(rows[0]) + " |", "|" + "---|" * width]
        lines += ["| " + " | ".join(r) + " |" for r in rows[1:]]
        self.out.append("\n".join(lines))


def convert(html):
    c = Converter()
    c.feed(html)
    c.flush()
    return "\n\n".join(c.out) + "\n"


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    target = sys.argv[1]
    url = target if target.startswith("http") else BASE + target
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "lesson_preview"
    req = urllib.request.Request(url, headers={"User-Agent": "lesson-to-md/1.0"})
    html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
    os.makedirs(out_dir, exist_ok=True)
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    path = os.path.join(out_dir, slug + ".md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(convert(html))
    print(path)


if __name__ == "__main__":
    main()
