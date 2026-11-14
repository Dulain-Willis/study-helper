#!/usr/bin/env python3
"""Convert a datadriven.io lesson page into a local Markdown file.

Usage:
    .venv/bin/python tools/lesson_to_md.py <lesson-url-or-slug> [out_dir] [--screenshots]

markdownify does the HTML -> Markdown work. The rules below only cover this
site's custom blocks (code tables, stat callouts, tips, the pipeline diagram).
--screenshots needs playwright (see screenshot_diagrams) and saves the
pipeline diagram as an image next to the Markdown.

For personal study use. Output defaults to ./lesson_preview/.
"""
import json
import os
import re
import sys
import urllib.request

from bs4 import BeautifulSoup
from bs4.builder import HTMLParserTreeBuilder
from markdownify import markdownify

BASE = "https://datadriven.io/learn/"
# The Pipeline Architecture exercises are not in the lesson HTML; the page loads them from here.
CHALLENGES_URL = "https://datadriven.io/proxy/challenges/"
# Visual blocks that plain text can't represent; each is saved as a screenshot.
DIAGRAM_LABELS = {"pipeline_diagram": "Pipeline diagram", "schema_diagram": "Schema diagram"}
DIAGRAM_SELECTOR = ", ".join(f'[data-note-block-type="{k}"]' for k in DIAGRAM_LABELS)
NOISE = ["script", "style", "nav", "footer", "button", "svg", "canvas", "head"]

# Block types with a custom rule below, plus ones markdownify handles well as-is.
# Anything else triggers a warning so a new rule can be added.
CUSTOM_BLOCKS = {
    "static_code", "big_number", "tip", "callout", "fun_fact", "key_takeaways",
    "synthesis", "icon_list", "info_list", *DIAGRAM_LABELS,
    "multiple_choice", "schema_story", "best_practices", "comparison",
}
# Interactive exercises: the answer options are buttons (stripped as noise) and the
# correct answers are not in the page, so what's left is a dangling question. Dropped.
SKIPPED_BLOCKS = {"multiple_choice", "schema_story"}
GENERIC_BLOCKS = {
    "lesson_title", "learning_objectives", "section_header", "h3", "body_text",
    "rich_text", "static_table", "side_by_side_tables", "comparison",
    "note_text", "rich_info_list", "h4",
}


def block_type(tag):
    return tag.get("data-note-block-type")


def parse(html):
    """Parse HTML, keeping whitespace inside table cells.

    The site indents code with a whitespace-only <span> and CSS (white-space: pre),
    not a <pre> tag, and BeautifulSoup would collapse those spans to one space.
    """
    builder = HTMLParserTreeBuilder()
    builder.preserve_whitespace_tags = set(builder.preserve_whitespace_tags) | {"td"}
    return BeautifulSoup(html, builder=builder)


def clean(soup):
    for t in soup.select(".sr-only"):
        t.decompose()
    # decorative "//" that sits beside some headings
    for span in soup.find_all("span"):
        if span.get_text(strip=True) == "//" and not span.find_parent("td"):
            span.decompose()
    for t in soup(NOISE):
        t.decompose()
    # decorative icons and logos carry no content
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if "data_icons" in src or "logo" in src:
            img.decompose()
    # the site puts a triangle in each list item; markdown adds its own bullet
    for text in soup.find_all(string=lambda s: s and s.lstrip().startswith("▸")):
        text.replace_with(text.lstrip().lstrip("▸").lstrip())


def code_block(soup, block):
    """Code arrives as a table with the code in the last column."""
    lines = [tr.find_all("td")[-1].get_text() for tr in block.find_all("tr") if tr.find_all("td")]
    pre = soup.new_tag("pre")
    code = soup.new_tag("code", attrs={"class": "language-sql"})
    code.string = "\n".join(lines)
    pre.append(code)
    block.replace_with(pre)


def big_numbers(soup, block):
    """Pair each figure with its caption: '- **400 GB** - scanned per day'."""
    ul = soup.new_tag("ul")
    for card in block.select("div.text-2xl"):
        figure, caption = card.get_text(strip=True), card.find_next_sibling("div")
        li = soup.new_tag("li")
        li.append(BeautifulSoup(f"<strong>{figure}</strong> — {caption.get_text(strip=True) if caption else ''}", "html.parser"))
        ul.append(li)
    block.replace_with(ul)


def callout(soup, block, label=None):
    quote = soup.new_tag("blockquote")
    if label:
        # the site already prints the label inside the box; bold it, don't repeat it
        existing = block.find(string=lambda s: s and s.strip().upper() == label)
        if existing:
            existing.replace_with(BeautifulSoup(f"<strong>{label}</strong>", "html.parser"))
        else:
            p = soup.new_tag("p")
            p.append(BeautifulSoup(f"<strong>{label}</strong>", "html.parser"))
            quote.append(p)
    for child in list(block.children):
        quote.append(child.extract())
    block.replace_with(quote)


def bullet_section(soup, block):
    """Key takeaways / putting it all together: a titled section of bullets.

    The site draws the bullets on a canvas, so the HTML has plain divs, not
    list items. Items are the long outermost spans; the title is the short
    all-caps span (with any decorative arrows removed).
    """
    title, items = None, []
    for span in block.find_all("span"):
        text = span.get_text(" ", strip=True).replace("❯", "").strip()
        if span.find_parent("span") or span.find("div") or not text:
            continue
        if len(text) >= 40:
            items.append(text)
        elif text.isupper() and not title:
            title = text.title()
    section = soup.new_tag("div")
    heading = soup.new_tag("h2")
    heading.string = title or "Summary"
    section.append(heading)
    intro = block.find("p")
    if intro:
        intro.string = intro.get_text(strip=True).removeprefix(">").strip()
        section.append(intro.extract())
    ul = soup.new_tag("ul")
    for text in items:
        li = soup.new_tag("li")
        li.string = text
        ul.append(li)
    section.append(ul)
    block.replace_with(section)


def info_list(soup, block):
    """Rows of label, optional gray tag, description, as sibling spans.

    The site makes the label bold and the tag gray with styling alone, so the
    spans fuse into one run of text. Rebuild each row as: **label** *tag*, then
    the description on its own line. One-span rows are plain text and stay as is.
    """
    for li in block.find_all("li"):
        parts = [s.get_text(" ", strip=True) for s in li.find_all("span")
                 if s.get_text(strip=True) and not s.find("span")]
        if len(parts) < 2:
            continue
        label, *tag, description = parts
        strong = soup.new_tag("strong")
        strong.string = label
        li.clear()
        li.append(strong)
        if tag:
            em = soup.new_tag("em")
            em.string = " ".join(tag)
            li.append(" ")
            li.append(em)
        li.append(soup.new_tag("br"))
        li.append(description)


def icon_list(soup, block):
    """Rows of a decorative icon plus a sentence: the icon is dropped (clean()
    removes it), and each row's text becomes a bullet. The HTML has no list tags."""
    ul = soup.new_tag("ul")
    for div in block.find_all("div"):
        text = div.get_text(" ", strip=True)
        if text and not div.find("div"):
            li = soup.new_tag("li")
            li.string = text
            ul.append(li)
    block.replace_with(ul)


def diagram(soup, block, image_name, number):
    label = DIAGRAM_LABELS[block_type(block)]
    if image_name:
        p = soup.new_tag("p")
        p.append(soup.new_tag("img", src=f"images/{image_name}", alt=f"{label} {number}"))
        block.replace_with(p)
    else:
        note = soup.new_tag("p")
        note.string = f"[{label}: not captured. Re-run with --screenshots]"
        block.replace_with(note)


def tick_cross(soup, block, tokens):
    """A tick/cross pair (Do and Don't, or any two labelled lists) as one table.

    The two labels are the headers and each side's bullets sit in one cell, joined by line
    breaks. Cells cannot hold real lists, so the bullet is a character; a long bullet that
    wraps goes back to the left edge. Built here as Markdown (swapped in after markdownify)
    because a line break inside a cell would otherwise end the table row.
    """
    sides = []
    for ul in block.find_all("ul"):
        head = ul.find_previous_sibling("div")
        spans = head.find_all("span", recursive=False) if head else []
        if len(spans) < 2:
            return  # a shape we haven't seen: leave it to the generic conversion
        items = []
        for li in ul.find_all("li", recursive=False):
            text = markdownify(li.find_all("span", recursive=False)[-1].decode_contents(),
                               escape_underscores=False)
            items.append("• " + " ".join(text.split()).replace("|", "\\|"))
        sides.append((f"{spans[0].get_text(strip=True)} {spans[1].get_text(strip=True)}", items))
    if len(sides) != 2:
        return
    token = f"@@TICK-CROSS-{len(tokens)}@@"
    tokens[token] = ("| " + " | ".join(h for h, _ in sides) + " |\n| --- | --- |\n| "
                     + " | ".join("<br><br>".join(items) for _, items in sides) + " |")
    marker = soup.new_tag("p")
    marker.string = token
    block.replace_with(marker)


def convert(html, diagram_images=(), exercises=()):
    """diagram_images: screenshot file names, in page order, one per diagram block.
    exercises: [(anchor block id or None, markdown)]; each goes right after its anchor block,
    and the ones with no anchor are returned by the caller to append."""
    soup = parse(html)
    clean(soup)
    tokens = {}
    for i, (anchor, md_text) in enumerate(exercises):
        block = soup.find(attrs={"data-note-block": anchor}) if anchor else None
        if block is None:
            print("warning: exercise position not found in the page; it goes at the end", file=sys.stderr)
            tokens[None] = tokens.get(None, "") + "\n\n" + md_text
            continue
        p = soup.new_tag("p")
        p.string = EXERCISE_TOKEN.format(i)
        block.insert_after(p)
        tokens[EXERCISE_TOKEN.format(i)] = md_text
    diagrams_seen = 0
    for block in soup.find_all(attrs={"data-note-block-type": True}):
        kind = block_type(block)
        if kind not in CUSTOM_BLOCKS | GENERIC_BLOCKS:
            print(f"warning: no rule for block type '{kind}'; used generic conversion",
                  file=sys.stderr)
        if kind in SKIPPED_BLOCKS:
            block.decompose()
        elif kind == "static_code":
            code_block(soup, block)
        elif kind == "big_number":
            big_numbers(soup, block)
        elif kind == "tip":
            callout(soup, block, "TIP")
        elif kind in ("callout", "fun_fact"):
            callout(soup, block)
        elif kind in ("key_takeaways", "synthesis"):
            bullet_section(soup, block)
        elif kind == "icon_list":
            icon_list(soup, block)
        elif kind == "info_list":
            info_list(soup, block)
        elif kind in ("best_practices", "comparison"):
            tick_cross(soup, block, tokens)
        elif kind in DIAGRAM_LABELS:
            name = diagram_images[diagrams_seen] if diagrams_seen < len(diagram_images) else None
            diagrams_seen += 1
            diagram(soup, block, name, diagrams_seen)
    # the site italicizes with an inline style, not <em>; follow the style so any
    # lesson that uses it is handled, not just specific paragraphs
    for tag in soup.find_all(style=re.compile(r"font-style:\s*italic")):
        if tag.find_parent("em") or not tag.get_text(strip=True):
            continue
        em = soup.new_tag("em")
        for child in list(tag.contents):
            em.append(child.extract())
        tag.append(em)
    # star ratings are canvas-drawn by JS; the bare labels carry no content
    for div in soup.find_all("div"):
        if div.get_text(strip=True) in ("Daily Life", "Interviews") and not div.find("div"):
            div.decompose()
    # escape_underscores=False keeps names like fare_amount readable
    # BACKSLASH line breaks survive the per-line rstrip in add_section_breaks;
    # the default two trailing spaces would not
    md = markdownify(str(soup), heading_style="ATX", bullets="-", code_language="sql",
                     escape_underscores=False, newline_style="BACKSLASH")
    for token, text in tokens.items():
        if token:
            md = md.replace(token, text)
    md = md.rstrip("\n") + (tokens[None] if None in tokens else "") + "\n"
    return add_section_breaks(md)


def add_section_breaks(md):
    """A gap and a rule above each section (##), a gap above each mini header (###).

    Blank lines alone collapse in Markdown, so the breaks must be visible.
    Nothing goes below a section header: a mini header right after one gets no gap.
    """
    out, in_code, last_heading = [], False, 0
    for line in (l.rstrip() for l in md.splitlines()):
        if line.startswith("```"):
            in_code = not in_code
        if not in_code and out:
            if line.startswith("## "):
                out += ["", "<br>", "", "---", ""]
            elif line.startswith("### ") and last_heading != 2:
                out += ["", "<br>", ""]
        if not in_code and line.strip():
            last_heading = 2 if line.startswith("## ") else 3 if line.startswith("### ") else 0
        out.append(line)
    return "\n".join(out).strip() + "\n"


# --- Pipeline Architecture exercises -------------------------------------------------
# A lesson can hold one or more exercise widgets (1-6 exercises in all): task, optional
# starting canvas, solution walkthrough. The widget is drawn by the browser and its
# content comes from a JSON endpoint, so there is nothing to scroll or click.

MERMAID_SHAPES = {  # node_type -> (open, close)
    "source": ("[(", ")]"), "storage": ("[(", ")]"), "transform": ("[", "]"),
    "consumer": ("([", "])"), "quality": ("{{", "}}"), "queue": ("[/", "/]"),
}
EXERCISE_TOKEN = "@@PIPELINE-ARCHITECTURE-{}@@"


def fetch_challenges(slug):
    """The lesson's exercises in order, or [] if it has none or the request fails."""
    req = urllib.request.Request(CHALLENGES_URL + slug, headers={"User-Agent": "lesson-to-md/1.0"})
    try:
        data = json.loads(urllib.request.urlopen(req, timeout=30).read().decode("utf-8"))
    except Exception as e:  # network, 404 or not JSON: the lesson text is still fine without them
        print(f"warning: no Pipeline Architecture exercises fetched ({e})", file=sys.stderr)
        return []
    return sorted(data.get("challenges", []), key=lambda c: c.get("order_index", 0))


def exercise_groups(url, challenges):
    """Split the exercises into one group per widget, each with the id of the lesson
    block it follows (None = no position known, goes at the end).

    The widget is missing from the downloaded HTML, so a browser pass reads where it sits
    and how many exercises it holds (data-challenge-ids). Needs playwright.
    """
    fallback = [(None, challenges)]
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 1000, "height": 900})
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(4000)
            widgets = page.evaluate("""() => {
                const out = []; let prev = null;
                for (const e of document.querySelectorAll('[data-note-block]')) {
                    if (e.getAttribute('data-note-block-type') === 'schema') {
                        const c = e.matches('[data-challenge-ids]') ? e : e.querySelector('[data-challenge-ids]');
                        const ids = c ? (c.getAttribute('data-challenge-ids') || '').trim().split(/\\s+/).filter(Boolean) : [];
                        out.push({anchor: prev, count: ids.length});
                    } else prev = e.getAttribute('data-note-block');
                }
                return out; }""")
            browser.close()
    except Exception as e:
        print(f"warning: could not place the exercises ({e}); they go at the end", file=sys.stderr)
        return fallback
    if sum(w["count"] for w in widgets) != len(challenges):
        print("warning: widget exercise counts do not match the data; exercises go at the end",
              file=sys.stderr)
        return fallback
    groups, i = [], 0
    for w in widgets:
        groups.append((w["anchor"], challenges[i:i + w["count"]]))
        i += w["count"]
    return groups


# Extra node fields the site shows as small label boxes (e.g. SLA | < 30s) -> their label.
NODE_EXTRAS = {
    "slaFreshness": "SLA", "sla_freshness": "SLA", "parallelism": "Partitions",
    "monitorAlert": "Alert", "idempotencyStrategy": "Idempotency",
    "backfillStrategy": "Backfill", "errorAction": "On error",
    "retryCount": "Retries", "retryBackoff": "Backoff",
}


def mermaid_text(t):
    """Escape text for a quoted Mermaid label; < and > are written as entity codes."""
    return str(t).replace('"', "#quot;").replace("<", "#60;").replace(">", "#62;")


def mermaid(block):
    """A solution diagram (nodes + edges, optional layers) as a Mermaid flowchart."""
    ids = {n["name"]: f"n{i}" for i, n in enumerate(block.get("nodes", []))}
    q = mermaid_text

    def node(n):
        # The site splits tech_label at commas: the first part is the caption above the
        # name (Kafka, Spark), the rest are small tags below. Extra fields such as an SLA
        # become small labels too. Mermaid has no label boxes, so tags and values go on one
        # extra line inside the node. With no tech_label the caption is the node type.
        parts = [x.strip() for x in (n.get("tech_label") or n.get("node_type") or "").split(",")]
        extras = parts[1:] + [f"{label}: {n[key]}" for key, label in NODE_EXTRAS.items() if key in n]
        label = f"{q(n['name'])}<br/>({q(parts[0])})"
        if extras:
            label += "<br/>" + q(" · ".join(extras))
        o, c = MERMAID_SHAPES.get(n.get("node_type"), ("[", "]"))
        return f'{ids[n["name"]]}{o}"{label}"{c}'

    by_name = {n["name"]: n for n in block.get("nodes", [])}
    lines, placed = ["```mermaid", "flowchart LR"], set()
    for i, layer in enumerate(block.get("layers") or []):
        members = [n for n in layer.get("node_names", []) if n in by_name and n not in placed]
        if not members:
            continue
        lines.append(f'    subgraph L{i}["{q(layer["name"])}"]')
        lines += [f"        {node(by_name[n])}" for n in members]
        lines.append("    end")
        placed.update(members)
    lines += [f"    {node(n)}" for n in block.get("nodes", []) if n["name"] not in placed]
    for e in block.get("edges", []):
        if e["from"] in ids and e["to"] in ids:
            label = f'|"{e["label"]}"|' if e.get("label") else ""
            lines.append(f"    {ids[e['from']]} -->{label} {ids[e['to']]}")
    lines.append("```")
    return "\n".join(lines)


def cell(text):
    return (text or "").replace("|", "\\|").replace("\n", "<br>")


def solution_block(b, image=None):
    """One solution block as Markdown, or None for blocks that add nothing (dividers)."""
    kind = b.get("blockType")
    if kind == "heading":
        return f"#### {b['blockText']}"
    if kind == "paragraph":
        return b["blockText"]
    if kind == "divider":
        return None
    if kind == "step":  # numbered item with the body indented under it
        body = b["body"].replace("\n", "\n   ")
        return f"{b['number']}. **{b['title']}**\n\n   {body}"
    if kind == "callout":  # one-column table: category and title on top, text below
        label = (b.get("variant") or "note").replace("_", " ").capitalize()
        return f"| {cell(label + ': ' + b['title'])} |\n| --- |\n| {cell(b['body'])} |"
    if kind == "comparison":  # the site draws a cross on the left and a tick on the right
        return (f"| ✕ {cell(b['left_label'])} | ✓ {cell(b['right_label'])} |\n| --- | --- |\n"
                f"| {cell(b['left_body'])} | {cell(b['right_body'])} |")
    if kind == "pipeline_diagram":
        return mermaid(b) + (f"\n\n*{b['caption']}*" if b.get("caption") else "")
    if kind == "follow_up":
        items = []
        for i, q in enumerate(b["questions"], 1):
            items.append(f"{i}. {q['question']}" + (f"\\\n   *{q['context']}*" if q.get("context") else ""))
        return "**Follow-up questions**\n\n" + "\n".join(items)
    if kind == "schema_diagram":  # tables and relationships: drawn by the site, so it is a screenshot
        out = (f"![Schema diagram](images/{image})" if image
               else "[Schema diagram: not captured. Re-run with --screenshots]")
        return out + (f"\n\n*{b['caption']}*" if b.get("caption") else "")
    if kind == "code":
        out = (f"**{b['title']}**\n\n" if b.get("title") else "")
        out += f"```{b.get('language') or ''}\n{b['code']}\n```"
        return out + (f"\n\n*{b['caption']}*" if b.get("caption") else "")
    print(f"warning: no rule for Pipeline Architecture solution block '{kind}'; "
          "printed its text fields", file=sys.stderr)
    return "\n\n".join(str(v) for k, v in b.items() if k != "blockType" and isinstance(v, str)) or None


def exercises_markdown(challenges, images=None, start=0):
    """One exercise section (titled as the site does): task, starting canvas, solution.

    images: {exercise number in the lesson: screenshot file name}; start: how many
    exercises of earlier widgets come before these.
    """
    domain = ((challenges[0].get("deep_dive") or {}).get("domain") or "pipeline_architecture")
    out = ["## " + domain.replace("_", " ").title()]
    for n, c in enumerate(challenges, 1):
        out += [f"### Exercise {n} of {len(challenges)}", f"**● TASK**\n\n{c['prompt']}"]
        nodes = (c.get("config") or {}).get("provided_pipeline_nodes") or []
        if nodes:
            rows = [f"- {x['name']} ({x['node_type']}" + (f", {x['tech_label']}" if x.get("tech_label") else "") + ")"
                    for x in nodes]
            out.append("**Starting canvas** (already placed for you):\n\n" + "\n".join(rows))
        # The solution is folded away until clicked, so an exercise reads as its task first.
        # The blank line after <summary> is what lets Markdown render inside the fold.
        out.append("<details>\n<summary><strong>Solution</strong></summary>")
        out += [m for m in (solution_block(b, (images or {}).get(start + n))
                            for b in (c.get("deep_dive") or {}).get("blocks") or []) if m]
        out.append("</details>")
    return "\n\n".join(out)


# Taking a solution's schema diagram: it sits in a scrolling box inside a widget (one
# page can hold several widgets, some with tabs). The diagram is found by its content, the
# smallest element holding every table and column name, and Playwright scrolls to it.
_FIND = """([el, names]) => { let n = el; while (n && n.parentElement) { n = n.parentElement;
    const t = n.innerText || ''; if (names.every(x => t.includes(x)) && n.getBoundingClientRect().width > 300) return n; }
    return null; }"""
_CLIPPED = """(el) => { const r = el.getBoundingClientRect(); let n = el.parentElement;
    while (n && n !== document.body) { const s = getComputedStyle(n);
        if (/(auto|scroll|hidden)/.test(s.overflowY)) { const p = n.getBoundingClientRect();
            if (r.top < p.top - 1 || r.bottom > p.bottom + 1) return true; } n = n.parentElement; }
    return false; }"""
_UNCLIP = """(el) => { let n = el.parentElement; while (n && n !== document.body) {
    if (/(auto|scroll|hidden)/.test(getComputedStyle(n).overflowY)) {
        n.style.overflow = 'visible'; n.style.maxHeight = 'none'; n.style.height = 'auto'; } n = n.parentElement; } }"""
_HIDE_FIXED = """() => { for (const e of document.querySelectorAll('body *')) {
    const p = getComputedStyle(e).position, r = e.getBoundingClientRect();
    if ((p === 'fixed' || p === 'sticky') && r.top <= 5 && r.height < 140) e.style.visibility = 'hidden'; } }"""


def screenshot_schema_diagrams(url, out_dir, slug, challenges):
    """Save each exercise's solution schema diagram as <slug>_exercise_<n>_schema.png.

    Returns {exercise number: file name}; exercises without a schema diagram are skipped.
    """
    wanted = {}
    for n, c in enumerate(challenges, 1):
        for b in (c.get("deep_dive") or {}).get("blocks") or []:
            if b.get("blockType") == "schema_diagram":
                names = [t["name"] for t in b["tables"]]
                wanted[n] = names + [col["name"] for t in b["tables"] for col in t["columns"]], names[0]
    if not wanted:
        return {}
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright not installed", file=sys.stderr)
        return {}
    img_dir = os.path.join(out_dir, "images")
    os.makedirs(img_dir, exist_ok=True)
    saved = {}
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1200, "height": 1000},
                                color_scheme="dark", device_scale_factor=2)

        def open_page():
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(5000)
            roots = page.locator("[data-challenge-ids]")
            return roots, [len((roots.nth(i).get_attribute("data-challenge-ids") or "").split())
                           for i in range(roots.count())]

        sizes = open_page()[1]
        if sum(sizes) != len(challenges):
            print("warning: widget exercise counts do not match the data; "
                  "schema diagrams not captured", file=sys.stderr)
            browser.close()
            return {}
        for n, (needles, first_name) in wanted.items():
            roots, _ = open_page()  # a fresh page each time keeps one exercise's state out of the next
            w, tab = 0, n - 1
            while tab >= sizes[w]:
                tab -= sizes[w]
                w += 1
            root = roots.nth(w)
            root.scroll_into_view_if_needed()
            if sizes[w] > 1:
                root.locator("button[role=tab]").nth(tab).click()
                page.wait_for_timeout(1500)
            solution = root.get_by_role("button", name="Solution").first
            solution.scroll_into_view_if_needed()
            solution.click()
            page.wait_for_timeout(600)
            gate = page.get_by_role("button", name="Show the solution")
            if gate.count():
                gate.first.click()
                page.wait_for_timeout(2500)
            best, best_area = None, None
            candidates = root.get_by_text(first_name, exact=True)
            for i in range(candidates.count()):  # the name also appears in prose: take the tightest container
                el = page.evaluate_handle(_FIND, [candidates.nth(i).element_handle(), needles]).as_element()
                if el:
                    box = el.bounding_box()
                    if best is None or box["width"] * box["height"] < best_area:
                        best, best_area = el, box["width"] * box["height"]
            if best is None:
                print(f"warning: schema diagram for exercise {n} not found on the page", file=sys.stderr)
                continue
            page.evaluate(_HIDE_FIXED)
            best.scroll_into_view_if_needed()
            page.wait_for_timeout(1000)
            if page.evaluate(_CLIPPED, best):  # taller than its scrolling box: lift the limit and retry
                page.evaluate(_UNCLIP, best)
                best.scroll_into_view_if_needed()
                page.wait_for_timeout(1000)
            name = f"{slug}_exercise_{n}_schema.png"
            best.screenshot(path=os.path.join(img_dir, name))
            saved[n] = name
        browser.close()
    return saved


def screenshot_diagrams(url, out_dir, slug):
    """Save every pipeline diagram block as <slug>_diagram_<n>.png.

    Returns the file names in page order (empty if none or playwright is missing).
    Names include the lesson slug so lessons never overwrite each other's images.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright not installed: .venv/bin/pip install playwright && "
              ".venv/bin/playwright install chromium", file=sys.stderr)
        return []
    img_dir = os.path.join(out_dir, "images")
    os.makedirs(img_dir, exist_ok=True)
    names = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        # same layout, 2x the pixels: sharper images without changing the page
        page = browser.new_page(viewport={"width": 1000, "height": 900},
                                color_scheme="light", device_scale_factor=2)
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(4000)
        blocks = page.locator(DIAGRAM_SELECTOR)  # all diagram kinds, in page order
        for i in range(blocks.count()):
            name = f"{slug}_diagram_{i + 1}.png"
            blocks.nth(i).scroll_into_view_if_needed()
            blocks.nth(i).screenshot(path=os.path.join(img_dir, name))
            names.append(name)
        browser.close()
    return names


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        sys.exit(__doc__)
    url = args[0] if args[0].startswith("http") else BASE + args[0]
    out_dir = args[1] if len(args) > 1 else "lesson_preview"
    req = urllib.request.Request(url, headers={"User-Agent": "lesson-to-md/1.0"})
    html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
    os.makedirs(out_dir, exist_ok=True)
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    images = screenshot_diagrams(url, out_dir, slug) if "--screenshots" in sys.argv else []
    path = os.path.join(out_dir, slug + ".md")
    with open(path, "w", encoding="utf-8") as f:
        challenges = fetch_challenges(slug)
        groups = exercise_groups(url, challenges) if challenges else []
        shots = (screenshot_schema_diagrams(url, out_dir, slug, challenges)
                 if challenges and "--screenshots" in sys.argv else {})
        sections, start = [], 0
        for anchor, group in groups:
            sections.append((anchor, exercises_markdown(group, shots, start)))
            start += len(group)
        f.write(convert(html, images, sections))
    print(path)


if __name__ == "__main__":
    main()
