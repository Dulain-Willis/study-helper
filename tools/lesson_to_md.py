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
import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
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
    "multiple_choice", "schema_story", "best_practices", "comparison", "placed_challenge",
}
# Interactive exercise with no answers in the page, so what's left is a dangling question.
# Dropped. (multiple_choice used to be here; its answers ARE in the page's embedded JSON.)
SKIPPED_BLOCKS = {"schema_story", "placed_challenge"}  # placed_challenge: empty stub for Airflow exercises
GENERIC_BLOCKS = {
    "lesson_title", "learning_objectives", "section_header", "h3", "body_text",
    "rich_text", "static_table", "side_by_side_tables", "comparison",
    "note_text", "rich_info_list", "h4", "full_width",
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


MC_TOKEN = "@@MULTIPLE-CHOICE-{}@@"


def question_key(text):
    """Match key for a question: inline code is rendered without its quote marks
    (JSON has DATE '2026-04-22', the page shows DATE 2026-04-22), so drop quotes,
    backticks and whitespace before comparing."""
    return re.sub(r"[\s'\"`]", "", text)


def embedded_questions(html):
    """{question text: question dict} for every multiple_choice block in the page's JSON.

    The page is a Next.js app: lesson data sits in self.__next_f.push([1, "<chunk>"])
    script strings. The rendered options are buttons with no correct-answer marker, but
    the JSON holds is_correct and an explanation per option.
    """
    chunks = re.findall(r'self\.__next_f\.push\(\[1,("(?:[^"\\]|\\.)*")\]\)', html)
    text = "".join(json.loads(c) for c in chunks)
    decoder, found = json.JSONDecoder(), {}
    # Key order varies (a question with a code snippet has "code" before "type"), so
    # "type" is not always the first key. Walk back from each marker to the "{" that
    # opens a block object which decodes and holds this type.
    for m in re.finditer(r'"type":"multiple_choice"', text):
        start = m.start()
        for _ in range(300):
            start = text.rfind("{", 0, start)
            if start < 0:
                break
            try:
                q, end = decoder.raw_decode(text, start)
            except ValueError:
                continue
            if isinstance(q, dict) and q.get("type") == "multiple_choice" and "question" in q:
                found[question_key(q["question"])] = q
                break
    return found


def multiple_choice_markdown(q):
    """Question, lettered options, then a closed <details> with the answer and why the rest are wrong."""
    opts = [(chr(ord("A") + i), o) for i, o in enumerate(q["options"])]
    lines = [f"**{q['question']}**", ""]
    if q.get("code"):
        lines += [f"```{q.get('language', '')}", q["code"], "```", ""]
    lines += [f"- **{letter}.** {o['text']}" for letter, o in opts]
    lines += ["", "<details>", "<summary>Solution</summary>", ""]
    for letter, o in opts:
        if o.get("is_correct"):
            lines += [f"**{letter}.** {o['text']}", ""]
            if o.get("explanation"):
                lines += [o["explanation"], ""]
    wrong = [(letter, o) for letter, o in opts if not o.get("is_correct")]
    if wrong:
        lines += ["**Why the others are wrong**", ""]
        lines += [f"- **{letter}.** {o.get('explanation') or o['text']}" for letter, o in wrong]
        lines.append("")
    lines.append("</details>")
    return "\n".join(lines)


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


def animation_info(html):
    """Each animation block in page order: its mode labels and the caption under the player.

    Read before clean() strips the buttons. A block with toggles (Query-based / Log-based)
    has one radio button per mode; the caption is a paragraph right after the player.
    """
    soup = parse(html)
    out = []
    for block in soup.find_all(attrs={"data-note-block-type": re.compile(r"_animation$")}):
        labels = [b.get_text(strip=True) for b in block.find_all("button", attrs={"role": "radio"})]
        caption = ""
        for para in block.find_all("p"):
            before = para.find_previous_sibling()
            if before is not None and before.find("button"):
                caption = " ".join(para.get_text().split())
                break
        out.append({"labels": labels, "caption": caption})
    return out


def animation(soup, block, tokens, info, files):
    """An animation block as its GIF(s), one per mode, and the caption as plain text.

    The playback is drawn by JavaScript, so it can only be a recording (see record_animations).
    Built as Markdown and swapped in after markdownify.
    """
    parts = []
    for label, name in files or []:
        parts.append((f"**{label}**\n\n" if label else "") + f"![{label or 'Animation'}](images/{name})")
    if not parts:
        parts.append("[Animation: not captured. Re-run with --screenshots]")
    if info.get("caption"):
        parts.append(f"*{info['caption']}*")
    token = f"@@ANIMATION-{len(tokens)}@@"
    tokens[token] = "\n\n".join(parts)
    marker = soup.new_tag("p")
    marker.string = token
    block.replace_with(marker)


def convert(html, diagram_images=(), exercises=(), animations=None):
    """diagram_images: screenshot file names, in page order, one per diagram block.
    exercises: [(anchor block id or None, markdown)]; each goes right after its anchor block,
    and the ones with no anchor are returned by the caller to append."""
    animation_infos = animation_info(html)
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
    diagrams_seen = animations_seen = 0
    questions = embedded_questions(html)
    for block in soup.find_all(attrs={"data-note-block-type": True}):
        kind = block_type(block)
        if kind not in CUSTOM_BLOCKS | GENERIC_BLOCKS and not kind.endswith("_animation"):
            print(f"warning: no rule for block type '{kind}'; used generic conversion",
                  file=sys.stderr)
        if kind in SKIPPED_BLOCKS:
            block.decompose()
        elif kind == "multiple_choice":
            p_el = block.find("p")
            q = questions.get(question_key(p_el.get_text())) if p_el else None
            if q is None:
                print("warning: multiple_choice answers not found in the page; block dropped",
                      file=sys.stderr)
                block.decompose()
                continue
            token = MC_TOKEN.format(len(tokens))
            tokens[token] = multiple_choice_markdown(q)
            new = soup.new_tag("p")
            new.string = token
            block.replace_with(new)
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
        elif kind.endswith("_animation"):
            info = animation_infos[animations_seen] if animations_seen < len(animation_infos) else {}
            animation(soup, block, tokens, info, (animations or {}).get(animations_seen))
            animations_seen += 1
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
    # Text that starts with ">" (e.g. Airflow's ">> reads as 'then'") would become a Markdown
    # blockquote; escape it. Code is left alone, and the quotes the converter builds are
    # <blockquote> tags, not text, so they are unaffected.
    for text in soup.find_all(string=re.compile(r"^\s*>")):
        # only where a line starts: first text of a paragraph, list item or div (table cells and
        # text after other inline tags are not line starts, so ">=" there stays as it is)
        if text.find_parent(["code", "pre", "script", "style"]) or not starts_line(text):
            continue
        text.replace_with(re.sub(r"^(\s*)((?:>\s*)+)",
                                 lambda m: m.group(1) + "".join("\\>" for _ in m.group(2).replace(" ", "")), str(text)))
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


def starts_line(text):
    """True if this text is the very first thing in its paragraph, list item or div, even
    when wrapped in inline tags (the site styles inline code as a span, not <code>)."""
    node = text
    while True:
        prev = node.previous_sibling
        if prev is not None and (not isinstance(prev, str) or prev.strip()):
            return False
        node = node.parent
        if node is None:
            return False
        if node.name in ("p", "li", "div", "blockquote"):
            return True
        if node.name not in ("span", "strong", "em", "b", "i", "a"):
            return False  # table cells etc.: the line starts with the pipe, not this text


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
            rows = [f"- {x['name']} (" + ", ".join(p for p in (x.get("node_type"), x.get("tech_label")) if p) + ")"
                    for x in nodes]
            out.append("**Starting canvas** (already placed for you):\n\n" + "\n".join(rows))
        # The solution is folded away until clicked, so an exercise reads as its task first.
        # The blank line after <summary> is what lets Markdown render inside the fold.
        out.append("<details>\n<summary><strong>Solution</strong></summary>")
        out += [m for m in (solution_block(b, (images or {}).get(start + n))
                            for b in (c.get("deep_dive") or {}).get("blocks") or []) if m]
        out.append("</details>")
    return "\n\n".join(out)


# --- Airflow exercises (tools/airflow lessons) ----------------------------------------
# Same endpoint as above, challenge_type "airflow". Two forms: dag_repair (fix the DAG
# file) and grid_match (edit the DAG until the run grid matches a target). The page's live
# editor + Airflow UI becomes: task, starter file, what the learner is shown, then a
# folded solution with the canonical code and walkthrough.
STATE_LABELS = {
    "success": "✅ success", "failed": "❌ failed", "upstream_failed": "🟧 upstream_failed",
    "running": "🔄 running", "none": "⬜ no state", "skipped": "⏭ skipped",
}
OUTCOME_TEXT = {"success": "every try succeeds", "fail": "fails", "fail_no_retry": "fails, no retry"}


def duration(seconds):
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def grid_table(runs):
    """Run grid as a table: one row per task, one column per run (the site's Grid view)."""
    tasks = list(runs[0]["tasks"])
    head = "| task | " + " | ".join(r["logicalDate"][5:16].replace("T", " ") for r in runs) + " |"
    rows = [f"| `{t}` | " + " | ".join(STATE_LABELS.get(r["tasks"].get(t), str(r["tasks"].get(t))) for r in runs) + " |"
            for t in tasks]
    return "\n".join([head, "|---" * (len(runs) + 1) + "|"] + rows)


def task_lines(behaviors):
    out = []
    for b in behaviors:
        outcome = OUTCOME_TEXT.get((b.get("outcomes") or ["success"])[0], b["outcomes"][0])
        line = f"- `{b['taskId']}` · runs {duration(b.get('durationSeconds', 0))} · {outcome}"
        for r in b.get("byRun") or []:
            what = r.get("raises") or OUTCOME_TEXT.get(r["outcomes"][0], r["outcomes"][0])
            line += f"\\\n  on {r['logicalDate'][:16].replace('T', ' ')}: {what}"
        out.append(line)
    return "\n".join(out)


def challenge_path(c):
    sc = c["config"].get("scenario") or {}
    files = sc.get("files") or []
    return sc.get("file") or (files[0]["path"] if files else "dag.py")


# The widgets are drawn by the browser. Two kinds: dr-widget (dag_repair: file, then the
# Grid/Graph of the DEPLOYED starter) and gm-root (grid_match: file, task facts, airflow.cfg,
# Grid with Target / As given). A pane is only captured when it holds something: Grid needs
# cells, Graph needs nodes or an import error, Tasks / airflow.cfg need their card.
_AF_ANCHOR = """(root) => { let anchor = null;
    for (const b of document.querySelectorAll('[data-note-block]')) {
        const pos = b.compareDocumentPosition(root);
        if (!(pos & 4) || (pos & 16)) continue;   // keep blocks that come before root, not its parents
        const id = b.getAttribute('data-note-block');
        if (/^[si]\\|/.test(id)) anchor = id; }
    return anchor; }"""


def airflow_widgets(url, out_dir, slug, shoot):
    """Each Airflow widget in page order: {path, anchor, shots: {key: (caption, file)}}."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright not installed; Airflow exercises go at the end", file=sys.stderr)
        return []
    img_dir = os.path.join(out_dir, "images")
    os.makedirs(img_dir, exist_ok=True)
    widgets = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1400, "height": 1000},
                                color_scheme="light", device_scale_factor=2)
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(6000)
        roots = page.locator('[data-testid="dr-widget"], [data-testid="gm-root"]')
        for i in range(roots.count()):
            root = roots.nth(i)
            root.scroll_into_view_if_needed()
            page.wait_for_timeout(500)
            pre = "dr" if root.get_attribute("data-testid") == "dr-widget" else "gm"
            header = root.locator('[data-testid="af-file-header"], [data-testid="af-file-card"]').first.inner_text()
            m = re.search(r"dags/[\w./-]+\.py", header)
            shots = {}

            def snap(key, caption, loc):
                if not shoot:
                    return
                name = f"{slug}_ex{i + 1}_{key}.png"
                path = os.path.join(img_dir, name)
                loc.first.screenshot(path=path)
                with open(path, "rb") as fh:  # PNG header: width at bytes 16-20
                    pixels = int.from_bytes(fh.read(24)[16:20], "big")
                shots[key] = (caption, name, round(pixels / 2.5))  # shown 2.5x smaller than full size

            def click(text):
                root.get_by_text(re.compile(rf"^{text}$", re.I)).first.click()
                page.wait_for_timeout(600)

            grid = root.locator(f'[data-testid="{pre}-grid-page"]')
            graph = root.locator(f'[data-testid="{pre}-graph-page"]')
            if pre == "gm":
                for key, cap, loc in (("tasks", "Tasks", root.locator('[data-testid="af-facts"]')),
                                      ("cfg", "airflow.cfg", root.locator('[data-testid="af-config-card"]'))):
                    if loc.count():
                        snap(key, cap, loc)
                click("Grid")
                click("As given")
                if grid.locator('[data-testid="af-cell"]').count():
                    snap("grid_given", "Grid as given (your starter code)", grid)
                click("Target")
                if grid.locator('[data-testid="af-cell"]').count():
                    snap("grid_target", "Target grid (match this)", grid)
                click("As given")
                click("Graph")
                if graph.locator('[data-testid="af-graph-node"]').count():
                    snap("graph", "Graph as given (your starter code)", graph)
            else:
                click("Grid")
                if grid.locator('[data-testid="af-cell"]').count():
                    snap("grid", "Grid, deployed (your starter code)", grid)
                click("Graph")
                if graph.locator('[data-testid="af-graph-node"], [data-testid="af-import-errors"]').count():
                    snap("graph", "Graph, deployed (your starter code)", graph)
            widgets.append({"path": m.group(0) if m else None,
                            "anchor": root.evaluate(_AF_ANCHOR), "shots": shots})
        browser.close()
    return widgets


SHOT_ORDER = ["tasks", "cfg", "grid_given", "grid_target", "grid", "graph"]


def airflow_exercise_markdown(c, n, total, shots):
    cfg, sc = c["config"], c["config"].get("scenario") or {}
    form = cfg.get("form")
    titles = {"dag_repair": "Repair the DAG", "grid_match": "Match the grid"}
    out = [f"### Exercise {n} of {total}: {titles.get(form, form)}", f"**● TASK**\n\n{c['prompt']}"]
    files = sc.get("files") or []
    starter = (cfg.get("starter") or {}).get("source") or (files[0]["source"] if files else "")
    out.append(f"**Starter** — `{challenge_path(c)}`\n\n```python\n{starter}\n```")
    if shots:  # panes the page really shows, as the page draws them
        out += [f'**{shots[k][0]}**\n\n<img src="images/{shots[k][1]}" alt="{shots[k][0]}" width="{shots[k][2]}">'
                for k in SHOT_ORDER if k in shots]
    elif form == "grid_match":  # no screenshots: text versions of the same panes
        if sc.get("target"):
            out.append("**Target grid**\n\n" + grid_table(sc["target"]))
        if sc.get("behaviors"):
            out.append("**Tasks**\n\n" + task_lines(sc["behaviors"]))
    out.append("<details>\n<summary><strong>Solution</strong></summary>")
    # The graph after a correct fix only appears once the code is Run, and Run needs a login,
    # so it is not captured; the correct code is in the walkthrough below.
    out += [m for m in (solution_block(b) for b in (c.get("deep_dive") or {}).get("blocks") or []) if m]
    out.append("</details>")
    return "\n\n".join(out)


def airflow_sections(url, out_dir, slug, challenges):
    """[(anchor block id or None, markdown)]: each exercise where the page puts it."""
    widgets = airflow_widgets(url, out_dir, slug, "--screenshots" in sys.argv)
    by_path = {challenge_path(c): c for c in challenges}
    ordered = [(w, by_path.get(w["path"])) for w in widgets]
    if len(ordered) != len(challenges) or any(c is None for _, c in ordered):
        print("warning: Airflow widgets do not match the data; exercises go at the end", file=sys.stderr)
        return [(None, "\n\n".join(airflow_exercise_markdown(c, n, len(challenges), {})
                                   for n, c in enumerate(challenges, 1)))]
    sections = []
    for n, (w, c) in enumerate(ordered, 1):
        md = airflow_exercise_markdown(c, n, len(ordered), w["shots"])
        if sections and sections[-1][0] == w["anchor"]:  # same spot: keep them in order
            sections[-1] = (w["anchor"], sections[-1][1] + "\n\n" + md)
        else:
            sections.append((w["anchor"], md))
    return sections


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


# Animations (sql_animation, kafka_animation, pipeline_animation ...): a player that plays by
# itself once scrolled into view, with a step counter ("3/9") and sometimes mode toggles.
# Each mode is recorded as a screen video of the player alone, cropped, and made into a GIF.
GIF_FILTER = ("fps=15,scale=1000:-1:flags=lanczos,split[a][b];[a]palettegen=stats_mode=diff[p];"
              "[b][p]paletteuse=dither=sierra2_4a")
_STEP = re.compile(r"(\d+)\s*/\s*(\d+)")
_PLAYER_BOX = """(block) => { const bs = [...block.querySelectorAll('button')]; if (!bs.length) return null;
    let a = bs[0]; while (a && !bs.every(b => a.contains(b))) a = a.parentElement;
    const r = a.getBoundingClientRect(); return {x: r.x + scrollX, y: r.y + scrollY, w: r.width, h: r.height}; }"""


def record_animations(url, out_dir, slug, info):
    """Save every animation block's playback as a GIF: one per mode (toggle) if it has any.

    Returns {block number: [(mode label or None, file name)]}. The capture runs from the
    moment the mode starts until the step counter reaches its last step (two seconds' grace),
    so it needs no fixed length. The crop is the smallest box holding all the player's buttons,
    which leaves out the caption paragraph that sits below it (that goes into the Markdown).
    """
    if not info:
        return {}
    if not shutil.which("ffmpeg"):
        print("warning: ffmpeg not found; animations not captured", file=sys.stderr)
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
        for k, entry in enumerate(info):
            for m, label in enumerate(entry["labels"] or [None]):
                with tempfile.TemporaryDirectory() as tmp:
                    ctx = browser.new_context(viewport={"width": 1000, "height": 900}, color_scheme="light")
                    page = ctx.new_page()
                    page.goto(url, wait_until="domcontentloaded")
                    page.wait_for_timeout(3000)
                    block = page.locator('[data-note-block-type$="_animation"]').nth(k)
                    block.scroll_into_view_if_needed()  # the first mode plays by itself once in view
                    if m > 0:
                        page.wait_for_timeout(300)
                    box = block.evaluate(_PLAYER_BOX)
                    if not box:
                        ctx.close()
                        print(f"warning: animation {k + 1} has no player controls; not captured", file=sys.stderr)
                        break
                    # Each frame is a 2x screenshot of the player alone, taken as fast as the browser
                    # allows (about 14 per second). Playwright's recorded video is compressed and
                    # its frames are 1x, so the small labels in the diagrams (down to 6.5 px) turned
                    # fuzzy; at 2x they stay readable after the GIF is scaled down.
                    cdp = ctx.new_cdp_session(page)
                    clip = {"x": box["x"], "y": box["y"], "width": box["w"], "height": box["h"], "scale": 2}
                    if m > 0:
                        block.locator("button[role=radio]").nth(m).click()
                        page.wait_for_timeout(300)
                        play = block.locator('button[aria-label="Play"]')
                        if play.count():  # a switched mode starts paused (Airflow animations)
                            play.first.click()
                    stamps, done_since, last_hash = [], None, None
                    began = still_since = time.monotonic()
                    while time.monotonic() - began < 150:
                        stamp = time.monotonic() - began
                        shot = cdp.send("Page.captureScreenshot", {"format": "jpeg", "quality": 92, "clip": clip})
                        with open(os.path.join(tmp, f"f{len(stamps):05d}.jpg"), "wb") as fh:
                            fh.write(base64.b64decode(shot["data"]))
                        stamps.append(stamp)
                        steps = _STEP.findall(block.inner_text())
                        if steps and int(steps[-1][0]) >= int(steps[-1][1]):
                            done_since = done_since or time.monotonic()
                            if time.monotonic() - done_since >= 2:
                                break
                        elif not steps:  # no counter: stop once nothing has moved for 3 seconds
                            if hash(shot["data"]) != last_hash:
                                last_hash, still_since = hash(shot["data"]), time.monotonic()
                            elif time.monotonic() - still_since >= 3:
                                break
                    ctx.close()
                    if len(stamps) < 2:
                        print(f"warning: animation {k + 1} produced no frames; not captured", file=sys.stderr)
                        break
                    with open(os.path.join(tmp, "list.txt"), "w") as fh:  # each frame lasts until the next
                        for i, stamp in enumerate(stamps):
                            span = stamps[i + 1] - stamp if i + 1 < len(stamps) else 1.0
                            fh.write(f"file 'f{i:05d}.jpg'\nduration {max(span, 0.001):.4f}\n")
                        fh.write(f"file 'f{len(stamps) - 1:05d}.jpg'\n")
                    name = f"{slug}_animation_{k + 1}" + (f"_{re.sub(r'[^a-z0-9]+', '-', label.lower()).strip('-')}"
                                                          if label else "") + ".gif"
                    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
                                    "-i", os.path.join(tmp, "list.txt"),
                                    "-vf", GIF_FILTER, "-loop", "0",
                                    os.path.join(img_dir, name)], check=True)
                    saved.setdefault(k, []).append((label, name))
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
    animations = (record_animations(url, out_dir, slug, animation_info(html))
                  if "--screenshots" in sys.argv else {})
    path = os.path.join(out_dir, slug + ".md")
    with open(path, "w", encoding="utf-8") as f:
        challenges = fetch_challenges(slug)
        airflow = bool(challenges) and challenges[0].get("challenge_type") == "airflow"
        if airflow:
            sections = airflow_sections(url, out_dir, slug, challenges)
        else:
            groups = exercise_groups(url, challenges) if challenges else []
            shots = (screenshot_schema_diagrams(url, out_dir, slug, challenges)
                     if challenges and "--screenshots" in sys.argv else {})
            sections, start = [], 0
            for anchor, group in groups:
                sections.append((anchor, exercises_markdown(group, shots, start)))
                start += len(group)
        f.write(convert(html, images, sections, animations))
    print(path)


if __name__ == "__main__":
    main()
