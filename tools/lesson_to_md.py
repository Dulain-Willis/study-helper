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
import os
import re
import sys
import urllib.request

from bs4 import BeautifulSoup
from bs4.builder import HTMLParserTreeBuilder
from markdownify import markdownify

BASE = "https://datadriven.io/learn/"
# Visual blocks that plain text can't represent; each is saved as a screenshot.
DIAGRAM_LABELS = {"pipeline_diagram": "Pipeline diagram", "schema_diagram": "Schema diagram"}
DIAGRAM_SELECTOR = ", ".join(f'[data-note-block-type="{k}"]' for k in DIAGRAM_LABELS)
NOISE = ["script", "style", "nav", "footer", "button", "svg", "canvas", "head"]

# Block types with a custom rule below, plus ones markdownify handles well as-is.
# Anything else triggers a warning so a new rule can be added.
CUSTOM_BLOCKS = {
    "static_code", "big_number", "tip", "callout", "fun_fact", "key_takeaways",
    "synthesis", "icon_list", "info_list", *DIAGRAM_LABELS,
    "multiple_choice", "schema_story",
}
# Interactive exercises: the answer options are buttons (stripped as noise) and the
# correct answers are not in the page, so what's left is a dangling question. Dropped.
SKIPPED_BLOCKS = {"multiple_choice", "schema_story"}
GENERIC_BLOCKS = {
    "lesson_title", "learning_objectives", "section_header", "h3", "body_text",
    "rich_text", "static_table", "side_by_side_tables", "comparison",
    "best_practices", "note_text", "rich_info_list", "h4",
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


def convert(html, diagram_images=()):
    """diagram_images: screenshot file names, in page order, one per diagram block."""
    soup = parse(html)
    clean(soup)
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
        f.write(convert(html, images))
    print(path)


if __name__ == "__main__":
    main()
