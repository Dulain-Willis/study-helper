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
import sys
import urllib.request

from bs4 import BeautifulSoup
from markdownify import markdownify

BASE = "https://datadriven.io/learn/"
DIAGRAM_BLOCK = "pipeline_diagram"
NOISE = ["script", "style", "nav", "footer", "button", "svg", "canvas", "head"]


def block_type(tag):
    return tag.get("data-note-block-type")


def clean(soup):
    for t in soup.select(".sr-only"):
        t.decompose()
    for t in soup(NOISE):
        t.decompose()
    # decorative icons and logos carry no content
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if "data_icons" in src or "logo" in src:
            img.decompose()


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
        p = soup.new_tag("p")
        p.append(BeautifulSoup(f"<strong>{label}</strong>", "html.parser"))
        quote.append(p)
    for child in list(block.children):
        quote.append(child.extract())
    block.replace_with(quote)


def diagram(soup, block, image_name):
    if image_name:
        p = soup.new_tag("p")
        p.append(soup.new_tag("img", src=f"images/{image_name}", alt="Pipeline diagram"))
        block.replace_with(p)
    else:
        note = soup.new_tag("p")
        note.string = "[Pipeline diagram: not captured. Re-run with --screenshots]"
        block.replace_with(note)


def convert(html, diagram_image=None):
    soup = BeautifulSoup(html, "html.parser")
    clean(soup)
    for block in soup.find_all(attrs={"data-note-block-type": True}):
        kind = block_type(block)
        if kind == "static_code":
            code_block(soup, block)
        elif kind == "big_number":
            big_numbers(soup, block)
        elif kind == "tip":
            callout(soup, block, "TIP")
        elif kind in ("callout", "fun_fact"):
            callout(soup, block)
        elif kind == DIAGRAM_BLOCK:
            diagram(soup, block, diagram_image)
    # star ratings are canvas-drawn by JS; the bare labels carry no content
    for div in soup.find_all("div"):
        if div.get_text(strip=True) in ("Daily Life", "Interviews") and not div.find("div"):
            div.decompose()
    md = markdownify(str(soup), heading_style="ATX", bullets="-", code_language="sql")
    return "\n".join(line.rstrip() for line in md.splitlines()).strip() + "\n"


def screenshot_diagrams(url, out_dir):
    """Save the pipeline diagram block as a PNG. Returns the file name or None."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright not installed: .venv/bin/pip install playwright && "
              ".venv/bin/playwright install chromium", file=sys.stderr)
        return None
    img_dir = os.path.join(out_dir, "images")
    os.makedirs(img_dir, exist_ok=True)
    name = "pipeline_diagram.png"
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1000, "height": 900}, color_scheme="light")
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(4000)
        target = page.locator(f'[data-note-block-type="{DIAGRAM_BLOCK}"]').first
        if target.count() == 0:
            browser.close()
            return None
        target.scroll_into_view_if_needed()
        target.screenshot(path=os.path.join(img_dir, name))
        browser.close()
    return name


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        sys.exit(__doc__)
    url = args[0] if args[0].startswith("http") else BASE + args[0]
    out_dir = args[1] if len(args) > 1 else "lesson_preview"
    req = urllib.request.Request(url, headers={"User-Agent": "lesson-to-md/1.0"})
    html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
    os.makedirs(out_dir, exist_ok=True)
    image = screenshot_diagrams(url, out_dir) if "--screenshots" in sys.argv else None
    path = os.path.join(out_dir, url.rstrip("/").rsplit("/", 1)[-1] + ".md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(convert(html, image))
    print(path)


if __name__ == "__main__":
    main()
