#!/usr/bin/env python3
"""Builds the standalone presentation from source/ (the slide files of the deck).

Usage: python3 docs/presentation/build.py
Writes freepastry-defence.html next to this script. To make the PDF, print that
page with Chrome headless (see README.md in this directory).
"""
import html
import json
import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent
SOURCE = HERE / "source"

deck = json.loads((SOURCE / "deck.json").read_text(encoding="utf-8"))
css = (SOURCE / "deck.css").read_text(encoding="utf-8")
font_links = "\n".join(
    f'<link rel="stylesheet" href="{html.escape(face["href"])}">'
    for face in deck.get("faces", {}).values()
    if "href" in face
)

slides = []
for slide_id in deck["order"]:
    text = (SOURCE / "slides" / f"{slide_id}.html").read_text(encoding="utf-8")
    notes = ""
    match = re.search(r"<aside>(.*?)</aside>", text, re.S)
    if match:
        notes = match.group(1).strip()
        text = text[: match.start()] + text[match.end() :]
    slides.append(
        f'<div class="frame">{text.strip()}</div>\n'
        + (f'<p class="speaker-notes">{notes}</p>' if notes else "")
    )

page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(deck["title"])}</title>
{font_links}
<style>
{css}
/* standalone viewer: what the Slides app adds on its own */
table {{ border-collapse: collapse; }}
th, td {{ text-align: left; vertical-align: top; padding: 0.35em 0.6em; border-bottom: 1px solid #D5D1C6; }}
x-shape {{ display: block; }}
x-shape[kind="arrow-right"] {{ clip-path: polygon(0 30%, 60% 30%, 60% 0, 100% 50%, 60% 100%, 60% 70%, 0 70%); }}
/* standalone viewer */
html, body {{ margin: 0; background: #2B2F36; }}
* {{ animation: none !important; }}
.deck {{ display: flex; flex-direction: column; align-items: center; gap: 24px; padding: 24px 0 48px; }}
.frame {{ width: 1920px; height: 1080px; transform-origin: top left; overflow: hidden; }}
.frame > section {{ position: relative; width: 1920px; height: 1080px; overflow: hidden; }}
.speaker-notes {{ max-width: 960px; color: #C9D2E3; font: 18px/1.5 'IBM Plex Sans', Arial, sans-serif; margin: 0 16px; }}
@media print {{
  @page {{ size: 1920px 1080px; margin: 0; }}
  html, body {{ background: none; }}
  .deck {{ display: block; padding: 0; }}
  .frame {{ transform: none !important; margin: 0 !important; break-after: page; }}
  .speaker-notes {{ display: none; }}
}}
</style>
</head>
<body>
<div class="deck">
{chr(10).join(slides)}
</div>
<script>
function fit() {{
  if (window.matchMedia('print').matches) return;
  const scale = Math.min(1, (window.innerWidth - 32) / 1920);
  document.querySelectorAll('.frame').forEach(f => {{
    f.style.transform = 'scale(' + scale + ')';
    f.style.marginBottom = (1080 * scale - 1080) + 'px';
    f.style.marginRight = (1920 * scale - 1920) + 'px';
  }});
}}
window.addEventListener('resize', fit);
fit();
</script>
</body>
</html>
"""
(HERE / "freepastry-defence.html").write_text(page, encoding="utf-8")
print("wrote", HERE / "freepastry-defence.html", "with", len(slides), "slides")
