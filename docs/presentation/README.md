# How FreePastry defends itself (slide deck)

A 16-slide deck that explains how Pastry routes, three use cases (joining the ring, sending a message, storing and finding an object), the threat model, which attacks the 2026 hardening stops and which it does not stop yet, the accepted risks (bootstrap nodes, the CA), and the roadmap (waves A to D). Status as of 27 November 2026.

- [freepastry-defence.pdf](freepastry-defence.pdf): the slides as a PDF, one slide per page.
- [freepastry-defence.html](freepastry-defence.html): the same slides with speaker notes under each slide. Open it in a browser after cloning (GitHub shows HTML as source).
- `source/`: the slide sources (`deck.json` sets the order, `deck.css` the shared look, `slides/<id>.html` one slide each, with speaker notes in `<aside>`).

## Rebuild after editing the sources

```sh
python3 docs/presentation/build.py
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --no-pdf-header-footer \
  --virtual-time-budget=8000 --print-to-pdf="$PWD/docs/presentation/freepastry-defence.pdf" \
  "file://$PWD/docs/presentation/freepastry-defence.html"
```

The fonts (IBM Plex Sans, JetBrains Mono) load from Google Fonts; without network access the page falls back to Arial and Courier New.
