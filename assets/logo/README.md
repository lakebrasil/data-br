# lakebrasil — brand assets

Logo files for the lakebrasil project. Public domain (MIT license,
same as the engine).

## Files

| File | Use case | Format |
|---|---|---|
| `icon.svg` | Master icon (128x128 viewport, scalable) | SVG |
| `icon-256.png` `icon-512.png` | App icons, slack avatar, etc. | PNG |
| `wordmark.svg` | Horizontal lockup (icon + text), light backgrounds | SVG |
| `wordmark.png` | Same, raster (960x256) | PNG |
| `wordmark-dark.svg` | Same lockup for dark backgrounds | SVG |
| `favicon.svg` | Browser tab favicon (32x32 optimized) | SVG |
| `social-card-1200.png` | GitHub social preview (1200x320) | PNG |

## Concept

A unified iceberg silhouette sliced into 4 horizontal data strata —
references both **lake** (waterbody / data lake) and **Apache Iceberg**
(the underlying lakehouse format that backs the engine).

Colors are the Brazilian palette — Amazon green at the peak, sun
yellow, river blue, iceberg navy at the base — ordered as geological
strata rather than the flag, so it's recognizable without being
flag-spam.

## Wordmark typography

System sans (San Francisco / Segoe UI / Inter) at weight 700,
letter-spacing -2. "lake" in navy `#143962`, "brasil" in green `#0a8754`.

## Color palette

```
Green  forest    #0a8754  →  #0d9963   (lake bottom / "brasil" wordmark)
Yellow warm sun  #e8b03a  →  #f4c542   (second layer)
Blue   river     #2d7eb0  →  #3b9ed4   (third layer)
Blue   navy      #143962  →  #1f4e8c   (top layer / "lake" wordmark)
```

## Usage

Free to use under MIT. Please don't:
- Stretch or distort the logo (use SVG to keep proportions)
- Recolor it to non-brand colors (the BR palette is intentional)
- Add drop shadows, glows, or 3D effects

Otherwise — embed it anywhere you mention lakebrasil. PRs welcome
for additional variants (monochrome, single-color, etc.).
