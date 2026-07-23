# dtex brand assets

`dtex-icon.svg` — the dtex mark: three source rows converging into a single
destination point (extract → load). The converging form doubles as an
arrowhead without being a generic arrow glyph.

The SVG uses `currentColor`, so it inherits the surrounding text colour and
works on light and dark backgrounds with no separate variant. PNGs are
rendered black-on-transparent; recolour by re-rendering the SVG.

Sizes: 512 / 256 / 128 / 64 / 32 / 16 px. Legible down to 16px (favicon).

Regenerate the PNGs after editing the SVG:

```sh
for s in 512 256 128 64 32 16; do
  sips -s format png --resampleWidth $s dtex-icon.svg --out "dtex-icon-$s.png"
done
```
