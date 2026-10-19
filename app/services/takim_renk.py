"""Logodan takım rengi: en çok piksele sahip doygun renk kümesinin ortalaması, koyu zeminde okunsun diye canlandırılır.
Nötr (siyah-beyaz-gri) logoda ya da okunamayan dosyada NOTR döner; admin gerekirse elle renk seçer."""
import colorsys
import io
from collections import Counter

NOTR = "#C9CED9"
_KOVA = 24


def baskin_renk(png: bytes) -> str:
    try:
        from PIL import Image
        im = Image.open(io.BytesIO(png)).convert("RGBA")
        im.thumbnail((64, 64))
        px = im.load()
        pikseller = [px[x, y] for y in range(im.height) for x in range(im.width)]
    except Exception:
        return NOTR
    sayac: Counter = Counter()
    kayit = []
    for r, g, b, a in pikseller:
        if a < 200:
            continue
        h, s, v = colorsys.rgb_to_hsv(r / 255, g / 255, b / 255)
        if s < 0.45 or v < 0.35:
            continue
        kova = round(h * _KOVA) % _KOVA
        sayac[kova] += 1
        kayit.append((kova, r, g, b))
    if not sayac:
        return NOTR
    en_cok = sayac.most_common(1)[0][0]
    secili = [(r, g, b) for k, r, g, b in kayit if k == en_cok]
    n = len(secili)
    r, g, b = (sum(c[i] for c in secili) / n for i in range(3))
    h, s, v = colorsys.rgb_to_hsv(r / 255, g / 255, b / 255)
    rr, gg, bb = (round(x * 255) for x in colorsys.hsv_to_rgb(h, max(s, 0.6), max(v, 0.85)))
    return f"#{rr:02X}{gg:02X}{bb:02X}"
