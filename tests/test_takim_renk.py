"""Takım rengi: logodan baskın doygun renk; koyu zeminde parlasın diye canlandırılır. Nötr (siyah-beyaz) logoda açık gri."""
import io

from PIL import Image

from app.services.takim_renk import baskin_renk, NOTR


def png(piksel_rengi, boyut=(40, 40), seffaf_cerceve=True):
    im = Image.new("RGBA", boyut, (0, 0, 0, 0) if seffaf_cerceve else piksel_rengi)
    for x in range(10, 30):
        for y in range(10, 30):
            im.putpixel((x, y), piksel_rengi)
    b = io.BytesIO(); im.save(b, "PNG"); return b.getvalue()


def test_kirmizi_logo_kirmizi_ve_parlak():
    r = baskin_renk(png((200, 20, 30, 255)))
    kanallar = [int(r[i:i + 2], 16) for i in (1, 3, 5)]
    assert r.startswith("#") and len(r) == 7
    assert kanallar[0] > 200 and kanallar[0] > 3 * kanallar[1]     # kırmızı baskın, canlandırılmış


def test_seffaf_pikseller_sayilmaz_ve_notr_logoda_notr_renk():
    assert baskin_renk(png((255, 255, 255, 255))) == NOTR          # beyaz
    assert baskin_renk(png((20, 20, 20, 255))) == NOTR              # siyah
    assert baskin_renk(png((0, 0, 0, 0))) == NOTR                   # tamamen şeffaf


def test_cok_renkli_logoda_en_cok_piksel_olan_renk_kazanir():
    im = Image.new("RGBA", (40, 40), (0, 0, 0, 0))
    for x in range(40):
        for y in range(40):
            im.putpixel((x, y), (20, 120, 220, 255) if x < 30 else (220, 60, 30, 255))   # %75 mavi, %25 turuncu
    b = io.BytesIO(); im.save(b, "PNG")
    r = baskin_renk(b.getvalue())
    k = [int(r[i:i + 2], 16) for i in (1, 3, 5)]
    assert k[2] > k[0]                                               # mavi


def test_bozuk_veri_notr():
    assert baskin_renk(b"png degil") == NOTR
