"""Kupon hesap çekirdeği (saf fonksiyonlar): bacak sonucu, toplam oran, sistem kombinasyonları, kupon sonucu.

Kupon iki biçimde: kombine (sistem_k=None; tek maç da kombine) ya da sistem k/n (n bacaktan k'li tüm kombinasyonlar).
Sistemde 1 birim yatırılır ve kombinasyonlara eşit bölünür: ödeme = tutan kombinasyonların oranları toplamı / kombinasyon sayısı.
İptal bacağın oranı 1,00 sayılır. Sistem sonucu: ödeme > 1 tuttu, = 1 iade (void), < 1 yattı.
"""
import re
import unicodedata
from itertools import combinations
from math import comb, prod
from typing import Any, Dict, List, Optional

_ESIK = 1e-9


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _cizgi(s: str) -> Optional[float]:
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*$", s)
    return float(m.group(1).replace(",", ".")) if m else None


VERI_YOK = "veri_yok"                # market tanınıyor ama bu maçta gerekli veri yok (admin elle işaretler)
DESTEKLENMIYOR = "desteklenmiyor"    # market tanınmıyor ya da elimizdeki verilerle belirlenemez (admin elle işaretler)


class _VeriYok(Exception):
    pass


def _tr(s: str) -> str:
    return (s or "").replace("İ", "i").replace("I", "ı").lower()


def _sade(s: str) -> str:
    """Aksan ve noktalama farkını yok sayan oyuncu/takım anahtarı."""
    s = unicodedata.normalize("NFKD", _tr(s))
    s = "".join(c for c in s if not unicodedata.combining(c)).replace("ı", "i")
    return " ".join(re.sub(r"[^a-z0-9 ]", " ", s).split())


def _ad_normalle(market: str):
    """Market adını tek yazıma indirir (iddaa ve scraper adları aynı aileye düşer); sondaki çizgi/handikap ayrılır."""
    s = _tr(market)
    s = re.sub(r"\(\s*uzt\.?\s*dahil\s*\)", "", s)
    s = re.sub(r"1\.\s*yarı\s*ve\s*2\.\s*yarıda", "iy ve 2y da", s)
    s = re.sub(r"\b(1\.\s*yarı|ilk yarı)\b", "iy", s)
    s = re.sub(r"\b(2\.\s*yarı|ikinci yarı)\b", "2y", s)
    s = re.sub(r"alt[ıi]?\s*/\s*üst[üu]?", "a/ü", s)
    s = re.sub(r"\s*/\s*", "/", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" ve iy a/ü", " ve a/ü").replace(" ve iy karşılıklı gol", " ve karşılıklı gol")
    s = s.replace("karşılıklı gol ve a/ü", "a/ü ve karşılıklı gol")
    s = s.replace("hat-trick", "hattrick").replace("her iki yarıda alt 1.5", "her iki yarıda da alt 1.5")
    s = s.replace("yarı kazanır mı", "yarı kazanır")
    if s.startswith("her iki yarıda da "):
        return s, None
    ort = re.search(r"a/ü\s+(\d+[.,]\d+)\s+ve\b", s)
    if ort:
        return (s[: ort.start(1)] + s[ort.end(1):]).replace("a/ü  ve", "a/ü ve").strip(), ort.group(1).replace(",", ".")
    m = re.search(r"\s+(-?\d+(?:[.,]\d+)?|\d+:\d+)$", s)
    if m and "oyuncu" not in s:
        return s[: m.start()].strip(), m.group(1).replace(",", ".")
    return s, None


def _p12(p: str) -> Optional[str]:
    return {"1": "1", "ev": "1", "ev sahibi": "1", "home": "1", "0": "0", "x": "0", "beraberlik": "0", "draw": "0", "2": "2", "dep": "2", "deplasman": "2", "away": "2"}.get(_tr(p).strip())


def _s12(h: int, a: int) -> str:
    return "1" if h > a else "2" if a > h else "0"


def _cs(pick: str) -> Optional[set]:
    ch = set(re.findall(r"[12x0]", re.sub(r"\bve\b", " ", _tr(pick))))
    ch = {"0" if c == "x" else c for c in ch}
    return ch if ch in ({"1", "0"}, {"1", "2"}, {"0", "2"}) else None


def _won(c: bool) -> str:
    return "won" if c else "lost"


def _evet(p: str) -> Optional[bool]:
    p = _tr(p).strip()
    return True if p in ("evet", "var", "yes") else False if p in ("hayır", "yok", "no") else None


def _au(toplam: float, cizgi: Optional[float], pick: str) -> str:
    p = _tr(pick).strip()
    if cizgi is None or p not in ("alt", "üst", "under", "over"):
        return DESTEKLENMIYOR
    if toplam == cizgi:
        return "void"
    return _won((toplam > cizgi) == (p in ("üst", "over")))


def _aralik(pick: str, deger: int) -> Optional[bool]:
    p = re.sub(r"[^0-9+\-–]", "", _tr(pick).replace("gol", ""))
    m = re.fullmatch(r"(\d+)[-–](\d+)", p)
    if m:
        return int(m.group(1)) <= deger <= int(m.group(2))
    m = re.fullmatch(r"(\d+)\+", p)
    if m:
        return deger >= int(m.group(1))
    return deger == int(p) if p.isdigit() else None


class _Mac:
    """Maç verisi sarmalayıcı: skor, devre skoru (olaylardan türetilebilir), istatistik, olaylar, oyuncular."""

    def __init__(self, d: Dict[str, Any]):
        self.h, self.a = d["home"], d["away"]
        self.home_name, self.away_name = d.get("home_name") or "", d.get("away_name") or ""
        self._ht = d.get("ht")
        self.events = d.get("events")
        self.stats = d.get("stats") or {}
        self.players = d.get("players") or []

    # -- goller
    def goller(self):
        if self.events is None:
            raise _VeriYok("olay yok")
        g = [e for e in self.events if e.get("tur") == "GOAL"]
        h = sum(1 for e in g if (e.get("skor_takimi") or e.get("takim")) == "home")
        a = sum(1 for e in g if (e.get("skor_takimi") or e.get("takim")) == "away")
        if (h, a) != (self.h, self.a):
            raise _VeriYok("olaylar skorla tutmuyor")
        return sorted(g, key=lambda e: (e.get("dakika") or 0, e.get("ek_dakika") or 0))

    def ht(self):
        if self._ht is not None:
            return tuple(self._ht)
        h = a = 0
        for e in self.goller():
            if e.get("dakika") is None:
                raise _VeriYok("dakika yok")
            if e["dakika"] <= 45:
                if (e.get("skor_takimi") or e.get("takim")) == "home":
                    h += 1
                else:
                    a += 1
        return (h, a)

    def y2(self):
        h, a = self.ht()
        return (self.h - h, self.a - a)

    def ist(self, anahtar: str):
        v = self.stats.get(anahtar)
        if not v or v[0] is None or v[1] is None:
            raise _VeriYok(anahtar)
        return v


def _sonuc_kapsam(ad: str):
    return "iy" if ad.startswith("iy ") or ad.startswith("ev sahibi iy ") or ad.startswith("deplasman iy ") else "2y" if ad.startswith("2y ") or " 2y " in ad else "ft"


def _kapsamsiz(ad: str) -> str:
    return re.sub(r"\b(iy|2y) ", "", ad).strip()


def _takim_ad_eslestir(isim: str, m: _Mac) -> Optional[str]:
    from app.services.iddaa_eslestir import takim_benzer
    e, d = takim_benzer(isim, m.home_name), takim_benzer(isim, m.away_name)
    return "home" if e and not d else "away" if d and not e else None


_ISTAT = {"takım şut": "shots", "takım isabetli şut": "sot", "takım faul": "fouls", "takım ofsayt": "offsides", "takım topa sahip olma": "possession"}


def _bacak_hesapla(market_name: str, pick: str, m: _Mac) -> str:
    ad, cz = _ad_normalle(market_name)
    cizgi = float(cz) if cz and re.fullmatch(r"-?\d+(?:\.\d+)?", cz) else None
    p = _tr(pick).strip()

    # ---------- oyuncu marketleri ----------
    if ad.startswith("oyuncu ") or ad.startswith("kaleci kurtarışı"):
        return _oyuncu_bacak(ad, pick, m)

    # ---------- takım istatistikleri ----------
    if ad in _ISTAT:
        mm = re.fullmatch(r"(.+?)\s+(\d+)\+", pick.strip())
        if not mm:
            return DESTEKLENMIYOR
        taraf = _takim_ad_eslestir(mm.group(1), m)
        if taraf is None:
            raise _VeriYok("takım adı eşleşmedi")
        v = m.ist(_ISTAT[ad])[0 if taraf == "home" else 1]
        return _won(v >= int(mm.group(2)))

    # ---------- korner ----------
    if "korner" in ad:
        if ad.startswith("iy ") or "iy toplam" in ad or "iy korner" in ad or ad.startswith(("ev sahibi iy", "deplasman iy")):
            raise _VeriYok("devre korneri saklanmıyor")
        h, a = m.ist("corners")
        if ad in ("toplam korner a/ü", "korner a/ü", "korner sayısı a/ü"):
            return _au(h + a, cizgi, pick)
        if ad == "ev sahibi toplam korner a/ü":
            return _au(h, cizgi, pick)
        if ad == "deplasman toplam korner a/ü":
            return _au(a, cizgi, pick)
        if ad in ("kim daha çok korner kullanır", "hangi takım daha çok korner kullanır"):
            sec = _p12(pick)
            return DESTEKLENMIYOR if sec is None else _won(sec == _s12(h, a))
        if ad == "korner tek/çift":
            return _won((h + a) % 2 == (1 if p == "tek" else 0)) if p in ("tek", "çift") else DESTEKLENMIYOR
        if ad == "toplam korner aralığı":
            r = _aralik(pick, h + a)
            return DESTEKLENMIYOR if r is None else _won(r)
        if ad == "korner handikap" and cizgi is not None and p in ("1", "2"):
            fark = (h + cizgi) - a
            if fark == 0:
                return "void"
            return _won((fark > 0) == (p == "1"))
        return DESTEKLENMIYOR

    # ---------- kart ----------
    if ad == "kırmızı kart olur mu":
        r = m.ist("red")
        e = _evet(pick)
        return DESTEKLENMIYOR if e is None else _won((r[0] + r[1] > 0) == e)
    if "kart" in ad and ad.split()[0] in ("toplam", "ev", "deplasman", "kim", "hangi"):
        y, r = m.ist("yellow"), m.ist("red")
        if r[0] + r[1] > 0:
            raise _VeriYok("kırmızı kartın kaç kart sayıldığı belirsiz")
        if ad in ("toplam kart a/ü", "toplam kart sayısı a/ü"):
            return _au(y[0] + y[1], cizgi, pick)
        if ad == "ev sahibi toplam kart a/ü":
            return _au(y[0], cizgi, pick)
        if ad == "deplasman toplam kart a/ü":
            return _au(y[1], cizgi, pick)
        if ad == "kim daha çok kart görür":
            sec = _p12(pick)
            return DESTEKLENMIYOR if sec is None else _won(sec == _s12(y[0], y[1]))
        return DESTEKLENMIYOR

    # ---------- olay marketleri ----------
    if ad in ("ilk golü hangi takım atar", "son golü hangi takım atar"):
        g = m.goller()
        if not g:
            return _won(p in ("gol olmaz", "0", "olmaz"))
        e = g[0] if ad.startswith("ilk") else g[-1]
        sec = _p12(pick)
        return DESTEKLENMIYOR if sec is None else _won(sec == ("1" if (e.get("skor_takimi") or e.get("takim")) == "home" else "2"))
    if ad == "kendi kalesine gol olur mu":
        g = m.goller()
        e = _evet(pick)
        return DESTEKLENMIYOR if e is None else _won(any(x.get("kendi_kalesine") for x in g) == e)
    if ad in ("penaltı olur mu",):
        g = m.goller()
        var = any(x.get("penalti") for x in g)
        e = _evet(pick)
        if e is None:
            return DESTEKLENMIYOR
        if var:
            return _won(e)
        raise _VeriYok("kaçan penaltı bilinmiyor")

    # ---------- skor ve yarı aileleri ----------
    kap = _sonuc_kapsam(ad)
    taban = _kapsamsiz(ad)
    sk = (m.h, m.a) if kap == "ft" else (m.ht() if kap == "iy" else m.y2())
    h, a = sk
    toplam = h + a

    if taban == "maç sonucu" or (kap != "ft" and taban == "sonucu") or ad in ("iy sonucu", "2y sonucu"):
        sec = _p12(pick)
        return DESTEKLENMIYOR if sec is None else _won(sec == _s12(h, a))
    if taban in ("çifte şans",) or ad in ("iy çifte şans", "2y çifte şans"):
        sec = _cs(pick)
        return DESTEKLENMIYOR if sec is None else _won(_s12(h, a) in sec)
    if taban == "karşılıklı gol":
        e = _evet(pick)
        return DESTEKLENMIYOR if e is None else _won((h > 0 and a > 0) == e)
    if taban == "a/ü":
        return _au(toplam, cizgi, pick)
    if taban in ("ev sahibi a/ü", "deplasman a/ü"):
        return _au(h if taban.startswith("ev") else a, cizgi, pick)
    if taban == "toplam gol":
        r = _aralik(pick, toplam)
        return DESTEKLENMIYOR if r is None else _won(r)
    if taban in ("tek/çift", "tek/çift sayı"):
        return _won(toplam % 2 == (1 if p == "tek" else 0)) if p in ("tek", "çift") else DESTEKLENMIYOR
    if taban == "maç skoru" or ad == "iy skoru":
        mm = re.fullmatch(r"(\d+)\s*[:\-]\s*(\d+)", p)
        return DESTEKLENMIYOR if not mm else _won((int(mm.group(1)), int(mm.group(2))) == (h, a))
    if ad in ("beraberlikte iade", "ev/deplasman", "iy beraberlikte iade"):
        sec = _p12(pick)
        if sec not in ("1", "2"):
            return DESTEKLENMIYOR
        return "void" if h == a else _won(sec == _s12(h, a))
    if ad == "handikaplı maç sonucu" and cz is not None:
        if ":" in cz:
            x, y = cz.split(":")
            hh, aa = h + int(x), a + int(y)
        else:
            hh, aa = h + float(cz), a
        sec = _p12(pick)
        return DESTEKLENMIYOR if sec is None else _won(sec == _s12(hh, aa))
    if ad == "hangi takım kaç farkla kazanır":
        fark = h - a
        mm = re.fullmatch(r"(ev|dep)\s*(\d)(\+?)", p)
        if p == "0":
            return _won(fark == 0)
        if not mm:
            return DESTEKLENMIYOR
        n = int(mm.group(2)) * (1 if mm.group(1) == "ev" else -1)
        return _won(fark >= n if (mm.group(3) and n > 0) else fark <= n if mm.group(3) else fark == n)
    if ad in ("ev sahibi gol yemeden bitirir", "deplasman gol yemeden bitirir", "ev sahibi gol yemeden kazanır", "deplasman gol yemeden kazanır"):
        ev = ad.startswith("ev")
        temiz = (a == 0) if ev else (h == 0)
        kazanir = (h > a) if ev else (a > h)
        e = _evet(pick)
        return DESTEKLENMIYOR if e is None else _won((temiz and (kazanir or "bitirir" in ad)) == e)

    # birleşik (tam maç ya da devre)
    mm = re.fullmatch(r"(1|0|2)\s+ve\s+(alt|üst|var|yok)", p)
    if taban == "sonucu ve a/ü" or ad in ("maç sonucu ve a/ü", "iy sonucu ve a/ü"):
        if not mm or mm.group(2) not in ("alt", "üst"):
            return DESTEKLENMIYOR
        k = _au(toplam, cizgi, mm.group(2).capitalize() if mm.group(2) == "alt" else "Üst")
        return _won(mm.group(1) == _s12(h, a) and k == "won") if k in ("won", "lost") else k
    if ad in ("maç sonucu ve karşılıklı gol", "iy sonucu ve karşılıklı gol"):
        if not mm or mm.group(2) not in ("var", "yok"):
            return DESTEKLENMIYOR
        return _won(mm.group(1) == _s12(h, a) and ((h > 0 and a > 0) == (mm.group(2) == "var")))
    if ad == "a/ü ve karşılıklı gol":
        mk = re.fullmatch(r"(alt|üst)\s+ve\s+(var|yok)", p)
        if not mk:
            return DESTEKLENMIYOR
        k = _au(toplam, cizgi, "Alt" if mk.group(1) == "alt" else "Üst")
        return _won(k == "won" and ((h > 0 and a > 0) == (mk.group(2) == "var"))) if k in ("won", "lost") else k

    # devre skoru isteyen diğerleri
    if ad == "iy/maç sonucu":
        mk = re.fullmatch(r"([102x])\s*/\s*([102x])", p.replace("x", "0"))
        if not mk:
            return DESTEKLENMIYOR
        ih, ia = m.ht()
        return _won(mk.group(1) == _s12(ih, ia) and mk.group(2) == _s12(m.h, m.a))
    if ad == "iy/maç sonucu skorları":
        mk = re.fullmatch(r"(\d+)-(\d+)\s+(\d+)-(\d+)", p)
        if not mk:
            return DESTEKLENMIYOR
        ih, ia = m.ht()
        return _won((int(mk.group(1)), int(mk.group(2)), int(mk.group(3)), int(mk.group(4))) == (ih, ia, m.h, m.a))
    if ad in ("hangi yarıda daha fazla gol", "hangi yarıda daha fazla gol olur"):
        (a1, b1), (a2, b2) = m.ht(), m.y2()
        t1, t2 = a1 + b1, a2 + b2
        sec = {"1.": t1 > t2, "eşit": t1 == t2, "2.": t2 > t1}.get(p)
        return DESTEKLENMIYOR if sec is None else _won(sec)
    if ad in ("ev sahibi hangi yarıda daha fazla gol atar", "deplasman hangi yarıda daha fazla gol atar"):
        i = 0 if ad.startswith("ev") else 1
        t1, t2 = m.ht()[i], m.y2()[i]
        sec = {"1.": t1 > t2, "eşit": t1 == t2, "2.": t2 > t1}.get(p)
        return DESTEKLENMIYOR if sec is None else _won(sec)
    if ad in ("ev sahibi yarı kazanır", "deplasman yarı kazanır", "ev sahibi her iki yarıyı kazanır", "deplasman her iki yarıyı kazanır"):
        ev = ad.startswith("ev")
        (a1, b1), (a2, b2) = m.ht(), m.y2()
        w1 = (a1 > b1) if ev else (b1 > a1)
        w2 = (a2 > b2) if ev else (b2 > a2)
        e = _evet(pick)
        return DESTEKLENMIYOR if e is None else _won((w1 and w2 if "her iki" in ad else w1 or w2) == e)
    if ad in ("ev sahibi her iki yarıda da gol atar", "deplasman her iki yarıda da gol atar"):
        i = 0 if ad.startswith("ev") else 1
        e = _evet(pick)
        return DESTEKLENMIYOR if e is None else _won((m.ht()[i] > 0 and m.y2()[i] > 0) == e)
    if ad.startswith("her iki yarıda da "):
        mk = re.fullmatch(r"her iki yarıda da (alt|üst) (\d+(?:\.\d+)?)", ad)
        e = _evet(pick)
        if not mk or e is None:
            return DESTEKLENMIYOR
        c = float(mk.group(2))
        t1, t2 = sum(m.ht()), sum(m.y2())
        ok = (t1 < c and t2 < c) if mk.group(1) == "alt" else (t1 > c and t2 > c)
        return _won(ok == e)
    if ad == "iy ve 2y da karşılıklı gol olur":
        mk = re.fullmatch(r"(evet|hayır)\s*/\s*(evet|hayır)", p)
        if not mk:
            return DESTEKLENMIYOR
        (a1, b1), (a2, b2) = m.ht(), m.y2()
        return _won(((a1 > 0 and b1 > 0) == (mk.group(1) == "evet")) and ((a2 > 0 and b2 > 0) == (mk.group(2) == "evet")))
    return DESTEKLENMIYOR


def _isim_uyar(isim: str, oyuncu_adi: str) -> bool:
    a, b = set(_sade(isim).split()), set(_sade(oyuncu_adi).split())
    return bool(a) and bool(b) and (a <= b or b <= a)


def _oyuncu_bul(isim: str, m: _Mac) -> Dict[str, Any]:
    if not m.players:
        raise _VeriYok("oyuncu verisi yok")
    q = _sade(isim)
    tam = [p for p in m.players if _sade(p["ad"]) == q]
    if len(tam) == 1:
        return tam[0]
    aday = [p for p in m.players if _isim_uyar(isim, p["ad"])]
    if len(aday) == 1:
        return aday[0]
    raise _VeriYok("oyuncu bulunamadı ya da belirsiz")


_OYUNCU_SAYAC = {
    "şut çeker": "sut", "kaleyi bulan şut çeker": "isabetli_sut", "faul yapar": "faul_yapma", "faul alır": "faul_alma",
    "kurtarış yapar": "kurtaris", "pas yapar": "pas", "isabetli pas yapar": "isabetli_pas",
}


def _gol_sayisi(m: _Mac, oyuncu: Dict[str, Any]) -> int:
    try:
        return sum(1 for e in m.goller() if not e.get("kendi_kalesine") and _isim_uyar(e.get("oyuncu") or "", oyuncu["ad"]))
    except _VeriYok:
        if "gol" not in oyuncu:
            raise
        return int(oyuncu["gol"] or 0)


def _asist_sayisi(m: _Mac, oyuncu: Dict[str, Any]) -> int:
    try:
        return sum(1 for e in m.goller() if e.get("asist") and _isim_uyar(e["asist"], oyuncu["ad"]))
    except _VeriYok:
        if "asist" not in oyuncu:
            raise
        return int(oyuncu["asist"] or 0)


def _kart_sayisi(m: _Mac, oyuncu: Dict[str, Any], kirmizi: bool = False) -> int:
    if m.events is None:
        raise _VeriYok("olay yok")
    return sum(1 for e in m.events if "CARD" in (e.get("tur") or "") and (not kirmizi or "RED" in e["tur"]) and _isim_uyar(e.get("oyuncu") or "", oyuncu["ad"]))


def _oyuncu_bacak(ad: str, pick: str, m: _Mac) -> str:
    esik = re.fullmatch(r"oyuncu (\d+)\+ (.+)", ad)
    if esik:      # eşik market adında ("Oyuncu 3+ Şut Çeker"), seçim yalnız oyuncu adı
        n, fiil = int(esik.group(1)), esik.group(2)
        if fiil == "gol atar":
            return _won(_gol_sayisi(m, _oyuncu_bul(pick, m)) >= n)
        if fiil == "asist yapar":
            return _won(_asist_sayisi(m, _oyuncu_bul(pick, m)) >= n)
        if fiil not in _OYUNCU_SAYAC:
            return DESTEKLENMIYOR
        return _won(int(_oyuncu_bul(pick, m).get(_OYUNCU_SAYAC[fiil]) or 0) >= n)
    govde = re.sub(r"^(oyuncu|kaleci) ", "", ad).replace("kurtarışı", "kurtarış yapar") if ad.startswith("kaleci") else re.sub(r"^oyuncu ", "", ad)
    # seçimde eşik ("Mauro Icardi 5+") olan markets
    if govde in _OYUNCU_SAYAC or ad.startswith("kaleci kurtarışı"):
        fiil = "kurtarış yapar" if ad.startswith("kaleci") else govde
        mm = re.fullmatch(r"(.+?)\s+(\d+)\+", pick.strip())
        if not mm:
            return DESTEKLENMIYOR
        n = int(mm.group(2))
        if ad.startswith("kaleci") and _sade(mm.group(1)) == "toplam":
            if not m.players:
                raise _VeriYok("oyuncu verisi yok")
            return _won(sum(int(p.get("kurtaris") or 0) for p in m.players) >= n)
        return _won(int(_oyuncu_bul(mm.group(1), m).get(_OYUNCU_SAYAC[fiil]) or 0) >= n)
    desteklenen = ("gol atar", "ilk golü atar", "son golü atar", "2+ gol atar", "hattrick yapar", "asist yapar",
                   "gol atar veya asist yapar", "gol atar ve asist yapar", "kart görür", "kırmızı kart görür", "gol atar ve kart görür")
    if govde not in desteklenen:
        return DESTEKLENMIYOR
    o = _oyuncu_bul(pick, m)
    if govde == "gol atar":
        return _won(_gol_sayisi(m, o) >= 1)
    if govde == "2+ gol atar":
        return _won(_gol_sayisi(m, o) >= 2)
    if govde == "hattrick yapar":
        return _won(_gol_sayisi(m, o) >= 3)
    if govde in ("ilk golü atar", "son golü atar"):
        g = m.goller()
        if not g:
            return "lost"
        e = g[0] if govde.startswith("ilk") else g[-1]
        return _won(not e.get("kendi_kalesine") and _isim_uyar(e.get("oyuncu") or "", o["ad"]))
    if govde == "asist yapar":
        return _won(_asist_sayisi(m, o) >= 1)
    if govde == "gol atar veya asist yapar":
        return _won(_gol_sayisi(m, o) >= 1 or _asist_sayisi(m, o) >= 1)
    if govde == "gol atar ve asist yapar":
        return _won(_gol_sayisi(m, o) >= 1 and _asist_sayisi(m, o) >= 1)
    if govde == "kart görür":
        return _won(_kart_sayisi(m, o) >= 1)
    if govde == "kırmızı kart görür":
        return _won(_kart_sayisi(m, o, kirmizi=True) >= 1)
    return _won(_gol_sayisi(m, o) >= 1 and _kart_sayisi(m, o) >= 1)


def bacak_sonucu(market_name: str, pick: str, mac: Dict[str, Any]) -> str:
    """Maç verisinden bacak sonucu: 'won' | 'lost' | 'void' | VERI_YOK | DESTEKLENMIYOR.
    mac: {home, away (maç sonu skoru), home_name, away_name, ht: (ev, dep) opsiyonel, events: olay_listesi biçimi, stats: {anahtar: (ev, dep)},
    players: [{ad, takim, gol, asist, sut, isabetli_sut, faul_yapma, faul_alma, kurtaris, pas, isabetli_pas}]}.
    Devre skoru verilmezse gol olaylarından türetilir (olaylar skorla tutmuyorsa veri yok sayılır)."""
    try:
        return _bacak_hesapla(market_name or "", pick or "", _Mac(mac))
    except _VeriYok:
        return VERI_YOK


def _oran(b: Dict[str, Any]) -> float:
    o = b.get("odds")
    if not isinstance(o, (int, float)) or isinstance(o, bool) or o <= 0:
        raise ValueError("Her bacağın oranı 0'dan büyük olmalı")
    return float(o)


def toplam_oran(bacaklar: List[Dict[str, Any]]) -> Optional[float]:
    """Kombine toplam oran = bacak oranlarının çarpımı (2 ondalık). Bacak yoksa None."""
    if not bacaklar:
        return None
    return round(prod(_oran(b) for b in bacaklar), 2)


def kombinasyon_sayisi(n: int, k: int) -> int:
    return comb(n, k)


def _k_dogrula(n: int, k: Optional[int]) -> None:
    if k is not None and not (2 <= k <= n - 1):
        raise ValueError(f"Sistem k, 2 ile n-1 arasında olmalı (n={n}, k={k})")


def en_yuksek_odeme(bacaklar: List[Dict[str, Any]], sistem_k: Optional[int]) -> Optional[float]:
    """Hepsi tutarsa ödeme: kombinede toplam oran, sistemde kombinasyon oranlarının ortalaması."""
    if not bacaklar:
        return None
    if sistem_k is None:
        return toplam_oran(bacaklar)
    _k_dogrula(len(bacaklar), sistem_k)
    oranlar = [_oran(b) for b in bacaklar]
    kombiler = [prod(c) for c in combinations(oranlar, sistem_k)]
    return round(sum(kombiler) / len(kombiler), 2)


def kupon_sonucu(bacaklar: List[Dict[str, Any]], sistem_k: Optional[int]) -> Dict[str, Any]:
    """Bacak sonuçlarından kupon sonucu. Dönen: {sonuc: pending|won|lost|void, odeme (1 birim için), kombinasyon?: {toplam, tutan}}."""
    n = len(bacaklar)
    if n == 0:
        raise ValueError("Kuponda en az bir bacak olmalı")
    _k_dogrula(n, sistem_k)
    sonuclar = [b.get("result") or "pending" for b in bacaklar]

    if sistem_k is None:   # kombine
        if "lost" in sonuclar:
            return {"sonuc": "lost", "odeme": 0}
        if "pending" in sonuclar:
            return {"sonuc": "pending", "odeme": None}
        if all(s == "void" for s in sonuclar):
            return {"sonuc": "void", "odeme": 1.0}
        odeme = prod(1.0 if s == "void" else _oran(b) for b, s in zip(bacaklar, sonuclar))
        return {"sonuc": "won", "odeme": round(odeme, 4)}

    if "pending" in sonuclar:
        return {"sonuc": "pending", "odeme": None, "kombinasyon": {"toplam": comb(n, sistem_k), "tutan": None}}
    if all(s == "void" for s in sonuclar):
        return {"sonuc": "void", "odeme": 1.0, "kombinasyon": {"toplam": comb(n, sistem_k), "tutan": 0}}
    etkin = [1.0 if s == "void" else _oran(b) for b, s in zip(bacaklar, sonuclar)]
    toplam = comb(n, sistem_k)
    tutan, kazanc = 0, 0.0
    for idx in combinations(range(n), sistem_k):
        if any(sonuclar[i] == "lost" for i in idx):
            continue
        tutan += 1
        kazanc += prod(etkin[i] for i in idx)
    odeme = kazanc / toplam
    sonuc = "void" if abs(odeme - 1.0) < _ESIK else ("won" if odeme > 1.0 else "lost")
    return {"sonuc": sonuc, "odeme": round(odeme, 4), "kombinasyon": {"toplam": toplam, "tutan": tutan}}
