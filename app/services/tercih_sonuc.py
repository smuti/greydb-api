"""Tercih izleme: bir tercihin (matchforge günlük listesindeki kategori + seçim metni) maç sonucuna göre tutup tutmadığı ve iddaa olayından oranı.

Saf fonksiyonlar (DB ve ağ yok). Kategori ve seçim metinleri matchforge `shot_model/liste.py` çıktısıdır; iddaa piyasa kodları (st) aynı dosyadaki `ST` tablosudur.
Maç sonucu verisi: `skor` {"home","away"}, `istat` matchforge `/matches/db` `stats` alanı (corners, yellow_cards, red_cards, shots_on_target; her biri {"home","away"}).
Kart puanı iddaa'daki gibi sarı 1, kırmızı 2. Karar verilemiyorsa (istatistik yok, bilinmeyen kategori, iade) None döner; sonuç ne tuttu ne tutmadı sayılır.
"""
import re
from typing import Optional

ST = dict(ms=1, cs=92, kg=89, gol=101, gol_ev=603, gol_dep=604, kor_kim=52, kor_top=48, kor_ev=822, kor_dep=823, kor_hcap=863, kart=658, kart_kim=824,
          n_faul=914, n_sot=913, n_sut=912)

_ALT_UST = re.compile(r"(Alt|Üst) (\d+(?:\.\d+)?)$")
_N_PLUS = re.compile(r"^(.*?) (\d+)\+(?: isabetli şut| faul| şut)?$")
_HCAP = re.compile(r"^Korner handikap (.+) ([+-]\d+(?:\.\d+)?)$")


def _yan(secim: str, ev: str, dep: str) -> Optional[str]:
    """Seçim metni ev ya da deplasman takım adıyla başlıyorsa "home" / "away"."""
    if secim.startswith(ev + " "):
        return "home"
    if secim.startswith(dep + " "):
        return "away"
    return None


def _cizgi(secim: str):
    m = _ALT_UST.search(secim)
    return (m.group(1), float(m.group(2))) if m else None


def _alt_ust(secim: str, deger: float) -> Optional[bool]:
    c = _cizgi(secim)
    if c is None:
        return None
    return deger > c[1] if c[0] == "Üst" else deger < c[1]


def _toplam(istat: dict, anahtar: str) -> Optional[float]:
    v = (istat or {}).get(anahtar)
    return None if not v or v.get("home") is None or v.get("away") is None else float(v["home"]) + float(v["away"])


def _yan_deger(istat: dict, anahtar: str, yan: str) -> Optional[float]:
    v = (istat or {}).get(anahtar)
    return None if not v or v.get(yan) is None else float(v[yan])


def _kart_puani(istat: dict, yan: str) -> Optional[float]:
    sari, kirmizi = _yan_deger(istat, "yellow_cards", yan), _yan_deger(istat, "red_cards", yan)
    return None if sari is None or kirmizi is None else sari + 2 * kirmizi


def _kim_daha_cok(secim: str, ev: str, dep: str, h: Optional[float], a: Optional[float]) -> Optional[bool]:
    if h is None or a is None:
        return None
    if secim.endswith("Eşit"):
        return h == a
    if secim.endswith(": " + ev):
        return h > a
    if secim.endswith(": " + dep):
        return a > h
    return None


def sonuclandir(kategori: str, secim: str, ev: str, dep: str, skor: Optional[dict], istat: Optional[dict]) -> Optional[bool]:
    istat = istat or {}
    h, a = (None, None) if not skor else (skor.get("home"), skor.get("away"))
    skor_var = h is not None and a is not None
    if kategori == "Maç sonucu":
        if not skor_var:
            return None
        t = secim.split()[1] if len(secim.split()) > 1 else ""
        return {"1": h > a, "0": h == a, "2": h < a}.get(t)
    if kategori == "Çifte şans":
        if not skor_var:
            return None
        return {"Çifte şans 1-0": h >= a, "Çifte şans 0-2": h <= a, "Çifte şans 1-2": h != a}.get(secim)
    if kategori == "Karşılıklı gol":
        if not skor_var:
            return None
        return (h > 0 and a > 0) == secim.endswith("Var") if secim.endswith(("Var", "Yok")) else None
    if kategori == "Toplam gol":
        return _alt_ust(secim, h + a) if skor_var else None
    if kategori == "Takım golü":
        y = _yan(secim, ev, dep)
        return _alt_ust(secim, float(h if y == "home" else a)) if skor_var and y else None
    if kategori == "Korner: kim daha çok":
        return _kim_daha_cok(secim, ev, dep, _yan_deger(istat, "corners", "home"), _yan_deger(istat, "corners", "away"))
    if kategori == "Takım korneri":
        y = _yan(secim, ev, dep)
        d = _yan_deger(istat, "corners", y) if y else None
        return None if d is None else _alt_ust(secim, d)
    if kategori == "Toplam korner":
        t = _toplam(istat, "corners")
        return None if t is None else _alt_ust(secim, t)
    if kategori == "Korner handikap":
        m = _HCAP.match(secim)
        ch, ca = _yan_deger(istat, "corners", "home"), _yan_deger(istat, "corners", "away")
        if not m or ch is None or ca is None or m.group(1) not in (ev, dep):
            return None
        fark = (ch - ca) if m.group(1) == ev else (ca - ch)
        toplam = fark + float(m.group(2))
        return None if toplam == 0 else toplam > 0                 # tam sayı handikapta eşitlik iade
    if kategori == "Toplam kart":
        ph, pa = _kart_puani(istat, "home"), _kart_puani(istat, "away")
        return None if ph is None or pa is None else _alt_ust(secim, ph + pa)
    if kategori == "Kim daha çok kart":
        return _kim_daha_cok(secim, ev, dep, _kart_puani(istat, "home"), _kart_puani(istat, "away"))
    if kategori in ("Takım isabetli şut", "Toplam isabetli şut", "Takım faul", "Toplam faul", "Takım şut"):
        anahtar = {"isabetli şut": "shots_on_target", "faul": "fouls", "şut": "shots"}[kategori.split(" ", 1)[1]]
        if _cizgi(secim) is not None:                                # "Toplam faul Üst 20.5"
            t = _toplam(istat, anahtar)
            return None if t is None else _alt_ust(secim, t)
        m = _N_PLUS.match(secim)
        if not m:
            return None
        k = int(m.group(2))
        if m.group(1).strip().lower() == "toplam" or m.group(1).startswith("Toplam"):
            t = _toplam(istat, anahtar)
            return None if t is None else t >= k
        y = "home" if m.group(1) == ev else "away" if m.group(1) == dep else None
        d = _yan_deger(istat, anahtar, y) if y else None
        return None if d is None else d >= k
    return None


def _pazar(olay: Optional[dict], st: int, cizgi: Optional[float] = None) -> Optional[dict]:
    for m in (olay or {}).get("m") or []:
        if m.get("st") == st and m.get("s") == 1 and (cizgi is None or ("sov" in m and abs(float(m["sov"]) - cizgi) < 1e-9)):
            return {x["n"]: x["odd"] for x in m.get("o") or []}
    return None


def _kim_oran(olay: Optional[dict], st: int, secim: str, ev: str, dep: str) -> Optional[float]:
    anahtar = "0" if secim.endswith("Eşit") else "1" if secim.endswith(": " + ev) else "2" if secim.endswith(": " + dep) else None
    return (_pazar(olay, st) or {}).get(anahtar) if anahtar else None


def oran_bul(kategori: str, secim: str, ev: str, dep: str, olay: Optional[dict]) -> Optional[float]:
    """Tercihin iddaa oranı (olay: iddaa olayının `data` alanı; `m` piyasa listesi). Piyasa ya da çizgi yoksa None."""
    if not olay or not olay.get("m"):
        return None
    if kategori == "Maç sonucu":
        return (_pazar(olay, ST["ms"]) or {}).get(secim.split()[1] if len(secim.split()) > 1 else "")
    if kategori == "Çifte şans":
        return (_pazar(olay, ST["cs"]) or {}).get({"Çifte şans 1-0": "1 ve 0", "Çifte şans 0-2": "0 ve 2", "Çifte şans 1-2": "1 ve 2"}.get(secim, ""))
    if kategori == "Karşılıklı gol":
        return (_pazar(olay, ST["kg"]) or {}).get("Var" if secim.endswith("Var") else "Yok" if secim.endswith("Yok") else "")
    c = _cizgi(secim)
    if kategori in ("Toplam gol", "Toplam korner", "Toplam kart") and c:
        st = {"Toplam gol": ST["gol"], "Toplam korner": ST["kor_top"], "Toplam kart": ST["kart"]}[kategori]
        return (_pazar(olay, st, c[1]) or {}).get(c[0])
    if kategori in ("Takım golü", "Takım korneri") and c:
        y = _yan(secim, ev, dep)
        if not y:
            return None
        st = (ST["gol_ev"], ST["gol_dep"]) if kategori == "Takım golü" else (ST["kor_ev"], ST["kor_dep"])
        return (_pazar(olay, st[0] if y == "home" else st[1], c[1]) or {}).get(c[0])
    if kategori == "Korner: kim daha çok":
        return _kim_oran(olay, ST["kor_kim"], secim, ev, dep)
    if kategori == "Kim daha çok kart":
        return _kim_oran(olay, ST["kart_kim"], secim, ev, dep)
    if kategori == "Korner handikap":
        m = _HCAP.match(secim)
        if not m or m.group(1) not in (ev, dep):
            return None
        v = float(m.group(2))
        return (_pazar(olay, ST["kor_hcap"], v if m.group(1) == ev else -v) or {}).get("1" if m.group(1) == ev else "2")
    if kategori in ("Takım isabetli şut", "Toplam isabetli şut", "Takım faul", "Toplam faul", "Takım şut"):
        st = ST["n_sot"] if "isabetli" in kategori else ST["n_faul"] if "faul" in kategori else ST["n_sut"]
        m = _N_PLUS.match(secim)
        if not m or secim.startswith("İY"):
            return None
        ad = "Toplam" if m.group(1).startswith("Toplam") else m.group(1)
        return (_pazar(olay, st) or {}).get(f"{ad} {m.group(2)}+")
    return None
