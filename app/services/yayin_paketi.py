"""Yayın sayfası paketi: ham kaynak yanıtlarından (FotMob, kendi veritabanımız, istatistik servisi) kart verisi üretir (saf fonksiyonlar).

Çıktı anahtarları sitenin okuduğu sözleşmeyle aynıdır (apps/web/src/lib/yayin/tipler.ts). Veri eksikse fonksiyon None döner;
kart hiç üretilmez, sayfada atlanır (yarım kart yok). Dış servise ve veritabanına dokunmaz."""
from typing import Any, Dict, List, Optional, Tuple

_TUR = {"injury": "sakat", "suspension": "cezali"}
_DILIMLER = ("0-15", "16-30", "31-45", "46-60", "61-75", "76-90")


def eksik_listesi(takim: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """FotMob lineup.<takım>.unavailable: sakat (injury) ve cezalı (suspension); piyasa değerine göre azalan, değeri bilinmeyen sonda."""
    cikti = []
    for k in (takim or {}).get("unavailable") or []:
        tur = _TUR.get(((k.get("unavailability") or {}).get("type")))
        if tur is None or not k.get("name"):
            continue
        satir: Dict[str, Any] = {"ad": k["name"], "tur": tur}
        if k.get("marketValue") is not None:
            satir["deger"] = k["marketValue"]
        cikti.append(satir)
    return sorted(cikti, key=lambda x: -(x.get("deger") if x.get("deger") is not None else -1))


def puan_karti(tablo: List[Dict[str, Any]], ev_id: Any, dep_id: Any) -> Optional[Dict[str, Any]]:
    """tablo: puan_durumu.hesapla çıktısı. İki takım da tabloda olmalı."""
    kayit = {t["takim_id"]: t for t in tablo}
    if ev_id not in kayit or dep_id not in kayit:
        return None
    al = lambda t: {"sira": t["sira"], "puan": t["puan"], "averaj": t["averaj"]}
    return {"ev": al(kayit[ev_id]), "dep": al(kayit[dep_id])}


def form_karti(form_ev: Dict[str, Any], form_dep: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """mac_baglam.form_ozeti çıktıları (dizi EN YENİDEN eskiye). Yayında soldan sağa eskiden yeniye gösterilir."""
    def taraf(f):
        dizi = (f.get("ozet") or {}).get("dizi") or ""
        if not dizi:
            return None
        o = f["ozet"]
        return {"sonuclar": [c.lower() for c in reversed(dizi)], "atilan": o["atilan"], "yenilen": o["yenilen"]}
    ev, dep = taraf(form_ev), taraf(form_dep)
    return {"ev": ev, "dep": dep} if ev and dep else None


def h2h_karti(h2h: Dict[str, Any], ev_ad: str) -> Optional[Dict[str, Any]]:
    """mac_baglam.h2h_ozeti çıktısı: son 5 karşılaşmayı BU maçın ev sahibine göre sayar."""
    son5 = [m for m in (h2h.get("maclar") or []) if m.get("ev_gol") is not None and m.get("dep_gol") is not None][:5]
    if not son5:
        return None
    ev_g = dep_g = ber = 0
    for m in son5:
        ev_gol, dep_gol = (m["ev_gol"], m["dep_gol"]) if m["ev"] == ev_ad else (m["dep_gol"], m["ev_gol"])
        if ev_gol > dep_gol:
            ev_g += 1
        elif ev_gol < dep_gol:
            dep_g += 1
        else:
            ber += 1
    return {"evGalibiyet": ev_g, "beraberlik": ber, "depGalibiyet": dep_g}


def ic_dis_karti(ev_evinde: List[Tuple[int, int]], dep_deplasmanda: List[Tuple[int, int]]) -> Optional[Dict[str, Any]]:
    """Girdiler (atılan, yenilen) çiftleri: ev sahibinin EVİNDEKİ, deplasman takımının DEPLASMANDAKİ bu sezon maçları."""
    def taraf(maclar):
        if not maclar:
            return None
        g = sum(1 for a, y in maclar if a > y)
        b = sum(1 for a, y in maclar if a == y)
        return {"g": g, "b": b, "m": len(maclar) - g - b, "farkMac": sum(a - y for a, y in maclar) / len(maclar)}
    ev, dep = taraf(ev_evinde), taraf(dep_deplasmanda)
    return {"ev": ev, "dep": dep} if ev and dep else None


def _stat_ok(s: Optional[Dict[str, Any]]) -> bool:
    return bool(s) and (s.get("match_count") or 0) > 0 and bool(s.get("stats")) and bool(s.get("totals"))


def gol_karti(ev: Optional[Dict[str, Any]], dep: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """İstatistik servisi takım istatistikleri (stats + totals + match_count)."""
    if not (_stat_ok(ev) and _stat_ok(dep)):
        return None
    al = lambda s: {"gMac": s["totals"]["goals_scored"] / s["match_count"], "xg": s["stats"]["xg_per_match"]}
    return {"ev": al(ev), "dep": al(dep)}


def savunma_karti(ev, dep, form_ev, form_dep) -> Optional[Dict[str, Any]]:
    if not (_stat_ok(ev) and _stat_ok(dep)):
        return None
    son5 = lambda f: (f.get("ozet") or {}).get("yenilen")
    if son5(form_ev) is None or son5(form_dep) is None:
        return None
    al = lambda s, f: {"yMac": s["totals"]["goals_conceded"] / s["match_count"], "xga": s["stats"]["xga_per_match"], "son5Yenilen": son5(f)}
    return {"ev": al(ev, form_ev), "dep": al(dep, form_dep)}


def zaman_karti(ev: Optional[Dict[str, Any]], dep: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Gol zamanlaması: altı dilim (0-15 ... 76-90), atılan ve yenilen."""
    def taraf(g):
        aralik = (g or {}).get("intervals") or {}
        if not all(d in aralik for d in _DILIMLER):
            return None
        return {"atilan": [aralik[d]["scored"] for d in _DILIMLER], "yenilen": [aralik[d]["conceded"] for d in _DILIMLER]}
    e, d = taraf(ev), taraf(dep)
    return {"ev": e, "dep": d} if e and d else None


def tip_karti(stat_ev, stat_dep, tip_ev, tip_dep) -> Optional[Dict[str, Any]]:
    """Takım tipi: tip adı, kısa açıklaması (oyun karakteri) ve topa sahip olma yüzdesi."""
    def taraf(stat, tip):
        t = (tip or {}).get("team_type") or {}
        sahip = ((stat or {}).get("stats") or {}).get("possession_avg")
        if not t.get("name") or sahip is None:
            return None
        return {"tip": t["name"], "karakter": t.get("description") or "", "topaSahip": sahip}
    e, d = taraf(stat_ev, tip_ev), taraf(stat_dep, tip_dep)
    return {"ev": e, "dep": d} if e and d else None


def kart_karti(ev: Optional[Dict[str, Any]], dep: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Kart profili (istatistik servisi cards/team-profile): maç başı sarı, 3.5 üstü kart oranı, kırmızı kart sayısı."""
    def taraf(p):
        o = (p or {}).get("overall") or {}
        n = o.get("n_matches") or 0
        oran = ((p or {}).get("thresholds") or {}).get("over_3_5_rate")
        if n <= 0 or o.get("yellow_for_avg") is None or oran is None:
            return None
        return {"sari": o["yellow_for_avg"], "kart35": round(oran), "kirmizi": round((o.get("red_for_avg") or 0) * n)}
    e, d = taraf(ev), taraf(dep)
    return {"ev": e, "dep": d} if e and d else None


def mac_paketi(sira: int, ev: Dict[str, Any], dep: Dict[str, Any], lig: str, hafta: Optional[str], saat: str, tarih: str, **kartlar: Any) -> Dict[str, Any]:
    """Bir maçın yayın paketi: yalnızca dolu (None olmayan) kartlar taşınır."""
    return {
        "sira": sira, "ev": ev, "dep": dep, "lig": lig, "hafta": hafta, "saat": saat, "tarih": tarih,
        "kartlar": {k: v for k, v in kartlar.items() if v is not None},
    }
