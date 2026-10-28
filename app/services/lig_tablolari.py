"""Lig tabloları: takım bazında sezon toplamları (gol, xG ve beklenen puan, şut, korner, kart, üst/alt çizgileri, KG).

Skorlar bitmiş maçlardan, istatistikler (match_stats) varsa o maçlardan gelir; istatistiği olmayan maç yalnız skor sayılarına girer.
Yanıttaki `stat_mac` / `xg_mac` takımın kaç maçında o veri bulunduğunu söyler: ön yüz kapsamı düşük ölçüyü göstermez.
Saf fonksiyonlar (veritabanı yok): hesapla() satır listesini alır.
"""
import math
from typing import Any, Dict, List, Optional, Tuple

CIZGILER = (1, 2, 3, 4)          # üst/alt çizgileri: 1.5, 2.5, 3.5, 4.5 (toplam gol > çizgi)


def _poisson(ort: float, maks: int) -> List[float]:
    return [math.exp(-ort) * ort ** k / math.factorial(k) for k in range(maks + 1)]


def beklenen_sonuc(xg_ev: float, xg_dep: float, maks: int = 12) -> Tuple[float, float, float]:
    """İki takımın xG değerlerinden, golleri bağımsız Poisson sayarak (ev galibiyeti, beraberlik, deplasman galibiyeti) olasılıkları."""
    ev, dep = _poisson(max(xg_ev, 0.0), maks), _poisson(max(xg_dep, 0.0), maks)
    pw = sum(ev[i] * dep[j] for i in range(maks + 1) for j in range(maks + 1) if i > j)
    pb = sum(ev[i] * dep[i] for i in range(maks + 1))
    pl = sum(ev[i] * dep[j] for i in range(maks + 1) for j in range(maks + 1) if i < j)
    toplam = pw + pb + pl
    return (pw / toplam, pb / toplam, pl / toplam) if toplam > 0 else (0.0, 1.0, 0.0)


_STAT = ("sut", "isabetli", "korner", "faul", "sari", "kirmizi", "top")


def _bos(kimlik: Any, ad: str) -> Dict[str, Any]:
    t: Dict[str, Any] = {"takim_id": kimlik, "takim": ad, "oynanan": 0, "puan": 0, "atilan": 0, "yenilen": 0, "temiz_sayfa": 0, "kg_var": 0,
                         "stat_mac": 0, "xg_mac": 0, "korner_mac": 0, "korner_puan": 0, "korner_g": 0, "korner_b": 0, "korner_m": 0, "xg_atilan": 0.0, "xg_yenilen": 0.0, "xp": 0.0}
    for c in CIZGILER:
        t[f"ust{c}5"] = 0
    for k in _STAT:
        t[k] = 0
        t[f"{k}_yenilen"] = 0
    return t


def _say(deger: Any) -> Optional[float]:
    return float(deger) if deger is not None else None


def hesapla(maclar: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """maclar: ev_id, ev_ad, dep_id, dep_ad, ev_gol, dep_gol; isteğe bağlı ev_/dep_ xg, sut, isabetli, korner, faul, sari, kirmizi, top."""
    takimlar: Dict[Any, Dict[str, Any]] = {}
    for m in maclar:
        if m.get("ev_gol") is None or m.get("dep_gol") is None:
            continue
        evg, depg = int(m["ev_gol"]), int(m["dep_gol"])
        toplam = evg + depg
        yan = (("ev", "dep", evg, depg), ("dep", "ev", depg, evg))
        xg_var = m.get("ev_xg") is not None and m.get("dep_xg") is not None
        pw, pb, pl = beklenen_sonuc(float(m["ev_xg"]), float(m["dep_xg"])) if xg_var else (0.0, 0.0, 0.0)
        istat_var = any(m.get(f"ev_{k}") is not None or m.get(f"dep_{k}") is not None for k in ("sut", "korner", "sari", "faul")) or xg_var
        for taraf, rakip, atilan, yenilen in yan:
            t = takimlar.setdefault(m[f"{taraf}_id"], _bos(m[f"{taraf}_id"], m[f"{taraf}_ad"]))
            t["oynanan"] += 1
            t["atilan"] += atilan
            t["yenilen"] += yenilen
            t["puan"] += 3 if atilan > yenilen else 1 if atilan == yenilen else 0
            if yenilen == 0:
                t["temiz_sayfa"] += 1
            if atilan > 0 and yenilen > 0:
                t["kg_var"] += 1
            for c in CIZGILER:
                if toplam > c:
                    t[f"ust{c}5"] += 1
            if istat_var:
                t["stat_mac"] += 1
                for k in _STAT:
                    t[k] += _say(m.get(f"{taraf}_{k}")) or 0
                    t[f"{k}_yenilen"] += _say(m.get(f"{rakip}_{k}")) or 0
            ka, kr = _say(m.get(f"{taraf}_korner")), _say(m.get(f"{rakip}_korner"))
            if ka is not None and kr is not None:       # korner puanı: rakipten çok korner 3, eşit 1, az 0
                t["korner_mac"] += 1
                if ka > kr:
                    t["korner_g"] += 1; t["korner_puan"] += 3
                elif ka == kr:
                    t["korner_b"] += 1; t["korner_puan"] += 1
                else:
                    t["korner_m"] += 1
            if xg_var:
                t["xg_mac"] += 1
                t["xg_atilan"] += float(m[f"{taraf}_xg"])
                t["xg_yenilen"] += float(m[f"{rakip}_xg"])
                t["xp"] += (3 * pw + pb) if taraf == "ev" else (3 * pl + pb)
    satirlar = list(takimlar.values())
    for t in satirlar:
        for k in ("xg_atilan", "xg_yenilen", "xp"):
            t[k] = round(t[k], 2)
        for k in _STAT:
            t[k] = round(t[k], 1)
            t[f"{k}_yenilen"] = round(t[f"{k}_yenilen"], 1)
    satirlar.sort(key=lambda t: (-t["puan"], -(t["atilan"] - t["yenilen"]), -t["atilan"], t["takim"]))
    return satirlar
