"""Puan durumu: kendi veritabanımızdaki bitmiş maçlardan hesaplanır (FotMob'a istek yok).

Sıralama: puan, averaj, atılan gol, takım adı (deterministik). Resmi sıralamada ikili averaj, puan silme gibi
kurallar farklı olabilir; bu hesap onları bilmez (yanıttaki `hesap` notu bunu söyler).
"""
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple


def sezon_araligi(sezon: int) -> Tuple[datetime, datetime]:
    """Sezon Temmuz'dan Temmuz'a: 2026 -> [2026-07-01, 2027-07-01)."""
    return datetime(sezon, 7, 1), datetime(sezon + 1, 7, 1)


def guncel_sezon(bugun: date) -> int:
    return bugun.year if bugun.month >= 7 else bugun.year - 1


def hesapla(maclar: List[Dict[str, Any]], saha: Optional[str] = None) -> List[Dict[str, Any]]:
    """maclar: tarih, ev_id, ev_ad, dep_id, dep_ad, ev_gol, dep_gol. Skoru olmayan maç sayılmaz.
    saha: None genel; "ev" yalnız ev sahibi olarak oynanan maçlar (iç saha); "dep" yalnız deplasmandakiler (dış saha)."""
    takimlar: Dict[Any, Dict[str, Any]] = {}

    def al(kimlik, ad):
        return takimlar.setdefault(kimlik, {
            "takim_id": kimlik, "takim": ad, "oynanan": 0, "galibiyet": 0, "beraberlik": 0, "maglubiyet": 0,
            "atilan": 0, "yenilen": 0, "averaj": 0, "puan": 0, "_form": [],
        })

    gecerli = [m for m in maclar if m.get("ev_gol") is not None and m.get("dep_gol") is not None]
    for m in sorted(gecerli, key=lambda x: x["tarih"]):
        taraflar = ((m["ev_id"], m["ev_ad"], m["ev_gol"], m["dep_gol"]), (m["dep_id"], m["dep_ad"], m["dep_gol"], m["ev_gol"]))
        if saha == "ev":
            taraflar = taraflar[:1]
        elif saha == "dep":
            taraflar = taraflar[1:]
        for kimlik, ad, atilan, yenilen in taraflar:
            t = al(kimlik, ad)
            t["oynanan"] += 1
            t["atilan"] += atilan
            t["yenilen"] += yenilen
            if atilan > yenilen:
                t["galibiyet"] += 1; t["puan"] += 3; t["_form"].append("G")
            elif atilan == yenilen:
                t["beraberlik"] += 1; t["puan"] += 1; t["_form"].append("B")
            else:
                t["maglubiyet"] += 1; t["_form"].append("M")

    satirlar = list(takimlar.values())
    for t in satirlar:
        t["averaj"] = t["atilan"] - t["yenilen"]
        t["form"] = t.pop("_form")[-5:]
    satirlar.sort(key=lambda t: (-t["puan"], -t["averaj"], -t["atilan"], t["takim"]))
    for i, t in enumerate(satirlar, 1):
        t["sira"] = i
    return satirlar
