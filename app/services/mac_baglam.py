"""Maç öncesi bağlam hesapları (saf fonksiyonlar): takım formu ve karşılaşma geçmişi özeti."""
from typing import Any, Dict, List


def _utc(d):
    return d.isoformat() + "Z" if d is not None else None


def _skorlu(s: Dict[str, Any]) -> bool:
    return s.get("home_score") is not None and s.get("away_score") is not None


def form_ozeti(satirlar: List[Dict[str, Any]], takim_ic_id: Any) -> Dict[str, Any]:
    """satirlar: takımın maçları, EN YENİDEN eskiye. G/B/M takımın penceresinden."""
    maclar = []
    ozet = {"galibiyet": 0, "beraberlik": 0, "maglubiyet": 0, "atilan": 0, "yenilen": 0}
    dizi = ""
    for s in satirlar:
        if not _skorlu(s):
            continue
        ev_mi = s["home_team_id"] == takim_ic_id
        atilan, yenilen = (s["home_score"], s["away_score"]) if ev_mi else (s["away_score"], s["home_score"])
        sonuc = "G" if atilan > yenilen else "B" if atilan == yenilen else "M"
        ozet["galibiyet" if sonuc == "G" else "beraberlik" if sonuc == "B" else "maglubiyet"] += 1
        ozet["atilan"] += atilan
        ozet["yenilen"] += yenilen
        dizi += sonuc
        maclar.append({
            "tarih": _utc(s.get("tarih")), "rakip": s["dep_ad"] if ev_mi else s["ev_ad"], "ev_mi": ev_mi,
            "atilan": atilan, "yenilen": yenilen, "sonuc": sonuc, "lig": s.get("lig_ad"),
        })
    return {"maclar": maclar, "ozet": {**ozet, "dizi": dizi}}


def h2h_ozeti(satirlar: List[Dict[str, Any]], ev_ic_id: Any, dep_ic_id: Any) -> Dict[str, Any]:
    """Karşılaşma geçmişi; galibiyetler ev/deplasman fark etmeden takıma yazılır (home = bu maçın ev sahibi)."""
    maclar = []
    ozet = {"mac_sayisi": 0, "home_galibiyet": 0, "beraberlik": 0, "away_galibiyet": 0, "home_gol": 0, "away_gol": 0}
    for s in satirlar:
        if not _skorlu(s):
            continue
        ev_gol, dep_gol = s["home_score"], s["away_score"]
        home_gol, away_gol = (ev_gol, dep_gol) if s["home_team_id"] == ev_ic_id else (dep_gol, ev_gol)
        ozet["mac_sayisi"] += 1
        ozet["home_gol"] += home_gol
        ozet["away_gol"] += away_gol
        ozet["home_galibiyet" if home_gol > away_gol else "beraberlik" if home_gol == away_gol else "away_galibiyet"] += 1
        maclar.append({"tarih": _utc(s.get("tarih")), "ev": s["ev_ad"], "dep": s["dep_ad"], "ev_gol": ev_gol, "dep_gol": dep_gol, "lig": s.get("lig_ad")})
    return {"maclar": maclar, "ozet": ozet}
