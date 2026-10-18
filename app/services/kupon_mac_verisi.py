"""Bacak sonucu motoru için maç verisi: veritabanı satırlarından 'mac' sözlüğü (saf) ve veritabanından yükleyici."""
from datetime import timedelta
from typing import Any, Dict, List, Optional

from app.services.iddaa_eslestir import takim_benzer
from app.services.mac_detay import olay_listesi

_Q_MAC = """
SELECT m.id, m.fotmob_match_id, m.match_date, m.home_score, m.away_score, m.finished, m.cancelled,
       ev.id AS ev_ic_id, ev.name AS ev_ad, dep.id AS dep_ic_id, dep.name AS dep_ad
FROM public.matches m
JOIN public.teams ev ON ev.id = m.home_team_id JOIN public.teams dep ON dep.id = m.away_team_id
"""


def _cift(satir: Optional[Dict[str, Any]], ad: str):
    if not satir:
        return None
    e, d = satir.get(f"home_{ad}"), satir.get(f"away_{ad}")
    return (e, d) if e is not None and d is not None else None


def mac_sozlugu(mac: Dict[str, Any], stat: Optional[Dict[str, Any]], ileri: Optional[Dict[str, Any]],
                olaylar: List[Dict[str, Any]], oyuncular: List[Dict[str, Any]]) -> Dict[str, Any]:
    ev, dep = mac["ev_ic_id"], mac["dep_ic_id"]
    stats = {}
    for anahtar, (tablo, sutun) in {"shots": ("s", "shots"), "sot": ("s", "shots_on_target"), "corners": ("s", "corners"), "fouls": ("s", "fouls"),
                                    "yellow": ("s", "yellow_cards"), "red": ("s", "red_cards"), "possession": ("s", "possession"),
                                    "offsides": ("i", "offsides")}.items():
        c = _cift(stat if tablo == "s" else ileri, sutun)
        if c is not None:
            stats[anahtar] = c
    taraf = lambda t: "home" if t == ev else "away" if t == dep else None   # noqa: E731
    return {
        "home": mac["home_score"], "away": mac["away_score"], "home_name": mac["ev_ad"], "away_name": mac["dep_ad"],
        "stats": stats,
        "events": olay_listesi(olaylar, ev, dep),
        "players": [
            {"ad": o["player_name"], "takim": taraf(o["team_id"]), "gol": o.get("goals"), "asist": o.get("assists"), "sut": o.get("total_shots"),
             "isabetli_sut": o.get("shots_on_target"), "faul_yapma": o.get("fouls_committed"), "faul_alma": o.get("fouls_won"),
             "kurtaris": o.get("saves"), "pas": o.get("total_passes"), "isabetli_pas": o.get("accurate_passes")}
            for o in oyuncular
        ],
    }


def maci_sec(adaylar: List[Dict[str, Any]], home: str, away: str) -> Optional[Dict[str, Any]]:
    """Ad benzerliğiyle tek maç seçer; birden çok ya da hiç aday varsa None."""
    uyan = [a for a in adaylar if takim_benzer(home, a["ev_ad"]) and takim_benzer(away, a["dep_ad"])]
    return uyan[0] if len(uyan) == 1 else None


def maci_bul(execute_query, home: str, away: str, tarih, pencere_saat: int = 36) -> Optional[Dict[str, Any]]:
    """Bitmiş maçı takım adları ve başlama tarihine (±pencere) göre bulur."""
    adaylar = execute_query(
        _Q_MAC + " WHERE m.finished AND m.home_score IS NOT NULL AND m.match_date BETWEEN :bas AND :son",
        {"bas": tarih - timedelta(hours=pencere_saat), "son": tarih + timedelta(hours=pencere_saat)},
    )
    return maci_sec(adaylar, home, away)


def mac_yukle(execute_query, mac: Dict[str, Any]) -> Dict[str, Any]:
    p = {"id": mac["id"]}
    stat = execute_query("SELECT * FROM public.match_stats WHERE match_id = :id", p)
    ileri = execute_query("SELECT * FROM public.match_advanced_stats WHERE match_id = :id", p)
    olaylar = execute_query("SELECT * FROM public.match_events WHERE match_id = :id", p)
    oyuncular = execute_query("SELECT * FROM public.match_player_stats WHERE match_id = :id", p)
    return mac_sozlugu(mac, stat[0] if stat else None, ileri[0] if ileri else None, olaylar, oyuncular)
