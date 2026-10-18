"""Bitmiş (ve işlenmiş) maçın bütün verisi, kendi veritabanımızdan. FotMob'a istek atmaz.
Tek maç sayfası: bitmiş maçlarda bu uç, canlıda matchforge kullanılır. Veritabanında yoksa 404 (sayfa "işleniyor" der)."""
from fastapi import APIRouter, HTTPException

from app.services.db import execute_query
from app.services.mac_detay import cift_istatistik, kadro_listesi, olay_listesi

router = APIRouter(tags=["match-detail"])

_ANA = """
SELECT m.id, m.fotmob_match_id, m.match_date, m.round, m.finished, m.cancelled, m.home_score, m.away_score,
       l.fotmob_league_id AS lig_id, l.name AS lig_ad,
       ev.id AS ev_ic_id, ev.fotmob_team_id AS ev_id, ev.name AS ev_ad,
       dep.id AS dep_ic_id, dep.fotmob_team_id AS dep_id, dep.name AS dep_ad
FROM public.matches m
LEFT JOIN public.leagues l ON l.id = m.league_id
JOIN public.teams ev ON ev.id = m.home_team_id
JOIN public.teams dep ON dep.id = m.away_team_id
WHERE m.fotmob_match_id = :fid
"""
_Q = {
    "istatistik": "SELECT * FROM public.match_stats WHERE match_id = :id",
    "ileri": "SELECT * FROM public.match_advanced_stats WHERE match_id = :id",
    "olaylar": "SELECT * FROM public.match_events WHERE match_id = :id",
    "kadro": "SELECT * FROM public.match_lineups WHERE match_id = :id",
    "dizilis": "SELECT * FROM public.match_formations WHERE match_id = :id",
    "sakat": "SELECT * FROM public.player_availability WHERE match_id = :id",
    "oyuncu": "SELECT * FROM public.match_player_stats WHERE match_id = :id",
}


def _iso(d):
    return d.isoformat() if d is not None else None


@router.get("/match-detail/{fotmob_match_id}")
async def match_detail(fotmob_match_id: int):
    ana = execute_query(_ANA, {"fid": fotmob_match_id})
    if not ana:
        raise HTTPException(status_code=404, detail="Maç veritabanında yok")
    m = ana[0]
    p = {"id": m["id"]}
    ev, dep = m["ev_ic_id"], m["dep_ic_id"]

    istatistik_satir = execute_query(_Q["istatistik"], p)
    ileri_satir = execute_query(_Q["ileri"], p)
    olay_satirlari = execute_query(_Q["olaylar"], p)
    kadro_satirlari = execute_query(_Q["kadro"], p)
    dizilis = execute_query(_Q["dizilis"], p)
    sakat = execute_query(_Q["sakat"], p)
    oyuncu = execute_query(_Q["oyuncu"], p)

    istatistik = cift_istatistik(istatistik_satir[0]) if istatistik_satir else None
    if istatistik is not None and ileri_satir:
        istatistik = {**istatistik, **cift_istatistik(ileri_satir[0])}

    taraf = lambda t: "home" if t == ev else "away" if t == dep else None   # noqa: E731
    return {
        "mac": {
            "fotmob_match_id": m["fotmob_match_id"],
            "lig": {"fotmob_league_id": m["lig_id"], "ad": m["lig_ad"]},
            "tur": str(m["round"]) if m["round"] is not None else None,
            "tarih": _iso(m["match_date"]),
            "bitti": bool(m["finished"]), "iptal": bool(m["cancelled"]),
            "home": {"fotmob_team_id": m["ev_id"], "ad": m["ev_ad"], "skor": m["home_score"]},
            "away": {"fotmob_team_id": m["dep_id"], "ad": m["dep_ad"], "skor": m["away_score"]},
        },
        "istatistik": istatistik,
        "olaylar": olay_listesi(olay_satirlari, ev, dep),
        "kadro": kadro_listesi(kadro_satirlari, ev, dep, dizilis[0] if dizilis else None),
        "sakat_cezali": [
            {"takim": taraf(s["team_id"]), "oyuncu": s["player_name"], "durum": s["status"], "neden": s["reason"], "donus": _iso(s["expected_return"])}
            for s in sakat
        ],
        "oyuncu_istatistik": sorted(
            [{"takim": taraf(o["team_id"]), "oyuncu": o["player_name"], "puan": o["rating"], "dakika": o["minutes_played"], "gol": o["goals"],
              "asist": o["assists"], "xg": o["xg"], "sut": o["total_shots"]} for o in oyuncu],
            key=lambda x: (x["puan"] is None, -(x["puan"] or 0)),
        ),
        "veri": {"istatistik": istatistik is not None, "olaylar": bool(olay_satirlari), "kadro": bool(kadro_satirlari)},
    }
