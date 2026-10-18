"""Puan durumu ucu: kendi veritabanımızdaki bitmiş maçlardan hesaplanır; FotMob'a istek atmaz.
Tek maç sayfası ve (yapılacak) lig sayfası aynı ucu kullanır."""
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.db import execute_query
from app.services.puan_durumu import guncel_sezon, hesapla, sezon_araligi

router = APIRouter(tags=["standings"])


def bugun() -> date:
    return date.today()


_LIG_SQL = "SELECT fotmob_league_id, name FROM public.leagues WHERE fotmob_league_id = :lig"

_MAC_SQL = """
SELECT m.match_date AS tarih, m.home_score AS ev_gol, m.away_score AS dep_gol,
       ev.fotmob_team_id AS ev_id, ev.name AS ev_ad, dep.fotmob_team_id AS dep_id, dep.name AS dep_ad
FROM public.matches m
JOIN public.leagues l ON l.id = m.league_id
JOIN public.teams ev ON ev.id = m.home_team_id
JOIN public.teams dep ON dep.id = m.away_team_id
WHERE l.fotmob_league_id = :lig
  AND m.finished AND NOT COALESCE(m.cancelled, false)
  AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND m.match_date >= :bas AND m.match_date < :bit
ORDER BY m.match_date
"""


@router.get("/standings/{fotmob_league_id}")
async def standings(fotmob_league_id: int, season: Optional[int] = Query(None, ge=2000, le=2100)):
    lig = execute_query(_LIG_SQL, {"lig": fotmob_league_id})
    if not lig:
        raise HTTPException(status_code=404, detail="Lig bulunamadı")
    sezon = season if season is not None else guncel_sezon(bugun())
    bas, bit = sezon_araligi(sezon)
    maclar = execute_query(_MAC_SQL, {"lig": fotmob_league_id, "bas": bas, "bit": bit})
    return {
        "lig": {"fotmob_league_id": lig[0]["fotmob_league_id"], "ad": lig[0]["name"]},
        "sezon": f"{sezon}/{sezon + 1}",
        "bitmis_mac_sayisi": len(maclar),
        "tablo": hesapla(maclar),
        "hesap": {
            "kaynak": "Kendi veritabanımızdaki bitmiş maçlardan hesaplanır",
            "siralama": "Puan, averaj, atılan gol",
            "not": "Oynanmakta olan (canlı) maçlar sayılmaz, bitince eklenir. Resmi sıralamada ikili averaj ve puan silme gibi kurallar farklı olabilir.",
        },
    }
