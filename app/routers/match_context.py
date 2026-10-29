"""Maç öncesi bağlam: iki takımın son formu ve aralarındaki karşılaşma geçmişi, GÜNCEL tablodan (public.matches).
Eski greydb.vw_* form/H2H görünümleri Aralık 2025'te donmuş olduğu için kullanılmaz. Form, maçtan ÖNCEKİ bitmiş maçlardan hesaplanır."""
from fastapi import APIRouter, HTTPException

from app.services.db import execute_query
from app.services.mac_baglam import form_ozeti, h2h_ozeti

router = APIRouter(tags=["match-context"])

_TEMEL = """
SELECT m.match_date AS tarih, m.home_team_id AS ev_ic_id, m.away_team_id AS dep_ic_id
FROM public.matches m WHERE m.fotmob_match_id = :fid
UNION ALL
SELECT u.match_date, u.home_team_id, u.away_team_id
FROM public.upcoming_matches u WHERE u.fotmob_match_id = :fid
LIMIT 1
"""

_ORTAK = """
SELECT m.match_date AS tarih, m.home_team_id, m.away_team_id, m.home_score, m.away_score,
       ev.name AS ev_ad, dep.name AS dep_ad, l.name AS lig_ad
FROM public.matches m
JOIN public.teams ev ON ev.id = m.home_team_id
JOIN public.teams dep ON dep.id = m.away_team_id
LEFT JOIN public.leagues l ON l.id = m.league_id
"""
_FILTRE = "AND m.finished AND NOT COALESCE(m.cancelled, false) AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL AND m.match_date < :tarih"

_FORM = f"-- HEAD_FORM\n{_ORTAK} WHERE (m.home_team_id = :t OR m.away_team_id = :t) {_FILTRE} ORDER BY m.match_date DESC LIMIT 5"
_H2H = f"-- HEAD_H2H\n{_ORTAK} WHERE ((m.home_team_id = :a AND m.away_team_id = :b) OR (m.home_team_id = :b AND m.away_team_id = :a)) {_FILTRE} ORDER BY m.match_date DESC LIMIT 10"


@router.get("/match-context/{fotmob_match_id}")
def match_context(fotmob_match_id: int):
    temel = execute_query(_TEMEL, {"fid": fotmob_match_id})
    if not temel:
        raise HTTPException(status_code=404, detail="Maç veritabanında yok")
    t = temel[0]
    ev, dep, tarih = t["ev_ic_id"], t["dep_ic_id"], t["tarih"]
    if ev is None or dep is None or tarih is None:
        return {"form": {"home": form_ozeti([], ev), "away": form_ozeti([], dep)}, "h2h": h2h_ozeti([], ev, dep)}
    return {
        "form": {
            "home": form_ozeti(execute_query(_FORM, {"t": ev, "tarih": tarih}), ev),
            "away": form_ozeti(execute_query(_FORM, {"t": dep, "tarih": tarih}), dep),
        },
        "h2h": h2h_ozeti(execute_query(_H2H, {"a": ev, "b": dep, "tarih": tarih}), ev, dep),
    }
