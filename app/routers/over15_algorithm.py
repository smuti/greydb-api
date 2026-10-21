"""
1.5 Gol Üstü Algoritması Router
Bkz. app/services/over15_algorithm.py (yöntem + backtest doğrulaması).
"""
from datetime import datetime, date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.db import query_to_df
from app.services.over15_algorithm import evaluate_match

router = APIRouter(prefix="/algorithms/over15", tags=["over15-algorithm"])


@router.get("/evaluate")
async def evaluate(
    home_team_id: int = Query(...),
    away_team_id: int = Query(...),
    league_id: int = Query(...),
    match_date: Optional[datetime] = Query(None, description="Verilmezse şu an kullanılır"),
):
    """Belirtilen eşleşme için 1.5 Gol Üstü algoritmasını çalıştırır (henüz oynanmamış/hipotetik maç dahil)."""
    when = match_date or datetime.utcnow()
    names = query_to_df(
        "SELECT id, name FROM public.teams WHERE id IN (:a, :b)",
        {"a": home_team_id, "b": away_team_id},
    )
    name_map = dict(zip(names["id"], names["name"])) if not names.empty else {}
    result = evaluate_match(
        home_team_id, away_team_id, league_id, when,
        home_team_name=name_map.get(home_team_id),
        away_team_name=name_map.get(away_team_id),
    )
    return {
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home_team_name": name_map.get(home_team_id),
        "away_team_name": name_map.get(away_team_id),
        "league_id": league_id,
        "match_date": when,
        **result,
    }


@router.get("/matches/{match_id}")
async def evaluate_existing_match(match_id: int):
    """public.matches'taki mevcut bir maç kaydı için algoritmayı çalıştırır."""
    df = query_to_df(
        """
        SELECT m.id, m.home_team_id, m.away_team_id, m.league_id, m.match_date,
               ht.name AS home_team_name, at.name AS away_team_name
        FROM public.matches m
        JOIN public.teams ht ON ht.id = m.home_team_id
        JOIN public.teams at ON at.id = m.away_team_id
        WHERE m.id = :id
        """,
        {"id": match_id},
    )
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Maç {match_id} bulunamadı")
    row = df.iloc[0]
    result = evaluate_match(
        int(row["home_team_id"]), int(row["away_team_id"]), int(row["league_id"]), row["match_date"],
        home_team_name=row["home_team_name"], away_team_name=row["away_team_name"],
    )
    return {
        "match_id": match_id,
        "home_team_id": int(row["home_team_id"]),
        "away_team_id": int(row["away_team_id"]),
        "home_team_name": row["home_team_name"],
        "away_team_name": row["away_team_name"],
        "league_id": int(row["league_id"]),
        "match_date": row["match_date"],
        **result,
    }


@router.get("/scan")
async def scan_day(
    scan_date: date = Query(..., alias="date", description="YYYY-MM-DD"),
):
    """O güne ait tüm planlı maçları algoritmadan geçirir, filtreleri geçenleri üstte listeler."""
    df = query_to_df(
        """
        SELECT m.id, m.home_team_id, m.away_team_id, m.league_id, m.match_date,
               ht.name AS home_team_name, at.name AS away_team_name
        FROM public.matches m
        JOIN public.teams ht ON ht.id = m.home_team_id
        JOIN public.teams at ON at.id = m.away_team_id
        WHERE m.cancelled = FALSE AND m.match_date::date = :d
        ORDER BY m.match_date
        """,
        {"d": scan_date},
    )
    results = []
    for _, row in df.iterrows():
        result = evaluate_match(
            int(row["home_team_id"]), int(row["away_team_id"]), int(row["league_id"]), row["match_date"],
            home_team_name=row["home_team_name"], away_team_name=row["away_team_name"],
        )
        results.append({
            "match_id": int(row["id"]),
            "home_team_name": row["home_team_name"],
            "away_team_name": row["away_team_name"],
            "league_id": int(row["league_id"]),
            "match_date": row["match_date"],
            **result,
        })
    results.sort(key=lambda r: (not r["verdict"]["passed"], r["verdict"]["insufficient_data"]))
    passed_count = sum(1 for r in results if r["verdict"]["passed"])
    return {
        "date": scan_date,
        "total_matches": len(results),
        "passed_count": passed_count,
        "matches": results,
    }
