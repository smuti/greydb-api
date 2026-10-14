"""
Player Stats Router - Oyuncu sezon istatistik özeti

match_player_stats tablosundan (maç başına kaydedilen gerçek FotMob verisi)
bir oyuncunun sezon toplamını/ortalamasını hesaplar — Yayın Modu sayfaları
için yeni bir oyuncu eklendiğinde uydurma sayı yazmamak amacıyla.
"""
from fastapi import APIRouter, HTTPException
from typing import Optional

from app.services.db import query_to_df

router = APIRouter(tags=["player_stats"])


@router.get("/players/season-stats")
async def get_player_season_stats(player_name: str, team_id: int, since: str = "2026-08-01"):
    """
    Bir oyuncunun belirtilen tarihten (varsayılan: bu sezon başlangıcı) bu yana
    tüm resmi maçlardaki (Avrupa kupaları dahil) toplam/ortalama istatistiği.
    player_name ILIKE ile aranır — aynı oyuncunun farklı yazımdaki (ör. "Škriniar"
    vs "Skriniar") kayıtları otomatik birleşir.
    """
    sql = """
        SELECT
            COUNT(DISTINCT mps.match_id) AS matches,
            SUM(mps.minutes_played) AS minutes,
            SUM(mps.goals) AS goals,
            SUM(mps.assists) AS assists,
            SUM(mps.xg) AS xg,
            SUM(mps.xa) AS xa,
            SUM(mps.total_shots) AS total_shots,
            SUM(mps.shots_on_target) AS shots_on_target,
            SUM(mps.total_passes) AS total_passes,
            SUM(mps.accurate_passes) AS accurate_passes,
            SUM(mps.key_passes) AS key_passes,
            SUM(mps.tackles) AS tackles,
            SUM(mps.interceptions) AS interceptions,
            SUM(mps.clearances) AS clearances,
            SUM(mps.duels_won) AS duels_won,
            SUM(mps.duels_lost) AS duels_lost,
            SUM(mps.saves) AS saves,
            SUM(mps.goals_conceded) AS goals_conceded,
            SUM(mps.rating * mps.minutes_played) / NULLIF(SUM(CASE WHEN mps.rating IS NOT NULL THEN mps.minutes_played END), 0) AS weighted_rating,
            AVG(mps.rating) AS avg_rating,
            array_agg(DISTINCT mps.player_name) AS name_variants
        FROM public.match_player_stats mps
        JOIN public.matches m ON m.id = mps.match_id
        WHERE mps.team_id = :team_id
          AND mps.player_name ILIKE :player_name
          AND m.match_date >= :since
    """
    df = query_to_df(sql, {"team_id": team_id, "player_name": f"%{player_name}%", "since": since})

    if df.empty or df.iloc[0]["matches"] in (0, None):
        raise HTTPException(status_code=404, detail=f"'{player_name}' için {since} sonrası maç verisi bulunamadı")

    row = df.iloc[0]

    def num(key, round_to=None):
        val = row.get(key)
        if val is None:
            return None
        try:
            if hasattr(val, "item"):
                val = val.item()
        except Exception:
            pass
        return round(float(val), round_to) if round_to is not None else int(val)

    return {
        "player_name": player_name,
        "name_variants": list(row["name_variants"]) if row.get("name_variants") is not None else [],
        "matches": num("matches"),
        "minutes": num("minutes"),
        "goals": num("goals"),
        "assists": num("assists"),
        "xg": num("xg", 2),
        "xa": num("xa", 2),
        "total_shots": num("total_shots"),
        "shots_on_target": num("shots_on_target"),
        "total_passes": num("total_passes"),
        "accurate_passes": num("accurate_passes"),
        "key_passes": num("key_passes"),
        "tackles": num("tackles"),
        "interceptions": num("interceptions"),
        "clearances": num("clearances"),
        "duels_won": num("duels_won"),
        "duels_lost": num("duels_lost"),
        "saves": num("saves"),
        "goals_conceded": num("goals_conceded"),
        "rating": num("weighted_rating", 2) or num("avg_rating", 2),
    }
