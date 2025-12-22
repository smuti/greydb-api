"""
İstatistik hesaplama servisi - pandas ile
"""
import pandas as pd
import numpy as np
from app.services.db import query_to_df


def get_team_form(team_fotmob_id: int, limit: int = 5, venue: str = None, league_fotmob_id: int = None) -> dict:
    """
    Takım formu hesapla
    
    Args:
        team_fotmob_id: Takım FotMob ID
        limit: Maç sayısı (default 5)
        venue: 'home', 'away' veya None (genel)
        league_fotmob_id: Lig filtresi (opsiyonel)
    """
    if venue == "home":
        view = "greydb.vw_team_home_form"
    elif venue == "away":
        view = "greydb.vw_team_away_form"
    elif league_fotmob_id:
        view = "greydb.vw_team_league_form"
    else:
        view = "greydb.vw_team_overall_form"
    
    sql = f"""
        SELECT * FROM {view}
        WHERE team_fotmob_id = :team_id
        {"AND league_fotmob_id = :league_id" if league_fotmob_id else ""}
        AND match_rank <= :limit
        ORDER BY match_date DESC
    """
    
    params = {"team_id": team_fotmob_id, "limit": limit}
    if league_fotmob_id:
        params["league_id"] = league_fotmob_id
    
    df = query_to_df(sql, params)
    
    if df.empty:
        return {"matches": [], "stats": None}
    
    # İstatistikler
    stats = {
        "played": len(df),
        "wins": int((df["result"] == "W").sum()),
        "draws": int((df["result"] == "D").sum()),
        "losses": int((df["result"] == "L").sum()),
        "points": int(df["points"].sum()),
        "goals_for": int(df["goals_for"].sum()),
        "goals_against": int(df["goals_against"].sum()),
        "goal_diff": int(df["goals_for"].sum() - df["goals_against"].sum()),
        "avg_goals_for": round(df["goals_for"].mean(), 2),
        "avg_goals_against": round(df["goals_against"].mean(), 2),
        "avg_total_goals": round(df["total_goals"].mean(), 2),
        "btts_pct": round(df["btts"].mean() * 100, 1) if "btts" in df.columns else None,
        "form_string": "".join(df["result"].tolist())  # "WWDLW"
    }
    
    # Maç listesi
    matches = df[[
        "match_date", "opponent", "goals_for", "goals_against", 
        "result", "league_name", "fotmob_url"
    ]].to_dict(orient="records")
    
    # Datetime'ı string'e çevir
    for m in matches:
        m["match_date"] = m["match_date"].isoformat() if m["match_date"] else None
    
    return {"matches": matches, "stats": stats}


def find_team_by_name(team_name: str) -> dict | None:
    """
    Takım adından FotMob ID bul (fuzzy match)
    """
    name_lower = team_name.lower().strip()
    
    # İlk kelimeyi al (örn: "Athletic Bilbao" -> "Athletic")
    first_word = name_lower.split()[0] if name_lower else name_lower
    
    sql = """
        SELECT fotmob_id, name, short_name
        FROM greydb.teams
        WHERE LOWER(name) LIKE :name 
           OR LOWER(short_name) LIKE :name
           OR LOWER(name) LIKE :name_start
           OR LOWER(name) LIKE :first_word_pattern
           OR LOWER(short_name) LIKE :first_word_pattern
        ORDER BY 
            CASE 
                WHEN LOWER(name) = :exact THEN 0
                WHEN LOWER(short_name) = :exact THEN 1
                WHEN LOWER(name) LIKE :name THEN 2
                WHEN LOWER(name) LIKE :first_word_pattern THEN 3
                ELSE 4
            END,
            LENGTH(name)
        LIMIT 1
    """
    
    params = {
        "name": f"%{name_lower}%",
        "name_start": f"{name_lower}%",
        "first_word_pattern": f"{first_word}%",
        "exact": name_lower
    }
    
    df = query_to_df(sql, params)
    
    if df.empty:
        return None
    
    row = df.iloc[0]
    return {
        "fotmob_id": int(row["fotmob_id"]),
        "name": row["name"],
        "short_name": row["short_name"]
    }


def get_team_form_by_name(team_name: str, limit: int = 5, venue: str = None) -> dict:
    """
    Takım adıyla form getir
    """
    team = find_team_by_name(team_name)
    
    if not team:
        return {"error": f"Takım bulunamadı: {team_name}", "matches": [], "stats": None}
    
    result = get_team_form(
        team_fotmob_id=team["fotmob_id"],
        limit=limit,
        venue=venue
    )
    
    result["team"] = team
    return result


def get_h2h(team1_fotmob_id: int, team2_fotmob_id: int, limit: int = 10, home_only: bool = False) -> dict:
    """
    İki takım arası H2H istatistikleri
    
    Args:
        team1_fotmob_id: Takım 1 FotMob ID
        team2_fotmob_id: Takım 2 FotMob ID
        limit: Maç sayısı (default 10)
        home_only: Sadece team1 ev sahibiyken
    """
    if home_only:
        sql = """
            SELECT * FROM greydb.vw_h2h_home
            WHERE home_fotmob_id = :team1 AND away_fotmob_id = :team2
            AND match_rank <= :limit
            ORDER BY match_date DESC
        """
    else:
        sql = """
            SELECT * FROM greydb.vw_h2h
            WHERE (home_fotmob_id = :team1 AND away_fotmob_id = :team2)
               OR (home_fotmob_id = :team2 AND away_fotmob_id = :team1)
            ORDER BY match_date DESC
            LIMIT :limit
        """
    
    df = query_to_df(sql, {"team1": team1_fotmob_id, "team2": team2_fotmob_id, "limit": limit})
    
    if df.empty:
        return {"matches": [], "stats": None}
    
    # İstatistikler
    team1_wins = int(((df["home_fotmob_id"] == team1_fotmob_id) & (df["result"] == "H")).sum() +
                     ((df["away_fotmob_id"] == team1_fotmob_id) & (df["result"] == "A")).sum())
    team2_wins = int(((df["home_fotmob_id"] == team2_fotmob_id) & (df["result"] == "H")).sum() +
                     ((df["away_fotmob_id"] == team2_fotmob_id) & (df["result"] == "A")).sum())
    draws = int((df["result"] == "D").sum())
    
    stats = {
        "total_matches": len(df),
        "team1_wins": team1_wins,
        "team2_wins": team2_wins,
        "draws": draws,
        "team1_goals": int(df.apply(lambda r: r["home_score"] if r["home_fotmob_id"] == team1_fotmob_id else r["away_score"], axis=1).sum()),
        "team2_goals": int(df.apply(lambda r: r["home_score"] if r["home_fotmob_id"] == team2_fotmob_id else r["away_score"], axis=1).sum()),
        "avg_total_goals": round(df["total_goals"].mean(), 2),
        "btts_pct": round(df["btts"].mean() * 100, 1),
    }
    
    # Maç listesi
    matches = df[[
        "match_date", "home_team", "home_score", "away_score", 
        "away_team", "result", "league_name", "season", "fotmob_url"
    ]].to_dict(orient="records")
    
    for m in matches:
        m["match_date"] = m["match_date"].isoformat() if m["match_date"] else None
    
    return {"matches": matches, "stats": stats}

