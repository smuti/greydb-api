"""
Spor Toto haftalik yorum otomasyonu - veri toplama katmani.

Akis: Sportoto.gov.tr'nin kendi (WAF'siz) REST API'sinden haftalik 15 mac
cekilir -> her takim greydb'nin kendi veritabanindaki public.teams tablosuna
isim eslestirmesiyle baglanir (sonuc greydb.spor_toto_team_map'te kalici
olarak saklanir, bir dahaki hafta tekrar denenmez) -> iki katmanli veri
toplanir (teamAnalyzer'in 17 ligi icin zengin HTTP verisi, disinda kalan
ligler icin ayni Postgres'teki ham public.* tablolarindan basit agregatlar)
-> Skorjin'in yeni /api/v1/spor-toto-scenarios ucuna gonderilip 6 senaryo
uretilir.
"""
import re
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd

from app.services.db import query_to_df, execute_insert
from app.services.skorjin_match_analysis import (
    MATCHFORGE_BASE,
    _get as _mf_get,
    _pick,
    translate_team_name,
    _translate_names_in_place,
)

SPORTOTO_BASE = "https://webapi.sportoto.gov.tr"
SKORJIN_BASE = "https://skorjin-production.up.railway.app"
HTTP_TIMEOUT = 20.0

_TR_MAP = str.maketrans({"İ": "i", "ı": "i", "ü": "u", "ö": "o", "ş": "s", "ç": "c", "ğ": "g"})


def _normalize(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").lower().translate(_TR_MAP)).strip()


class SporTotoFetchError(Exception):
    """Kullanıcıya gösterilebilecek, beklenen hata."""


# ==================== 1. Sportoto.gov.tr ====================

def list_available_rounds(year: str = "2026/2027") -> List[Dict[str, Any]]:
    """Yayinlanmis Sportoto round'larini doner (en guncel en basta)."""
    with httpx.Client() as client:
        resp = client.get(
            f"{SPORTOTO_BASE}/api/GameRound",
            params={"year": year, "isPublished": "true"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    rounds = data.get("object", data) if isinstance(data, dict) else data
    if not isinstance(rounds, list):
        raise SporTotoFetchError("Sportoto round listesi beklenmeyen formatta döndü.")
    return sorted(rounds, key=lambda r: r.get("id", 0), reverse=True)


def fetch_round_matches(game_round_id: int) -> List[Dict[str, Any]]:
    """Bir round'un (hafta) 15 maçını Sportoto'dan çeker."""
    with httpx.Client() as client:
        resp = client.get(
            f"{SPORTOTO_BASE}/api/GameMatch/GetGameMatches/",
            params={"gameRoundId": game_round_id},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    raw_matches = data.get("object", data) if isinstance(data, dict) else data
    if not isinstance(raw_matches, list):
        raise SporTotoFetchError("Sportoto maç listesi beklenmeyen formatta döndü.")

    matches = []
    for i, item in enumerate(raw_matches, start=1):
        m = item.get("match") or {}
        home = m.get("homeTeam") or {}
        away = m.get("awayTeam") or {}
        matches.append({
            "mac_no": i,
            "ev_takim": home.get("name"),
            "deplasman_takim": away.get("name"),
            "sportoto_home_id": home.get("id"),
            "sportoto_away_id": away.get("id"),
            "lig_haftasi": (m.get("round") or {}).get("name") or (m.get("stage") or {}).get("name"),
            "mac_tarihi": m.get("date"),
        })
    return matches


# ==================== 2. Takım eşleştirme (kalıcı harita) ====================

def resolve_team(sportoto_team_id: int, sportoto_team_name: str) -> Optional[Dict[str, Any]]:
    """Sportoto takımını public.teams'e bağlar. Sonuç spor_toto_team_map'te
    kalıcı olarak saklanır — aynı takım için bir dahaki hafta tekrar
    isim-eşleştirmesi denenmez."""
    cached = query_to_df(
        "SELECT matchforge_team_id FROM greydb.spor_toto_team_map WHERE sportoto_team_id = %s",
        (sportoto_team_id,),
    )
    if not cached.empty:
        team_id = cached.iloc[0]["matchforge_team_id"]
        if pd.isna(team_id):
            return None
        return {"team_id": int(team_id)}

    all_teams = query_to_df("SELECT id, fotmob_team_id, name FROM public.teams")
    normalized_target = _normalize(sportoto_team_name)

    match = None
    # 1. geçiş: tam eşleşme
    for _, row in all_teams.iterrows():
        if _normalize(row["name"]) == normalized_target:
            match = {"team_id": int(row["id"]), "fotmob_team_id": row["fotmob_team_id"], "name": row["name"]}
            break

    # 2. geçiş: sponsor önekli isimlerde DB'deki temiz ad, Sportoto adının
    # içinde geçer ("Arca Çorum FK" içinde "Çorum FK") — en uzun eşleşen
    # tercih edilir, yanlışlıkla kısa bir isme takılmayı önlemek için.
    if not match:
        best = None
        best_len = 0
        for _, row in all_teams.iterrows():
            normalized_db = _normalize(row["name"])
            if normalized_db and normalized_db in normalized_target and len(normalized_db) > best_len:
                best = {"team_id": int(row["id"]), "fotmob_team_id": row["fotmob_team_id"], "name": row["name"]}
                best_len = len(normalized_db)
        match = best

    execute_insert(
        """
        INSERT INTO greydb.spor_toto_team_map (sportoto_team_id, sportoto_team_name, matchforge_team_id)
        VALUES (:sportoto_team_id, :sportoto_team_name, :matchforge_team_id)
        ON CONFLICT (sportoto_team_id) DO NOTHING
        """,
        {
            "sportoto_team_id": sportoto_team_id,
            "sportoto_team_name": sportoto_team_name,
            "matchforge_team_id": match["team_id"] if match else None,
        },
    )
    return match


def manual_link_team(sportoto_team_id: int, sportoto_team_name: str, matchforge_team_id: int) -> None:
    """Admin'in arama kutusundan elle onayladığı eşleşmeyi kaydeder/günceller."""
    execute_insert(
        """
        INSERT INTO greydb.spor_toto_team_map (sportoto_team_id, sportoto_team_name, matchforge_team_id)
        VALUES (:sportoto_team_id, :sportoto_team_name, :matchforge_team_id)
        ON CONFLICT (sportoto_team_id) DO UPDATE SET matchforge_team_id = EXCLUDED.matchforge_team_id
        """,
        {
            "sportoto_team_id": sportoto_team_id,
            "sportoto_team_name": sportoto_team_name,
            "matchforge_team_id": matchforge_team_id,
        },
    )


def search_teams(query: str, limit: int = 15) -> List[Dict[str, Any]]:
    """Admin'in elle eşleştirme için kullandığı serbest metin takım araması."""
    df = query_to_df(
        "SELECT id, fotmob_team_id, name, league_id FROM public.teams WHERE name ILIKE %s ORDER BY name LIMIT %s",
        (f"%{query}%", limit),
    )
    return [
        {"team_id": int(r["id"]), "fotmob_team_id": r["fotmob_team_id"], "name": r["name"], "league_id": r["league_id"]}
        for _, r in df.iterrows()
    ]


# ==================== 3. İki katmanlı veri toplama ====================

def _fetch_rich_team_data(team_id: int, team_name: str) -> Dict[str, Any]:
    """teamAnalyzer'ın 17 ligini kapsayan zengin veri. Fuat'ın GERÇEK konuşma tarzı
    soyut metriklerden (xG, possession) çok somut maç sonuçlarına dayanıyor
    ("son 5 maçta 3 galibiyet aldı", "4-0 kazandılar") — bu yüzden son_5_mac_formu
    (W/D/L + rakip + skor) EN ÖNEMLİ alan, diğerleri destekleyici."""
    data: Dict[str, Any] = {}
    with httpx.Client() as client:
        form = _mf_get(client, f"{MATCHFORGE_BASE}/api/teams/{team_id}/form?last_n=5")
        stats = _mf_get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{team_id}/stats?last_n=5")
        team_type = _mf_get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{team_id}/type?last_n=5")
        corners = _mf_get(client, f"{MATCHFORGE_BASE}/api/matches/corners/team-profile/{team_name}?n_matches=10")
        cards = _mf_get(client, f"{MATCHFORGE_BASE}/api/matches/cards/team-profile/{team_name}?n_matches=10")
    if form:
        data["son_5_mac_formu"] = {
            "maclar": [
                {"rakip": m.get("opponent"), "saha": m.get("venue"), "skor": m.get("score"), "sonuc": m.get("result")}
                for m in (form.get("form") or [])
            ],
            "ozet": _pick(form.get("summary") or {}, "form_string", "points", "goals_for", "goals_against"),
        }
    if stats:
        data["sezon_istatistikleri"] = _pick(stats, "stats", "totals")
    if team_type:
        data["takim_tipi"] = _pick(team_type, "team_type")
    if corners:
        data["korner_profili"] = _pick(corners, "overall", "thresholds")
    if cards:
        data["kart_profili"] = _pick(cards, "overall", "thresholds")
    return data


def _fetch_fallback_team_data(team_id: int) -> Dict[str, Any]:
    """teamAnalyzer kapsamı dışındaki ligler için ham public.* tablolarından
    basit agregatlar — hâlâ gerçek veri, sadece daha az zengin."""
    data: Dict[str, Any] = {}

    df = query_to_df(
        "SELECT last5_avg_shots, last5_avg_sot, last5_avg_xg, last5_avg_goals, last5_avg_conceded_shots "
        "FROM public.team_recent_form WHERE team_id = %s",
        (team_id,),
    )
    if not df.empty:
        r = df.iloc[0]
        data["son_5_mac_ortalamalari"] = {
            "sut": r["last5_avg_shots"], "isabetli_sut": r["last5_avg_sot"],
            "xg": r["last5_avg_xg"], "gol": r["last5_avg_goals"], "yedigi_sut": r["last5_avg_conceded_shots"],
        }

    df = query_to_df(
        "SELECT season_matches, season_avg_goals, season_avg_conceded_goals, season_avg_xg "
        "FROM public.team_stats_season WHERE team_id = %s",
        (team_id,),
    )
    if not df.empty:
        r = df.iloc[0]
        data["sezon_ortalamalari"] = {
            "mac_sayisi": r["season_matches"], "gol": r["season_avg_goals"],
            "yedigi_gol": r["season_avg_conceded_goals"], "xg": r["season_avg_xg"],
        }

    df = query_to_df(
        "SELECT position, played, wins, draws, losses, points FROM public.season_standings "
        "WHERE team_id = %s ORDER BY last_updated DESC LIMIT 1",
        (team_id,),
    )
    if not df.empty:
        r = df.iloc[0]
        data["lig_durumu"] = {
            "sira": r["position"], "oynanan": r["played"], "galibiyet": r["wins"],
            "beraberlik": r["draws"], "maglubiyet": r["losses"], "puan": r["points"],
        }

    return data


def _clean_for_json(obj: Any) -> Any:
    """pandas/numpy tiplerini (int64, NaN...) JSON-uyumlu düz Python tiplerine çevirir."""
    import math

    if isinstance(obj, dict):
        return {k: _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_for_json(v) for v in obj]
    if isinstance(obj, float):
        return None if math.isnan(obj) else obj
    if hasattr(obj, "item"):  # numpy skaler (int64, float64, vb.)
        val = obj.item()
        return None if isinstance(val, float) and math.isnan(val) else val
    return obj


# teamAnalyzer'ın kapsadığı 17 ligin ID'leri — skorjin_match_analysis.py'deki
# LEAGUE_NAME_TO_ID map'in DEĞERLERİ. public.leagues.id ile AYNI ID uzayını
# paylaştığı doğrulandı (ör. "Super Lig" hem public.leagues'te hem teamAnalyzer'da id=4).
_RICH_TIER_LEAGUE_IDS = {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 17, 20, 21, 24, 25, 26, 27}


def get_team_league(team_id: int) -> Dict[str, Any]:
    """Takımın gerçek ligini public.teams + public.leagues'ten çeker — Sportoto'nun
    verdiği 'round' adı ("8. Hafta") GERÇEK lig adı DEĞİL, bu yüzden lig ancak
    takım eşleştikten sonra buradan öğrenilebiliyor."""
    df = query_to_df(
        "SELECT l.id, l.name FROM public.teams t JOIN public.leagues l ON t.league_id = l.id WHERE t.id = %s",
        (team_id,),
    )
    if df.empty:
        return {"league_id": None, "league_name": None}
    return {"league_id": int(df.iloc[0]["id"]), "league_name": df.iloc[0]["name"]}


def build_match_data(home: Dict[str, Any], away: Dict[str, Any], sportoto_round_label: Optional[str]) -> Dict[str, Any]:
    """home/away: {"team_id", "name"} — resolve_team'den gelen sonuç.
    sportoto_round_label sadece gerçek lig bulunamazsa yedek görüntüleme etiketi olarak kullanılır."""
    home_league = get_team_league(home["team_id"]) if home.get("team_id") else {}
    away_league = get_team_league(away["team_id"]) if away.get("team_id") else {}
    league_id = home_league.get("league_id") or away_league.get("league_id")
    league_name = home_league.get("league_name") or away_league.get("league_name") or sportoto_round_label

    data: Dict[str, Any] = {
        "ev_sahibi": translate_team_name(home.get("name")),
        "deplasman": translate_team_name(away.get("name")),
        "lig": league_name,
    }

    use_rich = league_id in _RICH_TIER_LEAGUE_IDS

    for side, team in (("ev_sahibi_verisi", home), ("deplasman_verisi", away)):
        team_id = team.get("team_id")
        if not team_id:
            continue
        team_data = {}
        if use_rich:
            try:
                team_data = _fetch_rich_team_data(team_id, team.get("name", ""))
            except Exception:
                team_data = {}
        if not team_data:
            team_data = _fetch_fallback_team_data(team_id)
        if team_data:
            data[side] = team_data

    _translate_names_in_place(data)
    return _clean_for_json(data)


# ==================== 4. Skorjin çağrısı ====================

def generate_scenarios(match_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    with httpx.Client() as client:
        try:
            resp = client.post(
                f"{SKORJIN_BASE}/api/v1/spor-toto-scenarios",
                json={"match_data": match_data},
                timeout=90.0,
            )
        except httpx.HTTPError as e:
            raise SporTotoFetchError(f"Skorjin spor-toto-scenarios ucuna ulaşılamadı: {e}")

    if resp.status_code != 200:
        raise SporTotoFetchError(f"Skorjin'den hata: {resp.status_code} {resp.text[:300]}")

    body = resp.json()
    scenarios = body.get("scenarios")
    if not scenarios:
        raise SporTotoFetchError("Skorjin boş senaryo listesi döndürdü.")
    return scenarios
