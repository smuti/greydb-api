"""
Maç Verisi Yönetimi Router
- Oynanmış maçları tespit et
- FotMob'dan detaylı veri çek
- Veritabanına kaydet
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timedelta
import httpx
import asyncio

from app.services.db import execute_query, execute_insert

router = APIRouter(
    prefix="/match-data",
    tags=["Match Data Management"]
)

# FotMob API config
FOTMOB_API_URL = "https://www.fotmob.com/api"
FOTMOB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


class UnprocessedMatch(BaseModel):
    id: int
    fotmob_match_id: int
    match_url: Optional[str]
    match_date: datetime
    home_team_name: str
    away_team_name: str
    round: Optional[str]
    league_name: str
    fotmob_league_id: int


class FinishedMatchInfo(BaseModel):
    id: int
    fotmob_match_id: int
    home_team_name: str
    away_team_name: str
    home_score: Optional[int]
    away_score: Optional[int]
    finished: bool


class ProcessResult(BaseModel):
    total_checked: int
    finished_count: int
    processed_count: int
    error_count: int
    errors: List[str]
    finished_matches: List[FinishedMatchInfo]


async def fetch_match_details(match_id: int) -> dict:
    """FotMob API'den maç detaylarını çek"""
    url = f"{FOTMOB_API_URL}/matchDetails?matchId={match_id}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, headers=FOTMOB_HEADERS)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"FotMob API error: {response.status_code}"
            )
        
        return response.json()


def parse_match_info(data: dict) -> dict:
    """FotMob API yanıtını parse et"""
    general = data.get("general", {})
    header = data.get("header", {})
    content = data.get("content", {})
    
    # Maç bitti mi?
    finished = header.get("status", {}).get("finished", False)
    
    # Skorlar
    teams = header.get("teams", [{}, {}])
    home_score = teams[0].get("score") if len(teams) >= 2 else None
    away_score = teams[1].get("score") if len(teams) >= 2 else None
    
    # Takım bilgileri
    home_team = general.get("homeTeam", {})
    away_team = general.get("awayTeam", {})
    
    # Hakem
    match_facts = content.get("matchFacts", {})
    info_box = match_facts.get("infoBox", {})
    referee_info = info_box.get("Referee", {})
    referee_name = referee_info.get("text", "") if referee_info else None
    
    # Maç tarihi
    match_date = general.get("matchTimeUTCDate", "")[:10] if general.get("matchTimeUTCDate") else None
    
    # Lig bilgileri
    league_name = general.get("leagueName", "")
    league_round = general.get("leagueRoundName", "")
    
    # Kadro bilgileri
    lineup_data = content.get("lineup", {})
    lineups = {"home": None, "away": None}
    
    for side, team_key in [("home", "homeTeam"), ("away", "awayTeam")]:
        team_data = lineup_data.get(team_key, {})
        if team_data:
            players = []
            for player in team_data.get("starters", []):
                pos_id = player.get("positionId", 0)
                
                # Pozisyon belirleme
                if pos_id == 11:
                    role = "GK"
                elif pos_id in range(32, 40):
                    role = "DEF"
                elif pos_id in range(51, 100):
                    role = "MID"
                else:
                    role = "FWD"
                
                name_data = player.get("name", "")
                if isinstance(name_data, dict):
                    player_name = name_data.get("fullName", name_data.get("firstName", ""))
                else:
                    player_name = str(name_data)
                
                shirt_num = player.get("shirtNumber", "0")
                if isinstance(shirt_num, str):
                    shirt_num = int(shirt_num) if shirt_num.isdigit() else 0
                
                players.append({
                    "name": player_name,
                    "shirt_number": shirt_num,
                    "position_id": pos_id,
                    "role": role
                })
            
            lineups[side] = {
                "lineup": players,
                "formation": team_data.get("formation", "N/A")
            }
    
    return {
        "finished": finished,
        "home_score": home_score,
        "away_score": away_score,
        "home_team_id": home_team.get("id"),
        "home_team_name": home_team.get("name", ""),
        "away_team_id": away_team.get("id"),
        "away_team_name": away_team.get("name", ""),
        "referee_name": referee_name,
        "match_date": match_date,
        "league_name": league_name,
        "league_round": league_round,
        "lineups": lineups
    }


@router.get("/unprocessed", response_model=List[UnprocessedMatch])
async def get_unprocessed_matches(
    fotmob_league_id: Optional[int] = None,
    limit: int = 100
):
    """
    İşlenmemiş (is_processed=false) upcoming maçları getir.
    Sadece geçmiş tarihlileri getir (bitmiş olabilir).
    """
    query = """
        SELECT 
            um.id,
            um.fotmob_match_id,
            um.match_url,
            um.match_date,
            um.home_team_name,
            um.away_team_name,
            um.round,
            l.name as league_name,
            l.fotmob_league_id
        FROM public.upcoming_matches um
        JOIN public.leagues l ON um.league_id = l.id
        WHERE um.is_processed = false
        AND um.match_date < NOW()
    """
    
    if fotmob_league_id:
        query += f" AND l.fotmob_league_id = {fotmob_league_id}"
    
    query += f" ORDER BY um.match_date ASC LIMIT {limit}"
    
    matches = execute_query(query)
    return matches


@router.post("/check-finished", response_model=List[FinishedMatchInfo])
async def check_finished_matches(match_ids: List[int]):
    """
    Verilen maç ID'lerinin FotMob'da bitip bitmediğini kontrol et.
    """
    finished_matches = []
    
    for match_id in match_ids:
        try:
            data = await fetch_match_details(match_id)
            info = parse_match_info(data)
            
            finished_matches.append(FinishedMatchInfo(
                id=0,  # Placeholder, will be filled by caller
                fotmob_match_id=match_id,
                home_team_name=info["home_team_name"],
                away_team_name=info["away_team_name"],
                home_score=info["home_score"],
                away_score=info["away_score"],
                finished=info["finished"]
            ))
            
            # Rate limiting - FotMob'u aşırı yüklemeyelim
            await asyncio.sleep(0.5)
            
        except Exception as e:
            print(f"Error checking match {match_id}: {e}")
            continue
    
    return finished_matches


@router.post("/process-finished", response_model=ProcessResult)
async def process_finished_matches(
    fotmob_league_id: Optional[int] = None,
    limit_per_league: int = 20,
    dry_run: bool = False
):
    """
    Bitmiş maçları tespit et ve veritabanına kaydet.
    
    LİGE GÖRE GRUPLA - Her lig için ayrı ayrı kontrol et.
    İlk oynanmamış maçta o ligi bırak, sonraki lige geç.
    
    Args:
        fotmob_league_id: Opsiyonel lig filtresi
        limit_per_league: Her lig için maksimum kontrol edilecek maç sayısı
        dry_run: True ise sadece kontrol et, kaydetme
    """
    result = ProcessResult(
        total_checked=0,
        finished_count=0,
        processed_count=0,
        error_count=0,
        errors=[],
        finished_matches=[]
    )
    
    # 1. Ligleri al
    leagues_query = """
        SELECT DISTINCT l.id, l.name, l.fotmob_league_id
        FROM public.upcoming_matches um
        JOIN public.leagues l ON um.league_id = l.id
        WHERE um.is_processed = false
        AND um.match_date < NOW()
    """
    if fotmob_league_id:
        leagues_query += f" AND l.fotmob_league_id = {fotmob_league_id}"
    
    leagues = execute_query(leagues_query)
    
    if not leagues:
        return result
    
    # 2. Her lig için ayrı ayrı işle
    for league in leagues:
        league_id = league["id"]
        league_name = league["name"]
        
        # Bu ligin maçlarını al (tarih sırasına göre)
        matches_query = f"""
            SELECT 
                um.id,
                um.fotmob_match_id,
                um.match_url,
                um.match_date,
                um.home_team_name,
                um.away_team_name,
                um.round,
                '{league_name}' as league_name,
                {league["fotmob_league_id"]} as fotmob_league_id,
                {league_id} as league_id
            FROM public.upcoming_matches um
            WHERE um.league_id = {league_id}
            AND um.is_processed = false
            AND um.match_date < NOW()
            ORDER BY um.match_date ASC
            LIMIT {limit_per_league}
        """
        
        league_matches = execute_query(matches_query)
        
        # Bu lig için maçları kontrol et
        for match in league_matches:
            result.total_checked += 1
            fotmob_match_id = match["fotmob_match_id"]
            
            try:
                data = await fetch_match_details(fotmob_match_id)
                info = parse_match_info(data)
                
                # Maç bitti mi?
                if info["finished"] and info["home_score"] is not None and info["away_score"] is not None:
                    result.finished_count += 1
                    
                    finished_info = FinishedMatchInfo(
                        id=match["id"],
                        fotmob_match_id=fotmob_match_id,
                        home_team_name=info["home_team_name"],
                        away_team_name=info["away_team_name"],
                        home_score=info["home_score"],
                        away_score=info["away_score"],
                        finished=True
                    )
                    result.finished_matches.append(finished_info)
                    
                    if not dry_run:
                        # public.matches'a kaydet (veya güncelle)
                        try:
                            check_query = f"""
                                SELECT id FROM public.matches 
                                WHERE fotmob_match_id = {fotmob_match_id}
                            """
                            existing = execute_query(check_query)
                            
                            if existing:
                                update_query = """
                                    UPDATE public.matches 
                                    SET home_score = :home_score,
                                        away_score = :away_score,
                                        updated_at = NOW()
                                    WHERE fotmob_match_id = :fotmob_match_id
                                """
                                execute_insert(update_query, {
                                    "home_score": info["home_score"],
                                    "away_score": info["away_score"],
                                    "fotmob_match_id": fotmob_match_id
                                })
                            else:
                                insert_query = """
                                    INSERT INTO public.matches (
                                        fotmob_match_id, league_id, season,
                                        match_date, round, round_name,
                                        home_team_fotmob_id, away_team_fotmob_id,
                                        home_score, away_score, page_url
                                    ) VALUES (
                                        :fotmob_match_id, :league_id, :season,
                                        :match_date, :round, :round_name,
                                        :home_team_id, :away_team_id,
                                        :home_score, :away_score, :page_url
                                    )
                                """
                                execute_insert(insert_query, {
                                    "fotmob_match_id": fotmob_match_id,
                                    "league_id": match["league_id"],
                                    "season": "2024-2025",
                                    "match_date": match["match_date"],
                                    "round": match.get("round", ""),
                                    "round_name": info.get("league_round", ""),
                                    "home_team_id": info.get("home_team_id"),
                                    "away_team_id": info.get("away_team_id"),
                                    "home_score": info["home_score"],
                                    "away_score": info["away_score"],
                                    "page_url": match.get("match_url", "")
                                })
                            
                            # upcoming_matches'ta is_processed=true yap
                            mark_processed_query = """
                                UPDATE public.upcoming_matches 
                                SET is_processed = true, updated_at = NOW()
                                WHERE id = :id
                            """
                            execute_insert(mark_processed_query, {"id": match["id"]})
                            
                            result.processed_count += 1
                            
                        except Exception as e:
                            result.error_count += 1
                            result.errors.append(f"{match['home_team_name']} vs {match['away_team_name']}: {str(e)}")
                else:
                    # Bu ligde ilk oynanmamış maça ulaştık
                    # Bu ligi bırak, sonraki lige geç
                    break
                
                # Rate limiting
                await asyncio.sleep(0.3)
                
            except Exception as e:
                result.error_count += 1
                result.errors.append(f"FotMob API error for {fotmob_match_id}: {str(e)}")
                continue
    
    return result


@router.get("/stats")
async def get_match_data_stats():
    """
    Maç verisi istatistiklerini getir.
    """
    # Upcoming matches stats
    upcoming_query = """
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE is_processed = true) as processed,
            COUNT(*) FILTER (WHERE is_processed = false) as unprocessed,
            COUNT(*) FILTER (WHERE is_processed = false AND match_date < NOW()) as ready_to_process
        FROM public.upcoming_matches
    """
    upcoming_stats = execute_query(upcoming_query)
    
    # Matches stats
    matches_query = """
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE home_score IS NOT NULL) as with_score
        FROM public.matches
    """
    matches_stats = execute_query(matches_query)
    
    return {
        "upcoming_matches": upcoming_stats[0] if upcoming_stats else {},
        "matches": matches_stats[0] if matches_stats else {}
    }

