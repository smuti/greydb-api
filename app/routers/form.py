"""
Takım Form Endpoint'leri
"""
from fastapi import APIRouter, Query
from app.services.stats import get_team_form, get_team_form_by_name

router = APIRouter(prefix="/form", tags=["Form"])


@router.get("/by-name/{team_name}")
async def team_form_by_name(
    team_name: str,
    limit: int = Query(5, ge=1, le=20, description="Maç sayısı"),
    venue: str = Query(None, description="home, away veya boş (genel)")
):
    """
    Takım adıyla form getir
    
    - **team_name**: Takım adı (örn: "Beşiktaş", "Galatasaray", "Barcelona")
    - **limit**: Kaç maç (default 5)
    - **venue**: 'home' (ev), 'away' (deplasman) veya boş (genel)
    """
    return get_team_form_by_name(
        team_name=team_name,
        limit=limit,
        venue=venue
    )


@router.get("/{team_fotmob_id}")
async def team_form(
    team_fotmob_id: int,
    limit: int = Query(5, ge=1, le=20, description="Maç sayısı"),
    venue: str = Query(None, description="home, away veya boş (genel)"),
    league_id: int = Query(None, description="Lig FotMob ID filtresi")
):
    """
    Takım formu - son X maç
    
    - **team_fotmob_id**: Takım FotMob ID (örn: 8637 = Galatasaray)
    - **limit**: Kaç maç (default 5)
    - **venue**: 'home' (ev), 'away' (deplasman) veya boş (genel)
    - **league_id**: Lig filtresi (örn: 71 = Süper Lig)
    """
    return get_team_form(
        team_fotmob_id=team_fotmob_id,
        limit=limit,
        venue=venue,
        league_fotmob_id=league_id
    )


@router.get("/{team_fotmob_id}/home")
async def team_home_form(
    team_fotmob_id: int,
    limit: int = Query(5, ge=1, le=20)
):
    """Takım ev formu - son X ev maçı"""
    return get_team_form(team_fotmob_id=team_fotmob_id, limit=limit, venue="home")


@router.get("/{team_fotmob_id}/away")
async def team_away_form(
    team_fotmob_id: int,
    limit: int = Query(5, ge=1, le=20)
):
    """Takım deplasman formu - son X deplasman maçı"""
    return get_team_form(team_fotmob_id=team_fotmob_id, limit=limit, venue="away")


@router.get("/{team_fotmob_id}/league/{league_fotmob_id}")
async def team_league_form(
    team_fotmob_id: int,
    league_fotmob_id: int,
    limit: int = Query(5, ge=1, le=20)
):
    """Takım lig formu - belirli ligdeki son X maç"""
    return get_team_form(
        team_fotmob_id=team_fotmob_id,
        limit=limit,
        league_fotmob_id=league_fotmob_id
    )

