"""
Head-to-Head Endpoint'leri
"""
from fastapi import APIRouter, Query
from app.services.stats import get_h2h

router = APIRouter(prefix="/h2h", tags=["H2H"])


@router.get("/{team1_fotmob_id}/{team2_fotmob_id}")
async def head_to_head(
    team1_fotmob_id: int,
    team2_fotmob_id: int,
    limit: int = Query(10, ge=1, le=50, description="Maç sayısı"),
    home_only: bool = Query(False, description="Sadece team1 ev sahibiyken")
):
    """
    İki takım arası H2H istatistikleri
    
    - **team1_fotmob_id**: Takım 1 FotMob ID (örn: 8637 = Galatasaray)
    - **team2_fotmob_id**: Takım 2 FotMob ID (örn: 8695 = Fenerbahçe)
    - **limit**: Kaç maç (default 10)
    - **home_only**: True ise sadece team1'in ev sahibi olduğu maçlar
    
    Örnek:
    - /h2h/8637/8695 → GS vs FB tüm maçlar
    - /h2h/8637/8695?home_only=true → GS evinde FB'ye karşı
    """
    return get_h2h(
        team1_fotmob_id=team1_fotmob_id,
        team2_fotmob_id=team2_fotmob_id,
        limit=limit,
        home_only=home_only
    )

