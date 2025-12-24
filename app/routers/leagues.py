"""
Leagues Router - League name lookup endpoints
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
from pydantic import BaseModel

from app.services.db import query_to_df

router = APIRouter(tags=["leagues"])


class LeagueResponse(BaseModel):
    id: int
    name: str
    country: Optional[str] = None


@router.get("/leagues/{league_id}", response_model=LeagueResponse)
async def get_league_by_id(league_id: int):
    """Get league name by FotMob ID"""
    sql = "SELECT id, name, country FROM greydb.leagues WHERE id = %s"
    df = query_to_df(sql, (league_id,))
    
    if df.empty:
        raise HTTPException(status_code=404, detail=f"League {league_id} not found")
    
    row = df.iloc[0]
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "country": row["country"] if row["country"] else None
    }


@router.post("/leagues/batch", response_model=dict)
async def get_leagues_batch(league_ids: List[int]):
    """Get multiple league names by IDs"""
    if not league_ids:
        return {}
    
    placeholders = ','.join(['%s'] * len(league_ids))
    sql = f"SELECT id, name, country FROM greydb.leagues WHERE id IN ({placeholders})"
    df = query_to_df(sql, tuple(league_ids))
    
    result = {}
    for _, row in df.iterrows():
        result[str(row["id"])] = {
            "id": int(row["id"]),
            "name": row["name"],
            "country": row["country"] if row["country"] else None
        }
    
    return result

