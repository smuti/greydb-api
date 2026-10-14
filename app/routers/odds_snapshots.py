"""
Odds Snapshots Router - İddaa oran anlık görüntüleri (append-only)

Admin bir maçın o anki İddaa oranlarını kaydettiğinde (ham veri + kural
bazlı yorum) yeni bir satır eklenir — güncelleme/üzerine yazma yok, zamanla
aynı maç için bir snapshot geçmişi birikir.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Any, Dict
from datetime import datetime
import json

from app.services.db import query_to_df

router = APIRouter(tags=["odds_snapshots"])


class OddsSnapshotCreate(BaseModel):
    """Oran snapshot oluşturma şeması"""
    iddaa_match_id: int
    home_team: str
    away_team: str
    total_markets: Optional[int] = None
    raw_data: Dict[str, Any]
    interpretation: Optional[Dict[str, Any]] = None
    created_by_email: str


class OddsSnapshotResponse(BaseModel):
    """Oran snapshot response şeması"""
    id: int
    iddaa_match_id: int
    home_team: str
    away_team: str
    total_markets: Optional[int]
    raw_data: Dict[str, Any]
    interpretation: Optional[Dict[str, Any]]
    created_by_email: str
    created_at: datetime


@router.post("/odds-snapshots", response_model=OddsSnapshotResponse)
async def create_odds_snapshot(snapshot: OddsSnapshotCreate):
    """Yeni oran snapshot'ı kaydet (append-only, her çağrı yeni bir satır ekler)"""
    sql = """
        INSERT INTO greydb.odds_snapshots (
            iddaa_match_id, home_team, away_team, total_markets,
            raw_data, interpretation, created_by_email
        ) VALUES (
            :iddaa_match_id, :home_team, :away_team, :total_markets,
            CAST(:raw_data AS jsonb), CAST(:interpretation AS jsonb), :created_by_email
        )
        RETURNING *
    """

    params = snapshot.model_dump()
    params["raw_data"] = json.dumps(params["raw_data"])
    params["interpretation"] = (
        json.dumps(params["interpretation"]) if params.get("interpretation") is not None else None
    )

    df = query_to_df(sql, params, commit=True)

    if df.empty:
        raise HTTPException(status_code=500, detail="Snapshot kaydedilemedi")

    return _row_to_response(df.iloc[0])


@router.get("/odds-snapshots", response_model=List[OddsSnapshotResponse])
async def list_odds_snapshots(
    iddaa_match_id: Optional[int] = None,
    limit: int = 50,
    offset: int = 0,
):
    """Oran snapshot geçmişini listele (en yeni önce)"""
    conditions = []
    params = {"limit": limit, "offset": offset}

    if iddaa_match_id is not None:
        conditions.append("iddaa_match_id = :iddaa_match_id")
        params["iddaa_match_id"] = iddaa_match_id

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT * FROM greydb.odds_snapshots
        {where_clause}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """

    df = query_to_df(sql, params)

    return [_row_to_response(row) for _, row in df.iterrows()]


def _row_to_response(row) -> dict:
    """DataFrame satırını response'a çevir"""
    import pandas as pd

    def safe_int(val):
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        return int(val)

    def safe_json(val):
        if val is None:
            return None
        if isinstance(val, str):
            try:
                return json.loads(val)
            except (TypeError, ValueError):
                return None
        return val

    return {
        "id": int(row["id"]),
        "iddaa_match_id": int(row["iddaa_match_id"]),
        "home_team": row["home_team"],
        "away_team": row["away_team"],
        "total_markets": safe_int(row.get("total_markets")),
        "raw_data": safe_json(row["raw_data"]) or {},
        "interpretation": safe_json(row.get("interpretation")),
        "created_by_email": row["created_by_email"],
        "created_at": row["created_at"],
    }
