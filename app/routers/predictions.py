"""
Predictions Router - Tahmin CRUD işlemleri
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

from app.services.db import execute_query, query_to_df

router = APIRouter(tags=["predictions"])


class PredictionCreate(BaseModel):
    """Tahmin oluşturma şeması"""
    # Maç bilgileri
    home_team: str
    away_team: str
    league: str
    match_date: datetime
    home_team_fotmob_id: Optional[int] = None
    away_team_fotmob_id: Optional[int] = None
    match_fotmob_id: Optional[int] = None
    # Bahis bilgileri
    market_name: str
    pick: str
    pick_name: Optional[str] = None
    odds: Optional[float] = None
    probability: Optional[float] = None
    # Tahmin detayları
    prediction_type: str = "text"  # text, audio
    content: Optional[str] = None
    audio_url: Optional[str] = None
    audio_file_name: Optional[str] = None
    analysis: Optional[str] = None
    # Durum
    status: str = "draft"  # draft, active
    # Meta
    created_by_email: str


class PredictionUpdate(BaseModel):
    """Tahmin güncelleme şeması"""
    market_name: Optional[str] = None
    pick: Optional[str] = None
    pick_name: Optional[str] = None
    odds: Optional[float] = None
    probability: Optional[float] = None
    prediction_type: Optional[str] = None
    content: Optional[str] = None
    audio_url: Optional[str] = None
    audio_file_name: Optional[str] = None
    analysis: Optional[str] = None
    status: Optional[str] = None
    result: Optional[str] = None


class PredictionResponse(BaseModel):
    """Tahmin response şeması"""
    id: int
    home_team: str
    away_team: str
    league: str
    match_date: datetime
    home_team_fotmob_id: Optional[int]
    away_team_fotmob_id: Optional[int]
    match_fotmob_id: Optional[int]
    market_name: str
    pick: str
    pick_name: Optional[str]
    odds: Optional[float]
    probability: Optional[float]
    prediction_type: str
    content: Optional[str]
    audio_url: Optional[str]
    audio_file_name: Optional[str]
    analysis: Optional[str]
    status: str
    result: Optional[str]
    created_by_email: str
    created_at: datetime
    updated_at: datetime


@router.post("/predictions", response_model=PredictionResponse)
async def create_prediction(prediction: PredictionCreate):
    """Yeni tahmin oluştur"""
    sql = """
        INSERT INTO greydb.predictions (
            home_team, away_team, league, match_date,
            home_team_fotmob_id, away_team_fotmob_id, match_fotmob_id,
            market_name, pick, pick_name, odds, probability,
            prediction_type, content, audio_url, audio_file_name, analysis,
            status, created_by_email
        ) VALUES (
            :home_team, :away_team, :league, :match_date,
            :home_team_fotmob_id, :away_team_fotmob_id, :match_fotmob_id,
            :market_name, :pick, :pick_name, :odds, :probability,
            :prediction_type, :content, :audio_url, :audio_file_name, :analysis,
            :status, :created_by_email
        )
        RETURNING *
    """
    
    df = query_to_df(sql, prediction.model_dump())
    
    if df.empty:
        raise HTTPException(status_code=500, detail="Tahmin oluşturulamadı")
    
    row = df.iloc[0]
    return _row_to_response(row)


@router.get("/predictions", response_model=List[PredictionResponse])
async def list_predictions(
    status: Optional[str] = None,
    created_by_email: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Tahminleri listele"""
    conditions = []
    params = {"limit": limit, "offset": offset}
    
    if status:
        conditions.append("status = :status")
        params["status"] = status
    
    if created_by_email:
        conditions.append("created_by_email = :created_by_email")
        params["created_by_email"] = created_by_email
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    sql = f"""
        SELECT * FROM greydb.predictions
        {where_clause}
        ORDER BY match_date DESC, created_at DESC
        LIMIT :limit OFFSET :offset
    """
    
    df = query_to_df(sql, params)
    
    return [_row_to_response(row) for _, row in df.iterrows()]


@router.get("/predictions/{prediction_id}", response_model=PredictionResponse)
async def get_prediction(prediction_id: int):
    """Tahmin detayı getir"""
    sql = "SELECT * FROM greydb.predictions WHERE id = :id"
    df = query_to_df(sql, {"id": prediction_id})
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Tahmin bulunamadı")
    
    return _row_to_response(df.iloc[0])


@router.put("/predictions/{prediction_id}", response_model=PredictionResponse)
async def update_prediction(prediction_id: int, update: PredictionUpdate):
    """Tahmin güncelle"""
    # Sadece dolu alanları güncelle
    update_data = {k: v for k, v in update.model_dump().items() if v is not None}
    
    if not update_data:
        raise HTTPException(status_code=400, detail="Güncellenecek alan yok")
    
    # updated_at ekle
    update_data["updated_at"] = datetime.now()
    update_data["id"] = prediction_id
    
    set_clause = ", ".join([f"{k} = :{k}" for k in update_data.keys() if k != "id"])
    
    sql = f"""
        UPDATE greydb.predictions
        SET {set_clause}
        WHERE id = :id
        RETURNING *
    """
    
    df = query_to_df(sql, update_data)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Tahmin bulunamadı")
    
    return _row_to_response(df.iloc[0])


@router.delete("/predictions/{prediction_id}")
async def delete_prediction(prediction_id: int):
    """Tahmin sil"""
    sql = "DELETE FROM greydb.predictions WHERE id = :id RETURNING id"
    df = query_to_df(sql, {"id": prediction_id})
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Tahmin bulunamadı")
    
    return {"message": "Tahmin silindi", "id": prediction_id}


def _row_to_response(row) -> dict:
    """DataFrame satırını response'a çevir"""
    return {
        "id": int(row["id"]),
        "home_team": row["home_team"],
        "away_team": row["away_team"],
        "league": row["league"],
        "match_date": row["match_date"],
        "home_team_fotmob_id": int(row["home_team_fotmob_id"]) if row["home_team_fotmob_id"] else None,
        "away_team_fotmob_id": int(row["away_team_fotmob_id"]) if row["away_team_fotmob_id"] else None,
        "match_fotmob_id": int(row["match_fotmob_id"]) if row["match_fotmob_id"] else None,
        "market_name": row["market_name"],
        "pick": row["pick"],
        "pick_name": row["pick_name"],
        "odds": float(row["odds"]) if row["odds"] else None,
        "probability": float(row["probability"]) if row["probability"] else None,
        "prediction_type": row["prediction_type"],
        "content": row["content"],
        "audio_url": row["audio_url"],
        "audio_file_name": row["audio_file_name"],
        "analysis": row["analysis"],
        "status": row["status"],
        "result": row["result"],
        "created_by_email": row["created_by_email"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }

