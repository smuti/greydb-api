"""
Match Comments Router - Günün Maç Yorumları CRUD işlemleri
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, date

from app.services.db import query_to_df

router = APIRouter(tags=["match-comments"])


class MatchCommentCreate(BaseModel):
    """Maç yorumu oluşturma şeması"""
    league: str
    home_team: str
    away_team: str
    match_date: datetime
    audio_url: Optional[str] = None
    summary: Optional[str] = None
    created_by_email: str


class MatchCommentUpdate(BaseModel):
    """Maç yorumu güncelleme şeması"""
    league: Optional[str] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    match_date: Optional[datetime] = None
    audio_url: Optional[str] = None
    summary: Optional[str] = None
    status: Optional[str] = None


class MatchCommentResponse(BaseModel):
    """Maç yorumu response şeması"""
    id: int
    league: str
    home_team: str
    away_team: str
    match_date: datetime
    audio_url: Optional[str]
    summary: Optional[str]
    status: str
    created_at: datetime
    created_by_email: Optional[str]


@router.post("/match-comments", response_model=MatchCommentResponse)
async def create_match_comment(comment: MatchCommentCreate):
    """Yeni maç yorumu oluştur"""
    sql = """
        INSERT INTO greydb.match_comments (
            league, home_team, away_team, match_date,
            audio_url, summary, created_by_email
        ) VALUES (
            :league, :home_team, :away_team, :match_date,
            :audio_url, :summary, :created_by_email
        )
        RETURNING *
    """
    
    df = query_to_df(sql, comment.model_dump(), commit=True)
    
    if df.empty:
        raise HTTPException(status_code=500, detail="Maç yorumu oluşturulamadı")
    
    row = df.iloc[0]
    return _row_to_response(row)


@router.get("/match-comments", response_model=List[MatchCommentResponse])
async def list_match_comments(
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Maç yorumlarını listele"""
    conditions = []
    params = {"limit": limit, "offset": offset}
    
    if status:
        conditions.append("status = :status")
        params["status"] = status
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    sql = f"""
        SELECT * FROM greydb.match_comments
        {where_clause}
        ORDER BY match_date DESC, created_at DESC
        LIMIT :limit OFFSET :offset
    """
    
    df = query_to_df(sql, params)
    
    return [_row_to_response(row) for _, row in df.iterrows()]


@router.get("/match-comments/active", response_model=List[MatchCommentResponse])
async def get_active_match_comments():
    """Bugünün aktif maç yorumlarını getir (ana sayfa için)"""
    sql = """
        SELECT * FROM greydb.match_comments
        WHERE status = 'active'
          AND match_date >= CURRENT_DATE
          AND match_date < CURRENT_DATE + INTERVAL '1 day'
        ORDER BY match_date ASC
    """
    
    df = query_to_df(sql)
    
    return [_row_to_response(row) for _, row in df.iterrows()]


@router.get("/match-comments/{comment_id}", response_model=MatchCommentResponse)
async def get_match_comment(comment_id: int):
    """Maç yorumu detayı getir"""
    sql = "SELECT * FROM greydb.match_comments WHERE id = :id"
    df = query_to_df(sql, {"id": comment_id})
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Maç yorumu bulunamadı")
    
    return _row_to_response(df.iloc[0])


@router.put("/match-comments/{comment_id}", response_model=MatchCommentResponse)
async def update_match_comment(comment_id: int, update: MatchCommentUpdate):
    """Maç yorumu güncelle"""
    update_data = {k: v for k, v in update.model_dump().items() if v is not None}
    
    if not update_data:
        raise HTTPException(status_code=400, detail="Güncellenecek alan yok")
    
    update_data["id"] = comment_id
    
    set_clause = ", ".join([f"{k} = :{k}" for k in update_data.keys() if k != "id"])
    
    sql = f"""
        UPDATE greydb.match_comments
        SET {set_clause}
        WHERE id = :id
        RETURNING *
    """
    
    df = query_to_df(sql, update_data, commit=True)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Maç yorumu bulunamadı")
    
    return _row_to_response(df.iloc[0])


@router.delete("/match-comments/{comment_id}")
async def delete_match_comment(comment_id: int):
    """Maç yorumu sil"""
    sql = "DELETE FROM greydb.match_comments WHERE id = :id RETURNING id"
    df = query_to_df(sql, {"id": comment_id}, commit=True)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Maç yorumu bulunamadı")
    
    return {"message": "Maç yorumu silindi", "id": comment_id}


@router.post("/match-comments/deactivate-expired")
async def deactivate_expired_comments():
    """Günü geçmiş yorumları inaktif yap"""
    sql = """
        UPDATE greydb.match_comments
        SET status = 'inactive'
        WHERE status = 'active'
          AND match_date < CURRENT_DATE
        RETURNING id
    """
    
    df = query_to_df(sql, commit=True)
    
    return {"message": f"{len(df)} yorum inaktif yapıldı", "count": len(df)}


def _row_to_response(row) -> dict:
    """DataFrame satırını response'a çevir"""
    import pandas as pd
    
    def safe_str(val):
        if pd.isna(val) or val is None:
            return None
        return str(val)
    
    return {
        "id": int(row["id"]),
        "league": row["league"],
        "home_team": row["home_team"],
        "away_team": row["away_team"],
        "match_date": row["match_date"],
        "audio_url": safe_str(row["audio_url"]),
        "summary": safe_str(row["summary"]),
        "status": row["status"],
        "created_at": row["created_at"],
        "created_by_email": safe_str(row["created_by_email"]),
    }

