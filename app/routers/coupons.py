"""
Coupons Router - Kupon CRUD işlemleri
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

from app.services.db import query_to_df

router = APIRouter(tags=["coupons"])


class CouponMatchCreate(BaseModel):
    """Kupon maçı oluşturma şeması"""
    home_team: str
    away_team: str
    league: Optional[str] = None
    prediction: str
    market_name: Optional[str] = None
    odds: Optional[float] = None
    match_date: Optional[str] = None
    prediction_id: Optional[int] = None


class CouponCreate(BaseModel):
    """Kupon oluşturma şeması"""
    type: str = "premium"  # premium, banko, populer, yukselen, uyem, editor
    image_url: Optional[str] = None
    winnings: Optional[str] = None
    total_odds: float
    status: str = "draft"  # draft, active, archived
    created_by_email: str
    matches: List[CouponMatchCreate]


class CouponUpdate(BaseModel):
    """Kupon güncelleme şeması"""
    image_url: Optional[str] = None
    winnings: Optional[str] = None
    total_odds: Optional[float] = None
    status: Optional[str] = None


class CouponMatchResponse(BaseModel):
    """Kupon maçı response şeması"""
    id: int
    coupon_id: int
    home_team: str
    away_team: str
    league: Optional[str]
    prediction: str
    market_name: Optional[str]
    odds: Optional[float]
    match_date: Optional[str]
    prediction_id: Optional[int]


class CouponResponse(BaseModel):
    """Kupon response şeması"""
    id: int
    type: str
    image_url: Optional[str]
    winnings: Optional[str]
    total_odds: float
    status: str
    created_by_email: str
    created_at: datetime
    updated_at: datetime
    matches: List[CouponMatchResponse]


@router.post("/coupons", response_model=CouponResponse)
async def create_coupon(coupon: CouponCreate):
    """Yeni kupon oluştur"""
    # Premium için resim zorunlu
    if coupon.type == "premium" and not coupon.image_url:
        raise HTTPException(status_code=400, detail="Premium kuponlar için resim zorunludur")
    
    # Kupon oluştur
    sql_coupon = """
        INSERT INTO greydb.coupons (
            type, image_url, winnings, total_odds, status, created_by_email
        ) VALUES (
            :type, :image_url, :winnings, :total_odds, :status, :created_by_email
        )
        RETURNING *
    """
    
    coupon_data = {
        "type": coupon.type,
        "image_url": coupon.image_url,
        "winnings": coupon.winnings,
        "total_odds": coupon.total_odds,
        "status": coupon.status,
        "created_by_email": coupon.created_by_email
    }
    
    df_coupon = query_to_df(sql_coupon, coupon_data, commit=True)
    
    if df_coupon.empty:
        raise HTTPException(status_code=500, detail="Kupon oluşturulamadı")
    
    coupon_id = int(df_coupon.iloc[0]["id"])
    
    # Maçları ekle
    for match in coupon.matches:
        sql_match = """
            INSERT INTO greydb.coupon_matches (
                coupon_id, home_team, away_team, league, prediction,
                market_name, odds, match_date, prediction_id
            ) VALUES (
                :coupon_id, :home_team, :away_team, :league, :prediction,
                :market_name, :odds, :match_date, :prediction_id
            )
        """
        match_data = {
            "coupon_id": coupon_id,
            **match.model_dump()
        }
        query_to_df(sql_match, match_data, commit=True)
    
    # Oluşturulan kuponu getir
    return await get_coupon(coupon_id)


@router.get("/coupons", response_model=List[CouponResponse])
async def list_coupons(
    type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Kuponları listele"""
    conditions = []
    params = {"limit": limit, "offset": offset}
    
    if type:
        conditions.append("type = :type")
        params["type"] = type
    
    if status:
        conditions.append("status = :status")
        params["status"] = status
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    sql = f"""
        SELECT * FROM greydb.coupons
        {where_clause}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """
    
    df = query_to_df(sql, params)
    
    coupons = []
    for _, row in df.iterrows():
        coupon_id = int(row["id"])
        matches = await _get_coupon_matches(coupon_id)
        coupons.append(_row_to_response(row, matches))
    
    return coupons


@router.get("/coupons/{coupon_id}", response_model=CouponResponse)
async def get_coupon(coupon_id: int):
    """Kupon detayı getir"""
    sql = "SELECT * FROM greydb.coupons WHERE id = :id"
    df = query_to_df(sql, {"id": coupon_id})
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    
    matches = await _get_coupon_matches(coupon_id)
    return _row_to_response(df.iloc[0], matches)


@router.put("/coupons/{coupon_id}", response_model=CouponResponse)
async def update_coupon(coupon_id: int, update: CouponUpdate):
    """Kupon güncelle"""
    update_data = {k: v for k, v in update.model_dump().items() if v is not None}
    
    if not update_data:
        raise HTTPException(status_code=400, detail="Güncellenecek alan yok")
    
    update_data["updated_at"] = datetime.now()
    update_data["id"] = coupon_id
    
    set_clause = ", ".join([f"{k} = :{k}" for k in update_data.keys() if k != "id"])
    
    sql = f"""
        UPDATE greydb.coupons
        SET {set_clause}
        WHERE id = :id
        RETURNING *
    """
    
    df = query_to_df(sql, update_data, commit=True)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    
    matches = await _get_coupon_matches(coupon_id)
    return _row_to_response(df.iloc[0], matches)


@router.delete("/coupons/{coupon_id}")
async def delete_coupon(coupon_id: int):
    """Kupon sil (cascade ile maçlar da silinir)"""
    sql = "DELETE FROM greydb.coupons WHERE id = :id RETURNING id"
    df = query_to_df(sql, {"id": coupon_id}, commit=True)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    
    return {"message": "Kupon silindi", "id": coupon_id}


async def _get_coupon_matches(coupon_id: int) -> List[dict]:
    """Kupon maçlarını getir"""
    sql = "SELECT * FROM greydb.coupon_matches WHERE coupon_id = :coupon_id ORDER BY id"
    df = query_to_df(sql, {"coupon_id": coupon_id})
    
    import pandas as pd
    
    matches = []
    for _, row in df.iterrows():
        matches.append({
            "id": int(row["id"]),
            "coupon_id": int(row["coupon_id"]),
            "home_team": row["home_team"],
            "away_team": row["away_team"],
            "league": row["league"] if not pd.isna(row["league"]) else None,
            "prediction": row["prediction"],
            "market_name": row["market_name"] if not pd.isna(row["market_name"]) else None,
            "odds": float(row["odds"]) if not pd.isna(row["odds"]) else None,
            "match_date": row["match_date"] if not pd.isna(row["match_date"]) else None,
            "prediction_id": int(row["prediction_id"]) if not pd.isna(row["prediction_id"]) else None,
        })
    
    return matches


def _row_to_response(row, matches: List[dict]) -> dict:
    """DataFrame satırını response'a çevir"""
    import pandas as pd
    
    return {
        "id": int(row["id"]),
        "type": row["type"],
        "image_url": row["image_url"] if not pd.isna(row["image_url"]) else None,
        "winnings": row["winnings"] if not pd.isna(row["winnings"]) else None,
        "total_odds": float(row["total_odds"]),
        "status": row["status"],
        "created_by_email": row["created_by_email"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "matches": matches,
    }

