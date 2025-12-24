"""
Coupons Router - Kupon CRUD işlemleri
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

from app.services.db import query_to_df, execute_insert, execute_insert_many

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
        RETURNING id, type, image_url, winnings, total_odds, status, created_by_email, created_at, updated_at
    """
    
    coupon_data = {
        "type": coupon.type,
        "image_url": coupon.image_url,
        "winnings": coupon.winnings,
        "total_odds": coupon.total_odds,
        "status": coupon.status,
        "created_by_email": coupon.created_by_email
    }
    
    coupon_row = execute_insert(sql_coupon, coupon_data)
    
    if not coupon_row:
        raise HTTPException(status_code=500, detail="Kupon oluşturulamadı")
    
    coupon_id = int(coupon_row["id"])
    
    # Maçları ekle
    sql_match = """
        INSERT INTO greydb.coupon_matches (
            coupon_id, home_team, away_team, league, prediction,
            market_name, odds, match_date, prediction_id
        ) VALUES (
            :coupon_id, :home_team, :away_team, :league, :prediction,
            :market_name, :odds, :match_date, :prediction_id
        )
    """
    
    match_params = []
    for match in coupon.matches:
        match_params.append({
            "coupon_id": coupon_id,
            **match.model_dump()
        })
    
    if match_params:
        execute_insert_many(sql_match, match_params)
    
    # Oluşturulan kuponu getir
    return await get_coupon(coupon_id)


@router.get("/coupons", response_model=List[CouponResponse])
async def list_coupons(
    type: Optional[str] = None,
    status: Optional[str] = None,
    exclude_finished: bool = True,  # Varsayılan olarak bitmiş maçları olan kuponları hariç tut
    limit: int = 50,
    offset: int = 0
):
    """Kuponları listele
    
    exclude_finished=True olduğunda, tüm maçları bitmiş kuponlar filtrelenir.
    """
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
        
        # Eğer exclude_finished=True ise, tüm maçları bitmiş kuponları atla
        if exclude_finished and matches:
            has_upcoming_match = _has_upcoming_matches(matches)
            if not has_upcoming_match:
                continue  # Tüm maçlar bitmiş, bu kuponu atla
        
        coupons.append(_row_to_response(row, matches))
    
    return coupons


def _has_upcoming_matches(matches: List[dict]) -> bool:
    """Kuponda henüz başlamamış maç var mı kontrol et"""
    from datetime import datetime, timezone, timedelta
    
    # Türkiye saati UTC+3
    turkey_tz_offset = timedelta(hours=3)
    now_utc = datetime.now(timezone.utc)
    now_turkey = now_utc + turkey_tz_offset
    
    # Türkçe ay isimleri
    turkish_months = {
        'Oca': 1, 'Şub': 2, 'Mar': 3, 'Nis': 4, 'May': 5, 'Haz': 6,
        'Tem': 7, 'Ağu': 8, 'Eyl': 9, 'Eki': 10, 'Kas': 11, 'Ara': 12
    }
    
    for match in matches:
        match_date_str = match.get("match_date")
        if not match_date_str:
            # Tarih yoksa henüz bitmemiş kabul et
            return True
        
        try:
            if isinstance(match_date_str, str):
                # Türkçe format: "23 Ara 17:30"
                parts = match_date_str.split()
                if len(parts) >= 3:
                    day = int(parts[0])
                    month_str = parts[1]
                    time_parts = parts[2].split(':')
                    hour = int(time_parts[0])
                    minute = int(time_parts[1]) if len(time_parts) > 1 else 0
                    
                    month = turkish_months.get(month_str, 1)
                    year = now_turkey.year
                    
                    # Eğer ay geçmişte kaldıysa gelecek yıl olabilir
                    match_date = datetime(year, month, day, hour, minute)
                    
                    # Maç henüz başlamamışsa True döndür
                    if match_date > now_turkey.replace(tzinfo=None):
                        return True
                else:
                    # Format anlaşılamadı, henüz bitmemiş kabul et
                    return True
            else:
                # datetime objesi ise direkt karşılaştır
                if match_date_str.tzinfo is None:
                    match_date = match_date_str
                else:
                    match_date = match_date_str.replace(tzinfo=None) + turkey_tz_offset
                
                if match_date > now_turkey.replace(tzinfo=None):
                    return True
        except Exception as e:
            # Parse hatası varsa henüz bitmemiş kabul et
            print(f"Date parse error: {e} for {match_date_str}")
            return True
    
    # Tüm maçlar bitmiş
    return False


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

