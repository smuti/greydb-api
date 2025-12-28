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
    fotmob_url: Optional[str] = None  # FotMob maç linki
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
    # Ana sayfa gösterimi
    show_on_homepage: Optional[bool] = None
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
    result: Optional[str] = None  # pending, won, lost, void
    show_on_homepage: Optional[bool] = None
    fotmob_url: Optional[str] = None


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
    fotmob_url: Optional[str]
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
    result: Optional[str]  # pending, won, lost, void
    show_on_homepage: Optional[bool]
    created_by_email: str
    created_at: datetime
    updated_at: datetime


@router.post("/predictions", response_model=PredictionResponse)
async def create_prediction(prediction: PredictionCreate):
    """Yeni tahmin oluştur"""
    sql = """
        INSERT INTO greydb.predictions (
            home_team, away_team, league, match_date,
            home_team_fotmob_id, away_team_fotmob_id, match_fotmob_id, fotmob_url,
            market_name, pick, pick_name, odds, probability,
            prediction_type, content, audio_url, audio_file_name, analysis,
            status, show_on_homepage, created_by_email
        ) VALUES (
            :home_team, :away_team, :league, :match_date,
            :home_team_fotmob_id, :away_team_fotmob_id, :match_fotmob_id, :fotmob_url,
            :market_name, :pick, :pick_name, :odds, :probability,
            :prediction_type, :content, :audio_url, :audio_file_name, :analysis,
            :status, :show_on_homepage, :created_by_email
        )
        RETURNING *
    """
    
    df = query_to_df(sql, prediction.model_dump(), commit=True)
    
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
    
    df = query_to_df(sql, update_data, commit=True)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Tahmin bulunamadı")
    
    return _row_to_response(df.iloc[0])


@router.delete("/predictions/{prediction_id}")
async def delete_prediction(prediction_id: int):
    """Tahmin sil"""
    sql = "DELETE FROM greydb.predictions WHERE id = :id RETURNING id"
    df = query_to_df(sql, {"id": prediction_id}, commit=True)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="Tahmin bulunamadı")
    
    return {"message": "Tahmin silindi", "id": prediction_id}


@router.post("/predictions/check-results")
async def check_prediction_results():
    """
    Maç bitmiş tahminlerin sonuçlarını kontrol et ve güncelle.
    - Maç saati geçmiş ve result = NULL olan aktif tahminleri bul
    - greydb.matches tablosundan maç sonucunu kontrol et
    - Tahmin tuttu mu tutmadı mı belirle
    - result'ı güncelle (won/lost/void)
    - Hata olursa status = 'problem' yap
    """
    import httpx
    from datetime import datetime, timedelta
    
    # Maç saati en az 2 saat önce geçmiş ve henüz sonuçlanmamış tahminleri bul
    sql = """
        SELECT * FROM greydb.predictions 
        WHERE status = 'active' 
          AND result IS NULL 
          AND match_date < NOW() - INTERVAL '2 hours'
        ORDER BY match_date ASC
        LIMIT 50
    """
    df = query_to_df(sql)
    
    if df.empty:
        return {"message": "Kontrol edilecek tahmin yok", "checked": 0, "updated": 0, "problems": 0}
    
    updated = 0
    problems = 0
    results = []
    
    for _, prediction in df.iterrows():
        pred_id = int(prediction["id"])
        match_fotmob_id = prediction.get("match_fotmob_id")
        market_name = prediction["market_name"]
        pick = prediction["pick"]
        home_team = prediction["home_team"]
        away_team = prediction["away_team"]
        
        try:
            # Önce greydb.matches tablosundan kontrol et
            match_result = None
            if match_fotmob_id:
                match_sql = """
                    SELECT home_score, away_score, status 
                    FROM greydb.matches 
                    WHERE fotmob_id = :fotmob_id
                """
                match_df = query_to_df(match_sql, {"fotmob_id": int(match_fotmob_id)})
                
                if not match_df.empty:
                    match_row = match_df.iloc[0]
                    home_score = match_row.get("home_score")
                    away_score = match_row.get("away_score")
                    match_status = match_row.get("status")
                    
                    if home_score is not None and away_score is not None:
                        match_result = {
                            "home_score": int(home_score),
                            "away_score": int(away_score),
                            "status": match_status
                        }
            
            if not match_result:
                # greydb.matches'te bulunamadı, FotMob API'den dene
                fotmob_url = prediction.get("fotmob_url")
                if fotmob_url:
                    try:
                        # URL'den match ID çıkar (örn: .../2t8gjc#4803201 -> 4803201)
                        import re
                        match_id_match = re.search(r'#(\d+)', fotmob_url)
                        if not match_id_match:
                            # URL formatı: /matches/.../MATCHID
                            match_id_match = re.search(r'/(\d+)(?:\?|$|#)', fotmob_url)
                        
                        if match_id_match:
                            fotmob_match_id = match_id_match.group(1)
                            async with httpx.AsyncClient(timeout=10.0) as client:
                                fotmob_api_url = f"https://www.fotmob.com/api/matchDetails?matchId={fotmob_match_id}"
                                response = await client.get(fotmob_api_url)
                                if response.status_code == 200:
                                    data = response.json()
                                    # Maç bitti mi kontrol et
                                    if data.get("general", {}).get("finished"):
                                        teams = data.get("header", {}).get("teams", [])
                                        if len(teams) >= 2:
                                            match_result = {
                                                "home_score": teams[0].get("score", 0),
                                                "away_score": teams[1].get("score", 0),
                                                "status": "finished"
                                            }
                    except Exception as fotmob_error:
                        print(f"FotMob API error for prediction {pred_id}: {fotmob_error}")
            
            if not match_result:
                # Hala bulunamadı, sonra tekrar dene
                results.append({"id": pred_id, "status": "pending", "reason": "Maç sonucu henüz yok"})
                continue
            
            # Tahmin sonucunu hesapla
            result = _calculate_prediction_result(
                market_name, 
                pick, 
                match_result["home_score"], 
                match_result["away_score"]
            )
            
            if result:
                # Sonucu güncelle
                update_sql = """
                    UPDATE greydb.predictions 
                    SET result = :result, updated_at = NOW() 
                    WHERE id = :id
                """
                query_to_df(update_sql, {"id": pred_id, "result": result}, commit=True)
                updated += 1
                results.append({"id": pred_id, "status": "updated", "result": result})
            else:
                results.append({"id": pred_id, "status": "unknown", "reason": "Tahmin türü desteklenmiyor"})
                
        except Exception as e:
            # Hata durumunda status = 'problem' yap
            error_msg = str(e)[:200]
            try:
                problem_sql = """
                    UPDATE greydb.predictions 
                    SET status = 'problem', updated_at = NOW() 
                    WHERE id = :id
                """
                query_to_df(problem_sql, {"id": pred_id}, commit=True)
                problems += 1
                results.append({"id": pred_id, "status": "problem", "error": error_msg})
            except:
                pass
    
    return {
        "message": f"{len(df)} tahmin kontrol edildi",
        "checked": len(df),
        "updated": updated,
        "problems": problems,
        "results": results
    }


def _calculate_prediction_result(market_name: str, pick: str, home_score: int, away_score: int) -> Optional[str]:
    """
    Bahis türüne göre tahmin sonucunu hesapla.
    Returns: 'won', 'lost', 'void', or None (desteklenmeyen bahis türü)
    """
    total_goals = home_score + away_score
    
    # Maç Sonucu
    if market_name in ["Maç Sonucu", "1X2"]:
        if pick in ["1", "Ev Sahibi", "Home"]:
            return "won" if home_score > away_score else "lost"
        elif pick in ["X", "Beraberlik", "Draw"]:
            return "won" if home_score == away_score else "lost"
        elif pick in ["2", "Deplasman", "Away"]:
            return "won" if away_score > home_score else "lost"
    
    # Alt/Üst
    if "Alt/Üst" in market_name or "Over/Under" in market_name:
        # Market name'den çizgiyi çıkar (örn: "Alt/Üst 2.5" -> 2.5)
        try:
            line = float(market_name.split()[-1])
            if pick in ["Üst", "Over"]:
                return "won" if total_goals > line else "lost"
            elif pick in ["Alt", "Under"]:
                return "won" if total_goals < line else "lost"
        except:
            pass
    
    # Karşılıklı Gol
    if market_name in ["Karşılıklı Gol", "Both Teams to Score", "KG"]:
        both_scored = home_score > 0 and away_score > 0
        if pick in ["Var", "Yes"]:
            return "won" if both_scored else "lost"
        elif pick in ["Yok", "No"]:
            return "won" if not both_scored else "lost"
    
    # Çifte Şans
    if market_name in ["Çifte Şans", "Double Chance"]:
        if pick in ["1X"]:
            return "won" if home_score >= away_score else "lost"
        elif pick in ["12"]:
            return "won" if home_score != away_score else "lost"
        elif pick in ["X2"]:
            return "won" if away_score >= home_score else "lost"
    
    # Toplam Gol (Tam sayı)
    if market_name in ["Toplam Gol", "Total Goals"]:
        try:
            target = int(pick.replace("+", "").replace("-", ""))
            if "+" in pick:
                return "won" if total_goals >= target else "lost"
            elif "-" in pick:
                return "won" if total_goals <= target else "lost"
            else:
                return "won" if total_goals == target else "lost"
        except:
            pass
    
    return None  # Desteklenmeyen bahis türü


def _row_to_response(row) -> dict:
    """DataFrame satırını response'a çevir"""
    import pandas as pd
    
    def safe_int(val):
        if pd.isna(val) or val is None:
            return None
        return int(val)
    
    def safe_float(val):
        if pd.isna(val) or val is None:
            return None
        return float(val)
    
    def safe_str(val):
        if pd.isna(val) or val is None:
            return None
        return str(val)
    
    def safe_bool(val):
        if pd.isna(val) or val is None:
            return None
        return bool(val)
    
    return {
        "id": int(row["id"]),
        "home_team": row["home_team"],
        "away_team": row["away_team"],
        "league": row["league"],
        "match_date": row["match_date"],
        "home_team_fotmob_id": safe_int(row["home_team_fotmob_id"]),
        "away_team_fotmob_id": safe_int(row["away_team_fotmob_id"]),
        "match_fotmob_id": safe_int(row["match_fotmob_id"]),
        "fotmob_url": safe_str(row.get("fotmob_url")),
        "market_name": row["market_name"],
        "pick": row["pick"],
        "pick_name": safe_str(row["pick_name"]),
        "odds": safe_float(row["odds"]),
        "probability": safe_float(row["probability"]),
        "prediction_type": row["prediction_type"],
        "content": safe_str(row["content"]),
        "audio_url": safe_str(row["audio_url"]),
        "audio_file_name": safe_str(row["audio_file_name"]),
        "analysis": safe_str(row["analysis"]),
        "status": row["status"],  # draft, active, archived, problem
        "result": safe_str(row["result"]),  # pending, won, lost, void
        "show_on_homepage": safe_bool(row.get("show_on_homepage")),
        "created_by_email": row["created_by_email"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }

