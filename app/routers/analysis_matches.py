"""Maçlar sayfasındaki "Analiz" rozeti için: yayında analizi olan maçların FotMob kimlikleri (içerik değil)."""
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.services.db import execute_query

router = APIRouter(tags=["predictions"])

_SQL = """
SELECT DISTINCT match_fotmob_id
FROM greydb.predictions
WHERE status = 'active'
  AND match_fotmob_id IS NOT NULL
  AND match_date >= :bas AND match_date < :bit
"""


@router.get("/analysis-matches")
def analysis_matches(date_: str = Query(..., alias="date", pattern=r"^\d{4}-\d{2}-\d{2}$")):
    try:
        gun = datetime.strptime(date_, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=422, detail="Geçersiz tarih")
    # Saat dilimi farkı yüzünden bir gün önce ve bir gün sonrasını da kapsar; site kendi listesiyle kesiştirir.
    bas = datetime.combine(gun - timedelta(days=1), datetime.min.time())
    bit = datetime.combine(gun + timedelta(days=2), datetime.min.time())
    try:
        satirlar = execute_query(_SQL, {"bas": bas, "bit": bit})
    except Exception as e:
        print(f"analysis_matches okunamadı: {e}")
        return {"match_ids": []}
    return {"match_ids": sorted({int(s["match_fotmob_id"]) for s in satirlar})}
