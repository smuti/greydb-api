"""Takip edilen ligler: Maçlar sayfası hangi ligleri, hangi sırayla ve hangi adla göstereceğini buradan okur."""
from fastapi import APIRouter

from app.services.db import execute_query

router = APIRouter(tags=["leagues"])

_SQL = """
SELECT anahtar, ad, oncelik, aktif, fotmob_ids, ulke, ad_kalibi
FROM greydb.tracked_leagues
WHERE aktif
ORDER BY oncelik, anahtar
"""


@router.get("/tracked-leagues")
async def tracked_leagues():
    try:
        satirlar = execute_query(_SQL)
    except Exception as e:  # tablo henüz yoksa site çökmesin; frontend yerleşik varsayılana düşer
        print(f"tracked_leagues okunamadı: {e}")
        return {"leagues": []}
    return {"leagues": [
        {
            "anahtar": s["anahtar"], "ad": s["ad"], "oncelik": s["oncelik"],
            "fotmob_ids": list(s["fotmob_ids"] or []),
            "ulke": s["ulke"], "ad_kalibi": s["ad_kalibi"],
        }
        for s in satirlar
    ]}
