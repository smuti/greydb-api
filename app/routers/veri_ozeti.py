"""Herkese açık veri özeti: ana sayfadaki etkileyici sayılar (işlenen maç, veri noktası)."""
from fastapi import APIRouter, Response

from app.services.db import execute_query
from app.services.veri_ozeti import onbellekli, ozet_olustur

router = APIRouter(tags=["veri-ozeti"])


@router.get("/veri-ozeti")
async def veri_ozeti(yanit: Response, ayrinti: bool = False):
    """mac_sayisi: bitmiş ve skoru olan maç. veri_noktasi: maç verisi tablolarındaki dolu, teknik olmayan değer sayısı. ?ayrinti=true tablo dökümü ekler."""
    ozet = onbellekli(lambda: ozet_olustur(execute_query), ttl=3600)
    yanit.headers["Cache-Control"] = "public, max-age=300, s-maxage=600, stale-while-revalidate=3600"   # tarayıcı 5 dk, CDN 10 dk; bayat değer arka planda tazelenir
    cikti = {k: ozet[k] for k in ("mac_sayisi", "veri_noktasi", "tablo_sayisi", "guncelleme")}
    if ayrinti:
        cikti["tablolar"] = ozet["tablolar"]
    return cikti
