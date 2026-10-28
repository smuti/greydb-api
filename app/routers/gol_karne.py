"""Gol modeli karnesi (salt okunur): maçtan önce kaydedilen skor matrisleri, bitmiş maçların gerçek skoruyla puanlanır.

GET /api/gol-karne?gun=30&kaynak=canli|geri_test  (X-Cron-Anahtari ile korunur; yönetim sayfası sonra bu ucu okuyacak)
Kayıtları günlük anlık görüntü işi yazar (skorjin_prematch.hazirla, gunluk kapsam); bu uç yazmaz.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Header, HTTPException

from app.routers.skorjin_prematch import _dogrula
from app.services import gol_karne

router = APIRouter(tags=["gol-karne"])


def _simdi() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def sonuclari_oku(bas: datetime, son: datetime) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    return execute_query(gol_karne.SONUC_SQL, {"bas": bas, "son": son})


@router.get("/gol-karne")
def gol_karne_raporu(gun: int = 30, kaynak: Optional[Literal["canli", "geri_test"]] = None, min_n: int = 40, esik_z: float = 2.0,
                     x_cron_anahtari: str = Header(default="")):
    _dogrula(x_cron_anahtari)
    simdi = _simdi()
    try:
        satirlar = sonuclari_oku(simdi - timedelta(days=gun), simdi)
    except Exception:  # noqa: BLE001 — ayrıntı (şema/tablo adı dışında) dışarı sızmaz
        raise HTTPException(status_code=503, detail="gol_karne verisi okunamadı (tablo henüz oluşmamış olabilir)")
    if kaynak:
        satirlar = [s for s in satirlar if s["kaynak"] == kaynak]
    return gol_karne.rapor(satirlar, min_n, esik_z)
