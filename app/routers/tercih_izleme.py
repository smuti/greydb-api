"""Tercih izleme uçları: elle kuru çalıştırma / tetikleme. Düzenli çalışmayı uygulama içi zamanlayıcı yapar (app/services/zamanlayici.py).

Yazan bir uç olduğu için X-Cron-Anahtari başlığıyla korunur; sunucuda CRON_ANAHTARI tanımlı değilse uç tamamen kapalıdır (503).
Varsayılan KURU çalışmadır: yalnızca raporlar. Yazmak için ?dry_run=false. İç analiz içindir; sitede gösterilmez.
  kaydet  kickoff'a 24 saatten az kalan maçların listesini oranlı alıp ekler (açılış / orta / kapanış pencereleri)
  sonuc   biten maçların kayıtlarına tuttu/tutmadı yazar
"""
import hmac
import os
from typing import Literal, Optional

from fastapi import APIRouter, Header, HTTPException, Query

from app.services import tercih_izleme as t

router = APIRouter(tags=["tercih-izleme"])


def _anahtar() -> Optional[str]:
    return os.environ.get("CRON_ANAHTARI") or None


def _dogrula(x_cron_anahtari: str) -> None:
    anahtar = _anahtar()
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest(x_cron_anahtari.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")


@router.post("/skorjin/tercih-izleme/{is_adi}")
def tercih_izleme_calistir(is_adi: Literal["kaydet", "sonuc"], dry_run: bool = True, x_cron_anahtari: str = Header(default="")):
    _dogrula(x_cron_anahtari)
    return t.calistir(is_adi, dry_run=dry_run)


@router.get("/skorjin/tercih-izleme/ozet")
def tercih_izleme_ozet(gun: int = Query(30, ge=1, le=365), x_cron_anahtari: str = Header(default="")):
    """Son `gun` günün sonuçlanmış tercihleri: tutma ve eşit tutarlı getiri; pencere (açılış/orta/kapanış), "olasılık × oran > 1" kuralı, olasılık bandı, kategori."""
    _dogrula(x_cron_anahtari)
    return t.ozet_getir(gun)
