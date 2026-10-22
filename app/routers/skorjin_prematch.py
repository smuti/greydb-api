"""Skorjin maç öncesi anlık görüntüsünü (greydb.match_prematch) hazırlar. Zamanlayıcı (GitHub Actions) çağırır.

Yazan bir uç olduğu için X-Cron-Anahtari başlığıyla korunur; sunucuda CRON_ANAHTARI tanımlı değilse uç tamamen kapalıdır (503).
Varsayılan KURU çalışmadır: yalnızca raporlar. Yazmak için ?dry_run=false. Aynı anda tek koşu (409).
  kapsam=gunluk  form, H2H, skor matrisi (matchforge), oran, oynanma yüzdesi: yaklaşan 7 gün
  kapsam=oran    yalnız oran ve oynanma yüzdesi: satırı olan, iddaa kimlikli, 48 saat içindeki maçlar
"""
import hmac
import os
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Header, HTTPException

from app.services.skorjin_prematch import gercek_kaynaklar, hazirla, tablo_hazirla, tek_kosu

router = APIRouter(tags=["skorjin-prematch"])
MODEL_SURUMU = "harman-v1"      # matchforge sürümü yanıtta gelir; gelmezse bu yazılır


def _anahtar() -> Optional[str]:
    return os.environ.get("CRON_ANAHTARI") or None


def _simdi() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@router.post("/skorjin/prematch-hazirla")
def prematch_hazirla(kapsam: Literal["gunluk", "oran"] = "gunluk", dry_run: bool = True, x_cron_anahtari: str = Header(default="")):
    anahtar = _anahtar()
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest(x_cron_anahtari.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")
    with tek_kosu() as alindi:
        if not alindi:
            raise HTTPException(status_code=409, detail="Başka bir koşu sürüyor")
        if not dry_run:
            tablo_hazirla()
        return hazirla(kapsam, dry_run, gercek_kaynaklar(), _simdi(), MODEL_SURUMU)
