"""skorjin_stat materialized view yenileme ucu: elle kuru çalıştırma / tetikleme. Düzenli çalışmayı uygulama içi zamanlayıcı yapar (günlük, UTC 02:30).

X-Cron-Anahtari ile korunur; sunucuda CRON_ANAHTARI tanımlı değilse uç tamamen kapalıdır (503). Varsayılan KURU: yalnızca yetkiyi raporlar; yenilemek için ?dry_run=false.
"""
import hmac
import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException

from app.services import stat_yenile as s

router = APIRouter(tags=["stat-yenile"])


def _anahtar() -> Optional[str]:
    return os.environ.get("CRON_ANAHTARI") or None


@router.post("/skorjin/stat-yenile")
def stat_yenile(dry_run: bool = True, x_cron_anahtari: str = Header(default="")):
    anahtar = _anahtar()
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest(x_cron_anahtari.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")
    return s.calistir(dry_run)
