"""Oyuncu istatistiği onarım ucu: geçmiş bitmiş maçların oyuncu satırlarını ham maç verisinden (FotMob playerStats) yeniden yazar.
Yazan bir uç olduğu için X-Cron-Anahtari ister, sunucuda CRON_ANAHTARI yoksa kapalıdır (503). Varsayılan KURU çalışır; her çağrı `limit` maçı işler,
yanıttaki `sonraki` değeri bir sonraki çağrının `sonra` parametresidir, `bitti` true olunca onarım tamamdır. `kadar` üst sınırdır: id aralıkları paralel çalıştırılabilir. `yalniz_eksik=true`: yalnız tabloda hiç satırı olmayan maçlar."""
import hmac
import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query

from app.services import oyuncu_stat as o

router = APIRouter(tags=["oyuncu-stat"])


@router.post("/skorjin/oyuncu-stat/geri-doldur")
def geri_doldur(dry_run: bool = True, sonra: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500), kadar: Optional[int] = Query(None, ge=1), yalniz_eksik: bool = False, x_cron_anahtari: Optional[str] = Header(default="")):
    anahtar = os.environ.get("CRON_ANAHTARI") or None
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest((x_cron_anahtari or "").encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")
    return o.calistir(sonra, limit, dry_run, kadar, yalniz_eksik)
