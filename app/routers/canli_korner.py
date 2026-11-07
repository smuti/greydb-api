"""Canlı korner izleme uçları (yalnız iç analiz). Kayıt ve özet yönetici içindir (ağ geçidi anahtarı + X-Yetki: admin, kuponlar.py ile aynı düzen);
elle sonuçlandırma yazan bir uç olduğu için X-Cron-Anahtari ister, varsayılan KURU çalışır. Düzenli sonuçlandırmayı zamanlayıcı yapar."""
import hmac
import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request

from app.routers.kuponlar import admin_gerek
from app.services import canli_korner_izleme as c
from app.services.canli_korner_izleme import db_ekle, db_ozet_satirlari

router = APIRouter(tags=["canli-korner"])


def _cron_dogrula(gelen: str) -> None:
    anahtar = os.environ.get("CRON_ANAHTARI") or None
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest(gelen.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")


@router.post("/skorjin/canli-korner/kayit")
async def kayit(request: Request):
    admin_gerek(request)
    try:
        govde = await request.json()
        k = c.kayit_hazirla(govde)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return {"yazildi": db_ekle(k) is not None}


@router.get("/skorjin/canli-korner/ozet")
def ozet(request: Request, gun: int = Query(30, ge=1, le=365)):
    """Son `gun` günün sonuçlanmış kayıtları: hedef başına sapma, MAE, olasılık bantlarında tutma ve dakika dilimi kırılımı."""
    admin_gerek(request)
    return {"gun": gun, **c.ozet(db_ozet_satirlari(gun)),
            "not": "Aynı maçın ardışık kayıtları birbirine bağlıdır; maç sayısı küçükken bantlardaki oranlar gürültülüdür. Yalnızca iç analiz."}


@router.post("/skorjin/canli-korner/sonuc")
def sonuc(dry_run: bool = True, x_cron_anahtari: Optional[str] = Header(default="")):
    _cron_dogrula(x_cron_anahtari or "")
    return c.calistir(dry_run=dry_run)
