"""Canlı gol olasılığı izleme uçları (yalnız iç analiz). Kayıt ve özet yönetici içindir; elle sonuçlandırma X-Cron-Anahtari ister, varsayılan KURU.
Düzenli sonuçlandırmayı zamanlayıcı yapar. Canlı korner / sayım uçlarıyla aynı düzen."""
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request

from app.routers.canli_korner import _cron_dogrula
from app.routers.kuponlar import admin_gerek
from app.services import canli_gol_izleme as c
from app.services.canli_gol_izleme import db_ekle, db_ozet_satirlari

router = APIRouter(tags=["canli-gol"])


@router.post("/skorjin/canli-gol/kayit")
async def kayit(request: Request):
    admin_gerek(request)
    try:
        govde = await request.json()
        k = c.kayit_hazirla(govde)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return {"yazildi": db_ekle(k) is not None}


@router.get("/skorjin/canli-gol/ozet")
def ozet(request: Request, gun: int = Query(30, ge=1, le=365)):
    """Son `gun` günün sonuçlanmış kayıtları: taraf (ev/dep/herhangi) × pencere (10 dk / maç sonu) başına kalibrasyon, bantlar ve dakika dilimleri."""
    admin_gerek(request)
    return {"gun": gun, **c.ozet(db_ozet_satirlari(gun)),
            "not": "Aynı maçın ardışık kayıtları birbirine bağlıdır; maç sayısı küçükken bantlardaki oranlar gürültülüdür. Yalnızca iç analiz."}


@router.post("/skorjin/canli-gol/sonuc")
def sonuc(dry_run: bool = True, x_cron_anahtari: Optional[str] = Header(default="")):
    _cron_dogrula(x_cron_anahtari or "")
    return c.calistir(dry_run=dry_run)
