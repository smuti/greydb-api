"""Skorjin maç öncesi anlık görüntüsünü (greydb.match_prematch) hazırlar. Uygulama içi zamanlayıcı (app/services/zamanlayici.py, ZAMANLAYICI=1) ya da elle çağrılır.

Yazan bir uç olduğu için X-Cron-Anahtari başlığıyla korunur; sunucuda CRON_ANAHTARI tanımlı değilse uç tamamen kapalıdır (503).
Varsayılan KURU çalışmadır: yalnızca raporlar. Yazmak için ?dry_run=false. Aynı anda tek koşu (409).
  kapsam=gunluk  form, H2H, skor matrisi (matchforge), oran, oynanma yüzdesi, sezon: yaklaşan 7 gün
  kapsam=oran    yalnız oran ve oynanma yüzdesi: satırı olan, iddaa kimlikli, 48 saat içindeki maçlar
Günlük koşu Railway'in 5 dakikalık proxy sınırına dayandığı için ?arka_plan=true işi arka planda başlatıp hemen 202 döner;
sonuç GET /skorjin/prematch-durum ile okunur (bellekte, son koşu; tek kopya varsayımı: yeniden başlatma durumu siler -> "yok").
"""
import hmac
import os
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Literal, Optional

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse

from app.services.skorjin_prematch import gercek_kaynaklar, hazirla, tablo_hazirla, tek_kosu

router = APIRouter(tags=["skorjin-prematch"])
MODEL_SURUMU = "harman-v1"      # matchforge sürümü yanıtta gelir; gelmezse bu yazılır

_kilit = threading.Lock()
_durum: Dict[str, Any] = {"durum": "yok"}


def _durum_sifirla() -> None:
    with _kilit:
        _durum.clear()
        _durum["durum"] = "yok"


def _anahtar() -> Optional[str]:
    return os.environ.get("CRON_ANAHTARI") or None


def _simdi() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _utc_metin() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _baslat(is_fn: Callable[[], None]) -> None:
    threading.Thread(target=is_fn, daemon=True, name="skorjin-prematch").start()


def _dogrula(x_cron_anahtari: str) -> None:
    anahtar = _anahtar()
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest(x_cron_anahtari.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")


def _calisiyor() -> bool:
    return _durum.get("durum") == "calisiyor"


def _kos(kapsam: str, dry_run: bool) -> Dict[str, Any]:
    """Kilidi alıp hazırlar. Kilit alınamazsa None (başka bir koşu sürüyor)."""
    with tek_kosu() as alindi:
        if not alindi:
            return None
        if not dry_run:
            tablo_hazirla()
        return hazirla(kapsam, dry_run, gercek_kaynaklar(), _simdi(), MODEL_SURUMU)


def _arka_plan_isi(kapsam: str, dry_run: bool) -> None:
    try:
        rapor = _kos(kapsam, dry_run)
        son = {"durum": "atlandi"} if rapor is None else {"durum": "bitti", "rapor": rapor}
    except Exception as e:  # noqa: BLE001 — hata durum ucunda görünür; sonraki koşu engellenmez
        son = {"durum": "hata", "hata": f"{type(e).__name__}: {e}"}
    with _kilit:
        _durum.update(son, bitis=_utc_metin())


def zamanli_prematch(kapsam: str) -> str:
    """Uygulama içi zamanlayıcıdan: yazan koşu, durum ucunda görünür. Başka koşu sürüyorsa 'atlandi'."""
    with _kilit:
        if _calisiyor():
            return "atlandi"
        _durum.clear()
        _durum.update(durum="calisiyor", kapsam=kapsam, dry_run=False, baslangic=_utc_metin(), kaynak="zamanlayici")
    _arka_plan_isi(kapsam, False)
    return _durum.get("durum", "yok")


@router.post("/skorjin/prematch-hazirla")
def prematch_hazirla(kapsam: Literal["gunluk", "oran"] = "gunluk", dry_run: bool = True, arka_plan: bool = False,
                     x_cron_anahtari: str = Header(default="")):
    _dogrula(x_cron_anahtari)
    if arka_plan:
        with _kilit:
            if _calisiyor():
                raise HTTPException(status_code=409, detail="Başka bir koşu sürüyor")
            _durum.clear()
            _durum.update(durum="calisiyor", kapsam=kapsam, dry_run=dry_run, baslangic=_utc_metin())
        _baslat(lambda: _arka_plan_isi(kapsam, dry_run))
        return JSONResponse(status_code=202, content={"durum": "basladi", "kapsam": kapsam, "dry_run": dry_run})
    if _calisiyor():
        raise HTTPException(status_code=409, detail="Başka bir koşu sürüyor")
    rapor = _kos(kapsam, dry_run)
    if rapor is None:
        raise HTTPException(status_code=409, detail="Başka bir koşu sürüyor")
    return rapor


@router.get("/skorjin/prematch-durum")
def prematch_durum(x_cron_anahtari: str = Header(default="")):
    """Son arka plan koşusunun durumu: yok | calisiyor | bitti (rapor) | hata | atlandi."""
    _dogrula(x_cron_anahtari)
    with _kilit:
        return dict(_durum)
