"""Skorjin Spor Toto sayfasının hazır hafta paketi: yazma yalnız admin (paket önceden hesaplanıp yüklenir), okuma herkese açık.

Pakette Fuat'ın tercihi/yorumu/modeli ve kullanıcı seçimleri OLMAZ: bu anahtarlar paketin herhangi bir yerinde görülürse yükleme reddedilir.
Bloklayan veritabanı çağrıları düz `def` (olay döngüsünü bloklamasın, bkz. #84).
"""
import json
from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Body, HTTPException, Query, Request

from app.routers.kuponlar import admin_gerek
from app.services.db import execute_insert, execute_query

router = APIRouter(tags=["spor-toto-skorjin"])

EN_FAZLA_BAYT = 500_000
YASAK_PARCALAR = ("tercih", "yorum", "fuat", "kullanici_secim")   # anahtar adında geçemez (küçük harfe çevrilmiş)


# ---------------- veritabanı katmanı (testlerde değiştirilir) ----------------
def paket_yaz(donem_no: int, kapanis: datetime, paket: Dict[str, Any]) -> None:
    execute_insert(
        "INSERT INTO greydb.spor_toto_skorjin_paketleri (donem_no, kapanis, paket, guncellendi) VALUES (:d, :k, CAST(:p AS jsonb), now()) "
        "ON CONFLICT (donem_no) DO UPDATE SET kapanis = EXCLUDED.kapanis, paket = EXCLUDED.paket, guncellendi = now() RETURNING donem_no",
        {"d": donem_no, "k": kapanis, "p": json.dumps(paket, ensure_ascii=False)})


def paket_oku(donem_no: Optional[int] = None) -> Optional[Dict[str, Any]]:
    if donem_no is None:
        satirlar = execute_query("SELECT donem_no, kapanis, paket, guncellendi FROM greydb.spor_toto_skorjin_paketleri ORDER BY donem_no DESC LIMIT 1", {})
    else:
        satirlar = execute_query("SELECT donem_no, kapanis, paket, guncellendi FROM greydb.spor_toto_skorjin_paketleri WHERE donem_no = :d", {"d": donem_no})
    return satirlar[0] if satirlar else None


# ---------------- doğrulama ----------------
def _yasak_anahtar(x: Any) -> Optional[str]:
    if isinstance(x, dict):
        for k, v in x.items():
            if any(p in str(k).lower() for p in YASAK_PARCALAR):
                return str(k)
            bulunan = _yasak_anahtar(v)
            if bulunan:
                return bulunan
    elif isinstance(x, list):
        for v in x:
            bulunan = _yasak_anahtar(v)
            if bulunan:
                return bulunan
    return None


def _kapanis(paket: Dict[str, Any]) -> datetime:
    try:
        return datetime.fromisoformat(str(paket["hafta"]["kapanis"]))
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=422, detail="hafta.kapanis geçerli bir tarih olmalı")


def _dogrula(paket: Dict[str, Any]) -> int:
    if len(json.dumps(paket, ensure_ascii=False)) > EN_FAZLA_BAYT:
        raise HTTPException(status_code=413, detail="Paket çok büyük")
    hafta = paket.get("hafta")
    no = hafta.get("no") if isinstance(hafta, dict) else None
    if not isinstance(no, int) or isinstance(no, bool) or no <= 0:
        raise HTTPException(status_code=422, detail="hafta.no pozitif tam sayı olmalı")
    maclar = paket.get("maclar")
    if not isinstance(maclar, list) or len(maclar) != 15:
        raise HTTPException(status_code=422, detail="maclar tam 15 maç olmalı")
    if not isinstance(paket.get("dagilim"), dict):
        raise HTTPException(status_code=422, detail="dagilim eksik")
    _kapanis(paket)
    yasak = _yasak_anahtar(paket)
    if yasak:
        raise HTTPException(status_code=422, detail=f"Pakette '{yasak}' alanı olamaz (tercih/yorum yayınlanmaz)")
    return no


# ---------------- uçlar ----------------
@router.put("/spor-toto/skorjin-paketi")
def paketi_yukle(request: Request, paket: Dict[str, Any] = Body(...)):
    admin_gerek(request)
    no = _dogrula(paket)
    paket_yaz(no, _kapanis(paket), paket)
    return {"donem_no": no, "maclar": len(paket["maclar"])}


@router.get("/spor-toto/skorjin-paketi")
def paketi_oku(donem_no: Optional[int] = Query(None, ge=1)):
    """Verilen dönemin (yoksa en yeni dönemin) hazır paketi. aktif: kapanış henüz geçmediyse true."""
    s = paket_oku(donem_no)
    if not s:
        raise HTTPException(status_code=404, detail="Paket yok")
    kapanis = s["kapanis"]
    simdi = datetime.now(ZoneInfo("Europe/Istanbul")).replace(tzinfo=None)
    paket = s["paket"] if isinstance(s["paket"], dict) else json.loads(s["paket"])
    return {"donem_no": s["donem_no"], "kapanis": kapanis.isoformat() if hasattr(kapanis, "isoformat") else kapanis,
            "aktif": bool(kapanis and kapanis > simdi), "guncellendi": s["guncellendi"].isoformat() if hasattr(s["guncellendi"], "isoformat") else s["guncellendi"], "paket": paket}
