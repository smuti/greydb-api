"""Kupon sonuçları: Sonuçlar sayfasının istatistik/liste ucu ve maç bitince bacakları otomatik sonuçlandıran iş (zamanlayıcı çağırır).
/kuponlar/{id}'den ÖNCE kaydedilmelidir (yoksa 'sonuclar' bir kimlik sanılır)."""
import hmac
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request

from app.routers import kuponlar as K
from app.services.db import execute_insert, execute_query
from app.services.kupon_istatistik import mac_tarihi, sonuc_istatistik
from app.services.kupon_mac_verisi import mac_bul_bacak, mac_yukle
from app.services.kupon_sonuclandir import sonuclandir
from app.services.kupon_fotmob import fotmob_mac_bul, fotmob_takim_kimlikleri, gun_getir
from app.services.kupon_takim import doldur, takim_kimlikleri

router = APIRouter(tags=["kuponlar"])

_BEKLEYEN = """
SELECT b.id, b.kupon_id, b.ev, b.dep, b.mac_zamani, b.bahis_turu, b.secim, b.fotmob_match_id, b.iddaa_event_id
FROM greydb.kupon_bacaklari b JOIN greydb.kuponlar k ON k.id = b.kupon_id
WHERE b.sonuc = 'pending' AND k.durum = 'yayinda' AND b.mac_zamani < :sinir ORDER BY b.mac_zamani LIMIT 500
"""


def _simdi() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def bekleyen_bacaklar(sinir: datetime):
    return execute_query(_BEKLEYEN, {"sinir": sinir})


def _cron_anahtari() -> Optional[str]:
    return os.environ.get("CRON_ANAHTARI") or None


def mac_bul_yedekli(b):
    """Önce kendi veritabanımız (istatistik ve olaylarla tam veri), bulunamazsa FotMob günlük listesi (yalnız maç sonu skoru)."""
    return mac_bul_bacak(execute_query, b) or fotmob_mac_bul(gun_getir, b)


def mac_yukle_yedekli(m):
    return m if m.get("_dis") else mac_yukle(execute_query, m)


@router.get("/kuponlar/sonuclar")
async def sonuclar(request: Request, days: Optional[int] = Query(None, ge=1, le=3650), seviye: Optional[str] = None, sonuc: Optional[str] = None,
                   bacak: Optional[str] = None, zamanlama: Optional[str] = None, yazar: Optional[str] = None, sayfa: int = Query(1, ge=1), sayfa_boyutu: int = Query(20, ge=1, le=100)):
    k = K.kimlik(request)
    kuponlar, _ = K.db_liste({"durum": "yayinda", "sonuclanmis": True, "seviye": seviye if seviye in K.SEVIYELER else None, "yazar": yazar.strip().lower() if yazar else None, "limit": 5000, "offset": 0})
    simdi = _simdi()
    if days:
        sinir = (simdi - timedelta(days=days)).isoformat() + "Z"
        kuponlar = [x for x in kuponlar if (x["sonuc_zamani"] or "") >= sinir]
    if sonuc in ("won", "lost", "void"):
        kuponlar = [x for x in kuponlar if x["sonuc"] == sonuc]
    if bacak == "single":
        kuponlar = [x for x in kuponlar if len(x["bacaklar"]) == 1]
    elif bacak == "multi":
        kuponlar = [x for x in kuponlar if len(x["bacaklar"]) > 1]
    if zamanlama in ("pre", "live"):
        kuponlar = [x for x in kuponlar if x["zamanlama"] == zamanlama]
    kuponlar.sort(key=lambda x: (mac_tarihi(x) or "", x["id"]), reverse=True)   # en yeni maç en üstte (toplu sonuçlanan eski kuponlar araya karışmaz)
    ist = sonuc_istatistik(kuponlar, simdi)
    sayfa_kuponlar = kuponlar[(sayfa - 1) * sayfa_boyutu: sayfa * sayfa_boyutu]
    return {**ist, "items": [K._cikti(x, k) for x in sayfa_kuponlar], "total": len(kuponlar), "sayfa": sayfa, "sayfa_boyutu": sayfa_boyutu}


@router.post("/kuponlar/sonuclandir")
def otomatik_sonuclandir(dry_run: bool = True, x_cron_anahtari: str = Header(default="")):
    anahtar = _cron_anahtari()
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest(x_cron_anahtari.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")
    simdi = _simdi()
    guncellemeler, rapor = sonuclandir(bekleyen_bacaklar(simdi), mac_bul_yedekli, mac_yukle_yedekli, simdi)
    if not dry_run:
        for kupon_id, bacak_id, s in guncellemeler:
            K.db_bacak_sonucu(kupon_id, bacak_id, s, "otomatik")
    return {"dry_run": dry_run, **rapor, "guncellemeler": [{"kupon_id": a, "bacak_id": b, "sonuc": s} for a, b, s in guncellemeler]}


_TAKIMSIZ = """
SELECT id, ev, dep, mac_zamani, fotmob_match_id, iddaa_event_id FROM greydb.kupon_bacaklari
WHERE ev_takim_id IS NULL OR dep_takim_id IS NULL ORDER BY mac_zamani DESC LIMIT 1000
"""


@router.post("/kuponlar/takim-kimlikleri")
def takim_kimliklerini_doldur(dry_run: bool = True, x_cron_anahtari: str = Header(default="")):
    """Takım kimliği (logo) eksik bacakları doldurur. Yeni kuponlar kayıtta zaten doldurulur; bu uç eskileri ve sonradan maç tablosuna girenleri tamamlar."""
    anahtar = _cron_anahtari()
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest(x_cron_anahtari.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")
    K._takim_sutunlarini_hazirla()
    return doldur(
        execute_query(_TAKIMSIZ),
        lambda b: takim_kimlikleri(execute_query, b, yedek=lambda x: fotmob_takim_kimlikleri(gun_getir, x)),
        lambda bid, e, d: execute_insert("UPDATE greydb.kupon_bacaklari SET ev_takim_id = :e, dep_takim_id = :d WHERE id = :i", {"e": e, "d": d, "i": bid}),
        kuru=dry_run,
        butce_sn=120,
    )
