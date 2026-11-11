"""Spor Toto arşivi (herkese açık okuma uçları): sezon özetleri, zorluk tablosu, bu sezon, benzer haftalar. Yazma yalnız admin senkronu ve zamanlayıcı.

Kaynak resmî sonuçlardır (webapi.sportoto.gov.tr); Fuat'ın tercihi/yorumu/modeli buradan ASLA dönmez (bkz. spor_toto_yayin).
Bloklayan veritabanı ve ağ çağrıları düz `def` (olay döngüsünü bloklamasın, bkz. #84).
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.routers.kuponlar import admin_gerek
from app.services import spor_toto_arsiv as a
from app.services import spor_toto_oynanma as oy
from app.services.db import execute_insert, execute_insert_many, execute_query, transaction

router = APIRouter(tags=["spor-toto-arsiv"])

_HAFTA_ALANLARI = ("round_id", "sezon", "oyun_no", "ad", "kapanis", "bilen_15", "bilen_14", "bilen_13", "bilen_12", "ikramiye_15", "ikramiye_14", "ikramiye_13", "ikramiye_12",
                   "mac_sayisi", "ev", "beraberlik", "deplasman", "noter_sayisi", "temiz")


# ---------------- veritabanı katmanı (testlerde değiştirilir) ----------------
def haftalar_oku(sezon: Optional[str] = None) -> List[Dict[str, Any]]:
    kosul, p = ("WHERE sezon = :s", {"s": sezon}) if sezon else ("", {})
    return execute_query(f"SELECT {', '.join(_HAFTA_ALANLARI)} FROM greydb.spor_toto_arsiv_haftalar {kosul} ORDER BY sezon, oyun_no", p)


def maclar_oku(round_id: int) -> List[Dict[str, Any]]:
    return execute_query("SELECT sira, tarih, ev, dep, skor_ev, skor_dep, sonuc, noter FROM greydb.spor_toto_arsiv_maclar WHERE round_id = :r ORDER BY sira", {"r": round_id})


class Depo:
    """senkronla()'nın deposu."""

    def mevcut_haftalar(self) -> Dict[int, bool]:
        """True = bir daha çekilmez: temiz hafta ya da sonucu yayınlanmış ve kapanışı 21 günden eski (noterli/eksik maçlı haftalar sonsuza kadar yeniden çekilmesin)."""
        return {r["round_id"]: bool(r["tamam"]) for r in execute_query(
            "SELECT round_id, (temiz OR (bilen_15 IS NOT NULL AND kapanis < now() - interval '21 days')) AS tamam FROM greydb.spor_toto_arsiv_haftalar")}

    def kaydet(self, hafta: Dict[str, Any], maclar: List[Dict[str, Any]]) -> None:
        alanlar = ", ".join(_HAFTA_ALANLARI)
        degerler = ", ".join(f":{k}" for k in _HAFTA_ALANLARI)
        guncelle = ", ".join(f"{k} = EXCLUDED.{k}" for k in _HAFTA_ALANLARI if k != "round_id")
        with transaction():
            execute_insert(f"INSERT INTO greydb.spor_toto_arsiv_haftalar ({alanlar}, guncellendi) VALUES ({degerler}, now()) "
                           f"ON CONFLICT (round_id) DO UPDATE SET {guncelle}, guncellendi = now() RETURNING round_id", hafta)
            execute_insert("DELETE FROM greydb.spor_toto_arsiv_maclar WHERE round_id = :r RETURNING round_id", {"r": hafta["round_id"]})
            if maclar:
                execute_insert_many("INSERT INTO greydb.spor_toto_arsiv_maclar (round_id, sira, tarih, ev, dep, skor_ev, skor_dep, sonuc, noter) "
                                    "VALUES (:round_id, :sira, :tarih, :ev, :dep, :skor_ev, :skor_dep, :sonuc, :noter)", maclar)


def oynanma_yaz(satirlar: List[Dict[str, Any]]) -> int:
    if satirlar:
        execute_insert_many("INSERT INTO greydb.spor_toto_oynanma (donem_no, mac_sira, ev, dep, mac_tarihi, kapanis, yuzde_1, yuzde_0, yuzde_2) "
                            "VALUES (:donem_no, :mac_sira, :ev, :dep, :mac_tarihi, :kapanis, :yuzde_1, :yuzde_0, :yuzde_2)", satirlar)
    return len(satirlar)


def istemci():
    return a.HttpIstemci()


def oynanma_cek() -> Dict[str, Any]:
    return oy.cek()


# ---------------- uçlar ----------------
def _hafta_json(h: Dict[str, Any]) -> Dict[str, Any]:
    cikti = {k: (v.isoformat() if hasattr(v, "isoformat") else (float(v) if k.startswith("ikramiye") and v is not None else v)) for k, v in h.items()}
    return cikti


def _guncel_sezon(haftalar) -> Optional[str]:
    return max((h["sezon"] for h in haftalar), default=None)


@router.get("/spor-toto/arsiv/ozet")
def arsiv_ozet():
    """Sezon özetleri, beraberliğe göre zorluk tablosu, korelasyon ve bu sezonun haftaları."""
    haftalar = haftalar_oku()
    guncel = _guncel_sezon(haftalar)
    return {"sezonlar": a.sezon_ozeti(haftalar), "zorluk": a.zorluk_tablosu(haftalar), "korelasyon": a.korelasyon(haftalar) if a.temiz_haftalar(haftalar) else None,
            "guncel_sezon": guncel, "bu_sezon": [_hafta_json(h) for h in haftalar if h["sezon"] == guncel and h.get("bilen_15") is not None],
            "temiz_hafta": len(a.temiz_haftalar(haftalar))}


@router.get("/spor-toto/arsiv/haftalar")
def arsiv_haftalar(sezon: Optional[str] = Query(default=None, max_length=9), maclar: bool = False):
    h = haftalar_oku(sezon)
    if not maclar:
        return [_hafta_json(x) for x in h]
    return [{**_hafta_json(x), "maclar": [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in m.items()} for m in maclar_oku(x["round_id"])]} for x in h]


@router.get("/spor-toto/arsiv/benzer")
def arsiv_benzer(ev: float = Query(ge=0, le=15), beraberlik: float = Query(ge=0, le=15), deplasman: float = Query(ge=0, le=15), k: int = Query(default=20, ge=1, le=60)):
    """Beklenen (ya da gerçekleşen) sonuç dağılımına en yakın geçmiş haftalar. Üç sayının toplamı 15 olmalı."""
    try:
        return a.benzer_haftalar(haftalar_oku(), ev, beraberlik, deplasman, k)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


class SenkronGovdesi(BaseModel):
    sezonlar: Optional[List[str]] = None


@router.post("/spor-toto/arsiv/senkronla")
def arsiv_senkronla(request: Request, govde: Optional[SenkronGovdesi] = None):
    """Eksik ve sonucu yayınlanmamış haftaları resmî API'den çeker (yalnız admin)."""
    admin_gerek(request)
    return a.senkronla(istemci(), Depo(), sezonlar=(govde.sezonlar if govde else None), aralik=0.25)


def senkron_isi() -> Dict[str, Any]:
    """Zamanlayıcı: yalnız güncel sezon (eksik ve sonucu bekleyen haftalar)."""
    ist = istemci()
    sezonlar = ist.yillar()[:1]
    return a.senkronla(ist, Depo(), sezonlar=sezonlar, aralik=0.25)


def oynanma_isi() -> int:
    """Zamanlayıcı: iddaa oynanma yüzdelerinin anlık kaydı."""
    return oynanma_yaz(oy.satirlar(oynanma_cek()))
