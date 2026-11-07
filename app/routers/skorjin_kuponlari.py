"""Skorjin kuponları: yönetici Piyasa özeti'nden seçim toplayıp seviyeye (Açık/Bayi/Elit) göre üyelere sitede gösterir. Gönderim yok.

Veri burada durur (greydb.skorjin_kuponlari); web yalnızca arayüzdür. Kimlik kuponlar.py ile aynı güvenilir ağ geçidi düzenidir:
yazma ve yönetici okuması X-Sunucu-Anahtari + X-Yetki: admin ister; yayındaki liste yalnız ağ geçidine (Next sunucusu) açılır, çünkü kayıtlarda
bizim olasılığımız (bizimP) bulunur: üyeye ham yüzde GİTMEZ, sinyal/risk etiketine ve seviye kilidine web sunucusu çevirir.
Tablo ilk kullanımda kendiliğinden oluşur (dağıtım ile migration sırası önemli olmasın).
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from app.routers.kuponlar import admin_gerek, kimlik
from app.services.db import execute_insert, execute_query

router = APIRouter(tags=["skorjin-kuponlari"])

MAKS_BACAK = 12


# ---------------- Girdi doğrulama ----------------
def _bos_degil(v: str) -> str:
    v = v.strip()
    if not v:
        raise ValueError("boş olamaz")
    return v


class Bacak(BaseModel):
    """Alan adları web'deki SepetBacagi ile aynıdır (jsonb olduğu gibi saklanır)."""
    model_config = ConfigDict(extra="forbid")
    macId: int
    ev: str
    dep: str
    lig: Optional[str] = None
    macZamani: str
    marketId: int
    marketAd: str
    secim: str
    oran: float
    bizimP: float

    _metinler = field_validator("ev", "dep", "marketAd", "secim")(_bos_degil)

    @field_validator("oran")
    @classmethod
    def _oran(cls, v: float) -> float:
        if not 0 < v <= 1000:
            raise ValueError("oran 0 ile 1000 arasında olmalı")
        return v

    @field_validator("bizimP")
    @classmethod
    def _p(cls, v: float) -> float:
        if not 0 < v <= 1:
            raise ValueError("olasılık 0 ile 1 arasında olmalı")
        return v

    @field_validator("macZamani")
    @classmethod
    def _zaman(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("maç zamanı ISO biçiminde olmalı")
        return v


class Girdi(BaseModel):
    model_config = ConfigDict(extra="forbid")
    baslik: str
    seviye: Literal["acik", "bayi", "elit"]
    durum: Literal["taslak", "yayinda", "arsiv"] = "taslak"
    yorum: Optional[str] = None
    one_cikan: bool = False
    bacaklar: List[Bacak]

    @field_validator("baslik")
    @classmethod
    def _baslik(cls, v: str) -> str:
        v = v.strip()
        if not 3 <= len(v) <= 80:
            raise ValueError("başlık 3 ile 80 karakter olmalı")
        return v

    @field_validator("yorum")
    @classmethod
    def _yorum(cls, v: Optional[str]) -> Optional[str]:
        v = (v or "").strip()
        if len(v) > 500:
            raise ValueError("yorum en fazla 500 karakter")
        return v or None

    @model_validator(mode="after")
    def _bacaklar(self):
        if not 1 <= len(self.bacaklar) <= MAKS_BACAK:
            raise ValueError(f"kuponda 1 ile {MAKS_BACAK} maç olmalı")
        if len({b.macId for b in self.bacaklar}) != len(self.bacaklar):
            raise ValueError("aynı maçtan birden fazla seçim eklenemez")
        return self


# ---------------- Veritabanı katmanı (testlerde değiştirilir) ----------------
_HAZIR = False
_DDL = """
CREATE TABLE IF NOT EXISTS greydb.skorjin_kuponlari (
    id SERIAL PRIMARY KEY,
    baslik TEXT NOT NULL,
    seviye TEXT NOT NULL CHECK (seviye IN ('acik','bayi','elit')),
    durum TEXT NOT NULL DEFAULT 'taslak' CHECK (durum IN ('taslak','yayinda','arsiv')),
    yorum TEXT,
    bacaklar JSONB NOT NULL,
    olusturan_email TEXT NOT NULL,
    one_cikan BOOLEAN NOT NULL DEFAULT FALSE,
    yayin_zamani TIMESTAMP,
    olusturma TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    guncelleme TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
)
"""
# Tablo bu alandan önce oluşmuş olabilir: eksikse eklenir (yeniden çalıştırılabilir)
_ONE_CIKAN = "ALTER TABLE greydb.skorjin_kuponlari ADD COLUMN IF NOT EXISTS one_cikan BOOLEAN NOT NULL DEFAULT FALSE"
_INDEKS = "CREATE INDEX IF NOT EXISTS idx_skorjin_kuponlari_durum ON greydb.skorjin_kuponlari (durum, yayin_zamani DESC)"
_KOLONLAR = "id, baslik, seviye, durum, yorum, bacaklar, olusturan_email, one_cikan, yayin_zamani, olusturma, guncelleme"


def _hazirla() -> None:
    global _HAZIR
    if _HAZIR:
        return
    execute_insert(_DDL, {})
    execute_insert(_ONE_CIKAN, {})
    execute_insert(_INDEKS, {})
    _HAZIR = True


def _z(v) -> Optional[str]:
    return None if v is None else v.isoformat() + "Z"


def _satir(s: Dict[str, Any]) -> Dict[str, Any]:
    bacaklar = s["bacaklar"]
    return {"id": str(s["id"]), "baslik": s["baslik"], "seviye": s["seviye"], "durum": s["durum"], "yorum": s["yorum"],
            "bacaklar": json.loads(bacaklar) if isinstance(bacaklar, str) else bacaklar, "olusturan_email": s["olusturan_email"],
            "one_cikan": bool(s.get("one_cikan")), "yayin_at": _z(s["yayin_zamani"]), "olusturma": _z(s["olusturma"]), "guncelleme": _z(s["guncelleme"])}


def db_ekle(k: Dict[str, Any], email: str, yayin: bool) -> Dict[str, Any]:
    _hazirla()
    s = execute_insert(
        f"INSERT INTO greydb.skorjin_kuponlari (baslik, seviye, durum, yorum, one_cikan, bacaklar, olusturan_email, yayin_zamani) "
        f"VALUES (:baslik, :seviye, :durum, :yorum, :one_cikan, CAST(:bacaklar AS JSONB), :email, CASE WHEN :yayin THEN (now() AT TIME ZONE 'utc') END) RETURNING {_KOLONLAR}",
        {**{x: k[x] for x in ("baslik", "seviye", "durum", "yorum", "one_cikan")}, "bacaklar": json.dumps(k["bacaklar"], ensure_ascii=False), "email": email, "yayin": yayin})
    return _satir(s)


def db_getir(kid: int) -> Optional[Dict[str, Any]]:
    _hazirla()
    s = execute_query(f"SELECT {_KOLONLAR} FROM greydb.skorjin_kuponlari WHERE id = :id", {"id": kid})
    return _satir(s[0]) if s else None


def db_liste(yalniz_yayinda: bool) -> List[Dict[str, Any]]:
    _hazirla()
    kosul = "WHERE durum = 'yayinda' " if yalniz_yayinda else ""
    sira = "yayin_zamani DESC NULLS LAST, id DESC" if yalniz_yayinda else "olusturma DESC, id DESC"
    return [_satir(s) for s in execute_query(f"SELECT {_KOLONLAR} FROM greydb.skorjin_kuponlari {kosul}ORDER BY {sira} LIMIT 100")]


def db_guncelle(kid: int, k: Dict[str, Any], yayin: bool) -> Optional[Dict[str, Any]]:
    _hazirla()
    s = execute_insert(
        f"UPDATE greydb.skorjin_kuponlari SET baslik = :baslik, seviye = :seviye, durum = :durum, yorum = :yorum, one_cikan = :one_cikan, bacaklar = CAST(:bacaklar AS JSONB), "
        f"guncelleme = (now() AT TIME ZONE 'utc'), yayin_zamani = CASE WHEN :yayin THEN (now() AT TIME ZONE 'utc') ELSE yayin_zamani END "
        f"WHERE id = :id RETURNING {_KOLONLAR}",
        {**{x: k[x] for x in ("baslik", "seviye", "durum", "yorum", "one_cikan")}, "bacaklar": json.dumps(k["bacaklar"], ensure_ascii=False), "yayin": yayin, "id": kid})
    return _satir(s) if s else None


def db_sil(kid: int) -> bool:
    _hazirla()
    return execute_insert("DELETE FROM greydb.skorjin_kuponlari WHERE id = :id RETURNING id", {"id": kid}) is not None


# ---------------- Uçlar ----------------
def _email(request: Request) -> str:
    e = (request.headers.get("x-kullanici-email") or "").strip()
    if not e:
        raise HTTPException(status_code=400, detail="X-Kullanici-Email başlığı gerekli")
    return e


@router.get("/skorjin-kuponlari/yayinda")
def yayindakiler(request: Request):
    """Yayındaki kuponlar (ham): yalnız güvenilir ağ geçidi. Üyeye gidecek hâle (kilit, etiket) web sunucusu çevirir."""
    if not kimlik(request).ag_gecidi:
        raise HTTPException(status_code=401, detail="Geçersiz ya da eksik sunucu anahtarı")
    return {"kuponlar": db_liste(True)}


@router.get("/skorjin-kuponlari")
def liste(request: Request):
    admin_gerek(request)
    return {"kuponlar": db_liste(False)}


@router.get("/skorjin-kuponlari/{kid}")
def getir(kid: int, request: Request):
    admin_gerek(request)
    k = db_getir(kid)
    if not k:
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    return k


@router.post("/skorjin-kuponlari", status_code=201)
def olustur(girdi: Girdi, request: Request):
    admin_gerek(request)
    email = _email(request)
    k = girdi.model_dump()
    return db_ekle(k, email, k["durum"] == "yayinda")


@router.put("/skorjin-kuponlari/{kid}")
def degistir(kid: int, girdi: Girdi, request: Request):
    admin_gerek(request)
    onceki = db_getir(kid)
    if not onceki:
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    k = girdi.model_dump()
    yeni = db_guncelle(kid, k, k["durum"] == "yayinda" and onceki["durum"] != "yayinda")
    if not yeni:
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    return yeni


@router.delete("/skorjin-kuponlari/{kid}")
def sil(kid: int, request: Request):
    admin_gerek(request)
    if not db_sil(kid):
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    return {"ok": True}
