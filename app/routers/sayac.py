"""Skor sayacı (admin → işçi Mac'ler): kesilmiş oyuncu görselleri, "Üret" ile açılan işler, işçi uçları.

Video SUNUCUDA üretilmez ve sunucuya yüklenmez: işçi Mac iş kuyruğundan alır, kendi diskinde üretir, yalnızca durumu bildirir.
Yazma/okuma yetkisi iki türlüdür: admin uçları yayın/kuponlardaki gibi ağ geçidi anahtarı + X-Yetki: admin (routers/kuponlar.py: admin_gerek);
işçi uçları ayrı X-Isci-Anahtari (ortam: SAYAC_ISCI_ANAHTARI) ister ve yalnızca kuyruğu görür. Anahtar tanımlı değilse işçi uçları kapalıdır (503).
Kurallar: iki oyuncu da zorunlu (rakip dahil); takımın "yıldız" pozu seçilmemişse otomatik gelir; aynı maç için aynı anda tek aktif iş.
"""
import hmac
import io
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from app.routers.kuponlar import admin_gerek
from app.services.db import execute_insert, execute_query, transaction

router = APIRouter(tags=["sayac"])

MIN_GENISLIK = 350
MAKS_BAYT = 8 * 1024 * 1024
KALP_ZAMAN_ASIMI_SN = 180      # bu kadar süre kalp atışı yoksa iş kuyruğa döner
MAKS_DENEME = 3
AYLAR = ["OCAK", "ŞUBAT", "MART", "NİSAN", "MAYIS", "HAZİRAN", "TEMMUZ", "AĞUSTOS", "EYLÜL", "EKİM", "KASIM", "ARALIK"]
VARSAYILAN_PARILTI = ("rgba(80,170,255,0.6)", "rgba(200,50,90,0.6)")


# ---------------- saf yardımcılar ----------------
def tr_buyuk(s: str) -> str:
    """Türkçe büyük harf (CSS/str.upper i→I yapar, İ olmalı)."""
    return s.replace("i", "İ").replace("ı", "I").upper()


def tsi(zaman_utc: datetime) -> datetime:
    return zaman_utc + timedelta(hours=3)


def kunye(mac_zamani: datetime, lig: str) -> Dict[str, str]:
    z = tsi(mac_zamani)
    return {"lig": tr_buyuk(lig.strip()), "tarih": f"{z.day} {AYLAR[z.month - 1]}", "saat": f"{z.hour:02d}:{z.minute:02d}"}


def tohumlar(fotmob_mac_id: int) -> Dict[str, int]:
    """Skor dizisi tohumu maç kimliğinden türer: aynı maç hep aynı videoyu verir, farklı maçlar farklı diziyi."""
    return {"evSeed": fotmob_mac_id % 997 + 11, "depSeed": (fotmob_mac_id * 7 + 31) % 991 + 13}


def parilti(hex_renk: Optional[str], varsayilan: str) -> str:
    if hex_renk and len(hex_renk) == 7 and hex_renk.startswith("#"):
        try:
            r, g, b = (int(hex_renk[i:i + 2], 16) for i in (1, 3, 5))
            return f"rgba({r},{g},{b},0.6)"
        except ValueError:
            pass
    return varsayilan


def gorsel_dogrula(veri: bytes) -> Dict[str, int]:
    """Yüklenen oyuncu görseli: PNG, şeffaf arka planlı, en az 350 px geniş. Aksi halde 422 sebebiyle reddedilir."""
    if len(veri) > MAKS_BAYT:
        raise HTTPException(status_code=422, detail="Dosya 8 MB'dan büyük")
    if not veri.startswith(b"\x89PNG\r\n\x1a\n"):
        raise HTTPException(status_code=422, detail="Yalnızca PNG kabul edilir (kesilmiş, şeffaf arka planlı)")
    from PIL import Image
    try:
        im = Image.open(io.BytesIO(veri)); im.load()
    except Exception:
        raise HTTPException(status_code=422, detail="Dosya okunamadı")
    if "A" not in im.getbands() and "transparency" not in im.info:
        raise HTTPException(status_code=422, detail="Arka plan şeffaf değil: önce kesilmiş PNG yükle")
    a = im.convert("RGBA").getchannel("A")
    seffaf = sum(1 for p in a.tobytes() if p < 10) / (im.width * im.height)
    if seffaf < 0.05:
        raise HTTPException(status_code=422, detail="Arka plan şeffaf değil: önce kesilmiş PNG yükle")
    if im.width < MIN_GENISLIK:
        raise HTTPException(status_code=422, detail=f"Görsel çok küçük ({im.width} px): en az {MIN_GENISLIK} px geniş olmalı, 4K'da bulanık çıkar")
    return {"genislik": im.width, "yukseklik": im.height}


# ---------------- veritabanı katmanı (testlerde değiştirilir) ----------------
_UTC_SIMDI = "(now() AT TIME ZONE 'utc')"
_OYUNCU_ALANLAR = "id, fotmob_player_id, ad, fotmob_team_id, poz, gorsel_tur, genislik, yukseklik, kafa_orani, yildiz, aktif, yukleyen, olusturma"


def oyuncu_ekle(o: Dict[str, Any]) -> Dict[str, Any]:
    return execute_insert(
        f"""INSERT INTO greydb.sayac_oyuncular (fotmob_player_id, ad, fotmob_team_id, poz, gorsel, genislik, yukseklik, kafa_orani, yukleyen)
            VALUES (:fotmob_player_id, :ad, :fotmob_team_id, :poz, :gorsel, :genislik, :yukseklik, :kafa_orani, :yukleyen)
            ON CONFLICT (fotmob_player_id, poz) DO UPDATE SET ad = EXCLUDED.ad, fotmob_team_id = EXCLUDED.fotmob_team_id, gorsel = EXCLUDED.gorsel,
              genislik = EXCLUDED.genislik, yukseklik = EXCLUDED.yukseklik, kafa_orani = EXCLUDED.kafa_orani, yukleyen = EXCLUDED.yukleyen, aktif = TRUE
            RETURNING {_OYUNCU_ALANLAR}""", o)


def oyuncular_listele(takim: Optional[int]) -> List[Dict[str, Any]]:
    return execute_query(f"SELECT {_OYUNCU_ALANLAR} FROM greydb.sayac_oyuncular WHERE aktif AND (CAST(:t AS INTEGER) IS NULL OR fotmob_team_id = :t) ORDER BY fotmob_team_id, yildiz DESC, ad, poz", {"t": takim})


def oyuncu_getir(oid: int) -> Optional[Dict[str, Any]]:
    r = execute_query(f"SELECT {_OYUNCU_ALANLAR} FROM greydb.sayac_oyuncular WHERE id = :i", {"i": oid})
    return r[0] if r else None


def oyuncu_gorseli(oid: int) -> Optional[Dict[str, Any]]:
    r = execute_query("SELECT gorsel, gorsel_tur FROM greydb.sayac_oyuncular WHERE id = :i", {"i": oid})
    return r[0] if r else None


def takim_yildizi(team_id: int) -> Optional[Dict[str, Any]]:
    r = execute_query(f"SELECT {_OYUNCU_ALANLAR} FROM greydb.sayac_oyuncular WHERE fotmob_team_id = :t AND yildiz AND aktif", {"t": team_id})
    return r[0] if r else None


def oyuncu_guncelle(oid: int, g: Dict[str, Any]) -> bool:
    """yildiz=True ise aynı takımın önceki yıldızı kapatılır (tek işlemde)."""
    with transaction():
        o = oyuncu_getir(oid)
        if not o:
            return False
        if g.get("yildiz"):
            execute_insert("UPDATE greydb.sayac_oyuncular SET yildiz = FALSE WHERE fotmob_team_id = :t AND yildiz", {"t": o["fotmob_team_id"]})
        for alan in ("yildiz", "aktif", "kafa_orani"):
            if alan in g and g[alan] is not None:
                execute_insert(f"UPDATE greydb.sayac_oyuncular SET {alan} = :v WHERE id = :i", {"v": g[alan], "i": oid})
        if g.get("aktif") is False:
            execute_insert("UPDATE greydb.sayac_oyuncular SET yildiz = FALSE WHERE id = :i", {"i": oid})
    return True


def takim_renkleri(idler: List[int]) -> Dict[int, str]:
    r = execute_query("SELECT fotmob_team_id, COALESCE(renk_elle, renk) AS renk FROM greydb.yayin_takimlari WHERE fotmob_team_id = ANY(:ids)", {"ids": idler})
    return {x["fotmob_team_id"]: x["renk"] for x in r}


def is_ekle(j: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Aynı maçın aktif (önizleme/video aktif) işi varsa None döner (benzersiz indeks)."""
    from sqlalchemy.exc import IntegrityError
    try:
        return execute_insert(
            """INSERT INTO greydb.sayac_isleri (fotmob_match_id, ev_team_id, dep_team_id, ev_ad, dep_ad, ev_oyuncu_id, dep_oyuncu_id, girdi, olusturan)
               VALUES (:fotmob_match_id, :ev_team_id, :dep_team_id, :ev_ad, :dep_ad, :ev_oyuncu_id, :dep_oyuncu_id, CAST(:girdi AS jsonb), :olusturan)
               RETURNING id""", {**j, "girdi": json.dumps(j["girdi"], ensure_ascii=False)})
    except IntegrityError:
        return None


def isler_listele(limit: int = 50) -> List[Dict[str, Any]]:
    return execute_query(
        """SELECT id, fotmob_match_id, ev_ad, dep_ad, ev_oyuncu_id, dep_oyuncu_id, girdi, durum, isci, dosya_yolu, deneme, hata, olusturan, olusturma, baslama, bitis
           , (onizleme IS NOT NULL) AS onizleme_var FROM greydb.sayac_isleri ORDER BY id DESC LIMIT :l""", {"l": limit})


def is_getir(iid: int) -> Optional[Dict[str, Any]]:
    r = execute_query("SELECT * FROM greydb.sayac_isleri WHERE id = :i", {"i": iid})
    return r[0] if r else None


def is_iptal_et(iid: int) -> bool:
    return bool(execute_insert(f"UPDATE greydb.sayac_isleri SET durum = 'iptal', bitis = {_UTC_SIMDI} WHERE id = :i AND durum IN ('onizleme_bekliyor','onizleme_calisiyor','onizleme_hazir','bekliyor','calisiyor') RETURNING id", {"i": iid}))


def isciler_listele() -> List[Dict[str, Any]]:
    return execute_query("SELECT ad, son_gorulme, surum FROM greydb.sayac_isciler ORDER BY ad")


def isci_gor(ad: str, surum: Optional[str]) -> None:
    execute_insert(
        f"""INSERT INTO greydb.sayac_isciler (ad, son_gorulme, surum) VALUES (:a, {_UTC_SIMDI}, :s)
            ON CONFLICT (ad) DO UPDATE SET son_gorulme = {_UTC_SIMDI}, surum = EXCLUDED.surum""", {"a": ad, "s": surum})


def kuyruga_geri_al() -> None:
    """Kalp atışı kesilen 'calisiyor' işleri kuyruğa döndürür; MAKS_DENEME dolduysa hata yapar."""
    execute_insert(
        f"""UPDATE greydb.sayac_isleri SET
              durum = CASE WHEN deneme >= :m THEN 'hata' WHEN durum = 'onizleme_calisiyor' THEN 'onizleme_bekliyor' ELSE 'bekliyor' END,
              hata = CASE WHEN deneme >= :m THEN 'İşçi yanıt vermedi ({MAKS_DENEME} deneme)' ELSE hata END,
              bitis = CASE WHEN deneme >= :m THEN {_UTC_SIMDI} ELSE NULL END,
              isci = NULL
            WHERE durum IN ('calisiyor','onizleme_calisiyor') AND kalp_atisi < {_UTC_SIMDI} - make_interval(secs => :s)""", {"m": MAKS_DENEME, "s": KALP_ZAMAN_ASIMI_SN})


def is_al(isci: str) -> Optional[Dict[str, Any]]:
    """Sıradaki işi atomik alır (iki Mac aynı işi alamaz). Önizlemeler videolardan önce gelir; sonuçta 'asama' = onizleme | video."""
    return execute_insert(
        f"""UPDATE greydb.sayac_isleri SET
              durum = CASE WHEN durum = 'onizleme_bekliyor' THEN 'onizleme_calisiyor' ELSE 'calisiyor' END,
              isci = :i, baslama = {_UTC_SIMDI}, kalp_atisi = {_UTC_SIMDI}, deneme = deneme + 1
            WHERE id = (SELECT id FROM greydb.sayac_isleri WHERE durum IN ('onizleme_bekliyor','bekliyor')
                        ORDER BY (durum = 'onizleme_bekliyor') DESC, id FOR UPDATE SKIP LOCKED LIMIT 1)
            RETURNING *, (durum = 'onizleme_calisiyor') AS onizleme_asamasi""", {"i": isci})


def onizleme_yaz(iid: int, isci: str, jpeg: bytes) -> bool:
    return bool(execute_insert(f"UPDATE greydb.sayac_isleri SET durum = 'onizleme_hazir', onizleme = :b, hata = NULL WHERE id = :i AND isci = :a AND durum = 'onizleme_calisiyor' RETURNING id",
                               {"i": iid, "a": isci, "b": jpeg}))


def onizleme_getir(iid: int) -> Optional[bytes]:
    r = execute_query("SELECT onizleme FROM greydb.sayac_isleri WHERE id = :i", {"i": iid})
    return bytes(r[0]["onizleme"]) if r and r[0]["onizleme"] else None


def is_onayla(iid: int) -> bool:
    """Önizleme onaylandı → 4K video kuyruğuna."""
    return bool(execute_insert("UPDATE greydb.sayac_isleri SET durum = 'bekliyor', deneme = 0, isci = NULL, baslama = NULL, kalp_atisi = NULL WHERE id = :i AND durum = 'onizleme_hazir' RETURNING id", {"i": iid}))


def kalp_yaz(iid: int, isci: str) -> bool:
    return bool(execute_insert(f"UPDATE greydb.sayac_isleri SET kalp_atisi = {_UTC_SIMDI} WHERE id = :i AND isci = :a AND durum IN ('calisiyor','onizleme_calisiyor') RETURNING id", {"i": iid, "a": isci}))


def bitti_yaz(iid: int, isci: str, dosya_yolu: str) -> bool:
    return bool(execute_insert(f"UPDATE greydb.sayac_isleri SET durum = 'hazir', dosya_yolu = :y, hata = NULL, bitis = {_UTC_SIMDI} WHERE id = :i AND isci = :a AND durum = 'calisiyor' RETURNING id", {"i": iid, "a": isci, "y": dosya_yolu}))


def hata_yaz(iid: int, isci: str, hata: str) -> bool:
    return bool(execute_insert(f"UPDATE greydb.sayac_isleri SET durum = 'hata', hata = :h, bitis = {_UTC_SIMDI} WHERE id = :i AND isci = :a AND durum IN ('calisiyor','onizleme_calisiyor') RETURNING id", {"i": iid, "a": isci, "h": hata[:500]}))


# ---------------- yetki ----------------
def isci_gerek(request: Request) -> None:
    anahtar = os.environ.get("SAYAC_ISCI_ANAHTARI")
    if not anahtar:
        raise HTTPException(status_code=503, detail="İşçi uçları kapalı: sunucuda SAYAC_ISCI_ANAHTARI tanımlı değil")
    gelen = request.headers.get("x-isci-anahtari", "")
    if not gelen or not hmac.compare_digest(gelen.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz ya da eksik işçi anahtarı")


def _admin_ya_da_isci(request: Request) -> None:
    if request.headers.get("x-isci-anahtari"):
        isci_gerek(request)
    else:
        admin_gerek(request)


# ---------------- şemalar ----------------
class OyuncuGuncelle(BaseModel):
    model_config = ConfigDict(extra="forbid")
    yildiz: Optional[bool] = None
    aktif: Optional[bool] = None
    kafa_orani: Optional[float] = Field(None, ge=0, le=1)


class IsAc(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fotmob_match_id: int
    ev_team_id: int
    dep_team_id: int
    ev_ad: str = Field(max_length=60)
    dep_ad: str = Field(max_length=60)
    lig: str = Field(max_length=60)
    mac_zamani: datetime                    # UTC (FotMob matchTimeUTCDate)
    ev_oyuncu_id: Optional[int] = None      # boşsa takımın yıldızı
    dep_oyuncu_id: Optional[int] = None


class IsciIstek(BaseModel):
    model_config = ConfigDict(extra="forbid")
    isci: str = Field(min_length=1, max_length=40)
    surum: Optional[str] = Field(None, max_length=40)


class IsciKimlik(BaseModel):
    model_config = ConfigDict(extra="forbid")
    isci: str = Field(min_length=1, max_length=40)


class Bitti(IsciKimlik):
    dosya_yolu: str = Field(min_length=1, max_length=500)


class Hata(IsciKimlik):
    hata: str = Field(min_length=1, max_length=500)


def _oyuncu_ozeti(o: Dict[str, Any]) -> Dict[str, Any]:
    o = dict(o)
    for k in ("olusturma",):
        if isinstance(o.get(k), datetime):
            o[k] = o[k].isoformat() + "Z"
    o["aspect"] = round(o["genislik"] / o["yukseklik"], 4)
    return o


def _is_ozeti(j: Dict[str, Any]) -> Dict[str, Any]:
    j = dict(j)
    for k in ("olusturma", "baslama", "bitis", "kalp_atisi"):
        if isinstance(j.get(k), datetime):
            j[k] = j[k].isoformat() + "Z"
    return j


# ---------------- admin uçları ----------------
@router.put("/sayac/oyuncular")
async def oyuncu_yukle(request: Request, fotmob_player_id: int = Query(...), fotmob_team_id: int = Query(...), ad: str = Query(..., min_length=1, max_length=80),
                       poz: str = Query("varsayilan", pattern=r"^[a-z0-9_]{1,30}$"), kafa_orani: float = Query(0.5, ge=0, le=1)):
    """Gövde = ham PNG baytları (multipart gerekmez)."""
    k = admin_gerek(request)
    boyut = gorsel_dogrula(await request.body())
    kayit = oyuncu_ekle({"fotmob_player_id": fotmob_player_id, "ad": ad.strip(), "fotmob_team_id": fotmob_team_id, "poz": poz, "gorsel": await request.body(),
                         "kafa_orani": kafa_orani, "yukleyen": k.email, **boyut})
    return _oyuncu_ozeti(kayit)


@router.get("/sayac/oyuncular")
def oyuncular(request: Request, takim: Optional[int] = None):
    admin_gerek(request)
    return {"oyuncular": [_oyuncu_ozeti(o) for o in oyuncular_listele(takim)]}


@router.patch("/sayac/oyuncular/{oid}")
def oyuncu_degistir(oid: int, g: OyuncuGuncelle, request: Request):
    admin_gerek(request)
    if not oyuncu_guncelle(oid, g.model_dump(exclude_none=True)):
        raise HTTPException(status_code=404, detail="Oyuncu bulunamadı")
    return {"ok": True}


@router.get("/sayac/oyuncular/{oid}/gorsel")
def oyuncu_gorsel(oid: int, request: Request):
    _admin_ya_da_isci(request)
    r = oyuncu_gorseli(oid)
    if not r:
        raise HTTPException(status_code=404, detail="Görsel yok")
    return Response(content=bytes(r["gorsel"]), media_type=r["gorsel_tur"], headers={"Cache-Control": "private, max-age=3600"})


def _oyuncu_sec(takim_id: int, secilen: Optional[int], taraf: str) -> Dict[str, Any]:
    o = oyuncu_getir(secilen) if secilen else takim_yildizi(takim_id)
    if not o or not o.get("aktif", True):
        raise HTTPException(status_code=422, detail=f"{taraf} için oyuncu görseli yok: önce yükle ya da yıldız seç")
    if o["fotmob_team_id"] != takim_id:
        raise HTTPException(status_code=422, detail=f"Seçilen oyuncu {taraf} takımına ait değil")
    return o


@router.post("/sayac/isler", status_code=201)
def is_ac(g: IsAc, request: Request):
    k = admin_gerek(request)
    if g.ev_team_id == g.dep_team_id:
        raise HTTPException(status_code=422, detail="İki takım aynı olamaz")
    ev = _oyuncu_sec(g.ev_team_id, g.ev_oyuncu_id, "ev sahibi")
    dep = _oyuncu_sec(g.dep_team_id, g.dep_oyuncu_id, "deplasman")
    z = g.mac_zamani if g.mac_zamani.tzinfo is None else g.mac_zamani.astimezone(timezone.utc).replace(tzinfo=None)
    renkler = takim_renkleri([g.ev_team_id, g.dep_team_id])
    girdi = {**kunye(z, g.lig), **tohumlar(g.fotmob_match_id),
             "evGlow": parilti(renkler.get(g.ev_team_id), VARSAYILAN_PARILTI[0]), "depGlow": parilti(renkler.get(g.dep_team_id), VARSAYILAN_PARILTI[1])}
    r = is_ekle({"fotmob_match_id": g.fotmob_match_id, "ev_team_id": g.ev_team_id, "dep_team_id": g.dep_team_id, "ev_ad": g.ev_ad, "dep_ad": g.dep_ad,
                 "ev_oyuncu_id": ev["id"], "dep_oyuncu_id": dep["id"], "girdi": girdi, "olusturan": k.email})
    if not r:
        raise HTTPException(status_code=409, detail="Bu maç için bekleyen ya da çalışan bir iş zaten var")
    return {"id": r["id"], "girdi": girdi}


@router.get("/sayac/isler")
def isler(request: Request):
    admin_gerek(request)
    return {"isler": [_is_ozeti(j) for j in isler_listele()], "isciler": [{**i, "son_gorulme": i["son_gorulme"].isoformat() + "Z"} for i in isciler_listele()]}


@router.post("/sayac/isler/{iid}/iptal")
def is_iptal(iid: int, request: Request):
    admin_gerek(request)
    if not is_iptal_et(iid):
        raise HTTPException(status_code=409, detail="İş bulunamadı ya da bitmiş")
    return {"ok": True}


@router.get("/sayac/isler/{iid}/onizleme")
def is_onizleme(iid: int, request: Request):
    admin_gerek(request)
    b = onizleme_getir(iid)
    if not b:
        raise HTTPException(status_code=404, detail="Önizleme yok")
    return Response(content=b, media_type="image/jpeg", headers={"Cache-Control": "private, no-store"})


@router.post("/sayac/isler/{iid}/onayla")
def is_onay(iid: int, request: Request):
    admin_gerek(request)
    if not is_onayla(iid):
        raise HTTPException(status_code=409, detail="Onaylanacak önizleme yok (iş önizleme bekliyor ya da bitmiş/iptal)")
    return {"ok": True}


# ---------------- işçi uçları ----------------
@router.post("/sayac/isci/al")
def isci_al(g: IsciIstek, request: Request):
    isci_gerek(request)
    isci_gor(g.isci, g.surum)
    kuyruga_geri_al()
    j = is_al(g.isci)
    if not j:
        return {"is": None}
    ev, dep = oyuncu_getir(j["ev_oyuncu_id"]), oyuncu_getir(j["dep_oyuncu_id"])
    girdi = j["girdi"] if isinstance(j["girdi"], dict) else json.loads(j["girdi"])
    return {"is": {
        "asama": "onizleme" if j.get("onizleme_asamasi") else "video", "id": j["id"], "fotmob_match_id": j["fotmob_match_id"], "girdi": girdi, "ev_ad": j["ev_ad"], "dep_ad": j["dep_ad"],
        "ev": {"team_id": j["ev_team_id"], "oyuncu": _oyuncu_ozeti(ev)}, "dep": {"team_id": j["dep_team_id"], "oyuncu": _oyuncu_ozeti(dep)},
        "logo_yolu": "/api/yayin/logo/{team_id}", "gorsel_yolu": "/api/sayac/oyuncular/{id}/gorsel",
    }}


def _kendi_isim(iid: int, isci: str, fonk, *a) -> Dict[str, bool]:
    if not fonk(iid, isci, *a):
        j = is_getir(iid)
        if not j:
            raise HTTPException(status_code=404, detail="İş bulunamadı")
        raise HTTPException(status_code=409, detail="İş bu işçiye ait ve çalışıyor durumunda değil (iptal edilmiş ya da kuyruğa dönmüş olabilir)")
    return {"ok": True}


@router.post("/sayac/isci/isler/{iid}/kalp")
def isci_kalp(iid: int, g: IsciKimlik, request: Request):
    isci_gerek(request)
    return _kendi_isim(iid, g.isci, kalp_yaz)


@router.post("/sayac/isci/isler/{iid}/onizleme")
async def isci_onizleme(iid: int, request: Request, isci: str = Query(..., min_length=1, max_length=40)):
    """Gövde = ham JPEG (en çok 2 MB)."""
    isci_gerek(request)
    b = await request.body()
    if len(b) > 2 * 1024 * 1024 or b[:3] != b"\xff\xd8\xff":
        raise HTTPException(status_code=422, detail="Önizleme JPEG olmalı ve 2 MB'ı geçmemeli")
    return _kendi_isim(iid, isci, onizleme_yaz, b)


@router.post("/sayac/isci/isler/{iid}/bitti")
def isci_bitti(iid: int, g: Bitti, request: Request):
    isci_gerek(request)
    return _kendi_isim(iid, g.isci, bitti_yaz, g.dosya_yolu)


@router.post("/sayac/isci/isler/{iid}/hata")
def isci_hata(iid: int, g: Hata, request: Request):
    isci_gerek(request)
    return _kendi_isim(iid, g.isci, hata_yaz, g.hata)
