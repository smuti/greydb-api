"""CapCut projelerine otomatik altyazı (admin → işçi Mac): proje listesi, döküm/yazma/geri alma işleri, sözlük.

Video ve ses SUNUCUYA yüklenmez: işçi Mac Whisper'ı yerelde çalıştırır, sunucuya yalnız metin, zamanlar ve küçük bir 360p önizleme gider.
Yetki sayaçtaki gibi: admin uçları ağ geçidi anahtarı + X-Yetki: admin; işçi uçları X-Isci-Anahtari (SAYAC_ISCI_ANAHTARI, sayaçla aynı anahtar).
İş HER ZAMAN belirli bir işçiye aittir (proje o Mac'te olduğu için): işçi yalnız kendi işini alır.

Akış (asama / durum):  dokum: sirada → calisiyor → dokum_hazir → (admin düzeltir) onay_bekliyor
                       yaz:  sirada → calisiyor (⇄ capcut_acik_bekliyor) → yazildi | hata
                       geri_al: sirada → calisiyor → geri_alindi | hata
Bloklayan veritabanı çağrıları düz `def` (olay döngüsünü bloklamasın, bkz. #84); gövde okuyan uçlar veritabanına iş parçacığı havuzundan gider.
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from app.routers.kuponlar import admin_gerek
from app.routers.sayac import isci_gerek
from app.services.db import execute_insert, execute_insert_many, execute_query, transaction

router = APIRouter(tags=["altyazi"])

ASAMALAR = ("dokum", "yaz", "geri_al")
DURUMLAR = ("sirada", "calisiyor", "dokum_hazir", "onay_bekliyor", "capcut_acik_bekliyor", "yazildi", "geri_alindi", "hata", "iptal")
YAZILABILIR = ("dokum_hazir", "onay_bekliyor", "yazildi", "geri_alindi", "hata")   # döküm hazırsa (ya da yazılmış/geri alınmış/hata almışsa) yeniden yazılabilir
AKTIF = ("sirada", "calisiyor", "capcut_acik_bekliyor")          # aynı projede aynı anda tek aktif iş
KALP_ZAMAN_ASIMI_SN = 180
MAKS_DENEME = 3
ONIZLEME_MAKS_BAYT = 25 * 1024 * 1024
_UTC_SIMDI = "(now() AT TIME ZONE 'utc')"
_IS_ALANLARI = "id, isci, draft_id, proje_ad, asama, durum, alt_adim, ayarlar, imza, tuval, sure, uyarilar, yedek_yolu, hata, deneme, olusturan, olusturuldu, guncellendi, (onizleme IS NOT NULL) AS onizleme_var"


def _sql_liste(degerler) -> str:
    return ",".join(f"'{d}'" for d in degerler)


# ---------------- veritabanı katmanı (testlerde değiştirilir) ----------------
def projeleri_yenile(isci: str, projeler: List[Dict[str, Any]]) -> None:
    """İşçinin proje listesini bütünüyle değiştirir (tek işlemde)."""
    with transaction():
        execute_insert("DELETE FROM greydb.capcut_projeleri WHERE isci = :i RETURNING isci", {"i": isci})
        if projeler:
            execute_insert_many(
                """INSERT INTO greydb.capcut_projeleri (isci, draft_id, ad, klasor, degisme, sure_sn, tuval, capcut_surum, medya, altyazi_var, guncellendi)
                   VALUES (:isci, :draft_id, :ad, :klasor, CAST(:degisme AS timestamptz), :sure_sn, CAST(:tuval AS jsonb), :surum, CAST(:medya AS jsonb), :altyazi_var, """ + _UTC_SIMDI + ")",
                [{"isci": isci, "draft_id": p["draft_id"], "ad": p["ad"], "klasor": p["klasor"], "degisme": p.get("degisme"), "sure_sn": p.get("sure_sn"),
                  "tuval": json.dumps(p.get("tuval") or {}), "surum": p.get("surum"), "medya": json.dumps(p.get("medya") or [], ensure_ascii=False),
                  "altyazi_var": bool(p.get("altyazi_var"))} for p in projeler])
        execute_insert(f"INSERT INTO greydb.altyazi_isciler (ad, son_gorulme) VALUES (:i, {_UTC_SIMDI}) ON CONFLICT (ad) DO UPDATE SET son_gorulme = {_UTC_SIMDI} RETURNING ad", {"i": isci})


def projeler_listele(isci: str) -> List[Dict[str, Any]]:
    return execute_query("SELECT draft_id, ad, klasor, degisme, sure_sn, tuval, capcut_surum AS surum, medya, altyazi_var, guncellendi FROM greydb.capcut_projeleri WHERE isci = :i ORDER BY degisme DESC NULLS LAST", {"i": isci})


def isciler_listele() -> List[Dict[str, Any]]:
    return execute_query("SELECT ad, son_gorulme, surum FROM greydb.altyazi_isciler ORDER BY ad")


def isci_gor(ad: str, surum: Optional[str]) -> None:
    execute_insert(f"INSERT INTO greydb.altyazi_isciler (ad, son_gorulme, surum) VALUES (:a, {_UTC_SIMDI}, :s) ON CONFLICT (ad) DO UPDATE SET son_gorulme = {_UTC_SIMDI}, surum = EXCLUDED.surum RETURNING ad", {"a": ad, "s": surum})


def is_ekle(j: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Aynı işçi + proje için aktif iş varsa None (benzersiz indeks)."""
    from sqlalchemy.exc import IntegrityError
    try:
        return execute_insert("INSERT INTO greydb.altyazi_isleri (isci, draft_id, proje_ad, ayarlar, olusturan) VALUES (:isci, :draft_id, :proje_ad, CAST(:ayarlar AS jsonb), :olusturan) RETURNING id",
                              {**j, "ayarlar": json.dumps(j["ayarlar"], ensure_ascii=False)})
    except IntegrityError:
        return None


def isler_listele(isci: Optional[str] = None, draft_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    return execute_query(f"SELECT {_IS_ALANLARI} FROM greydb.altyazi_isleri WHERE (CAST(:i AS TEXT) IS NULL OR isci = :i) AND (CAST(:d AS TEXT) IS NULL OR draft_id = :d) ORDER BY id DESC LIMIT :l",
                         {"i": isci, "d": draft_id, "l": limit})


def is_getir(iid: int) -> Optional[Dict[str, Any]]:
    rows = execute_query(f"SELECT {_IS_ALANLARI}, kelimeler, parcalar FROM greydb.altyazi_isleri WHERE id = :i", {"i": iid})
    return rows[0] if rows else None


def kuyruga_geri_al() -> None:
    """Kalp atışı kesilen işleri kuyruğa döndürür; MAKS_DENEME dolduysa hata yapar."""
    execute_insert(
        f"""UPDATE greydb.altyazi_isleri SET
              durum = CASE WHEN deneme >= :m THEN 'hata' ELSE 'sirada' END,
              hata = CASE WHEN deneme >= :m THEN 'İşçi yanıt vermedi ({MAKS_DENEME} deneme)' ELSE hata END
            WHERE durum IN ('calisiyor','capcut_acik_bekliyor') AND kalp_atisi < {_UTC_SIMDI} - make_interval(secs => :s) RETURNING id""", {"m": MAKS_DENEME, "s": KALP_ZAMAN_ASIMI_SN})


def is_al(isci: str) -> Optional[Dict[str, Any]]:
    """Bu işçinin sıradaki işini atomik alır; yazma/geri alma için gereken veriyle birlikte döner."""
    return execute_insert(
        f"""UPDATE greydb.altyazi_isleri SET durum = 'calisiyor', kalp_atisi = {_UTC_SIMDI}, deneme = deneme + 1, guncellendi = {_UTC_SIMDI}
            WHERE id = (SELECT id FROM greydb.altyazi_isleri WHERE durum = 'sirada' AND isci = :i ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
            RETURNING id, isci, draft_id, proje_ad, asama, ayarlar, imza, parcalar, kelimeler, yedek_yolu""", {"i": isci})


_BENIM = "id = :i AND isci = :a AND durum IN ('calisiyor','capcut_acik_bekliyor')"


def kalp_yaz(iid: int, isci: str, alt_adim: Optional[str]) -> bool:
    return bool(execute_insert(f"UPDATE greydb.altyazi_isleri SET kalp_atisi = {_UTC_SIMDI}, alt_adim = COALESCE(:s, alt_adim) WHERE {_BENIM} RETURNING id", {"i": iid, "a": isci, "s": alt_adim}))


def dokum_yaz(iid: int, isci: str, d: Dict[str, Any]) -> bool:
    return bool(execute_insert(
        f"""UPDATE greydb.altyazi_isleri SET durum = 'dokum_hazir', alt_adim = NULL, hata = NULL, kelimeler = CAST(:k AS jsonb), parcalar = CAST(:p AS jsonb), imza = :im,
              tuval = CAST(:t AS jsonb), sure = :su, uyarilar = CAST(:u AS jsonb), guncellendi = {_UTC_SIMDI} WHERE id = :i AND isci = :a AND asama = 'dokum' AND durum = 'calisiyor' RETURNING id""",
        {"i": iid, "a": isci, "k": json.dumps(d["kelimeler"], ensure_ascii=False), "p": json.dumps(d["parcalar"], ensure_ascii=False), "im": d["imza"],
         "t": json.dumps(d["tuval"]), "su": d["sure"], "u": json.dumps(d["uyarilar"], ensure_ascii=False)}))


def onizleme_yaz(iid: int, isci: str, b: bytes) -> bool:
    return bool(execute_insert(f"UPDATE greydb.altyazi_isleri SET onizleme = :b, guncellendi = {_UTC_SIMDI} WHERE {_BENIM} RETURNING id", {"i": iid, "a": isci, "b": b}))


def onizleme_getir(iid: int) -> Optional[bytes]:
    rows = execute_query("SELECT onizleme FROM greydb.altyazi_isleri WHERE id = :i", {"i": iid})
    return bytes(rows[0]["onizleme"]) if rows and rows[0]["onizleme"] else None


def durum_yaz(iid: int, isci: str, durum: str, alt_adim: Optional[str]) -> bool:
    return bool(execute_insert(f"UPDATE greydb.altyazi_isleri SET durum = :d, alt_adim = :s, kalp_atisi = {_UTC_SIMDI}, guncellendi = {_UTC_SIMDI} WHERE {_BENIM} RETURNING id", {"i": iid, "a": isci, "d": durum, "s": alt_adim}))


def bitti_yaz(iid: int, isci: str, yedek_yolu: Optional[str], parca_sayisi: Optional[int]) -> bool:
    """Yazma/geri alma bitti; önizleme artık gerekmez, silinir."""
    return bool(execute_insert(
        f"""UPDATE greydb.altyazi_isleri SET durum = CASE WHEN asama = 'yaz' THEN 'yazildi' ELSE 'geri_alindi' END, yedek_yolu = COALESCE(:y, yedek_yolu),
              alt_adim = NULL, hata = NULL, onizleme = NULL, guncellendi = {_UTC_SIMDI} WHERE {_BENIM} AND asama IN ('yaz','geri_al') RETURNING id""", {"i": iid, "a": isci, "y": yedek_yolu}))


def hata_yaz(iid: int, isci: str, hata: str) -> bool:
    return bool(execute_insert(f"UPDATE greydb.altyazi_isleri SET durum = 'hata', hata = :h, alt_adim = NULL, guncellendi = {_UTC_SIMDI} WHERE {_BENIM} RETURNING id", {"i": iid, "a": isci, "h": hata[:500]}))


def parcalar_kaydet(iid: int, parcalar: List[Dict[str, Any]]) -> bool:
    return bool(execute_insert(
        f"""UPDATE greydb.altyazi_isleri SET parcalar = CAST(:p AS jsonb), durum = 'onay_bekliyor', guncellendi = {_UTC_SIMDI}
            WHERE id = :i AND kelimeler IS NOT NULL AND durum IN ({_sql_liste(YAZILABILIR)}) RETURNING id""", {"i": iid, "p": json.dumps(parcalar, ensure_ascii=False)}))


def asama_baslat(iid: int, asama: str) -> bool:
    """yaz: düzeltilmiş parçalar varsa; geri_al: yedek varsa. Aynı projede başka aktif iş varsa başlamaz."""
    from sqlalchemy.exc import IntegrityError
    izinli = {"yaz": f"({_sql_liste(YAZILABILIR)})", "geri_al": "('yazildi')"}[asama]
    ek = "AND parcalar IS NOT NULL" if asama == "yaz" else "AND yedek_yolu IS NOT NULL"
    try:
        return bool(execute_insert(f"UPDATE greydb.altyazi_isleri SET asama = :a, durum = 'sirada', hata = NULL, alt_adim = NULL, deneme = 0, guncellendi = {_UTC_SIMDI} WHERE id = :i AND durum IN {izinli} {ek} RETURNING id", {"i": iid, "a": asama}))
    except IntegrityError:
        return False


def is_iptal_et(iid: int) -> bool:
    return bool(execute_insert(f"UPDATE greydb.altyazi_isleri SET durum = 'iptal', guncellendi = {_UTC_SIMDI} WHERE id = :i AND durum IN ({_sql_liste(AKTIF)}) RETURNING id", {"i": iid}))


def sozluk_listele() -> List[Dict[str, Any]]:
    return execute_query("SELECT id, yanlis, dogru, tur, ekleyen, eklendi FROM greydb.altyazi_sozluk ORDER BY dogru, id")


def sozluk_ekle(yanlis: str, dogru: str, tur: str, ekleyen: str) -> Dict[str, Any]:
    return execute_insert("INSERT INTO greydb.altyazi_sozluk (yanlis, dogru, tur, ekleyen) VALUES (:y, :d, :t, :e) RETURNING id, yanlis, dogru, tur, ekleyen, eklendi", {"y": yanlis, "d": dogru, "t": tur, "e": ekleyen})


def sozluk_sil(sid: int) -> bool:
    return bool(execute_insert("DELETE FROM greydb.altyazi_sozluk WHERE id = :i RETURNING id", {"i": sid}))


# ---------------- şemalar ----------------
class Proje(BaseModel):
    model_config = ConfigDict(extra="ignore")
    draft_id: str = Field(min_length=1, max_length=100)
    ad: str = Field(max_length=300)
    klasor: str = Field(max_length=300)
    degisme: Optional[str] = None
    sure_sn: Optional[float] = None
    tuval: Optional[Dict[str, Any]] = None
    surum: Optional[str] = Field(None, max_length=40)
    medya: List[Dict[str, Any]] = Field(default_factory=list, max_length=100)
    altyazi_var: bool = False


class ProjelerGonder(BaseModel):
    model_config = ConfigDict(extra="forbid")
    isci: str = Field(min_length=1, max_length=40)
    projeler: List[Proje] = Field(max_length=500)


class IsciIstek(BaseModel):
    model_config = ConfigDict(extra="forbid")
    isci: str = Field(min_length=1, max_length=40)
    surum: Optional[str] = Field(None, max_length=40)


class Kalp(BaseModel):
    model_config = ConfigDict(extra="forbid")
    isci: str = Field(min_length=1, max_length=40)
    alt_adim: Optional[str] = Field(None, max_length=200)


class Durum(Kalp):
    durum: Literal["calisiyor", "capcut_acik_bekliyor"]


class DokumGovde(BaseModel):
    model_config = ConfigDict(extra="forbid")
    isci: str = Field(min_length=1, max_length=40)
    kelimeler: List[Dict[str, Any]] = Field(max_length=20000)
    parcalar: List[Dict[str, Any]] = Field(max_length=5000)
    imza: str = Field(min_length=1, max_length=100)
    tuval: Dict[str, Any]
    sure: float
    uyarilar: List[str] = Field(default_factory=list, max_length=200)


class Bitti(BaseModel):
    model_config = ConfigDict(extra="forbid")
    isci: str = Field(min_length=1, max_length=40)
    yedek_yolu: Optional[str] = Field(None, max_length=500)
    parca_sayisi: Optional[int] = None


class Hata(BaseModel):
    model_config = ConfigDict(extra="forbid")
    isci: str = Field(min_length=1, max_length=40)
    hata: str = Field(min_length=1, max_length=500)


class IsAc(BaseModel):
    model_config = ConfigDict(extra="forbid")
    isci: str = Field(min_length=1, max_length=40)
    draft_id: str = Field(min_length=1, max_length=100)
    proje_ad: Optional[str] = Field(None, max_length=300)
    ayarlar: Dict[str, Any] = Field(default_factory=dict)


class DuzeltilmisParca(BaseModel):
    model_config = ConfigDict(extra="allow")
    t: str = Field(min_length=1, max_length=300)
    s: float = Field(ge=0)
    e: float = Field(ge=0)


class ParcalarGovde(BaseModel):
    model_config = ConfigDict(extra="forbid")
    parcalar: List[DuzeltilmisParca] = Field(max_length=5000)


class SozlukGirdi(BaseModel):
    model_config = ConfigDict(extra="forbid")
    yanlis: str = Field("", max_length=100)       # boşsa yalnız Whisper'a ipucu
    dogru: str = Field(min_length=1, max_length=100)
    tur: Literal["takim", "oyuncu", "genel"] = "genel"


def _zaman(d: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(d)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat() + ("Z" if v.tzinfo is None else "")
    return d


def _kendi_isim(iid: int, fonk, *a) -> Dict[str, bool]:
    if not fonk(iid, *a):
        raise HTTPException(status_code=409, detail="İş bu işçiye ait ve çalışıyor durumunda değil (iptal edilmiş ya da kuyruğa dönmüş olabilir)")
    return {"ok": True}


# ---------------- işçi uçları ----------------
@router.post("/altyazi/isci/projeler")
def isci_projeler(g: ProjelerGonder, request: Request):
    isci_gerek(request)
    projeleri_yenile(g.isci, [p.model_dump() for p in g.projeler])
    return {"ok": True, "adet": len(g.projeler)}


@router.post("/altyazi/isci/al")
def isci_al(g: IsciIstek, request: Request):
    isci_gerek(request)
    isci_gor(g.isci, g.surum)
    kuyruga_geri_al()
    j = is_al(g.isci)
    return {"is": _zaman(j) if j else None}


@router.post("/altyazi/isci/isler/{iid}/kalp")
def isci_kalp(iid: int, g: Kalp, request: Request):
    isci_gerek(request)
    return _kendi_isim(iid, kalp_yaz, g.isci, g.alt_adim)


@router.post("/altyazi/isci/isler/{iid}/durum")
def isci_durum(iid: int, g: Durum, request: Request):
    isci_gerek(request)
    return _kendi_isim(iid, durum_yaz, g.isci, g.durum, g.alt_adim)


@router.post("/altyazi/isci/isler/{iid}/dokum")
def isci_dokum(iid: int, g: DokumGovde, request: Request):
    isci_gerek(request)
    return _kendi_isim(iid, dokum_yaz, g.isci, g.model_dump(exclude={"isci"}))


@router.post("/altyazi/isci/isler/{iid}/onizleme")
async def isci_onizleme(iid: int, request: Request, isci: str = Query(..., min_length=1, max_length=40)):
    """Gövde = ham MP4 (360p, en çok 25 MB)."""
    isci_gerek(request)
    b = await request.body()
    if len(b) > ONIZLEME_MAKS_BAYT or b[4:8] != b"ftyp":
        raise HTTPException(status_code=422, detail="Önizleme MP4 olmalı ve 25 MB'ı geçmemeli")
    return await run_in_threadpool(_kendi_isim, iid, onizleme_yaz, isci, b)


@router.post("/altyazi/isci/isler/{iid}/bitti")
def isci_bitti(iid: int, g: Bitti, request: Request):
    isci_gerek(request)
    return _kendi_isim(iid, bitti_yaz, g.isci, g.yedek_yolu, g.parca_sayisi)


@router.post("/altyazi/isci/isler/{iid}/hata")
def isci_hata(iid: int, g: Hata, request: Request):
    isci_gerek(request)
    return _kendi_isim(iid, hata_yaz, g.isci, g.hata)


@router.get("/altyazi/isci/sozluk")
def isci_sozluk(request: Request):
    isci_gerek(request)
    return {"sozluk": [_zaman(x) for x in sozluk_listele()]}


# ---------------- admin uçları ----------------
@router.get("/altyazi/isciler")
def isciler(request: Request):
    admin_gerek(request)
    return {"isciler": [_zaman(x) for x in isciler_listele()]}


@router.get("/altyazi/projeler")
def projeler(request: Request, isci: str = Query(..., min_length=1, max_length=40)):
    admin_gerek(request)
    return {"projeler": [_zaman(p) for p in projeler_listele(isci)]}


@router.post("/altyazi/isler", status_code=201)
def is_ac(g: IsAc, request: Request):
    k = admin_gerek(request)
    r = is_ekle({"isci": g.isci, "draft_id": g.draft_id, "proje_ad": g.proje_ad, "ayarlar": g.ayarlar, "olusturan": k.email})
    if not r:
        raise HTTPException(status_code=409, detail="Bu proje için bekleyen ya da çalışan bir iş zaten var")
    return {"id": r["id"]}


@router.get("/altyazi/isler")
def isler(request: Request, isci: Optional[str] = Query(None, max_length=40), draft_id: Optional[str] = Query(None, max_length=100)):
    admin_gerek(request)
    return {"isler": [_zaman({k: v for k, v in j.items() if k not in ("kelimeler", "parcalar")}) for j in isler_listele(isci, draft_id)]}     # liste hafif: kelime/parça ayrı uçta


@router.get("/altyazi/isler/{iid}")
def is_oku(iid: int, request: Request):
    admin_gerek(request)
    j = is_getir(iid)
    if not j:
        raise HTTPException(status_code=404, detail="İş bulunamadı")
    return _zaman(j)


@router.get("/altyazi/isler/{iid}/onizleme")
def is_onizleme(iid: int, request: Request):
    admin_gerek(request)
    b = onizleme_getir(iid)
    if not b:
        raise HTTPException(status_code=404, detail="Önizleme yok")
    return Response(content=b, media_type="video/mp4", headers={"Cache-Control": "private, no-store"})


@router.put("/altyazi/isler/{iid}/parcalar")
def is_parcalar(iid: int, g: ParcalarGovde, request: Request):
    admin_gerek(request)
    if not parcalar_kaydet(iid, [p.model_dump() for p in g.parcalar]):
        raise HTTPException(status_code=409, detail="Önce döküm hazır olmalı")
    return {"ok": True}


def _asama(iid: int, asama: str, hata: str) -> Dict[str, bool]:
    if not asama_baslat(iid, asama):
        raise HTTPException(status_code=409, detail=hata)
    return {"ok": True}


@router.post("/altyazi/isler/{iid}/yaz")
def is_yaz(iid: int, request: Request):
    admin_gerek(request)
    return _asama(iid, "yaz", "Yazılamaz: döküm hazır değil ya da bu projede başka bir iş çalışıyor")


@router.post("/altyazi/isler/{iid}/geri-al")
def is_geri_al(iid: int, request: Request):
    admin_gerek(request)
    return _asama(iid, "geri_al", "Geri alınamaz: yazılmış ve yedeği olan bir iş değil ya da bu projede başka bir iş çalışıyor")


@router.post("/altyazi/isler/{iid}/iptal")
def is_iptal(iid: int, request: Request):
    admin_gerek(request)
    if not is_iptal_et(iid):
        raise HTTPException(status_code=409, detail="İş bulunamadı ya da aktif değil")
    return {"ok": True}


@router.get("/altyazi/sozluk")
def sozluk(request: Request):
    admin_gerek(request)
    return {"sozluk": [_zaman(x) for x in sozluk_listele()]}


@router.post("/altyazi/sozluk", status_code=201)
def sozluk_yeni(g: SozlukGirdi, request: Request):
    k = admin_gerek(request)
    return _zaman(sozluk_ekle(g.yanlis.strip(), g.dogru.strip(), g.tur, k.email))


@router.delete("/altyazi/sozluk/{sid}")
def sozluk_kaldir(sid: int, request: Request):
    admin_gerek(request)
    if not sozluk_sil(sid):
        raise HTTPException(status_code=404, detail="Kayıt bulunamadı")
    return {"ok": True}
