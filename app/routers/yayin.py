"""Yayın sayfaları: admin'de hazırlanan, OBS'e bağlanan kodlu sayfalar. Sayfa yalnızca burada saklanan HAZIR paketi okur (dış servise istek yok).

Okuma (GET /yayin/{kod}) ve logo herkese açıktır: OBS giriş yapamaz, kod sırdır. Yazma yalnızca admin: Next.js sunucusu güvenilir ağ geçididir
(X-Sunucu-Anahtari + X-Yetki: admin); bkz. routers/kuponlar.py. Hazırlama arka planda çalışır (15 maç 1-2 dk sürebilir), durum yoklanır."""
import json
import secrets
from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from app.routers.kuponlar import admin_gerek
from app.services.db import execute_insert, execute_query
from app.services import yayin_toplayici as T

router = APIRouter(tags=["yayin"])
MARKA = "GREYSPORTS"
BAYI_KODU = "300908"
PROGRAM = {"toto": "Ekonomik ve Tek Atış", "gun": "AÇIK RADAR MASASI"}
AYLAR = ["Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran", "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"]
GUNLER = ["Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi", "Pazar"]


def bugun_tsi(simdi: Optional[datetime] = None) -> date:
    from datetime import timedelta, timezone
    return ((simdi or datetime.now(timezone.utc)) + timedelta(hours=3)).date()


def yayin_gun_metni(yayin_tarihi: date, bugun: date) -> str:
    """Kayıtlı gerçek tarihi izleyiciye gösterilen güne çevirir: Bugün / Yarın / gün adı. Böylece 'Yarın' seçilen sayfa ertesi gün yanlış kalmaz."""
    fark = (yayin_tarihi - bugun).days
    return "Bugün" if fark == 0 else "Yarın" if fark == 1 else GUNLER[yayin_tarihi.weekday()]


def etiket_gun(tarih: date) -> str:
    return f"{tarih.day} {AYLAR[tarih.month - 1]}"


class Hazirla(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tur: Literal["toto", "gun"]
    mac_idler: List[int] = Field(default_factory=list, max_length=15)     # gun: seçilen FotMob maç kimlikleri (sıralı)
    hafta_no: Optional[int] = None                                         # toto: Spor Toto haftası
    tarih: Optional[date] = None                                           # gun: maçların günü
    program_adi: Optional[str] = Field(None, max_length=60)
    yayin_tarihi: date
    yayin_saat: str = Field(pattern=r"^\d{2}:\d{2}$")


class Ayar(BaseModel):
    model_config = ConfigDict(extra="forbid")
    program_adi: Optional[str] = Field(None, max_length=60)
    etiket: Optional[str] = Field(None, max_length=30)
    yayin_tarihi: Optional[date] = None
    yayin_saat: Optional[str] = Field(None, pattern=r"^\d{2}:\d{2}$")
    bayi_kodu: Optional[str] = Field(None, pattern=r"^\d{3,10}$")


class EksikSecim(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mac_sira: int
    ev: List[str] = Field(default_factory=list, max_length=3)     # kartta gösterilecek oyuncu adları
    dep: List[str] = Field(default_factory=list, max_length=3)


class Renk(BaseModel):
    model_config = ConfigDict(extra="forbid")
    renk: Optional[str] = Field(None, pattern=r"^#[0-9A-Fa-f]{6}$")      # None: elle seçimi kaldır


# ---------------- Veritabanı katmanı (testlerde değiştirilir) ----------------
def _kod() -> str:
    return secrets.token_urlsafe(9).replace("_", "x").replace("-", "y")[:12].lower()


def kaydi_olustur(k: Dict[str, Any]) -> Dict[str, Any]:
    return execute_insert(
        """INSERT INTO greydb.yayin_sayfalari (kod, tur, etiket, tarih, program_adi, marka, yayin_tarihi, yayin_saat, bayi_kodu, kaynak, olusturan)
           VALUES (:kod, :tur, :etiket, :tarih, :program_adi, :marka, :yayin_tarihi, :yayin_saat, :bayi_kodu, CAST(:kaynak AS jsonb), :olusturan)
           RETURNING id, kod""",
        {**k, "kaynak": json.dumps(k["kaynak"])},
    )


def kaydi_getir(kod: str) -> Optional[Dict[str, Any]]:
    r = execute_query("SELECT * FROM greydb.yayin_sayfalari WHERE kod = :kod", {"kod": kod})
    return r[0] if r else None


def kayitlari_listele() -> List[Dict[str, Any]]:
    return execute_query(
        """SELECT kod, tur, etiket, tarih, program_adi, yayin_tarihi, yayin_saat, durum, hata, olusturan, olusturma, guncelleme,
                  jsonb_array_length(maclar) AS mac_sayisi FROM greydb.yayin_sayfalari ORDER BY olusturma DESC LIMIT 100""", {})


def maclari_yaz(kod: str, maclar: List[Dict[str, Any]], durum: str, hata: Optional[str]) -> None:
    execute_insert(
        "UPDATE greydb.yayin_sayfalari SET maclar = CAST(:m AS jsonb), durum = :d, hata = :h, guncelleme = (now() AT TIME ZONE 'utc') WHERE kod = :kod",
        {"m": json.dumps(maclar, ensure_ascii=False), "d": durum, "h": hata, "kod": kod},
    )


def ilerleme_yaz(kod: str, biten: int) -> None:
    """Hazırlama ilerlemesi: kaynak JSON'unda 'biten' (işlenen maç sayısı) tutulur; şema değişikliği gerekmez."""
    execute_insert("UPDATE greydb.yayin_sayfalari SET kaynak = jsonb_set(kaynak, '{biten}', to_jsonb(CAST(:b AS integer))) WHERE kod = :kod", {"b": biten, "kod": kod})


def durumu_yaz(kod: str, durum: str) -> None:
    execute_insert("UPDATE greydb.yayin_sayfalari SET durum = :d, hata = NULL WHERE kod = :kod", {"d": durum, "kod": kod})


def ayari_yaz(kod: str, a: Dict[str, Any]) -> None:
    alanlar = ", ".join(f"{k} = :{k}" for k in a)
    execute_insert(f"UPDATE greydb.yayin_sayfalari SET {alanlar}, guncelleme = (now() AT TIME ZONE 'utc') WHERE kod = :kod", {**a, "kod": kod})


def sil(kod: str) -> None:
    execute_insert("DELETE FROM greydb.yayin_sayfalari WHERE kod = :kod", {"kod": kod})


def toto_maclari(hafta_no: int) -> List[Dict[str, Any]]:
    """Haftanın maçları. Spor Toto satırlarında FotMob maç kimliği çoğu zaman boştur: kendi eşlediğimiz takım kimlikleri (home/away_team_id) ve maç tarihiyle
    public.upcoming_matches / public.matches içinden bulunur (±3 gün, en yakın tarih). Takımı eşleşmemiş ya da maçı bulunamayan satırda None kalır."""
    return execute_query(
        """SELECT m.mac_no, m.ev_takim, m.deplasman_takim,
                  COALESCE(m.fotmob_match_id, (
                      SELECT q.fotmob_match_id FROM (
                          SELECT fotmob_match_id, match_date FROM public.upcoming_matches WHERE home_team_id = m.home_team_id AND away_team_id = m.away_team_id
                          UNION ALL
                          SELECT fotmob_match_id, match_date FROM public.matches WHERE home_team_id = m.home_team_id AND away_team_id = m.away_team_id
                      ) q
                      WHERE q.fotmob_match_id IS NOT NULL AND m.mac_tarihi IS NOT NULL AND abs(extract(epoch FROM (q.match_date - m.mac_tarihi))) < 3 * 86400
                      ORDER BY abs(extract(epoch FROM (q.match_date - m.mac_tarihi))) LIMIT 1
                  )) AS fotmob_match_id
           FROM greydb.spor_toto_matches m JOIN greydb.spor_toto_weeks w ON w.id = m.week_id WHERE w.hafta_no = :h ORDER BY m.mac_no""", {"h": hafta_no})


def toto_etiketi(hafta_no: int) -> str:
    """Açılışta görünen hafta adı: Spor Toto'nun kendi etiketi ("11. Hafta"). hafta_no oyun turu numarasıdır (1538), izleyiciye gösterilmez."""
    r = execute_query("SELECT hafta_etiketi FROM greydb.spor_toto_weeks WHERE hafta_no = :h", {"h": hafta_no})
    return (r[0]["hafta_etiketi"] if r and r[0]["hafta_etiketi"] else f"Hafta {hafta_no}")


def takim_logosu(fotmob_team_id: int) -> Optional[Dict[str, Any]]:
    r = execute_query("SELECT logo, logo_tur FROM greydb.yayin_takimlari WHERE fotmob_team_id = :f", {"f": fotmob_team_id})
    return r[0] if r else None


def renk_yaz(fotmob_team_id: int, renk: Optional[str]) -> bool:
    r = execute_insert("UPDATE greydb.yayin_takimlari SET renk_elle = :r, guncelleme = (now() AT TIME ZONE 'utc') WHERE fotmob_team_id = :f RETURNING fotmob_team_id",
                       {"r": renk, "f": fotmob_team_id})
    return bool(r)


# ---------------- Hazırlama ----------------
def _mac_kimlikleri(kaynak: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Kaynaktan sıralı [{sira, fotmob_id}] listesi. Spor Toto'da eşleşmemiş maç (FotMob kimliği yok) listede kalır, kartsız."""
    if kaynak["tur"] == "gun":
        return [{"sira": i + 1, "fotmob_id": mid} for i, mid in enumerate(kaynak["mac_idler"])]
    return [{"sira": m["mac_no"], "fotmob_id": m["fotmob_match_id"], "ev": m["ev_takim"], "dep": m["deplasman_takim"]} for m in toto_maclari(kaynak["hafta_no"])]


def hazirla_calistir(kod: str) -> None:
    """Arka plan işi: maçları toplayıp kaydeder. Hiçbir maç alınamazsa 'hata'; bazıları alınamazsa hazır + hata notu."""
    k = kaydi_getir(kod)
    if not k:
        return
    try:
        kaynak = k["kaynak"] if isinstance(k["kaynak"], dict) else json.loads(k["kaynak"])
        maclar, eksikler = [], []
        hedefler = _mac_kimlikleri(kaynak)
        for i, hedef in enumerate(hedefler):
            p = T.mac_hazirla(hedef["sira"], hedef["fotmob_id"], "/api/yayin/logo") if hedef.get("fotmob_id") else None
            if p:
                maclar.append(p)
            else:
                eksikler.append(f"{hedef['sira']}. maç" + (f" ({hedef['ev']} - {hedef['dep']})" if hedef.get("ev") else ""))
            try:
                ilerleme_yaz(kod, i + 1)
            except Exception:  # noqa: BLE001 - ilerleme bilgisi hazırlamayı asla düşürmez
                pass
        if not maclar:
            maclari_yaz(kod, [], "hata", "Hiçbir maçın verisi alınamadı")
            return
        maclari_yaz(kod, maclar, "hazir", ("Alınamayan maçlar: " + ", ".join(eksikler)) if eksikler else None)
    except Exception as e:  # noqa: BLE001
        maclari_yaz(kod, [], "hata", f"Hazırlama hatası: {e}"[:300])


def _ilerleme(k: Dict[str, Any], hazir_mac: int) -> Dict[str, int]:
    kaynak = k["kaynak"] if isinstance(k["kaynak"], dict) else json.loads(k["kaynak"] or "{}")
    toplam = kaynak.get("toplam") or hazir_mac
    return {"biten": toplam if k["durum"] != "hazirlaniyor" else min(kaynak.get("biten", 0), toplam), "toplam": toplam}


def _paket(k: Dict[str, Any], bugun: date) -> Dict[str, Any]:
    """Sitenin okuduğu sayfa paketi (apps/web/src/lib/yayin/tipler.ts YayinSayfasi)."""
    maclar = k["maclar"] if isinstance(k["maclar"], list) else json.loads(k["maclar"] or "[]")
    return {
        "kod": k["kod"], "tur": k["tur"], "etiket": k["etiket"], "tarih": k["tarih"].isoformat(), "programAdi": k["program_adi"], "marka": k["marka"],
        "yayinGun": yayin_gun_metni(k["yayin_tarihi"], bugun), "yayinSaat": k["yayin_saat"], "bayiKodu": k["bayi_kodu"],
        "durum": k["durum"], "hata": k["hata"], "maclar": maclar, "ilerleme": _ilerleme(k, len(maclar)),
    }


# ---------------- Uçlar ----------------
@router.post("/yayin/hazirla", status_code=202)
def hazirla(g: Hazirla, request: Request, arka: BackgroundTasks):
    kimlik = admin_gerek(request)
    if g.tur == "gun" and (not g.mac_idler or g.tarih is None):
        raise HTTPException(status_code=422, detail="Günün Maçları için maç seçimi ve tarih gerekli")
    if g.tur == "toto" and g.hafta_no is None:
        raise HTTPException(status_code=422, detail="Spor Toto için hafta gerekli")
    if g.tur == "toto" and not toto_maclari(g.hafta_no):
        raise HTTPException(status_code=404, detail="Spor Toto haftası bulunamadı")
    tarih = g.tarih or bugun_tsi()
    kayit = kaydi_olustur({
        "kod": _kod(), "tur": g.tur, "etiket": toto_etiketi(g.hafta_no) if g.tur == "toto" else etiket_gun(tarih), "tarih": tarih,
        "program_adi": g.program_adi or PROGRAM[g.tur], "marka": MARKA, "yayin_tarihi": g.yayin_tarihi, "yayin_saat": g.yayin_saat, "bayi_kodu": BAYI_KODU,
        "kaynak": {"tur": g.tur, "mac_idler": g.mac_idler, "hafta_no": g.hafta_no, "toplam": len(g.mac_idler) if g.tur == "gun" else len(toto_maclari(g.hafta_no)), "biten": 0},
        "olusturan": kimlik.email,
    })
    arka.add_task(hazirla_calistir, kayit["kod"])
    return {"kod": kayit["kod"], "durum": "hazirlaniyor"}


@router.get("/yayin")
def listele(request: Request):
    admin_gerek(request)
    b = bugun_tsi()
    return {"sayfalar": [{**k, "tarih": k["tarih"].isoformat(), "yayin_tarihi": k["yayin_tarihi"].isoformat(), "yayin_gun": yayin_gun_metni(k["yayin_tarihi"], b),
                          "olusturma": k["olusturma"].isoformat() + "Z", "guncelleme": k["guncelleme"].isoformat() + "Z"} for k in kayitlari_listele()]}


@router.get("/yayin/logo/{fotmob_team_id}")
def logo(fotmob_team_id: int):
    r = takim_logosu(fotmob_team_id)
    if not r or not r["logo"]:
        raise HTTPException(status_code=404, detail="Logo yok")
    return Response(content=bytes(r["logo"]), media_type=r["logo_tur"], headers={"Cache-Control": "public, max-age=31536000, immutable"})


@router.put("/yayin/takim/{fotmob_team_id}/renk")
def renk_sec(fotmob_team_id: int, g: Renk, request: Request):
    admin_gerek(request)
    if not renk_yaz(fotmob_team_id, g.renk.upper() if g.renk else None):
        raise HTTPException(status_code=404, detail="Takım bulunamadı")
    return {"ok": True}


@router.get("/yayin/{kod}")
def getir(kod: str):
    k = kaydi_getir(kod)
    if not k:
        raise HTTPException(status_code=404, detail="Yayın sayfası yok")
    return _paket(k, bugun_tsi())


@router.put("/yayin/{kod}/ayar")
def ayar(kod: str, g: Ayar, request: Request):
    admin_gerek(request)
    if not kaydi_getir(kod):
        raise HTTPException(status_code=404, detail="Yayın sayfası yok")
    degisen = {k: v for k, v in g.model_dump().items() if v is not None}
    if degisen:
        ayari_yaz(kod, degisen)
    return _paket(kaydi_getir(kod), bugun_tsi())


@router.put("/yayin/{kod}/eksik")
def eksik_sec(kod: str, g: EksikSecim, request: Request):
    """Kartta tutulacak oyuncuları işaretler (en fazla 3); işaretsiz takımda en değerli 3 görünür."""
    admin_gerek(request)
    k = kaydi_getir(kod)
    if not k:
        raise HTTPException(status_code=404, detail="Yayın sayfası yok")
    maclar = k["maclar"] if isinstance(k["maclar"], list) else json.loads(k["maclar"] or "[]")
    hedef = next((m for m in maclar if m["sira"] == g.mac_sira), None)
    if not hedef or "eksik" not in hedef["kartlar"]:
        raise HTTPException(status_code=404, detail="Maç ya da eksik verisi yok")
    for taraf, secili in (("ev", g.ev), ("dep", g.dep)):
        for o in hedef["kartlar"]["eksik"][taraf]:
            if secili:
                o["goster"] = o["ad"] in secili
            else:
                o.pop("goster", None)
    maclari_yaz(kod, maclar, k["durum"], k["hata"])
    return {"ok": True}


@router.post("/yayin/{kod}/yenile", status_code=202)
def yenile(kod: str, request: Request, arka: BackgroundTasks):
    admin_gerek(request)
    if not kaydi_getir(kod):
        raise HTTPException(status_code=404, detail="Yayın sayfası yok")
    durumu_yaz(kod, "hazirlaniyor")
    ilerleme_yaz(kod, 0)
    arka.add_task(hazirla_calistir, kod)
    return {"kod": kod, "durum": "hazirlaniyor"}


@router.delete("/yayin/{kod}")
def sil_(kod: str, request: Request):
    admin_gerek(request)
    if not kaydi_getir(kod):
        raise HTTPException(status_code=404, detail="Yayın sayfası yok")
    sil(kod)
    return {"ok": True}
