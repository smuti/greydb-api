"""Kuponlar: tek model (eski tahmin ve eski kupon yapılarının yerine). 1+ maç (bacak), kombine ya da sistem k/n, seviye Açık/Bayi/Elit.

Kimlik greydb-api'de doğrulanmaz: Supabase oturumunu bilen Next.js sunucusu GÜVENİLİR AĞ GEÇİDİdir ve X-Sunucu-Anahtari (ortam: SUNUCU_ANAHTARI)
ile tanıtır; üyenin seviyesini X-Uye-Seviyesi, admin yetkisini X-Yetki: admin, e-postayı X-Kullanici-Email başlığıyla bildirir.
Anahtarsız ya da yanlış anahtarlı istek anonimdir (yalnız Açık kuponlar açık görünür). Anahtar tanımlı değilse yazma kapalıdır (503).
"""
import hmac
import json
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict

from app.services.db import execute_insert, execute_query, transaction
from app.services.kupon_model import KuponHatasi, SEVIYELER, dogrula_olustur, gorunur_mu, kilitle, kupon_ozeti

router = APIRouter(tags=["kuponlar"])


def _anahtar() -> Optional[str]:
    return os.environ.get("SUNUCU_ANAHTARI") or None


class Kimlik:
    def __init__(self, ag_gecidi: bool, admin: bool, seviye: Optional[str], email: Optional[str]):
        self.ag_gecidi, self.admin, self.seviye, self.email = ag_gecidi, admin, seviye, email

    @property
    def etkin_seviye(self) -> Optional[str]:
        return "elit" if self.admin else self.seviye


def kimlik(request: Request) -> Kimlik:
    anahtar = _anahtar()
    gelen = request.headers.get("x-sunucu-anahtari", "")
    gecit = bool(anahtar) and bool(gelen) and hmac.compare_digest(gelen.encode(), anahtar.encode())
    if not gecit:
        return Kimlik(False, False, None, None)
    seviye = request.headers.get("x-uye-seviyesi")
    return Kimlik(True, request.headers.get("x-yetki") == "admin", seviye if seviye in SEVIYELER else None, request.headers.get("x-kullanici-email"))


def admin_gerek(request: Request) -> Kimlik:
    if not _anahtar():
        raise HTTPException(status_code=503, detail="Yazma kapalı: sunucuda SUNUCU_ANAHTARI tanımlı değil")
    k = kimlik(request)
    if not k.ag_gecidi:
        raise HTTPException(status_code=401, detail="Geçersiz ya da eksik sunucu anahtarı")
    if not k.admin:
        raise HTTPException(status_code=403, detail="Admin yetkisi gerekli")
    return k


# ---------------- Veritabanı katmanı (testlerde değiştirilir) ----------------
def _bacak_satiri(b: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": b["id"], "sira": b["sira"], "iddaa_event_id": b["iddaa_event_id"], "fotmob_match_id": b["fotmob_match_id"], "ev": b["ev"], "dep": b["dep"],
        "lig": b["lig"], "mac_zamani": b["mac_zamani"].isoformat() + "Z", "bahis_turu": b["bahis_turu"], "secim": b["secim"], "oran": float(b["oran"]),
        "sonuc": b["sonuc"], "sonuc_kaynagi": b["sonuc_kaynagi"],
    }


def _kupon_satiri(k: Dict[str, Any], bacaklar: List[Dict[str, Any]]) -> Dict[str, Any]:
    f = lambda v: None if v is None else float(v)   # noqa: E731
    z = lambda v: None if v is None else v.isoformat() + "Z"   # noqa: E731
    return {
        "id": k["id"], "seviye": k["seviye"], "durum": k["durum"], "zamanlama": k["zamanlama"], "one_cikan": k["one_cikan"], "sistem_k": k["sistem_k"],
        "nedenler": k["nedenler"] or [], "ana_risk": k["ana_risk"], "yorum": k["yorum"], "sinyal": k["sinyal"], "risk_seviyesi": k["risk_seviyesi"],
        "toplam_oran": f(k["toplam_oran"]), "en_yuksek_odeme": f(k["en_yuksek_odeme"]), "kombinasyon_sayisi": k["kombinasyon_sayisi"],
        "sonuc": k["sonuc"], "odeme": f(k["odeme"]), "sonuc_zamani": z(k["sonuc_zamani"]), "yayin_zamani": z(k["yayin_zamani"]),
        "olusturan_email": k["olusturan_email"], "olusturma": z(k["olusturma"]), "guncelleme": z(k["guncelleme"]),
        "bacaklar": sorted([_bacak_satiri(b) for b in bacaklar], key=lambda b: (b["sira"], b["id"])),
    }


def _bacaklari_yaz(kupon_id: int, bacaklar: List[Dict[str, Any]]) -> None:
    for i, b in enumerate(bacaklar):
        execute_insert(
            """INSERT INTO greydb.kupon_bacaklari (kupon_id, sira, iddaa_event_id, fotmob_match_id, ev, dep, lig, mac_zamani, bahis_turu, secim, oran, sonuc)
               VALUES (:kid, :sira, :iddaa, :fotmob, :ev, :dep, :lig, CAST(:zaman AS timestamptz) AT TIME ZONE 'utc', :tur, :secim, :oran, :sonuc)""",
            {"kid": kupon_id, "sira": i, "iddaa": b["iddaa_event_id"], "fotmob": b["fotmob_match_id"], "ev": b["ev"], "dep": b["dep"], "lig": b["lig"],
             "zaman": b["mac_zamani"], "tur": b["bahis_turu"], "secim": b["secim"], "oran": b["oran"], "sonuc": b["sonuc"]},
        )


def db_getir(kupon_id: int) -> Optional[Dict[str, Any]]:
    satir = execute_query("SELECT * FROM greydb.kuponlar WHERE id = :id", {"id": kupon_id})
    if not satir:
        return None
    return _kupon_satiri(satir[0], execute_query("SELECT * FROM greydb.kupon_bacaklari WHERE kupon_id = :id", {"id": kupon_id}))


def db_ekle(k: Dict[str, Any], email: str) -> Dict[str, Any]:
    with transaction():
        yayin = "now() AT TIME ZONE 'utc'" if k["durum"] == "yayinda" else "NULL"
        satir = execute_insert(
            f"""INSERT INTO greydb.kuponlar (seviye, durum, zamanlama, one_cikan, sistem_k, nedenler, ana_risk, yorum, sinyal, risk_seviyesi,
                    toplam_oran, en_yuksek_odeme, kombinasyon_sayisi, olusturan_email, yayin_zamani)
                VALUES (:seviye, :durum, :zamanlama, :one_cikan, :sistem_k, CAST(:nedenler AS jsonb), :ana_risk, :yorum, :sinyal, :risk,
                    :toplam, :odeme, :komb, :email, {yayin}) RETURNING id""",
            {"seviye": k["seviye"], "durum": k["durum"], "zamanlama": k["zamanlama"], "one_cikan": k["one_cikan"], "sistem_k": k["sistem_k"],
             "nedenler": json.dumps(k["nedenler"]), "ana_risk": k["ana_risk"], "yorum": k["yorum"], "sinyal": k["sinyal"], "risk": k["risk_seviyesi"],
             "toplam": k["toplam_oran"], "odeme": k["en_yuksek_odeme"], "komb": k["kombinasyon_sayisi"], "email": email},
        )
        _bacaklari_yaz(satir["id"], k["bacaklar"])
    return db_getir(satir["id"])


def db_liste(filtre: Dict[str, Any]):
    kosul, p = ["1=1"], {"limit": filtre.get("limit", 20), "offset": filtre.get("offset", 0)}
    if filtre.get("durum"):
        kosul.append("durum = :durum"); p["durum"] = filtre["durum"]
    if filtre.get("seviye"):
        kosul.append("seviye = :seviye"); p["seviye"] = filtre["seviye"]
    if filtre.get("one_cikan") is not None:
        kosul.append("one_cikan = :one"); p["one"] = filtre["one_cikan"]
    if filtre.get("yazar"):
        kosul.append("lower(olusturan_email) = :yazar"); p["yazar"] = filtre["yazar"]
    if filtre.get("sonuclanmis") is True:
        kosul.append("sonuc IS NOT NULL")
    elif filtre.get("sonuclanmis") is False:
        kosul.append("sonuc IS NULL")
    nerede = " AND ".join(kosul)
    toplam = execute_query(f"SELECT count(*) AS n FROM greydb.kuponlar WHERE {nerede}", {k: v for k, v in p.items() if k not in ("limit", "offset")})[0]["n"]
    satirlar = execute_query(
        f"SELECT * FROM greydb.kuponlar WHERE {nerede} ORDER BY COALESCE(yayin_zamani, olusturma) DESC, id DESC LIMIT :limit OFFSET :offset", p)
    if not satirlar:
        return [], toplam
    bac = execute_query("SELECT * FROM greydb.kupon_bacaklari WHERE kupon_id = ANY(:ids)", {"ids": [s["id"] for s in satirlar]})
    return [_kupon_satiri(s, [b for b in bac if b["kupon_id"] == s["id"]]) for s in satirlar], toplam


def db_guncelle(kupon_id: int, k: Dict[str, Any]) -> Dict[str, Any]:
    with transaction():
        execute_insert(
            """UPDATE greydb.kuponlar SET seviye=:seviye, durum=:durum, zamanlama=:zamanlama, one_cikan=:one_cikan, sistem_k=:sistem_k,
                   nedenler=CAST(:nedenler AS jsonb), ana_risk=:ana_risk, yorum=:yorum, sinyal=:sinyal, risk_seviyesi=:risk,
                   toplam_oran=:toplam, en_yuksek_odeme=:odeme, kombinasyon_sayisi=:komb, guncelleme=(now() AT TIME ZONE 'utc'),
                   yayin_zamani = CASE WHEN :durum = 'yayinda' AND yayin_zamani IS NULL THEN (now() AT TIME ZONE 'utc') ELSE yayin_zamani END
               WHERE id=:id""",
            {"id": kupon_id, "seviye": k["seviye"], "durum": k["durum"], "zamanlama": k["zamanlama"], "one_cikan": k["one_cikan"], "sistem_k": k["sistem_k"],
             "nedenler": json.dumps(k["nedenler"]), "ana_risk": k["ana_risk"], "yorum": k["yorum"], "sinyal": k["sinyal"], "risk": k["risk_seviyesi"],
             "toplam": k["toplam_oran"], "odeme": k["en_yuksek_odeme"], "komb": k["kombinasyon_sayisi"]},
        )
        execute_insert("DELETE FROM greydb.kupon_bacaklari WHERE kupon_id = :id", {"id": kupon_id})
        _bacaklari_yaz(kupon_id, k["bacaklar"])
        _sonucu_yeniden_hesapla(kupon_id)
    return db_getir(kupon_id)


def db_sil(kupon_id: int) -> bool:
    return execute_insert("DELETE FROM greydb.kuponlar WHERE id = :id RETURNING id", {"id": kupon_id}) is not None


def _sonucu_yeniden_hesapla(kupon_id: int) -> None:
    k = execute_query("SELECT sistem_k FROM greydb.kuponlar WHERE id = :id", {"id": kupon_id})[0]
    bac = [{"oran": float(b["oran"]), "sonuc": b["sonuc"]} for b in execute_query("SELECT oran, sonuc FROM greydb.kupon_bacaklari WHERE kupon_id = :id", {"id": kupon_id})]
    o = kupon_ozeti(bac, k["sistem_k"]) if bac else {"sonuc": "pending", "odeme": None}
    belli = o["sonuc"] in ("won", "lost", "void")
    execute_insert(
        "UPDATE greydb.kuponlar SET sonuc = :s, odeme = :o, sonuc_zamani = CASE WHEN :s IS NULL THEN NULL ELSE COALESCE(sonuc_zamani, now() AT TIME ZONE 'utc') END, "
        "guncelleme = (now() AT TIME ZONE 'utc') WHERE id = :id",
        {"id": kupon_id, "s": o["sonuc"] if belli else None, "o": o.get("odeme") if belli else None},
    )


def db_bacak_sonucu(kupon_id: int, bacak_id: int, sonuc: str, kaynak: str = "elle") -> Optional[Dict[str, Any]]:
    with transaction():
        s = execute_insert(
            "UPDATE greydb.kupon_bacaklari SET sonuc = :s, sonuc_kaynagi = :k, sonuc_zamani = CASE WHEN :s = 'pending' THEN NULL ELSE (now() AT TIME ZONE 'utc') END "
            "WHERE id = :b AND kupon_id = :id RETURNING id",
            {"s": sonuc, "k": None if sonuc == "pending" else kaynak, "b": bacak_id, "id": kupon_id},
        )
        if s is None:
            return None
        _sonucu_yeniden_hesapla(kupon_id)
    return db_getir(kupon_id)


# ---------------- Uçlar ----------------
class KuponGirdisi(BaseModel):
    model_config = ConfigDict(extra="allow")


class BacakSonucu(BaseModel):
    sonuc: str


def _cikti(kupon: Dict[str, Any], k: "Kimlik") -> Dict[str, Any]:
    """Seviyeye göre kilitle; editör e-postası yalnız admin'e (gizlilik)."""
    g = kilitle(kupon, k.etkin_seviye)
    if not k.admin:
        g.pop("olusturan_email", None)
    return g


def _hata(e: KuponHatasi):
    return HTTPException(status_code=422, detail={"alan": e.alan, "mesaj": e.mesaj})


@router.post("/kuponlar", status_code=201)
async def kupon_olustur(govde: KuponGirdisi, request: Request):
    k = admin_gerek(request)
    if not k.email:
        raise HTTPException(status_code=400, detail="X-Kullanici-Email gerekli")
    try:
        temiz = dogrula_olustur(govde.model_dump(), siki=True)
    except KuponHatasi as e:
        raise _hata(e)
    return _cikti(db_ekle(temiz, k.email), k)


@router.get("/kuponlar")
async def kupon_listele(request: Request, durum: Optional[str] = None, seviye: Optional[str] = None, one_cikan: Optional[bool] = None,
                        sonuclanmis: Optional[bool] = None, sayfa: int = Query(1, ge=1), sayfa_boyutu: int = Query(20, ge=1, le=100)):
    k = kimlik(request)
    filtre = {"durum": durum if k.admin else "yayinda", "seviye": seviye if seviye in SEVIYELER else None, "one_cikan": one_cikan,
              "sonuclanmis": sonuclanmis, "limit": sayfa_boyutu, "offset": (sayfa - 1) * sayfa_boyutu}
    items, toplam = db_liste(filtre)
    return {"items": [_cikti(i, k) for i in items if k.admin or gorunur_mu(i, k.seviye)], "total": toplam, "sayfa": sayfa, "sayfa_boyutu": sayfa_boyutu}


@router.get("/kuponlar/{kupon_id}")
async def kupon_getir(kupon_id: int, request: Request):
    k = kimlik(request)
    kupon = db_getir(kupon_id)
    if kupon is None or not (k.admin or gorunur_mu(kupon, k.seviye)):
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    return _cikti(kupon, k)


@router.put("/kuponlar/{kupon_id}")
async def kupon_guncelle(kupon_id: int, govde: KuponGirdisi, request: Request):
    yazar = admin_gerek(request)
    mevcut = db_getir(kupon_id)
    if mevcut is None:
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    girdi = govde.model_dump()
    tam = "bacaklar" in girdi          # tam güncelleme yayın zorunluluklarına tabidir; yalnız üst bilgi (ör. durum) değişikliği değil
    if not tam:        # yalnız üst bilgi: mevcut bacaklar (sonuçlarıyla) korunur
        girdi = {**{a: mevcut[a] for a in ("seviye", "durum", "zamanlama", "one_cikan", "sistem_k", "nedenler", "ana_risk", "yorum", "sinyal", "risk_seviyesi")},
                 **girdi, "bacaklar": mevcut["bacaklar"]}
    elif any(b["sonuc"] != "pending" for b in mevcut["bacaklar"]):
        raise HTTPException(status_code=409, detail="Sonuçlanmış bacağı olan kuponun bacakları değiştirilemez")
    try:
        temiz = dogrula_olustur(girdi, siki=tam)
    except KuponHatasi as e:
        raise _hata(e)
    return _cikti(db_guncelle(kupon_id, temiz), yazar)


@router.delete("/kuponlar/{kupon_id}")
async def kupon_sil(kupon_id: int, request: Request):
    admin_gerek(request)
    if not db_sil(kupon_id):
        raise HTTPException(status_code=404, detail="Kupon bulunamadı")
    return {"silindi": kupon_id}


@router.post("/kuponlar/{kupon_id}/bacaklar/{bacak_id}/sonuc")
async def bacak_sonucu_isaretle(kupon_id: int, bacak_id: int, govde: BacakSonucu, request: Request):
    yazar = admin_gerek(request)
    if govde.sonuc not in ("pending", "won", "lost", "void"):
        raise HTTPException(status_code=422, detail={"alan": "sonuc", "mesaj": "pending, won, lost ya da void olmalı"})
    kupon = db_bacak_sonucu(kupon_id, bacak_id, govde.sonuc)
    if kupon is None:
        raise HTTPException(status_code=404, detail="Kupon ya da bacak bulunamadı")
    return _cikti(kupon, yazar)
