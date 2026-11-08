"""/api/skorjin-kuponlari: Skorjin kuponu verisi greydb'de durur. Yazma ve yönetici okuması yalnız admin (ağ geçidi anahtarı + X-Yetki: admin);
yayındaki kuponlar yalnız ağ geçidine (Next sunucusu) açılır: ham olasılık (bizimP) herkese açık uçta ASLA yoktur. Veritabanı sahte."""
import copy

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import skorjin_kuponlari as r

client = TestClient(app)
GW = {"X-Sunucu-Anahtari": "gizli"}
ADMIN = {**GW, "X-Yetki": "admin", "X-Kullanici-Email": "editor@greysports.tv"}

BACAK = {"macId": 1, "ev": "Ajax", "dep": "PSV", "lig": "Eredivisie", "macZamani": "2026-11-08T17:00:00Z", "marketId": 48,
         "marketAd": "Toplam Korner Alt/Üst 9.5", "secim": "Üst", "oran": 1.8, "bizimP": 0.72}
GIRDI = {"baslik": "Hafta sonu okuması", "seviye": "bayi", "durum": "taslak", "yorum": None, "bacaklar": [BACAK]}


class Sahte:
    def __init__(self):
        self.satirlar, self.sayac = {}, 0

    def ekle(self, k, email, yayin):
        self.sayac += 1
        self.satirlar[self.sayac] = {**copy.deepcopy(k), "id": str(self.sayac), "olusturan_email": email, "yayin_at": "2026-11-07T09:00:00Z" if yayin else None,
                                     "olusturma": "2026-11-07T08:00:00Z", "guncelleme": "2026-11-07T08:00:00Z"}
        return copy.deepcopy(self.satirlar[self.sayac])

    def getir(self, kid):
        return copy.deepcopy(self.satirlar.get(kid))

    def liste(self, yalniz_yayinda):
        return [copy.deepcopy(k) for k in self.satirlar.values() if not yalniz_yayinda or k["durum"] == "yayinda"]

    def guncelle(self, kid, k, yayin):
        if kid not in self.satirlar:
            return None
        onceki = self.satirlar[kid]
        yayin_at = onceki["yayin_at"] if not yayin else "2026-11-07T10:00:00Z"
        self.satirlar[kid] = {**onceki, **copy.deepcopy(k), "yayin_at": yayin_at}
        return copy.deepcopy(self.satirlar[kid])

    def sil(self, kid):
        return self.satirlar.pop(kid, None) is not None


@pytest.fixture
def db(monkeypatch):
    s = Sahte()
    monkeypatch.setattr("app.routers.kuponlar._anahtar", lambda: "gizli")
    for ad, f in {"db_ekle": s.ekle, "db_getir": s.getir, "db_liste": s.liste, "db_guncelle": s.guncelle, "db_sil": s.sil}.items():
        monkeypatch.setattr(r, ad, f)
    monkeypatch.setattr(r, "takim_kimlikleri", lambda q, b: (None, None))      # gerçek veritabanına gitmesin
    r._TAKIM_ONBELLEK.clear()
    return s


# ---- Yetki
@pytest.mark.parametrize("yontem,yol", [("get", "/api/skorjin-kuponlari"), ("post", "/api/skorjin-kuponlari"), ("get", "/api/skorjin-kuponlari/1"),
                                       ("put", "/api/skorjin-kuponlari/1"), ("delete", "/api/skorjin-kuponlari/1")])
@pytest.mark.parametrize("basliklar,kod", [({}, 401), ({"X-Sunucu-Anahtari": "yanlis", "X-Yetki": "admin"}, 401), (GW, 403), ({**GW, "X-Yetki": "uye"}, 403)])
def test_yonetici_uclari_yetkisiz_reddedilir(db, yontem, yol, basliklar, kod):
    kw = {"json": GIRDI} if yontem in ("post", "put") else {}
    assert getattr(client, yontem)(yol, headers=basliklar, **kw).status_code == kod
    assert db.satirlar == {}


def test_anahtar_tanimli_degilse_yazma_kapali(db, monkeypatch):
    monkeypatch.setattr("app.routers.kuponlar._anahtar", lambda: None)
    assert client.post("/api/skorjin-kuponlari", json=GIRDI, headers=ADMIN).status_code == 503


# ---- Yayındaki liste: yalnız ağ geçidi
def test_yayinda_ucu_anahtarsiz_acilmaz_ve_taslak_donmez(db):
    client.post("/api/skorjin-kuponlari", json=GIRDI, headers=ADMIN)
    client.post("/api/skorjin-kuponlari", json={**GIRDI, "durum": "yayinda"}, headers=ADMIN)
    assert client.get("/api/skorjin-kuponlari/yayinda").status_code == 401
    assert client.get("/api/skorjin-kuponlari/yayinda", headers={"X-Sunucu-Anahtari": "yanlis"}).status_code == 401
    j = client.get("/api/skorjin-kuponlari/yayinda", headers=GW)
    assert j.status_code == 200
    assert [k["durum"] for k in j.json()["kuponlar"]] == ["yayinda"]


# ---- Oluşturma
def test_admin_olusturur_id_metin_yayin_zamani_yalniz_yayindaysa(db):
    j = client.post("/api/skorjin-kuponlari", json=GIRDI, headers=ADMIN)
    assert j.status_code == 201
    k = j.json()
    assert k["id"] == "1" and k["olusturan_email"] == "editor@greysports.tv" and k["yayin_at"] is None
    assert k["bacaklar"][0]["bizimP"] == 0.72
    y = client.post("/api/skorjin-kuponlari", json={**GIRDI, "durum": "yayinda"}, headers=ADMIN).json()
    assert y["yayin_at"] is not None


def test_eksik_kullanici_epostasi_400(db):
    assert client.post("/api/skorjin-kuponlari", json=GIRDI, headers={**GW, "X-Yetki": "admin"}).status_code == 400


@pytest.mark.parametrize("degisiklik,alan", [
    ({"seviye": "gri"}, "seviye"), ({"durum": "x"}, "durum"), ({"baslik": "ab"}, "baslik"), ({"baslik": "x" * 81}, "baslik"),
    ({"yorum": "x" * 501}, "yorum"), ({"bacaklar": []}, "bacaklar"), ({"bacaklar": [BACAK] * 2}, "bacaklar"),
    ({"bacaklar": [{**BACAK, "oran": 0}]}, "bacaklar"), ({"bacaklar": [{**BACAK, "bizimP": 1.2}]}, "bacaklar"), ({"bacaklar": [{**BACAK, "bizimP": 0}]}, "bacaklar"),
    ({"bacaklar": [{**BACAK, "secim": " "}]}, "bacaklar"),
])
def test_gecersiz_girdi_422(db, degisiklik, alan):
    j = client.post("/api/skorjin-kuponlari", json={**GIRDI, **degisiklik}, headers=ADMIN)
    assert j.status_code == 422, j.text
    assert db.satirlar == {}


def test_en_fazla_12_maç(db):
    bacaklar = [{**BACAK, "macId": i} for i in range(1, 14)]
    assert client.post("/api/skorjin-kuponlari", json={**GIRDI, "bacaklar": bacaklar}, headers=ADMIN).status_code == 422
    assert client.post("/api/skorjin-kuponlari", json={**GIRDI, "bacaklar": bacaklar[:12]}, headers=ADMIN).status_code == 201


def test_bilinmeyen_alan_reddedilir(db):
    assert client.post("/api/skorjin-kuponlari", json={**GIRDI, "olusturan_email": "x@y.z"}, headers=ADMIN).status_code == 422
    assert client.post("/api/skorjin-kuponlari", json={**GIRDI, "bacaklar": [{**BACAK, "gizli": 1}]}, headers=ADMIN).status_code == 422


# ---- Okuma, güncelleme, silme
def test_admin_listeler_ve_getirir(db):
    client.post("/api/skorjin-kuponlari", json=GIRDI, headers=ADMIN)
    assert [k["id"] for k in client.get("/api/skorjin-kuponlari", headers=ADMIN).json()["kuponlar"]] == ["1"]
    assert client.get("/api/skorjin-kuponlari/1", headers=ADMIN).json()["baslik"] == "Hafta sonu okuması"
    assert client.get("/api/skorjin-kuponlari/9", headers=ADMIN).status_code == 404


def test_put_tum_alanlari_degistirir_yayin_zamani_ilk_yayinda_yazilir(db):
    client.post("/api/skorjin-kuponlari", json=GIRDI, headers=ADMIN)
    j = client.put("/api/skorjin-kuponlari/1", json={**GIRDI, "baslik": "Yeni başlık", "durum": "yayinda"}, headers=ADMIN)
    assert j.status_code == 200 and j.json()["baslik"] == "Yeni başlık" and j.json()["yayin_at"] == "2026-11-07T10:00:00Z"
    # zaten yayındaki kuponun yayın zamanı değişmez
    j2 = client.put("/api/skorjin-kuponlari/1", json={**GIRDI, "baslik": "Üçüncü", "durum": "yayinda"}, headers=ADMIN)
    assert j2.json()["yayin_at"] == "2026-11-07T10:00:00Z"
    assert client.put("/api/skorjin-kuponlari/9", json=GIRDI, headers=ADMIN).status_code == 404
    assert client.put("/api/skorjin-kuponlari/1", json={**GIRDI, "seviye": "gri"}, headers=ADMIN).status_code == 422


def test_sil(db):
    client.post("/api/skorjin-kuponlari", json=GIRDI, headers=ADMIN)
    assert client.delete("/api/skorjin-kuponlari/1", headers=ADMIN).status_code == 200
    assert db.satirlar == {}
    assert client.delete("/api/skorjin-kuponlari/1", headers=ADMIN).status_code == 404


# ---- Öne çıkarma
def test_one_cikan_varsayilan_kapali_acilip_kapanir_ve_yayindaki_listede_gelir(db):
    k = client.post("/api/skorjin-kuponlari", json={**GIRDI, "durum": "yayinda"}, headers=ADMIN).json()
    assert k["one_cikan"] is False
    j = client.put("/api/skorjin-kuponlari/1", json={**GIRDI, "durum": "yayinda", "one_cikan": True}, headers=ADMIN).json()
    assert j["one_cikan"] is True
    assert client.get("/api/skorjin-kuponlari/yayinda", headers=GW).json()["kuponlar"][0]["one_cikan"] is True
    assert client.put("/api/skorjin-kuponlari/1", json={**GIRDI, "durum": "yayinda"}, headers=ADMIN).json()["one_cikan"] is False   # tam değiştirme: gönderilmezse kapanır


def test_one_cikan_yalniz_mantiksal_deger(db):
    assert client.post("/api/skorjin-kuponlari", json={**GIRDI, "one_cikan": "belki"}, headers=ADMIN).status_code == 422


def test_satir_one_cikan_alanini_tasir():
    from datetime import datetime
    s = {"id": 3, "baslik": "x", "seviye": "acik", "durum": "yayinda", "yorum": None, "bacaklar": "[]", "olusturan_email": "a@b.c", "one_cikan": True,
         "yayin_zamani": None, "olusturma": datetime(2026, 11, 7), "guncelleme": datetime(2026, 11, 7)}
    assert r._satir(s)["one_cikan"] is True


def test_takim_ekle_bulunan_bacaga_kimlik_koyar_bulunamayana_koymaz(monkeypatch):
    r._TAKIM_ONBELLEK.clear()
    monkeypatch.setattr(r, "takim_kimlikleri", lambda q, b: (10228, 10235) if b["ev"] == "SC Heerenveen" else (None, None))
    k = [{"bacaklar": [{**BACAK, "ev": "SC Heerenveen", "dep": "Feyenoord"}, {**BACAK, "ev": "Yok", "dep": "Yok2"}]}]
    b = r.takim_ekle(k)[0]["bacaklar"]
    assert (b[0]["evTakimId"], b[0]["depTakimId"]) == (10228, 10235)
    assert "evTakimId" not in b[1]


def test_takim_ekle_hata_kuponu_bozmaz(monkeypatch):
    r._TAKIM_ONBELLEK.clear()
    def boz(q, b): raise RuntimeError("db")
    monkeypatch.setattr(r, "takim_kimlikleri", boz)
    assert "evTakimId" not in r.takim_ekle([{"bacaklar": [dict(BACAK)]}])[0]["bacaklar"][0]
