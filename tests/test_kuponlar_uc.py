"""/api/kuponlar: yazma yalnız admin (güvenilir ağ geçidi anahtarı + admin yetkisi), okuma seviyeye göre kilitli. Veritabanı sahte."""
import copy

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import kuponlar as r

client = TestClient(app)
GW = {"X-Sunucu-Anahtari": "gizli"}
ADMIN = {**GW, "X-Yetki": "admin", "X-Kullanici-Email": "editor@greysports.tv"}


def uye(seviye):
    return {**GW, "X-Uye-Seviyesi": seviye}


GIRDI = {"seviye": "elit", "durum": "yayinda", "nedenler": ["Form iyi"], "ana_risk": "Erken gol",
         "bacaklar": [{"ev": "Galatasaray", "dep": "Fenerbahçe", "lig": "Süper Lig", "mac_zamani": "2026-10-25T17:00:00Z",
                       "bahis_turu": "Maç Sonucu", "secim": "1", "oran": 1.8, "iddaa_event_id": 123}]}


class Sahte:
    def __init__(self):
        self.kuponlar, self.sayac = {}, 0

    def ekle(self, k, email):
        self.sayac += 1
        kayit = {**copy.deepcopy(k), "id": self.sayac, "sonuc": None, "odeme": None, "olusturan_email": email}
        for i, b in enumerate(kayit["bacaklar"], 1):
            b["id"] = self.sayac * 100 + i
        self.kuponlar[self.sayac] = kayit
        return copy.deepcopy(kayit)

    def getir(self, kid):
        return copy.deepcopy(self.kuponlar.get(kid))

    def liste(self, filtre):
        items = [copy.deepcopy(k) for k in self.kuponlar.values()]
        if filtre.get("durum"):
            items = [k for k in items if k["durum"] == filtre["durum"]]
        if filtre.get("seviye"):
            items = [k for k in items if k["seviye"] == filtre["seviye"]]
        if filtre.get("one_cikan") is not None:
            items = [k for k in items if k["one_cikan"] == filtre["one_cikan"]]
        return items, len(items)

    def guncelle(self, kid, k):
        self.kuponlar[kid] = {**self.kuponlar[kid], **copy.deepcopy(k)}
        return copy.deepcopy(self.kuponlar[kid])

    def sil(self, kid):
        return self.kuponlar.pop(kid, None) is not None

    def bacak_sonucu(self, kid, bid, sonuc):
        k = self.kuponlar.get(kid)
        if not k:
            return None
        for b in k["bacaklar"]:
            if b["id"] == bid:
                b["sonuc"] = sonuc
                return copy.deepcopy(k)
        return None


@pytest.fixture
def db(monkeypatch):
    s = Sahte()
    monkeypatch.setattr(r, "_anahtar", lambda: "gizli")
    for ad, f in {"db_ekle": s.ekle, "db_getir": s.getir, "db_liste": s.liste, "db_guncelle": s.guncelle, "db_sil": s.sil, "db_bacak_sonucu": s.bacak_sonucu}.items():
        monkeypatch.setattr(r, ad, f)
    return s


# ---- Yazma yetkisi ----
def test_anahtar_tanimli_degilse_yazma_kapali(db, monkeypatch):
    monkeypatch.setattr(r, "_anahtar", lambda: None)
    assert client.post("/api/kuponlar", json=GIRDI, headers=ADMIN).status_code == 503


@pytest.mark.parametrize("basliklar,kod", [({}, 401), ({"X-Sunucu-Anahtari": "yanlis", "X-Yetki": "admin"}, 401), (GW, 403), ({**GW, "X-Yetki": "uye"}, 403)])
def test_yazma_yetkisiz_reddedilir(db, basliklar, kod):
    assert client.post("/api/kuponlar", json=GIRDI, headers=basliklar).status_code == kod
    assert db.kuponlar == {}


def test_admin_kupon_olusturur_oran_sunucuda(db):
    j = client.post("/api/kuponlar", json={**GIRDI, "toplam_oran": 99}, headers=ADMIN)
    assert j.status_code == 201
    k = j.json()
    assert k["toplam_oran"] == 1.8 and k["olusturan_email"] == "editor@greysports.tv" and k["kilitli"] is False


def test_gecersiz_girdi_422_alanla(db):
    j = client.post("/api/kuponlar", json={**GIRDI, "seviye": "gri"}, headers=ADMIN)
    assert j.status_code == 422 and j.json()["detail"]["alan"] == "seviye"


def test_eksik_kullanici_epostasi_400(db):
    assert client.post("/api/kuponlar", json=GIRDI, headers={**GW, "X-Yetki": "admin"}).status_code == 400


# ---- Okuma ve kilit ----
def hazir(db):
    for seviye, durum in (("acik", "yayinda"), ("bayi", "yayinda"), ("elit", "yayinda"), ("elit", "taslak")):
        client.post("/api/kuponlar", json={**GIRDI, "seviye": seviye, "durum": durum, "bacaklar": [{**GIRDI["bacaklar"][0], "iddaa_event_id": len(db.kuponlar) + 1}]}, headers=ADMIN)


def test_anonim_liste_taslaksiz_ve_kilitli(db):
    hazir(db)
    j = client.get("/api/kuponlar").json()
    assert j["total"] == 3
    acik = next(k for k in j["items"] if k["seviye"] == "acik")
    assert acik["kilitli"] is False and acik["bacaklar"][0]["secim"] == "1"
    for k in j["items"]:
        if k["seviye"] != "acik":
            assert k["kilitli"] is True and k["bacaklar"][0]["secim"] is None and k["nedenler"] is None and k["ana_risk"] is None


def test_seviyeye_gore_acilir_birikimli(db):
    hazir(db)
    bayi = {k["seviye"]: k["kilitli"] for k in client.get("/api/kuponlar", headers=uye("bayi")).json()["items"]}
    assert bayi == {"acik": False, "bayi": False, "elit": True}
    elit = {k["seviye"]: k["kilitli"] for k in client.get("/api/kuponlar", headers=uye("elit")).json()["items"]}
    assert elit == {"acik": False, "bayi": False, "elit": False}


def test_seviye_basligi_anahtarsiz_gecmez(db):
    hazir(db)
    sahte = client.get("/api/kuponlar", headers={"X-Uye-Seviyesi": "elit"}).json()["items"]          # anahtar yok: yok sayılır
    assert all(k["kilitli"] for k in sahte if k["seviye"] != "acik")
    yanlis = client.get("/api/kuponlar", headers={"X-Sunucu-Anahtari": "yanlis", "X-Uye-Seviyesi": "elit"}).json()["items"]
    assert all(k["kilitli"] for k in yanlis if k["seviye"] != "acik")


def test_admin_taslagi_gorur_ve_hepsi_acik(db):
    hazir(db)
    j = client.get("/api/kuponlar?durum=taslak", headers=ADMIN).json()
    assert j["total"] == 1 and j["items"][0]["kilitli"] is False
    tum = client.get("/api/kuponlar", headers=ADMIN).json()
    assert tum["total"] == 4


def test_tekil_kupon_taslak_anonimden_404_kilitli_gorunum(db):
    hazir(db)
    assert client.get("/api/kuponlar/4").status_code == 404                 # taslak
    assert client.get("/api/kuponlar/4", headers=ADMIN).status_code == 200
    k = client.get("/api/kuponlar/3").json()
    assert k["kilitli"] is True and k["bacaklar"][0]["oran"] is None
    assert client.get("/api/kuponlar/999").status_code == 404


# ---- Güncelle / sil / bacak sonucu ----
def test_guncelle_ve_sil_yalniz_admin(db):
    hazir(db)
    assert client.put("/api/kuponlar/1", json={**GIRDI, "seviye": "bayi"}, headers=uye("elit")).status_code == 403
    assert client.delete("/api/kuponlar/1", headers=uye("elit")).status_code == 403
    assert client.put("/api/kuponlar/1", json={**GIRDI, "seviye": "bayi"}, headers=ADMIN).json()["seviye"] == "bayi"
    assert client.delete("/api/kuponlar/1", headers=ADMIN).status_code == 200
    assert client.delete("/api/kuponlar/1", headers=ADMIN).status_code == 404


def test_bacak_sonucu_admin_isaretler_kupon_sonucu_hesaplanir(db):
    hazir(db)
    j = client.post("/api/kuponlar/1/bacaklar/101/sonuc", json={"sonuc": "won"}, headers=ADMIN)
    assert j.status_code == 200 and j.json()["bacaklar"][0]["sonuc"] == "won"
    assert client.post("/api/kuponlar/1/bacaklar/101/sonuc", json={"sonuc": "belki"}, headers=ADMIN).status_code == 422
    assert client.post("/api/kuponlar/1/bacaklar/101/sonuc", json={"sonuc": "won"}, headers=uye("elit")).status_code == 403
    assert client.post("/api/kuponlar/1/bacaklar/999/sonuc", json={"sonuc": "won"}, headers=ADMIN).status_code == 404


# ---- Gizlilik: editör e-postası yalnız admin'e ----
def test_olusturan_epostasi_anonim_ve_uyeye_gosterilmez_admine_gosterilir(db):
    hazir(db)
    for basliklar in ({}, uye("elit")):
        k = client.get("/api/kuponlar/1", headers=basliklar).json()
        assert "olusturan_email" not in k, basliklar
        assert all("olusturan_email" not in x for x in client.get("/api/kuponlar", headers=basliklar).json()["items"])
    assert client.get("/api/kuponlar/1", headers=ADMIN).json()["olusturan_email"] == "editor@greysports.tv"


def test_yazar_suzgeci_epostayla_filtreler_ama_eposta_yanita_girmez(db, monkeypatch):
    from app.routers import kupon_sonuclari as S
    gelen = {}
    monkeypatch.setattr(r, "db_liste", lambda f: (gelen.update(f) or ([], 0)))
    client.get("/api/kuponlar/sonuclar?yazar=Editor@Greysports.tv")
    assert gelen["yazar"] == "editor@greysports.tv"
