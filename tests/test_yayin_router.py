"""/api/yayin: yazma yalnız admin (ağ geçidi anahtarı), okuma ve logo herkese açık, hazırlama arka planda. Veritabanı ve dış servisler sahte."""
import copy
from datetime import date

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import yayin as r

client = TestClient(app)
ADMIN = {"X-Sunucu-Anahtari": "gizli", "X-Yetki": "admin", "X-Kullanici-Email": "editor@greysports.tv"}
GW = {"X-Sunucu-Anahtari": "gizli"}


class Sahte:
    def __init__(self):
        self.kayitlar, self.sayac, self.logolar, self.toto, self.ilerlemeler = {}, 0, {}, [], []

    def olustur(self, k):
        self.sayac += 1
        self.kayitlar[k["kod"]] = {**copy.deepcopy(k), "id": self.sayac, "maclar": [], "durum": "hazirlaniyor", "hata": None, "tarih": k["tarih"],
                                  "olusturma": __import__("datetime").datetime(2026, 10, 20, 12), "guncelleme": __import__("datetime").datetime(2026, 10, 20, 12)}
        return {"id": self.sayac, "kod": k["kod"]}

    def getir(self, kod):
        return copy.deepcopy(self.kayitlar.get(kod))

    def maclari_yaz(self, kod, maclar, durum, hata):
        self.kayitlar[kod].update({"maclar": copy.deepcopy(maclar), "durum": durum, "hata": hata})

    def ilerleme_yaz(self, kod, biten):
        self.kayitlar[kod]["kaynak"]["biten"] = biten
        self.ilerlemeler.append(biten)

    def durumu_yaz(self, kod, durum):
        self.kayitlar[kod].update({"durum": durum, "hata": None})

    def ayari_yaz(self, kod, a):
        self.kayitlar[kod].update(a)

    def sil(self, kod):
        self.kayitlar.pop(kod, None)


@pytest.fixture(autouse=True)
def sahte(monkeypatch):
    s = Sahte()
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    monkeypatch.setattr(r, "kaydi_olustur", s.olustur)
    monkeypatch.setattr(r, "kaydi_getir", s.getir)
    monkeypatch.setattr(r, "maclari_yaz", s.maclari_yaz)
    monkeypatch.setattr(r, "durumu_yaz", s.durumu_yaz)
    monkeypatch.setattr(r, "ilerleme_yaz", s.ilerleme_yaz)
    monkeypatch.setattr(r, "ayari_yaz", s.ayari_yaz)
    monkeypatch.setattr(r, "sil", s.sil)
    monkeypatch.setattr(r, "kayitlari_listele", lambda: [{**k, "mac_sayisi": len(k["maclar"])} for k in s.kayitlar.values()])
    monkeypatch.setattr(r, "toto_maclari", lambda h: s.toto)
    monkeypatch.setattr(r, "takim_logosu", lambda i: s.logolar.get(i))
    monkeypatch.setattr(r, "bugun_tsi", lambda simdi=None: date(2026, 10, 24))
    monkeypatch.setattr(r.T, "mac_hazirla", lambda sira, mid, taban: None if mid == 0 else {
        "sira": sira, "ev": {"ad": "A", "logo": f"{taban}/1", "renk": "#fff"}, "dep": {"ad": "B", "logo": None, "renk": "#000"}, "lig": "Süper Lig", "hafta": "9. Hafta",
        "saat": "20:00", "tarih": "2026-10-24", "kartlar": {"eksik": {"ev": [{"ad": "X", "tur": "sakat", "deger": 5}, {"ad": "Y", "tur": "sakat", "deger": 1}], "dep": []}}})
    return s


GUN = {"tur": "gun", "mac_idler": [11, 22], "tarih": "2026-10-24", "yayin_tarihi": "2026-10-25", "yayin_saat": "21:00"}


def test_yayin_gun_metni_bugun_yarin_gun_adi():
    b = date(2026, 10, 24)
    assert r.yayin_gun_metni(date(2026, 10, 24), b) == "Bugün"
    assert r.yayin_gun_metni(date(2026, 10, 25), b) == "Yarın"
    assert r.yayin_gun_metni(date(2026, 10, 30), b) == "Cuma"
    assert r.etiket_gun(date(2026, 10, 24)) == "24 Ekim"


def test_yazma_uclari_anahtarsiz_401_admin_degilse_403():
    assert client.post("/api/yayin/hazirla", json=GUN).status_code == 401
    assert client.post("/api/yayin/hazirla", json=GUN, headers=GW).status_code == 403
    assert client.get("/api/yayin").status_code == 401
    assert client.delete("/api/yayin/abc", headers=GW).status_code == 403


def test_hazirla_gun_arka_planda_toplar_ve_getir_hazir_paketi_verir(sahte):
    res = client.post("/api/yayin/hazirla", json=GUN, headers=ADMIN)
    assert res.status_code == 202
    kod = res.json()["kod"]
    p = client.get(f"/api/yayin/{kod}").json()               # okuma herkese açık, anahtar yok
    assert p["durum"] == "hazir" and len(p["maclar"]) == 2
    assert p["etiket"] == "24 Ekim" and p["programAdi"] == "AÇIK RADAR MASASI" and p["marka"] == "GREYSPORTS" and p["bayiKodu"] == "300908"
    assert p["yayinGun"] == "Yarın" and p["yayinSaat"] == "21:00"           # gerçek tarih saklanır, gösterim bugüne göre
    assert p["maclar"][0]["ev"]["logo"] == "/api/yayin/logo/1"
    assert sahte.kayitlar[kod]["olusturan"] == "editor@greysports.tv"


def test_hazirla_dogrulama():
    assert client.post("/api/yayin/hazirla", json={**GUN, "mac_idler": []}, headers=ADMIN).status_code == 422
    assert client.post("/api/yayin/hazirla", json={"tur": "toto", "yayin_tarihi": "2026-10-25", "yayin_saat": "21:00"}, headers=ADMIN).status_code == 422
    assert client.post("/api/yayin/hazirla", json={"tur": "toto", "hafta_no": 99, "yayin_tarihi": "2026-10-25", "yayin_saat": "21:00"}, headers=ADMIN).status_code == 404
    assert client.post("/api/yayin/hazirla", json={**GUN, "yayin_saat": "9:5"}, headers=ADMIN).status_code == 422
    assert client.post("/api/yayin/hazirla", json={**GUN, "fazla": 1}, headers=ADMIN).status_code == 422


def test_toto_eslesmeyen_mac_kalmaz_ama_not_duser(sahte):
    sahte.toto = [{"mac_no": 1, "fotmob_match_id": 5, "ev_takim": "A", "deplasman_takim": "B"}, {"mac_no": 2, "fotmob_match_id": None, "ev_takim": "C", "deplasman_takim": "D"}]
    kod = client.post("/api/yayin/hazirla", json={"tur": "toto", "hafta_no": 11, "yayin_tarihi": "2026-10-25", "yayin_saat": "12:30"}, headers=ADMIN).json()["kod"]
    p = client.get(f"/api/yayin/{kod}").json()
    assert p["etiket"] == "11. Hafta" and p["programAdi"] == "Ekonomik ve Tek Atış" and [m["sira"] for m in p["maclar"]] == [1]
    assert "2. maç (C - D)" in p["hata"]


def test_hicbir_mac_alinamazsa_hata_durumu(sahte):
    kod = client.post("/api/yayin/hazirla", json={**GUN, "mac_idler": [0]}, headers=ADMIN).json()["kod"]
    assert client.get(f"/api/yayin/{kod}").json()["durum"] == "hata"


def test_ayar_eksik_secim_yenile_sil(sahte):
    kod = client.post("/api/yayin/hazirla", json=GUN, headers=ADMIN).json()["kod"]
    p = client.put(f"/api/yayin/{kod}/ayar", json={"yayin_saat": "12:30", "yayin_tarihi": "2026-10-24"}, headers=ADMIN).json()
    assert p["yayinSaat"] == "12:30" and p["yayinGun"] == "Bugün"
    assert client.put(f"/api/yayin/{kod}/eksik", json={"mac_sira": 1, "ev": ["Y"], "dep": []}, headers=ADMIN).status_code == 200
    ev = client.get(f"/api/yayin/{kod}").json()["maclar"][0]["kartlar"]["eksik"]["ev"]
    assert [o.get("goster") for o in ev] == [False, True]
    client.put(f"/api/yayin/{kod}/eksik", json={"mac_sira": 1, "ev": [], "dep": []}, headers=ADMIN)       # seçimi temizle
    assert all("goster" not in o for o in client.get(f"/api/yayin/{kod}").json()["maclar"][0]["kartlar"]["eksik"]["ev"])
    assert client.put(f"/api/yayin/{kod}/eksik", json={"mac_sira": 9, "ev": [], "dep": []}, headers=ADMIN).status_code == 404
    assert client.put(f"/api/yayin/{kod}/eksik", json={"mac_sira": 1, "ev": ["a", "b", "c", "d"], "dep": []}, headers=ADMIN).status_code == 422
    assert client.post(f"/api/yayin/{kod}/yenile", headers=ADMIN).status_code == 202
    assert client.get(f"/api/yayin/{kod}").json()["durum"] == "hazir"
    assert client.delete(f"/api/yayin/{kod}", headers=ADMIN).status_code == 200
    assert client.get(f"/api/yayin/{kod}").status_code == 404


def test_logo_herkese_acik_ve_onbellekli(sahte):
    sahte.logolar[7] = {"logo": b"\x89PNGxx", "logo_tur": "image/png"}
    res = client.get("/api/yayin/logo/7")
    assert res.status_code == 200 and res.content == b"\x89PNGxx" and "immutable" in res.headers["cache-control"]
    assert client.get("/api/yayin/logo/8").status_code == 404


def test_liste_yalniz_admin(sahte):
    client.post("/api/yayin/hazirla", json=GUN, headers=ADMIN)
    assert len(client.get("/api/yayin", headers=ADMIN).json()["sayfalar"]) == 1


def test_ilerleme_her_mac_sonrasi_yazilir_ve_pakette_gorunur(sahte):
    kod = client.post("/api/yayin/hazirla", json={**GUN, "mac_idler": [11, 0, 22]}, headers=ADMIN).json()["kod"]
    assert sahte.ilerlemeler == [1, 2, 3]                       # alınamayan maç da sayılır: çubuk takılmaz
    assert client.get(f"/api/yayin/{kod}").json()["ilerleme"] == {"biten": 3, "toplam": 3}


def test_hazirlanirken_ilerleme_kismi_biten_toplami_gecmez(sahte):
    kod = client.post("/api/yayin/hazirla", json=GUN, headers=ADMIN).json()["kod"]
    sahte.kayitlar[kod].update({"durum": "hazirlaniyor"})
    sahte.kayitlar[kod]["kaynak"]["biten"] = 1
    assert client.get(f"/api/yayin/{kod}").json()["ilerleme"] == {"biten": 1, "toplam": 2}
    sahte.kayitlar[kod]["kaynak"]["biten"] = 99
    assert client.get(f"/api/yayin/{kod}").json()["ilerleme"]["biten"] == 2
