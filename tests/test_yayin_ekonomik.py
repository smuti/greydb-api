"""Ekonomik kupon yayını: tahmin doğrulama (saf mantık) ve uçlar. Veritabanı sahte."""
import copy
from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import yayin as r
from app.services import yayin_ekonomik as E

client = TestClient(app)
ADMIN = {"X-Sunucu-Anahtari": "gizli", "X-Yetki": "admin", "X-Kullanici-Email": "editor@greysports.tv"}
GW = {"X-Sunucu-Anahtari": "gizli"}


def test_secenek_normallestirir():
    assert E.secenek("1") == "1"
    assert E.secenek("0-1") == "1-0" and E.secenek("2 0") == "0-2" and E.secenek("201") == "1-0-2" and E.secenek(" 1,0 ") == "1-0"
    assert E.secenek("") == "" and E.secenek(None) == ""
    with pytest.raises(ValueError):
        E.secenek("3")
    with pytest.raises(ValueError):
        E.secenek("x")


def test_tahmin_bos_gecerli_ve_alanlar_normallesir():
    assert E.tahmin_dogrula({}) == {"once": "", "nihai": "", "daralma": "", "tek": ""}
    assert E.tahmin_dogrula({"once": "0 1", "nihai": "1", "daralma": "", "tek": "1"}) == {"once": "1-0", "nihai": "1", "daralma": "", "tek": "1"}


def test_daralma_nihainin_alt_kumesi_olmali():
    assert E.tahmin_dogrula({"once": "1-0-2", "nihai": "1-0", "daralma": "1"})["daralma"] == "1"
    with pytest.raises(ValueError, match="daralma"):
        E.tahmin_dogrula({"once": "1-0", "nihai": "1-0", "daralma": "2"})
    # nihai boşsa önceki geçerli sayılır
    with pytest.raises(ValueError, match="daralma"):
        E.tahmin_dogrula({"once": "1", "nihai": "", "daralma": "0"})
    assert E.tahmin_dogrula({"once": "1-0", "daralma": "0"})["daralma"] == "0"


def test_tek_atis_tek_isaret():
    with pytest.raises(ValueError, match="tek"):
        E.tahmin_dogrula({"tek": "1-0"})
    assert E.tahmin_dogrula({"tek": "2"})["tek"] == "2"


def test_kolon_sayisi_carpim():
    assert E.kolon_sayisi(["1", "1-0", "1-0-2"]) == 6
    assert E.kolon_sayisi(["1"] * 4 + ["1-0"] * 5 + ["1-0-2"] * 6) == 23328
    assert E.kolon_sayisi(["1", ""]) is None and E.kolon_sayisi([]) is None


def test_gecerli_deger_oncelik_daralma_nihai_once():
    assert E.gecerli({"once": "1-0-2", "nihai": "1-0", "daralma": "1"}) == "1"
    assert E.gecerli({"once": "1-0-2", "nihai": "1-0", "daralma": ""}) == "1-0"
    assert E.gecerli({"once": "1-0-2", "nihai": "", "daralma": ""}) == "1-0-2"


def test_tahminler_dogrula_15_mac_ve_sira():
    t = [{"sira": i, "fuat": {"once": "1"}, "skorjin": {"once": "0"}} for i in range(1, 16)]
    s = E.tahminleri_dogrula(t, [i for i in range(1, 16)])
    assert len(s) == 15 and s[0] == {"sira": 1, "fuat": {"once": "1", "nihai": "", "daralma": "", "tek": ""}, "skorjin": {"once": "0", "nihai": "", "daralma": "", "tek": ""}}
    with pytest.raises(ValueError, match="sira"):
        E.tahminleri_dogrula([{"sira": 99, "fuat": {}, "skorjin": {}}], list(range(1, 16)))
    with pytest.raises(ValueError, match="tekrar"):
        E.tahminleri_dogrula([{"sira": 1, "fuat": {}, "skorjin": {}}, {"sira": 1, "fuat": {}, "skorjin": {}}], [1, 2])


# ---------------- Uçlar ----------------
class Sahte:
    def __init__(self):
        self.toto, self.ek = {}, {}

    def getir(self, kod):
        return copy.deepcopy(self.toto.get(kod))


@pytest.fixture
def s(monkeypatch):
    f = Sahte()
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    monkeypatch.setattr(r, "kaydi_getir", f.getir)
    monkeypatch.setattr(r, "kayitlari_listele", lambda: [])
    monkeypatch.setattr(r, "bugun_tsi", lambda simdi=None: date(2026, 10, 24))
    d = datetime(2026, 10, 20, 12)

    def olustur(k):
        f.ek[k["kod"]] = {**copy.deepcopy(k), "tahminler": [], "olusturma": d, "guncelleme": d}
        return {"id": 1, "kod": k["kod"]}

    monkeypatch.setattr(r, "ekonomik_olustur", olustur)
    monkeypatch.setattr(r, "ekonomik_getir", lambda kod: copy.deepcopy(f.ek.get(kod)))
    monkeypatch.setattr(r, "ekonomik_listele", lambda: [{**k, "mac_sayisi": len(k["maclar"])} for k in f.ek.values()])
    monkeypatch.setattr(r, "ekonomik_tahmin_yaz", lambda kod, t: f.ek[kod].update({"tahminler": copy.deepcopy(t)}))
    monkeypatch.setattr(r, "ekonomik_sil", lambda kod: f.ek.pop(kod, None))
    f.toto["toto123456"] = {"kod": "toto123456", "tur": "toto", "durum": "hazir", "etiket": "11. Hafta",
                            "maclar": [{"sira": i, "ev": {"ad": f"E{i}", "logo": None, "renk": "#fff"}, "dep": {"ad": f"D{i}", "logo": None, "renk": "#000"}, "lig": "TFF", "saat": "20:00",
                                        "tarih": "2026-10-24", "kartlar": {"puan": {}}} for i in range(1, 16)]}
    return f


GOVDE = {"kaynak_kod": "toto123456", "yayin_tarihi": "2026-10-25", "yayin_saat": "21:00"}


def test_yazma_yalniz_admin(s):
    assert client.post("/api/yayin/ekonomik", json=GOVDE, headers=GW).status_code in (401, 403)
    assert client.put("/api/yayin/ekonomik/x/tahminler", json={"tahminler": []}, headers=GW).status_code in (401, 403)


def test_ekonomik_maclar_toto_sayfasindan_kopyalanir_kartsiz(s):
    kod = client.post("/api/yayin/ekonomik", json=GOVDE, headers=ADMIN).json()["kod"]
    p = client.get(f"/api/yayin/{kod}").json()                     # OBS: giriş yok, kod sır
    assert p["tur"] == "ekonomik" and p["durum"] == "hazir" and len(p["maclar"]) == 15 and p["yayinGun"] == "Yarın"
    m = p["maclar"][0]
    assert m["ev"]["ad"] == "E1" and "kartlar" not in m and m["fuat"] == {"once": "", "nihai": "", "daralma": "", "tek": ""}


def test_kaynak_yok_ya_da_toto_degil_ya_da_hazir_degil(s):
    assert client.post("/api/yayin/ekonomik", json={**GOVDE, "kaynak_kod": "yokyok1234"}, headers=ADMIN).status_code == 404
    s.toto["toto123456"]["tur"] = "gun"
    assert client.post("/api/yayin/ekonomik", json=GOVDE, headers=ADMIN).status_code == 404
    s.toto["toto123456"].update({"tur": "toto", "durum": "hazirlaniyor"})
    assert client.post("/api/yayin/ekonomik", json=GOVDE, headers=ADMIN).status_code == 409


def test_tahminler_kaydedilir_ve_okunur(s):
    kod = client.post("/api/yayin/ekonomik", json=GOVDE, headers=ADMIN).json()["kod"]
    govde = {"tahminler": [{"sira": 1, "fuat": {"once": "0 1 2", "nihai": "1-0", "daralma": "1", "tek": "1"}, "skorjin": {"once": "2"}}]}
    assert client.put(f"/api/yayin/ekonomik/{kod}/tahminler", json=govde, headers=ADMIN).status_code == 200
    m = client.get(f"/api/yayin/{kod}").json()["maclar"][0]
    assert m["fuat"] == {"once": "1-0-2", "nihai": "1-0", "daralma": "1", "tek": "1"} and m["skorjin"]["once"] == "2"
    assert client.get(f"/api/yayin/{kod}").json()["maclar"][1]["fuat"]["once"] == ""


def test_gecersiz_tahmin_422(s):
    kod = client.post("/api/yayin/ekonomik", json=GOVDE, headers=ADMIN).json()["kod"]
    for kotu in ({"sira": 1, "fuat": {"once": "1-0", "daralma": "2"}}, {"sira": 1, "fuat": {"tek": "1-0"}}, {"sira": 1, "fuat": {"once": "3"}}, {"sira": 99, "fuat": {}}):
        assert client.put(f"/api/yayin/ekonomik/{kod}/tahminler", json={"tahminler": [kotu]}, headers=ADMIN).status_code == 422


def test_liste_ve_silme(s):
    kod = client.post("/api/yayin/ekonomik", json=GOVDE, headers=ADMIN).json()["kod"]
    satir = next(x for x in client.get("/api/yayin", headers=ADMIN).json()["sayfalar"] if x["kod"] == kod)
    assert satir["tur"] == "ekonomik" and satir["mac_sayisi"] == 15 and satir["durum"] == "hazir"
    assert client.delete(f"/api/yayin/{kod}", headers=ADMIN).status_code == 200
    assert client.get(f"/api/yayin/{kod}").status_code == 404
