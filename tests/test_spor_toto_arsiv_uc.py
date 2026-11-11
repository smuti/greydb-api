"""/api/spor-toto/arsiv: herkese açık okuma uçları, admin senkronu, oynanma kaydı, zamanlayıcı işleri. Veritabanı ve ağ sahte."""
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import spor_toto_arsiv as r
from app.services import spor_toto_arsiv as a
from app.services import zamanlayici

client = TestClient(app)
ADMIN = {"X-Sunucu-Anahtari": "gizli", "X-Yetki": "admin", "X-Kullanici-Email": "fuat@greysports.tv"}
FIX = json.loads((Path(__file__).parent / "fixtures" / "sporoto_arsiv.json").read_text())


def hf(i, sezon, ev, ber, dep, f15, oyun=None):
    return {"round_id": i, "sezon": sezon, "oyun_no": oyun or i, "ad": f"{oyun or i}. Hafta", "kapanis": None, "bilen_15": f15, "bilen_14": f15 * 10, "bilen_13": 1, "bilen_12": 1,
            "ikramiye_15": 100.5, "ikramiye_14": 1, "ikramiye_13": 1, "ikramiye_12": 1, "mac_sayisi": 15, "ev": ev, "beraberlik": ber, "deplasman": dep, "noter_sayisi": 0, "temiz": True}


HAFTALAR = [hf(1, "2025/2026", 8, 3, 4, 10), hf(2, "2025/2026", 7, 4, 4, 0), hf(3, "2026/2027", 8, 3, 4, 40, 1), hf(4, "2026/2027", 6, 5, 4, 2, 2)]


@pytest.fixture(autouse=True)
def sahte(monkeypatch):
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    monkeypatch.setattr(r, "haftalar_oku", lambda sezon=None: [h for h in HAFTALAR if sezon is None or h["sezon"] == sezon])
    monkeypatch.setattr(r, "maclar_oku", lambda rid: [{"sira": 1, "tarih": None, "ev": "A", "dep": "B", "skor_ev": 1, "skor_dep": 0, "sonuc": "1", "noter": False}])


def test_ozet_sezonlar_zorluk_ve_bu_sezon():
    j = client.get("/api/spor-toto/arsiv/ozet").json()
    assert [s["sezon"] for s in j["sezonlar"]] == ["2025/2026", "2026/2027"]
    assert j["guncel_sezon"] == "2026/2027" and [h["oyun_no"] for h in j["bu_sezon"]] == [1, 2]
    assert j["temiz_hafta"] == 4 and {z["beraberlik"] for z in j["zorluk"]} == {3, 4, 5}
    assert set(j["korelasyon"]) == {"ev", "beraberlik", "deplasman"}


def test_ozet_veri_yokken_bos_doner():
    r_ = r.haftalar_oku
    try:
        r.haftalar_oku = lambda sezon=None: []
        j = client.get("/api/spor-toto/arsiv/ozet").json()
    finally:
        r.haftalar_oku = r_
    assert j["sezonlar"] == [] and j["korelasyon"] is None and j["guncel_sezon"] is None and j["bu_sezon"] == []


def test_haftalar_sezon_filtresi_ve_maclar():
    assert len(client.get("/api/spor-toto/arsiv/haftalar").json()) == 4
    j = client.get("/api/spor-toto/arsiv/haftalar", params={"sezon": "2026/2027", "maclar": "true"}).json()
    assert len(j) == 2 and j[0]["maclar"][0]["ev"] == "A" and isinstance(j[0]["ikramiye_15"], float)


def test_benzer_haftalar_ve_gecersiz_dagilim():
    j = client.get("/api/spor-toto/arsiv/benzer", params={"ev": 8, "beraberlik": 3, "deplasman": 4, "k": 2}).json()
    assert [x["round_id"] for x in j["haftalar"]] == [3, 1] and j["ozet"]["k"] == 2
    assert client.get("/api/spor-toto/arsiv/benzer", params={"ev": 8, "beraberlik": 3, "deplasman": 3}).status_code == 400
    assert client.get("/api/spor-toto/arsiv/benzer", params={"ev": 8}).status_code == 422


def test_tercih_ve_model_alani_hicbir_cevapta_yok():
    govde = (client.get("/api/spor-toto/arsiv/ozet").text + client.get("/api/spor-toto/arsiv/haftalar", params={"maclar": "true"}).text).lower()
    for yasak in ("secilen_senaryo", "final_metin", "tercih", "model"):
        assert yasak not in govde


def test_senkron_yalniz_admin(monkeypatch):
    assert client.post("/api/spor-toto/arsiv/senkronla", headers={"X-Sunucu-Anahtari": "gizli"}).status_code in (401, 403)
    monkeypatch.setattr(r, "istemci", lambda: Istemci())
    monkeypatch.setattr(r, "Depo", SahteDepo)
    j = client.post("/api/spor-toto/arsiv/senkronla", headers=ADMIN, json={"sezonlar": ["2026/2027"]})
    assert j.status_code == 200 and j.json()["yazilan"] == 1


class Istemci:
    def yillar(self):
        return ["2026/2027", "2025/2026"]

    def adlar(self, sezon):
        return [{"id": 1541, "name": "14. Oyun Programı"}]

    def sonuc(self, rid):
        return FIX["sonuc_1541"]["object"]

    def maclar(self, rid):
        return FIX["maclar_1541"]["object"]


class SahteDepo:
    def mevcut_haftalar(self):
        return {}

    def kaydet(self, hafta, maclar):
        SahteDepo.son = (hafta, maclar)


def test_zamanlayici_senkron_yalniz_guncel_sezonu_ceker(monkeypatch):
    cagri = []
    class I(Istemci):
        def adlar(self, sezon):
            cagri.append(sezon)
            return super().adlar(sezon)
    monkeypatch.setattr(r, "istemci", lambda: I())
    monkeypatch.setattr(r, "Depo", SahteDepo)
    assert r.senkron_isi()["yazilan"] == 1 and cagri == ["2026/2027"]


def test_oynanma_kaydi_satirlari_yazar(monkeypatch):
    iddaa = json.loads((Path(__file__).parent / "fixtures" / "iddaa_spor_toto.json").read_text())
    yazilan = []
    monkeypatch.setattr(r, "oynanma_cek", lambda: iddaa)
    monkeypatch.setattr(r, "oynanma_yaz", lambda s: yazilan.extend(s) or len(s))
    assert r.oynanma_isi() == 15 == len(yazilan)
    assert yazilan[0] == {"donem_no": 367, "mac_sira": 1, "ev": "Fransa", "dep": "Türkiye", "mac_tarihi": "2026-11-15T22:45:00", "kapanis": "2026-11-13T22:40:00", "yuzde_1": 80, "yuzde_0": 9, "yuzde_2": 11}


def test_oynanma_yuzdesiz_mac_atlanir():
    from app.services import spor_toto_oynanma as o
    s = o.satirlar({"gameCycleNo": 1, "payinEndDate": "x", "events": [{"eventNo": 1, "eventName": "A-B", "eventDate": "d", "homePercentage": None, "drawPercentage": 1, "awayPercentage": 1},
                                                                       {"eventNo": 2, "eventName": "Bosna-Hersek-X", "eventDate": "d", "homePercentage": 40, "drawPercentage": 30, "awayPercentage": 30}]})
    assert len(s) == 1 and s[0]["ev"] == "Bosna" and s[0]["dep"] == "Hersek-X"          # ilk tireden böler (resmî adlarda tire nadir; kayıt kaybolmasın)


def test_zamanlayici_isleri_kayitli():
    kimlikler = {k for k, _, _ in zamanlayici.isler()}
    assert {"spor-toto-arsiv", "spor-toto-oynanma"} <= kimlikler


def test_istemci_yeniden_dener_ve_en_sonunda_hata_verir():
    sayac = {"n": 0}
    class Yanit:
        def __init__(self, ok): self.ok = ok
        def raise_for_status(self):
            if not self.ok: raise RuntimeError("522")
        def json(self): return {"object": []}
    def get(url, params):
        sayac["n"] += 1
        return Yanit(sayac["n"] >= 3)
    bekle = []
    assert a.HttpIstemci(get=get, bekle=bekle.append).adlar("2026/2027") == []
    assert sayac["n"] == 3 and bekle == [2, 5]
    with pytest.raises(RuntimeError):
        a.HttpIstemci(deneme=2, get=lambda u, p: Yanit(False), bekle=lambda s: None).yillar()
