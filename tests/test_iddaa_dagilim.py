"""iddaa oynanma dağılımı: en çok oynanan seçimler ve market içi yüzde. Saf dönüşüm + uç (iddaa'ya gidilmez)."""
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import iddaa as r
from app.services.iddaa_dagilim import dagilim_olustur

CFG = {"1_1": {"n": "Maç Sonucu"}, "1_89": {"n": "Karşılıklı Gol"}, "1_101": {"n": "Alt/Üst {0}"}, "1_700": {"n": "Alt/Üst {0} ve Karşılıklı Gol"}}
OLAY = {"i": 3219202, "hn": "Trabzonspor", "an": "Beşiktaş", "m": [
    {"i": 100, "t": 1, "st": 1, "o": [{"no": 1, "odd": 2.41, "n": "1"}, {"no": 2, "odd": 3.42, "n": "0"}, {"no": 3, "odd": 2.3, "n": "2"}]},
    {"i": 101, "t": 1, "st": 89, "o": [{"no": 1, "odd": 1.38, "n": "Var"}, {"no": 2, "odd": 2.6, "n": "Yok"}]},
    {"i": 102, "t": 1, "st": 101, "sov": "2.5", "o": [{"no": 1, "odd": 2.7, "n": "Alt"}, {"no": 2, "odd": 1.45, "n": "Üst"}]},
    {"i": 103, "t": 1, "st": 999, "o": [{"no": 1, "odd": 9.0, "n": "x"}]},
]}
YUZDE = {"100": {"1": 11.0, "2": 5.0, "3": 5.0}, "101": {"1": 16.0}, "102": {"2": 11.0, "1": 2.0}, "103": {"1": 1.0}, "555": {"1": 9.0}}


def test_en_cok_oynanan_siralama_ve_adlar():
    d = dagilim_olustur(OLAY, YUZDE, 21.67, CFG, ilk=3)
    assert d["mac_payi"] == 21.67
    assert [(x["bahis_turu"], x["secim"], x["yuzde"]) for x in d["en_cok_oynanan"]] == [
        ("Karşılıklı Gol", "Var", 16.0), ("Maç Sonucu", "1", 11.0), ("Alt/Üst 2.5", "Üst", 11.0)]
    assert d["en_cok_oynanan"][0]["oran"] == 1.38


def test_esit_yuzdede_daha_dusuk_oran_once_degil_kararli_sira():
    d = dagilim_olustur(OLAY, YUZDE, None, CFG, ilk=10)
    esit = [x for x in d["en_cok_oynanan"] if x["yuzde"] == 11.0]
    assert [x["bahis_turu"] for x in esit] == ["Maç Sonucu", "Alt/Üst 2.5"]      # kaynak sırası korunur


def test_market_ici_dagilim():
    d = dagilim_olustur(OLAY, YUZDE, 21.67, CFG)
    ms = next(m for m in d["marketler"] if m["bahis_turu"] == "Maç Sonucu")
    assert {s["secim"]: s["market_ici_yuzde"] for s in ms["secimler"]} == {"1": 52.4, "0": 23.8, "2": 23.8}
    assert ms["toplam_yuzde"] == 21.0
    au = next(m for m in d["marketler"] if m["bahis_turu"] == "Alt/Üst 2.5")
    assert au["secimler"][0]["secim"] == "Üst"                                     # yüzdeye göre sıralı


def test_adi_bilinmeyen_market_ve_olmayan_id_atlanir_ama_cokmez():
    d = dagilim_olustur(OLAY, YUZDE, None, CFG)
    assert not any(x["yuzde"] == 9.0 for x in d["en_cok_oynanan"])                   # 555 numaralı market olayda yok
    assert dagilim_olustur({"m": []}, {}, None, CFG) == {"mac_payi": None, "en_cok_oynanan": [], "marketler": []}
    assert dagilim_olustur(OLAY, {}, None, CFG)["en_cok_oynanan"] == []


# ---- Uç ----
istemci = TestClient(app)


def test_dagilim_ucu(monkeypatch):
    monkeypatch.setattr(r, "iddaa_dagilim_verisi", lambda eid: (OLAY, YUZDE, 21.67, CFG))
    j = istemci.get("/api/iddaa/dagilim/3219202").json()
    assert j["mac_payi"] == 21.67 and j["en_cok_oynanan"][0]["secim"] == "Var" and len(j["en_cok_oynanan"]) <= 10


def test_dagilim_ucu_iddaa_hatasinda_502(monkeypatch):
    def patla(eid):
        raise RuntimeError("kapalı")
    monkeypatch.setattr(r, "iddaa_dagilim_verisi", patla)
    assert istemci.get("/api/iddaa/dagilim/1").status_code == 502


def test_dagilim_ucu_olmayan_mac_404(monkeypatch):
    monkeypatch.setattr(r, "iddaa_dagilim_verisi", lambda eid: (None, {}, None, {}))
    assert istemci.get("/api/iddaa/dagilim/1").status_code == 404
