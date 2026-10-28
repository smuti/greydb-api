"""Spor Toto yayın uçları: bağlantı ayrıştırma, hafta numarası, kaydetme, herkese açık veri (tercih/yorum/model SIZMAZ)."""
from datetime import date

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers import spor_toto_yayin as y

HAFTALAR = pd.DataFrame([
    {"id": 2, "hafta_no": 902, "hafta_etiketi": "12. Hafta", "youtube_linki": "https://www.youtube.com/watch?v=6guok3KL1mA&t=67s", "mac_sayisi": 15},
    {"id": 1, "hafta_no": 901, "hafta_etiketi": "11. Hafta", "youtube_linki": "https://youtu.be/AbCdEfGhIjK?t=5", "mac_sayisi": 15},
    {"id": 0, "hafta_no": 800, "hafta_etiketi": "12. Hafta", "youtube_linki": "https://youtu.be/ZZZZZZZZZZZ", "mac_sayisi": 15},     # eski sezonun aynı numaralı haftası
    {"id": 9, "hafta_no": 950, "hafta_etiketi": "13. Hafta", "youtube_linki": "bozuk", "mac_sayisi": 15},
    {"id": 8, "hafta_no": 960, "hafta_etiketi": "14. Hafta", "youtube_linki": None, "mac_sayisi": 15},     # ne hafta ne maç bağlantısı var
])
MACLAR = pd.DataFrame([{"mac_no": 1, "ev_takim": "Konyaspor", "deplasman_takim": "Galatasaray", "lig": "Süper Lig", "mac_tarihi": date(2026, 10, 30), "ev_fotmob_id": 8689.0, "dep_fotmob_id": 8637.0},
                       {"mac_no": 2, "ev_takim": "Erzurumspor FK", "deplasman_takim": "Çorum FK", "lig": None, "mac_tarihi": None, "ev_fotmob_id": None, "dep_fotmob_id": float("nan")}])


@pytest.fixture
def istemci(monkeypatch):
    sorgular = []
    def sorgu(sql, params=None):
        sorgular.append(sql)
        return MACLAR if "spor_toto_matches m" in sql and "LEFT JOIN" in sql else HAFTALAR
    monkeypatch.setattr(y, "query_to_df", sorgu)
    app = FastAPI(); app.include_router(y.router, prefix="/api")
    c = TestClient(app); c.sorgular = sorgular
    return c


def test_youtube_id_bicimleri():
    assert y.youtube_id("https://www.youtube.com/watch?v=6guok3KL1mA&t=67s") == "6guok3KL1mA"
    assert y.youtube_id("https://youtu.be/6guok3KL1mA?t=10") == "6guok3KL1mA"
    assert y.youtube_id("https://www.youtube.com/live/6guok3KL1mA") == "6guok3KL1mA"
    assert y.youtube_id("https://example.com/watch?v=6guok3KL1mA") is None
    assert y.youtube_id("https://www.youtube.com/watch?v=kisa") is None
    assert y.youtube_id("") is None


def test_hafta_numarasi():
    assert y.hafta_numarasi("12. Hafta", 5) == 12
    assert y.hafta_numarasi(None, 5) == 5
    assert y.hafta_numarasi("Final", 7) == 7


def test_liste_gecerli_yayinlar_tekrarsiz_ve_yeniden_eskiye(istemci):
    r = istemci.get("/api/spor-toto/yayinlar").json()["haftalar"]
    assert [h["hafta"] for h in r] == [12, 11]                       # bozuk bağlantılı 13. hafta yok; eski sezonun 12'si elendi
    assert r[0] == {"hafta": 12, "etiket": "12. Hafta", "video_id": "6guok3KL1mA", "mac_sayisi": 15}


def test_hafta_detayi_guvenli_alanlar(istemci):
    j = istemci.get("/api/spor-toto/yayinlar/12").json()
    assert j["video_id"] == "6guok3KL1mA" and j["hafta"] == 12
    assert j["maclar"][0] == {"mac_no": 1, "ev": "Konyaspor", "dep": "Galatasaray", "lig": "Süper Lig", "tarih": "2026-10-30", "ev_fotmob_id": 8689, "dep_fotmob_id": 8637}
    assert j["maclar"][1]["ev_fotmob_id"] is None and j["maclar"][1]["dep_fotmob_id"] is None and j["maclar"][1]["lig"] is None
    sorgu = " ".join(istemci.sorgular)
    assert "~* 'youtu'" in sorgu                                       # haftanın kendi bağlantısı yoksa maçlara önceden girilmiş YouTube bağlantısı kullanılır
    for yasak in ("secilen_senaryo", "final_metin", "model_", "entries", "metin"):
        assert yasak not in sorgu                                      # tercih ve yorum bu uçlarda hiç okunmaz
    assert istemci.get("/api/spor-toto/yayinlar/99").status_code == 404


def test_kaydet_dogrular_ve_bos_kaldirir(monkeypatch):
    yazilan = []
    monkeypatch.setattr(y, "execute_insert", lambda sql, p=None: (yazilan.append(p), {"id": 1})[1])
    app = FastAPI(); app.include_router(y.router, prefix="/api"); c = TestClient(app)
    assert c.put("/api/spor-toto/weeks/902/youtube", json={"youtube_linki": "https://example.com/x"}).status_code == 400
    assert c.put("/api/spor-toto/weeks/902/youtube", json={"youtube_linki": " https://youtu.be/6guok3KL1mA "}).status_code == 200
    assert yazilan[-1]["link"] == "https://youtu.be/6guok3KL1mA"
    assert c.put("/api/spor-toto/weeks/902/youtube", json={"youtube_linki": ""}).json()["youtube_linki"] is None
    assert yazilan[-1]["link"] is None
    monkeypatch.setattr(y, "execute_insert", lambda sql, p=None: None)
    assert c.put("/api/spor-toto/weeks/1/youtube", json={"youtube_linki": "https://youtu.be/6guok3KL1mA"}).status_code == 404
