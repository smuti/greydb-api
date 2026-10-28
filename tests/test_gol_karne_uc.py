"""GET /api/gol-karne: anahtarla korunur; bitmiş maçların karnesini döner. Veritabanı yok."""
from datetime import datetime

from fastapi.testclient import TestClient

from app.main import app
from app.routers import gol_karne as r
from app.routers import skorjin_prematch as sp

client = TestClient(app)
ANAHTAR = {"X-Cron-Anahtari": "gizli"}


def m11(kutle):
    m = [[0.0] * 11 for _ in range(11)]
    for (i, j), p in kutle.items():
        m[i][j] = p
    return m


def satir(kaynak="canli", surum="m2", hs=2, as_=0):
    return {"fotmob_match_id": 1, "matrix": m11({(2, 0): 1.0}), "home_score": hs, "away_score": as_, "model_version": surum,
            "kaynak": kaynak, "kickoff": datetime(2026, 10, 27, 19, 45)}


def kur(monkeypatch, satirlar=None, hata=None, anahtar="gizli"):
    cagrilar = []
    monkeypatch.setattr(sp, "_anahtar", lambda: anahtar)

    def oku(bas, son):
        cagrilar.append((bas, son))
        if hata:
            raise hata
        return satirlar if satirlar is not None else []
    monkeypatch.setattr(r, "sonuclari_oku", oku)
    monkeypatch.setattr(r, "_simdi", lambda: datetime(2026, 10, 28, 12, 0))
    return cagrilar


def test_anahtarsiz_istek_reddedilir(monkeypatch):
    kur(monkeypatch)
    assert client.get("/api/gol-karne").status_code == 401


def test_anahtar_tanimli_degilse_kapali(monkeypatch):
    kur(monkeypatch, anahtar=None)
    assert client.get("/api/gol-karne", headers=ANAHTAR).status_code == 503


def test_rapor_doner_ve_gun_penceresi_uygulanir(monkeypatch):
    cagrilar = kur(monkeypatch, [satir()])
    j = client.get("/api/gol-karne?gun=7", headers=ANAHTAR).json()
    assert j["mac"] == 1 and j["atlanan"] == 0 and any(t["pazar"] == "toplam_ust_1.5" for t in j["tablo"])
    assert cagrilar == [(datetime(2026, 10, 21, 12, 0), datetime(2026, 10, 28, 12, 0))]


def test_kaynak_filtresi_canli_ile_geri_testi_ayirir(monkeypatch):
    kur(monkeypatch, [satir("canli"), satir("geri_test"), satir("geri_test")])
    assert client.get("/api/gol-karne?kaynak=canli", headers=ANAHTAR).json()["mac"] == 1
    assert client.get("/api/gol-karne?kaynak=geri_test", headers=ANAHTAR).json()["mac"] == 2
    assert client.get("/api/gol-karne", headers=ANAHTAR).json()["mac"] == 3


def test_okuma_hatasi_503_doner_ayrinti_sizdirmaz(monkeypatch):
    kur(monkeypatch, hata=RuntimeError("relation greydb.gol_karne_tahmin does not exist"))
    resp = client.get("/api/gol-karne", headers=ANAHTAR)
    assert resp.status_code == 503 and "gol_karne" in resp.json()["detail"]
