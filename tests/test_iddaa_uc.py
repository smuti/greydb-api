"""POST /api/iddaa/eslestir: anahtarla korunur, varsayılan kuru çalışma, yalnızca eşleşenleri yazar. Gerçek veritabanı/ağ yok."""
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from app.main import app
from app.routers import iddaa as r

client = TestClient(app)
ANAHTAR = {"X-Cron-Anahtari": "gizli"}
KICK = datetime(2026, 10, 19, 19, 0)   # UTC, saat dilimsiz (match_date böyle)
D = int(KICK.replace(tzinfo=timezone.utc).timestamp())


def kur(monkeypatch, anahtar="gizli", olaylar=None, satirlar=None):
    yazilan = []
    monkeypatch.setattr(r, "_anahtar", lambda: anahtar)
    monkeypatch.setattr(r, "olaylari_cek", lambda: olaylar if olaylar is not None else [
        {"i": 3320626, "bri": 7, "hn": "Tottenham ", "an": "Coventry", "d": D}])
    monkeypatch.setattr(r, "yakin_maclar", lambda bas, son: satirlar if satirlar is not None else [
        {"fotmob_match_id": 11, "home": "Tottenham Hotspur", "away": "Coventry City", "match_date": KICK}])
    monkeypatch.setattr(r, "iddaa_yaz", lambda eslesen: yazilan.append(dict(eslesen)))
    return yazilan


def test_anahtarsiz_istek_reddedilir(monkeypatch):
    kur(monkeypatch)
    assert client.post("/api/iddaa/eslestir").status_code == 401
    assert client.post("/api/iddaa/eslestir", headers={"X-Cron-Anahtari": "yanlis"}).status_code == 401


def test_sunucuda_anahtar_tanimli_degilse_kapali(monkeypatch):
    kur(monkeypatch, anahtar=None)
    assert client.post("/api/iddaa/eslestir", headers=ANAHTAR).status_code == 503
    assert client.post("/api/iddaa/eslestir", headers={"X-Cron-Anahtari": ""}).status_code == 503


def test_varsayilan_kuru_calisma_yazmaz(monkeypatch):
    yazilan = kur(monkeypatch)
    j = client.post("/api/iddaa/eslestir", headers=ANAHTAR).json()
    assert j["dry_run"] is True and j["eslesen"] == 1 and yazilan == []


def test_dry_run_false_ile_eslesenleri_yazar(monkeypatch):
    yazilan = kur(monkeypatch)
    j = client.post("/api/iddaa/eslestir?dry_run=false", headers=ANAHTAR).json()
    assert j["dry_run"] is False and yazilan == [{11: 3320626}]


def test_rapor_belirsiz_ve_eslesmeyenleri_adlariyla_verir(monkeypatch):
    satirlar = [
        {"fotmob_match_id": 11, "home": "Tottenham Hotspur", "away": "Coventry City", "match_date": KICK},
        {"fotmob_match_id": 12, "home": "Zzz", "away": "Yyy", "match_date": KICK},
    ]
    j = client.post("/api/iddaa/eslestir", headers=ANAHTAR).json() if kur(monkeypatch, satirlar=satirlar) is not None else None
    assert j["toplam_mac"] == 2 and j["eslesen"] == 1 and j["belirsiz"] == []
    assert j["eslesmeyen"] == [{"fotmob_match_id": 12, "mac": "Zzz - Yyy"}]


def test_iddaa_ulasilamazsa_502_ve_yazmaz(monkeypatch):
    yazilan = kur(monkeypatch)
    def patla():
        raise RuntimeError("iddaa kapalı")
    monkeypatch.setattr(r, "olaylari_cek", patla)
    assert client.post("/api/iddaa/eslestir?dry_run=false", headers=ANAHTAR).status_code == 502
    assert yazilan == []
