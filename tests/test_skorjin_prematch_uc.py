"""POST /api/skorjin/prematch-hazirla: anahtarla korunur, varsayılan kuru çalışma, tek koşu kilidi. Veritabanı/ağ yok."""
from contextlib import contextmanager
from datetime import datetime

from fastapi.testclient import TestClient

from app.main import app
from app.routers import skorjin_prematch as r
from app.services.skorjin_prematch import Kaynaklar

client = TestClient(app)
ANAHTAR = {"X-Cron-Anahtari": "gizli"}
URL = "/api/skorjin/prematch-hazirla"
SIMDI = datetime(2026, 10, 22, 12, 0)
MAC = {"fotmob_match_id": 1, "iddaa_event_id": None, "home_team_id": 1, "away_team_id": 2, "home_name": "A", "away_name": "B",
       "league_id": 99, "league_name": "X", "match_date": datetime(2026, 10, 25, 17, 0)}


def kur(monkeypatch, anahtar="gizli", kilit=True):
    yazilan, tablo = [], []
    monkeypatch.setattr(r, "_anahtar", lambda: anahtar)
    monkeypatch.setattr(r, "_simdi", lambda: SIMDI)

    @contextmanager
    def tek_kosu(adi="skorjin_prematch"):
        yield kilit
    monkeypatch.setattr(r, "tek_kosu", tek_kosu)
    monkeypatch.setattr(r, "tablo_hazirla", lambda: tablo.append(1))
    monkeypatch.setattr(r, "gercek_kaynaklar", lambda: Kaynaklar(
        maclar=lambda bas, son: [MAC], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {},
        iddaa=lambda e: (None, {}, {}), mevcut=lambda ids: set(), yaz=lambda kapsam, satir: yazilan.append((kapsam, satir)),
        fotmob=lambda fid: None))
    return yazilan, tablo


def test_anahtarsiz_istek_reddedilir(monkeypatch):
    kur(monkeypatch)
    assert client.post(URL).status_code == 401
    assert client.post(URL, headers={"X-Cron-Anahtari": "yanlis"}).status_code == 401


def test_sunucuda_anahtar_tanimli_degilse_kapali(monkeypatch):
    kur(monkeypatch, anahtar=None)
    assert client.post(URL, headers=ANAHTAR).status_code == 503


def test_varsayilan_kuru_calisma_yazmaz_ve_tablo_olusturmaz(monkeypatch):
    yazilan, tablo = kur(monkeypatch)
    j = client.post(URL, headers=ANAHTAR).json()
    assert j["dry_run"] is True and j["kapsam"] == "gunluk" and j["hazirlanan"] == 1
    assert yazilan == [] and tablo == []


def test_dry_run_false_tabloyu_hazirlar_ve_yazar(monkeypatch):
    yazilan, tablo = kur(monkeypatch)
    j = client.post(URL + "?dry_run=false", headers=ANAHTAR).json()
    assert j["dry_run"] is False and tablo == [1]
    assert yazilan[0][0] == "gunluk" and yazilan[0][1]["fotmob_match_id"] == 1


def test_oran_kapsami_kabul_edilir_gecersiz_kapsam_422(monkeypatch):
    kur(monkeypatch)
    assert client.post(URL + "?kapsam=oran", headers=ANAHTAR).json()["kapsam"] == "oran"
    assert client.post(URL + "?kapsam=saatlik", headers=ANAHTAR).status_code == 422


def test_baska_kosu_surerken_409(monkeypatch):
    yazilan, _ = kur(monkeypatch, kilit=False)
    assert client.post(URL + "?dry_run=false", headers=ANAHTAR).status_code == 409
    assert yazilan == []
