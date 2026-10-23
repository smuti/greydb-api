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


# ---------------------------------------------------------------- arka plan (Railway proxy 5 dk sınırı) + durum ucu
DURUM = "/api/skorjin/prematch-durum"


def arka(monkeypatch, hemen=True, **kw):
    """Arka plan işini kuyruğa alır: hemen=True -> aynı iş parçacığında hemen çalıştırır; False -> tutar (çalışıyor görünümü)."""
    yazilan, tablo = kur(monkeypatch, **kw)
    r._durum_sifirla()
    bekleyen = []
    monkeypatch.setattr(r, "_baslat", lambda fn: fn() if hemen else bekleyen.append(fn))
    return yazilan, tablo, bekleyen


def test_arka_plan_hemen_202_doner_sonra_durum_ucu_raporu_verir(monkeypatch):
    yazilan, tablo, bekleyen = arka(monkeypatch, hemen=False)
    res = client.post(URL + "?dry_run=false&arka_plan=true", headers=ANAHTAR)
    assert res.status_code == 202 and res.json()["durum"] == "basladi" and res.json()["kapsam"] == "gunluk"
    assert yazilan == [] and len(bekleyen) == 1                               # istek dönerken iş henüz yapılmadı
    cal = client.get(DURUM, headers=ANAHTAR).json()
    assert cal["durum"] == "calisiyor" and cal["dry_run"] is False and cal["kapsam"] == "gunluk" and "rapor" not in cal
    bekleyen[0]()
    bitti = client.get(DURUM, headers=ANAHTAR).json()
    assert bitti["durum"] == "bitti" and bitti["rapor"]["hazirlanan"] == 1 and bitti["rapor"]["dry_run"] is False
    assert bitti["bitis"] and tablo == [1] and yazilan[0][1]["fotmob_match_id"] == 1


def test_arka_planda_kuru_calisma_yazmaz(monkeypatch):
    yazilan, tablo, _ = arka(monkeypatch)
    assert client.post(URL + "?arka_plan=true", headers=ANAHTAR).status_code == 202
    d = client.get(DURUM, headers=ANAHTAR).json()
    assert d["durum"] == "bitti" and d["rapor"]["dry_run"] is True and yazilan == [] and tablo == []


def test_calisirken_ikinci_istek_409(monkeypatch):
    _, _, bekleyen = arka(monkeypatch, hemen=False)
    assert client.post(URL + "?arka_plan=true", headers=ANAHTAR).status_code == 202
    assert client.post(URL + "?arka_plan=true", headers=ANAHTAR).status_code == 409
    assert client.post(URL, headers=ANAHTAR).status_code == 409                # senkron istek de beklemez
    bekleyen[0]()
    assert client.post(URL + "?arka_plan=true", headers=ANAHTAR).status_code == 202   # bitince yenisi başlar


def test_baska_kopyada_kilit_alinamazsa_atlandi(monkeypatch):
    arka(monkeypatch, kilit=False)
    assert client.post(URL + "?arka_plan=true&dry_run=false", headers=ANAHTAR).status_code == 202
    assert client.get(DURUM, headers=ANAHTAR).json()["durum"] == "atlandi"


def test_arka_plan_hatasi_durumda_gorunur_ve_sonraki_koşuyu_engellemez(monkeypatch):
    arka(monkeypatch)
    def patlar():
        raise RuntimeError("db koptu")
    monkeypatch.setattr(r, "gercek_kaynaklar", patlar)
    assert client.post(URL + "?arka_plan=true", headers=ANAHTAR).status_code == 202
    d = client.get(DURUM, headers=ANAHTAR).json()
    assert d["durum"] == "hata" and "RuntimeError" in d["hata"] and "db koptu" in d["hata"]
    assert client.post(URL + "?arka_plan=true", headers=ANAHTAR).status_code == 202


def test_durum_ucu_anahtarla_korunur_ve_ilk_halde_yok(monkeypatch):
    arka(monkeypatch)
    assert client.get(DURUM).status_code == 401
    assert client.get(DURUM, headers={"X-Cron-Anahtari": "yanlis"}).status_code == 401
    assert client.get(DURUM, headers=ANAHTAR).json() == {"durum": "yok"}


def test_anahtar_tanimli_degilse_durum_ucu_kapali(monkeypatch):
    arka(monkeypatch, anahtar=None)
    assert client.get(DURUM, headers=ANAHTAR).status_code == 503
    assert client.post(URL + "?arka_plan=true", headers=ANAHTAR).status_code == 503
