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
    monkeypatch.setattr(r, "_simdi", lambda: KICK - timedelta(hours=5))
    monkeypatch.setattr(r, "olaylari_cek", lambda: olaylar if olaylar is not None else [
        {"i": 3222006, "bri": 7, "hn": "Tottenham ", "an": "Coventry", "d": D}])
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
    assert j["dry_run"] is False and yazilan == [{11: 3222006}]


def mac(fid, home, away, saat):
    return {"fotmob_match_id": fid, "home": home, "away": away, "match_date": saat}


def test_eslesmeyenlerin_nedeni_acik_yazilir(monkeypatch):
    # iddaa listesi KICK'e kadar uzanıyor; "şimdi" = KICK - 5 saat
    satirlar = [
        mac(11, "Tottenham Hotspur", "Coventry City", KICK),                                # eşleşir
        mac(12, "Oynanmis", "Mac", KICK - timedelta(hours=8)),                              # şimdiden önce başlamış
        mac(13, "Uzak", "Mac", KICK + timedelta(days=2)),                                   # liste ufkunun ötesinde
        mac(14, "Tottenham Hotspur", "Wolverhampton", KICK),                                # aynı saatte benzer adlı olay var, ev benziyor deplasman değil
        mac(15, "Hic", "Yok", KICK),                                                        # aynı saatte olay var ama hiçbiri benzemiyor
        mac(16, "Bos", "Saat", KICK - timedelta(hours=2)),                                  # saatte iddaa olayı yok
    ]
    kur(monkeypatch, satirlar=satirlar)
    j = client.post("/api/iddaa/eslestir", headers=ANAHTAR).json()
    neden = {m["fotmob_match_id"]: m["neden"] for m in j["eslesmeyen"]}
    assert j["eslesen"] == 1
    assert neden == {12: "basladi", 13: "pencere_disi", 14: "ad_sorunu_olasi", 15: "iddaa_de_yok", 16: "iddaa_de_yok"}
    ornek = next(m for m in j["eslesmeyen"] if m["fotmob_match_id"] == 14)
    assert "Tottenham" in ornek["aday"][0] and ornek["aciklama"]
    assert all(m["aciklama"] and m["saat"] for m in j["eslesmeyen"])


def test_ayni_maci_iki_satir_olsa_da_bir_kez_sayar(monkeypatch):
    satirlar = [mac(11, "Tottenham Hotspur", "Coventry City", KICK), mac(11, "Tottenham", "Coventry", KICK + timedelta(minutes=15))]
    kur(monkeypatch, satirlar=satirlar)
    j = client.post("/api/iddaa/eslestir", headers=ANAHTAR).json()
    assert j["toplam_mac"] == 1 and j["eslesen"] == 1


def test_rapor_eslesenleri_adlariyla_listeler(monkeypatch):
    kur(monkeypatch)
    j = client.post("/api/iddaa/eslestir", headers=ANAHTAR).json()
    assert j["eslesenler"] == [{"fotmob_match_id": 11, "mac": "Tottenham Hotspur - Coventry City", "iddaa_event_id": 3222006}]


def test_iddaa_ulasilamazsa_502_ve_yazmaz(monkeypatch):
    yazilan = kur(monkeypatch)
    def patla():
        raise RuntimeError("iddaa kapalı")
    monkeypatch.setattr(r, "olaylari_cek", patla)
    assert client.post("/api/iddaa/eslestir?dry_run=false", headers=ANAHTAR).status_code == 502
    assert yazilan == []


def test_liste_ufku_yalniz_mac_oncesi_listeden_alinir(monkeypatch):
    # Canlı listede çok ileri tarihli tek tük olay olabilir; "henüz programa girmedi" ufku bunlarla uzamaz
    olaylar = [
        {"i": 1, "bri": 1, "hn": "Tottenham ", "an": "Coventry", "d": D, "_liste": 0},
        {"i": 2, "bri": 2, "hn": "Baska", "an": "Takim", "d": D + 5 * 86400, "_liste": 1},
    ]
    satirlar = [mac(20, "Uzak", "Mac", KICK + timedelta(days=2))]
    kur(monkeypatch, olaylar=olaylar, satirlar=satirlar)
    j = client.post("/api/iddaa/eslestir", headers=ANAHTAR).json()
    assert j["eslesmeyen"][0]["neden"] == "pencere_disi"
