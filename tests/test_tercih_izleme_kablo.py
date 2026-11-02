"""Tercih izleme bağlantıları: gerçek kaynakların SQL'i, zamanlayıcı işleri, anahtarla korunan kuru/yazan uç. Veritabanı ve ağ yok."""
from datetime import datetime

from fastapi.testclient import TestClient

from app.main import app
from app.routers import tercih_izleme as r
from app.services import tercih_izleme as t
from app.services import zamanlayici as Z

client = TestClient(app)
ANAHTAR = {"X-Cron-Anahtari": "gizli"}
SIMDI = datetime(2026, 11, 2, 10, 0)


def test_sql_yazma_cakisan_satiri_atlar_ve_id_dondurur():
    assert "ON CONFLICT (fotmob_match_id, pencere, kategori, secim) DO NOTHING RETURNING id" in t.YAZ_SQL
    assert "INSERT INTO greydb.tercih_izleme" in t.YAZ_SQL


def test_sql_bekleyen_yalniz_islenmemisleri_okur():
    assert "sonuclandi IS NULL" in t.BEKLEYEN_SQL and "kickoff < :son" in t.BEKLEYEN_SQL
    assert "UPDATE greydb.tercih_izleme SET sonuc = :sonuc, sonuclandi = :an WHERE id = :id" in t.SONUC_SQL
    assert "fotmob_match_id = ANY(:ids)" in t.MEVCUT_SQL


def test_calistir_kaydet_ve_sonuc_kaynaklari_kullanir(monkeypatch):
    cagri = []
    monkeypatch.setattr(t, "gercek_kaynaklar", lambda: "KAYNAK")
    monkeypatch.setattr(t, "kaydet", lambda simdi, k, dry_run: cagri.append(("kaydet", k, dry_run)) or {"yazilan": 3})
    monkeypatch.setattr(t, "sonuclandir_bekleyenler", lambda simdi, k, dry_run: cagri.append(("sonuc", k, dry_run)) or {"islenen": 2})
    assert t.calistir("kaydet", dry_run=False) == {"yazilan": 3}
    assert t.calistir("sonuc", dry_run=True) == {"islenen": 2}
    assert cagri == [("kaydet", "KAYNAK", False), ("sonuc", "KAYNAK", True)]


def test_calistir_bilinmeyen_is_hata():
    import pytest
    with pytest.raises(ValueError):
        t.calistir("baska", dry_run=True)


def test_zamanlayici_isleri():
    tablo = {ad: kw for ad, kw, _ in Z.isler()}
    assert tablo["tercih-kaydet"] == dict(hour="5-22", minute="10,40")
    assert tablo["tercih-sonuc"] == dict(hour="*/2", minute="50")


def test_zamanlayici_is_hatayi_yutar(monkeypatch):
    monkeypatch.setattr(t, "calistir", lambda is_, dry_run=False: (_ for _ in ()).throw(RuntimeError("db yok")))
    dict((ad, is_) for ad, _, is_ in Z.isler())["tercih-kaydet"]()           # istisna dışarı çıkmamalı (zamanlayıcı iş parçacığı ölmesin)


# ---- uç ----
def kur(monkeypatch, anahtar="gizli"):
    cagri = []
    monkeypatch.setattr(r, "_anahtar", lambda: anahtar)
    monkeypatch.setattr(r.t, "calistir", lambda is_, dry_run=True: cagri.append((is_, dry_run)) or {"is": is_, "dry_run": dry_run})
    return cagri


def test_uc_anahtarsiz_reddedilir_ve_anahtar_tanimsizsa_kapali(monkeypatch):
    kur(monkeypatch)
    assert client.post("/api/skorjin/tercih-izleme/kaydet").status_code == 401
    assert client.post("/api/skorjin/tercih-izleme/kaydet", headers={"X-Cron-Anahtari": "yanlis"}).status_code == 401
    kur(monkeypatch, anahtar=None)
    assert client.post("/api/skorjin/tercih-izleme/kaydet", headers=ANAHTAR).status_code == 503


def test_uc_varsayilan_kuru_yazmak_icin_dry_run_false(monkeypatch):
    cagri = kur(monkeypatch)
    assert client.post("/api/skorjin/tercih-izleme/kaydet", headers=ANAHTAR).json() == {"is": "kaydet", "dry_run": True}
    assert client.post("/api/skorjin/tercih-izleme/sonuc?dry_run=false", headers=ANAHTAR).json() == {"is": "sonuc", "dry_run": False}
    assert cagri == [("kaydet", True), ("sonuc", False)]


def test_uc_gecersiz_is_422(monkeypatch):
    kur(monkeypatch)
    assert client.post("/api/skorjin/tercih-izleme/baska", headers=ANAHTAR).status_code == 422
