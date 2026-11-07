"""Canlı korner izleme uçları: yazma/okuma yalnız admin (ağ geçidi anahtarı + X-Yetki: admin); elle sonuçlandırma X-Cron-Anahtari ister. Veritabanı sahte."""
from fastapi.testclient import TestClient

from app.main import app
from app.routers import canli_korner as r
from app.services import canli_korner_izleme as c
from app.services import zamanlayici as Z

client = TestClient(app)
GW = {"X-Sunucu-Anahtari": "gizli"}
ADMIN = {**GW, "X-Yetki": "admin", "X-Kullanici-Email": "editor@greysports.tv"}
GOVDE = {"fotmob_match_id": 1, "model_surum": "canli-korner-1.0", "faz": "ilk", "dakika": 10, "skor": {"ev": 0, "dep": 0}, "korner": {"ev": 1, "dep": 0}, "kirmizi": {"ev": 0, "dep": 0},
         "onceki": {"ev": 5.0, "dep": 4.0, "kaynak": "yerel"}, "hedefler": {"ev": {"ortalama": 5.0, "mevcut": 1, "olasilik": {"4": 0.7}}}}


def test_kayit_yalniz_admin(monkeypatch):
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    yazilan = []
    monkeypatch.setattr(r, "db_ekle", lambda k: yazilan.append(k) or 7)
    assert client.post("/api/skorjin/canli-korner/kayit", json=GOVDE).status_code == 401
    assert client.post("/api/skorjin/canli-korner/kayit", json=GOVDE, headers=GW).status_code == 403
    o = client.post("/api/skorjin/canli-korner/kayit", json=GOVDE, headers=ADMIN)
    assert o.status_code == 200 and o.json() == {"yazildi": True} and yazilan[0]["kova"] == 2


def test_kayit_ayni_kova_ikinci_kez_yazilmaz(monkeypatch):
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    monkeypatch.setattr(r, "db_ekle", lambda k: None)          # çakışma: yazılmadı
    assert client.post("/api/skorjin/canli-korner/kayit", json=GOVDE, headers=ADMIN).json() == {"yazildi": False}


def test_kayit_gecersiz_govde_422(monkeypatch):
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    monkeypatch.setattr(r, "db_ekle", lambda k: pytest_fail())
    assert client.post("/api/skorjin/canli-korner/kayit", json={**GOVDE, "faz": "x"}, headers=ADMIN).status_code == 422


def pytest_fail():
    raise AssertionError("yazılmamalı")


def test_ozet_yalniz_admin_ve_gun_siniri(monkeypatch):
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    monkeypatch.setattr(r, "db_ozet_satirlari", lambda gun: [])
    assert client.get("/api/skorjin/canli-korner/ozet").status_code == 401
    assert client.get("/api/skorjin/canli-korner/ozet", headers=GW).status_code == 403
    o = client.get("/api/skorjin/canli-korner/ozet?gun=30", headers=ADMIN)
    assert o.status_code == 200 and o.json()["kayit"] == 0 and o.json()["gun"] == 30
    assert client.get("/api/skorjin/canli-korner/ozet?gun=0", headers=ADMIN).status_code == 422


def test_sonuc_ucu_cron_anahtari_ister_varsayilan_kuru(monkeypatch):
    monkeypatch.setenv("CRON_ANAHTARI", "cron")
    cagri = []
    monkeypatch.setattr(c, "calistir", lambda dry_run: cagri.append(dry_run) or {"islenen": 0})
    assert client.post("/api/skorjin/canli-korner/sonuc").status_code == 401
    assert client.post("/api/skorjin/canli-korner/sonuc", headers={"X-Cron-Anahtari": "cron"}).status_code == 200
    client.post("/api/skorjin/canli-korner/sonuc?dry_run=false", headers={"X-Cron-Anahtari": "cron"})
    assert cagri == [True, False]


def test_zamanlayicida_sonuc_isi_var():
    t = {ad: kw for ad, kw, _ in Z.isler()}
    assert t["canli-korner-sonuc"] == dict(hour="*/2", minute="55")


def test_sql_cakisan_kovayi_atlar():
    assert "ON CONFLICT (fotmob_match_id, faz, kova) DO NOTHING RETURNING id" in c.YAZ_SQL
    assert "sonuclandi IS NULL" in c.BEKLEYEN_SQL
