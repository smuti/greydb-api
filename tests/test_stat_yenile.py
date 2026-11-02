"""skorjin_stat.oyuncu_json_mat yenileme: günlük zamanlayıcı işi ve anahtarlı kuru/yazan uç. Veritabanı yok."""
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import stat_yenile as r
from app.services import stat_yenile as s
from app.services import zamanlayici as Z

client = TestClient(app)
ANAHTAR = {"X-Cron-Anahtari": "gizli"}


def test_sql_eszamanli_yenileme_ve_yetki_sorgusu():
    assert s.YENILE_SQL == "REFRESH MATERIALIZED VIEW CONCURRENTLY skorjin_stat.oyuncu_json_mat"
    assert "pg_matviews" in s.YETKI_SQL and "oyuncu_json_mat" in s.YETKI_SQL and "pg_has_role" in s.YETKI_SQL


def test_kuru_yalniz_yetkiyi_okur_yenilemez():
    yenilenen = []
    rapor = s.calistir(True, durum=lambda: {"sahip": "postgres", "kullanici": "postgres", "yetkili": True}, yenile=lambda: yenilenen.append(1))
    assert yenilenen == [] and rapor["dry_run"] is True and rapor["yetkili"] is True and rapor["yenilendi"] is False


def test_yazan_yenileme_yapar_ve_sureyi_raporlar():
    yenilenen = []
    rapor = s.calistir(False, durum=lambda: {"sahip": "postgres", "kullanici": "postgres", "yetkili": True}, yenile=lambda: yenilenen.append(1))
    assert yenilenen == [1] and rapor["yenilendi"] is True and rapor["sure_sn"] >= 0


def test_yetkisizse_yenilemeyi_denemez():
    yenilenen = []
    rapor = s.calistir(False, durum=lambda: {"sahip": "baska", "kullanici": "uygulama", "yetkili": False}, yenile=lambda: yenilenen.append(1))
    assert yenilenen == [] and rapor["yenilendi"] is False and "yetki" in rapor["neden"]


def test_gorunum_yoksa_denemez():
    rapor = s.calistir(False, durum=lambda: None, yenile=lambda: pytest.fail("çalışmamalı"))
    assert rapor["yenilendi"] is False and "yok" in rapor["neden"]


def test_zamanlayici_gunluk_is_ve_hatayi_yutar(monkeypatch):
    assert {ad: kw for ad, kw, _ in Z.isler()}["oyuncu-json-mat"] == dict(hour="2", minute="30")
    monkeypatch.setattr(s, "calistir", lambda dry_run, **kw: (_ for _ in ()).throw(RuntimeError("db yok")))
    dict((ad, is_) for ad, _, is_ in Z.isler())["oyuncu-json-mat"]()            # istisna dışarı çıkmamalı


def kur(monkeypatch, anahtar="gizli"):
    cagri = []
    monkeypatch.setattr(r, "_anahtar", lambda: anahtar)
    monkeypatch.setattr(r.s, "calistir", lambda dry_run, **kw: cagri.append(dry_run) or {"dry_run": dry_run})
    return cagri


def test_uc_korumali_ve_varsayilan_kuru(monkeypatch):
    cagri = kur(monkeypatch)
    assert client.post("/api/skorjin/stat-yenile").status_code == 401
    assert client.post("/api/skorjin/stat-yenile", headers=ANAHTAR).json() == {"dry_run": True}
    assert client.post("/api/skorjin/stat-yenile?dry_run=false", headers=ANAHTAR).json() == {"dry_run": False}
    assert cagri == [True, False]
    kur(monkeypatch, anahtar=None)
    assert client.post("/api/skorjin/stat-yenile", headers=ANAHTAR).status_code == 503
