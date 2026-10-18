"""Maçlar sayfasındaki "Analiz" rozeti: yayında (status='active') analizi olan maçların FotMob kimlikleri.
Yalnızca kimlik döner; ipucu/oran/içerik sızmaz (kademe kilidi sunucuda henüz yok)."""
from fastapi.testclient import TestClient

from app.main import app
from app.routers import analysis_matches as am

client = TestClient(app)


def test_yayindaki_analizlerin_mac_kimliklerini_doner(monkeypatch):
    monkeypatch.setattr(am, "execute_query", lambda sql, params=None: [{"match_fotmob_id": 4813530}, {"match_fotmob_id": 4813531}])
    r = client.get("/api/analysis-matches?date=2026-10-18")
    assert r.status_code == 200 and r.json() == {"match_ids": [4813530, 4813531]}


def test_yalnizca_yayindaki_ve_kimligi_olan_analizler_gunun_penceresinde_istenir(monkeypatch):
    kayit = {}

    def sahte(sql, params=None):
        kayit["sql"], kayit["params"] = sql, params
        return []

    monkeypatch.setattr(am, "execute_query", sahte)
    client.get("/api/analysis-matches?date=2026-10-18")
    sql = kayit["sql"].lower()
    assert "status = 'active'" in sql and "match_fotmob_id is not null" in sql and "greydb.predictions" in sql
    assert ":bas" in sql and ":bit" in sql, "tarih parametreli olmalı (f-string yok)"
    # saat dilimi farkı için bir gün önce/bir gün sonrasıyla geniş pencere: [17 Eki, 20 Eki)
    assert str(kayit["params"]["bas"])[:10] == "2026-10-17" and str(kayit["params"]["bit"])[:10] == "2026-10-20"


def test_ayni_mac_birden_cok_analizde_bir_kez_gelir(monkeypatch):
    monkeypatch.setattr(am, "execute_query", lambda sql, params=None: [{"match_fotmob_id": 7}, {"match_fotmob_id": 7}])
    assert client.get("/api/analysis-matches?date=2026-10-18").json() == {"match_ids": [7]}


def test_gecersiz_tarih_422(monkeypatch):
    monkeypatch.setattr(am, "execute_query", lambda sql, params=None: [])
    assert client.get("/api/analysis-matches?date=18-10-2026").status_code == 422
    assert client.get("/api/analysis-matches").status_code == 422


def test_veritabani_hatasinda_rozet_susar_sayfa_cokmez(monkeypatch):
    def patla(sql, params=None):
        raise RuntimeError("db yok")

    monkeypatch.setattr(am, "execute_query", patla)
    r = client.get("/api/analysis-matches?date=2026-10-18")
    assert r.status_code == 200 and r.json() == {"match_ids": []}
