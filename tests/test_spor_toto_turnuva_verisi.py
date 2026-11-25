"""Spor Toto maç verisi maçın oynandığı turnuvadan gelir: lig maçında lig, kupa maçında kupa (son 5 maç, istatistik, tip, korner, kart)."""
import pytest

from app.services import spor_toto_fetch as stf


def _url_topla(monkeypatch):
    urller = []
    monkeypatch.setattr(stf, "_mf_get", lambda client, url: urller.append(url) or None)
    return urller


def test_turnuva_verilince_butun_isteklere_comp_id_eklenir(monkeypatch):
    urller = _url_topla(monkeypatch)
    stf._fetch_rich_team_data(1, "Galatasaray", comp_id=10)
    assert len(urller) == 5 and all("comp_id=10" in u for u in urller)


def test_turnuva_yoksa_istekler_eskisi_gibi(monkeypatch):
    urller = _url_topla(monkeypatch)
    stf._fetch_rich_team_data(1, "Galatasaray")
    assert len(urller) == 5 and not any("comp_id" in u for u in urller)


def test_build_match_data_turnuvayi_iletir(monkeypatch):
    gelen = []
    monkeypatch.setattr(stf, "get_team_league", lambda tid: {"league_id": 4, "league_name": "Super Lig"})
    monkeypatch.setattr(stf, "_fetch_rich_team_data", lambda tid, name, comp_id=None: gelen.append(comp_id) or {"x": 1})
    stf.build_match_data({"team_id": 1, "name": "Galatasaray"}, {"team_id": 2, "name": "Aston Villa"}, "5. Hafta", comp_id=10)
    assert gelen == [10, 10]


def test_kapsam_disi_ligde_turnuva_biliniyorsa_zengin_veri_denenir(monkeypatch):
    gelen = []
    monkeypatch.setattr(stf, "get_team_league", lambda tid: {"league_id": 999, "league_name": "X"})
    monkeypatch.setattr(stf, "_fetch_rich_team_data", lambda tid, name, comp_id=None: gelen.append(comp_id) or {"x": 1})
    stf.build_match_data({"team_id": 1, "name": "A"}, {"team_id": 2, "name": "B"}, None, comp_id=34)
    assert gelen == [34, 34]


def test_eksik_bilgide_turnuva_yok():
    assert stf.mac_turnuvasi(None, 2, "2026-11-24") is None
    assert stf.mac_turnuvasi(1, 2, None) is None


def test_fikstürden_turnuva_bulunur():
    from app.services.db import query_to_df
    try:
        r = query_to_df("SELECT home_team_id, away_team_id, league_id, match_date FROM public.matches WHERE league_id = 10 ORDER BY match_date DESC LIMIT 1").iloc[0]
    except Exception:
        pytest.skip("gerçek DB yok")
    tarih = r["match_date"] + __import__("pandas").Timedelta(hours=3)          # Spor Toto saati TSİ, fikstür UTC
    assert stf.mac_turnuvasi(int(r["home_team_id"]), int(r["away_team_id"]), tarih) == 10
    assert stf.mac_turnuvasi(int(r["away_team_id"]), int(r["home_team_id"]), "2001-01-01") is None


def test_korner_kart_icin_veritabani_adi_kullanilir(monkeypatch):
    gelen = []
    monkeypatch.setattr(stf, "get_team_league", lambda tid: {"league_id": 4, "league_name": "Super Lig"})
    monkeypatch.setattr(stf, "_takim_db_adi", lambda tid: {1: "Rizespor", 2: None}[tid])
    monkeypatch.setattr(stf, "_fetch_rich_team_data", lambda tid, name, comp_id=None: gelen.append(name) or {"x": 1})
    stf.build_match_data({"team_id": 1, "name": "Çaykur Rizespor"}, {"team_id": 2, "name": "Kasımpaşa"}, None, comp_id=4)
    assert gelen == ["Rizespor", "Kasımpaşa"]          # veritabanı adı yoksa Spor Toto adı
