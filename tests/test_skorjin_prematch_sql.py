"""Skorjin anlık görüntüsü: SQL ve tablo sözleşmesi (veritabanına bağlanmadan metin düzeyinde)."""
import json
from datetime import datetime

from app.services import skorjin_prematch as s

SATIR = {"fotmob_match_id": 1, "iddaa_event_id": 5, "home_team_id": 1, "away_team_id": 2, "home_name": "A", "away_name": "B",
         "league_id": 4, "league_name": "Süper Lig", "kickoff": datetime(2026, 10, 25, 17, 0),
         "form": {"home": [], "away": []}, "form_at": datetime(2026, 10, 22), "h2h": None, "h2h_at": None,
         "scoreline": None, "scoreline_at": None, "odds": {"home": 2.0}, "odds_at": datetime(2026, 10, 22), "crowd": None, "crowd_at": None}


def test_gunluk_upsert_tek_satir_per_mac_ve_cakismada_gunceller():
    sql, params = s.yazma_sorgusu("gunluk", SATIR)
    assert "INSERT INTO greydb.match_prematch" in sql and "ON CONFLICT (fotmob_match_id) DO UPDATE" in sql
    assert "absences" not in sql                                  # kaynak yok: tabloya yazılmaz, mevcut değere dokunulmaz
    assert json.loads(params["form"]) == {"home": [], "away": []} and params["h2h"] is None   # None -> SQL NULL, "null" JSON değil


def test_oran_yalniz_oran_alanlarini_gunceller_satir_olusturmaz():
    sql, params = s.yazma_sorgusu("oran", {"fotmob_match_id": 1, "odds": None, "odds_at": None, "crowd": {"home": .5}, "crowd_at": datetime(2026, 10, 22)})
    assert sql.lstrip().startswith("UPDATE greydb.match_prematch") and "INSERT" not in sql
    for kolon in ("form", "h2h", "scoreline"):
        assert kolon not in sql
    assert params["odds"] is None and json.loads(params["crowd"]) == {"home": 0.5}


def test_tablo_ddl_sozlesmesi():
    ddl = s.TABLO_DDL
    for parca in ("fotmob_match_id BIGINT PRIMARY KEY", "form JSONB", "h2h JSONB", "scoreline JSONB", "odds JSONB", "crowd JSONB",
                  "absences JSONB", "absences_source TEXT", "kickoff TIMESTAMPTZ", "idx_match_prematch_kickoff"):
        assert parca in ddl


def test_yaklasan_mac_sorgusu_takim_adlarini_teams_tablosundan_alir():
    assert "public.upcoming_matches" in s.MACLAR_SQL and "JOIN public.teams ev" in s.MACLAR_SQL and "fotmob_match_id IS NOT NULL" in s.MACLAR_SQL


def test_form_ve_h2h_sorgulari_yalniz_maca_kadar_bitmis_skorlu_maclari_alir():
    for sql in (s.FORM_SQL, s.H2H_SQL):
        assert "m.finished" in sql and "m.match_date < :tarih" in sql and "NOT COALESCE(m.cancelled, false)" in sql
    assert "LIMIT 20" in s.FORM_SQL and "LIMIT 10" in s.H2H_SQL
