"""Gol karnesi: tablo sözleşmesi, yazma sorgusu (maç başlayınca donar), kayıt satırı ve rapor birleştirme. Veritabanı/ağ yok."""
import json
from datetime import datetime

import pytest

from app.services import gol_karne as g

SIMDI = datetime(2026, 10, 28, 12, 0)
MAC = {"fotmob_match_id": 77, "home_team_id": 1, "away_team_id": 2, "home_name": "A", "away_name": "B", "league_id": 8,
       "league_name": "Serie A", "match_date": datetime(2026, 10, 30, 19, 45)}


def m11(kutle):
    m = [[0.0] * 11 for _ in range(11)]
    for (i, j), p in kutle.items():
        m[i][j] = p
    return m


SONUC = {"score_matrix": m11({(1, 0): 0.6, (1, 1): 0.4}), "p1": 0.6, "p0": 0.4, "p2": 0.0, "lam_h": 1.4, "lam_a": 0.9,
         "model_version": "harman-v1-2026-10-m2", "as_of": "2026-10-28", "new_team_home": False, "new_team_away": False}


# ---------------------------------------------------------------- tablo
def test_tablo_ddl_sozlesmesi():
    for parca in ("CREATE TABLE IF NOT EXISTS greydb.gol_karne_tahmin", "PRIMARY KEY (fotmob_match_id, model_version, kaynak)",
                  "matrix JSONB NOT NULL", "kickoff TIMESTAMPTZ NOT NULL", "kaynak TEXT NOT NULL", "model_version TEXT NOT NULL",
                  "idx_gol_karne_kickoff"):
        assert parca in g.TABLO_DDL


# ---------------------------------------------------------------- kayıt satırı
def test_kayit_satiri_alanlari_matchforge_sonucundan_alir():
    s = g.tahmin_satiri(MAC, SONUC, SIMDI)
    assert s["fotmob_match_id"] == 77 and s["model_version"] == "harman-v1-2026-10-m2" and s["kaynak"] == "canli"
    assert s["kickoff"] == MAC["match_date"] and s["league_id"] == 8 and s["matrix"] == SONUC["score_matrix"]
    assert (s["p1"], s["p0"], s["p2"], s["lam_h"], s["lam_a"]) == (0.6, 0.4, 0.0, 1.4, 0.9) and s["as_of"] == "2026-10-28"


@pytest.mark.parametrize("sonuc", [None, {}, {**SONUC, "score_matrix": None}, {**SONUC, "new_team_home": True}, {**SONUC, "new_team_away": True}])
def test_matris_yoksa_ya_da_yeni_takimsa_kayit_yok(sonuc):
    assert g.tahmin_satiri(MAC, sonuc, SIMDI) is None


def test_baslamis_mac_icin_kayit_yok():
    gec = {**MAC, "match_date": datetime(2026, 10, 28, 11, 0)}
    assert g.tahmin_satiri(gec, SONUC, SIMDI) is None


def test_gecersiz_matris_kayit_acmaz():
    assert g.tahmin_satiri(MAC, {**SONUC, "score_matrix": [[1.0]]}, SIMDI) is None


# ---------------------------------------------------------------- yazma
def test_yazma_sorgusu_maca_kadar_gunceller_basladiktan_sonra_dondurur():
    sql, params = g.yazma_sorgusu(g.tahmin_satiri(MAC, SONUC, SIMDI))
    assert "INSERT INTO greydb.gol_karne_tahmin" in sql
    assert "ON CONFLICT (fotmob_match_id, model_version, kaynak) DO UPDATE" in sql
    assert "WHERE greydb.gol_karne_tahmin.kickoff > now()" in sql          # başlamış maçın kaydı değişmez
    assert json.loads(params["matrix"]) == SONUC["score_matrix"] and params["model_version"] == "harman-v1-2026-10-m2"


# ---------------------------------------------------------------- sonuç sorgusu ve rapor
def test_sonuc_sorgusu_bitmis_skorlu_maclarla_birlestirir():
    sql = g.SONUC_SQL
    assert "greydb.gol_karne_tahmin" in sql and "JOIN public.matches m ON m.fotmob_match_id = t.fotmob_match_id" in sql
    assert "m.finished" in sql and "NOT COALESCE(m.cancelled, false)" in sql and "m.home_score IS NOT NULL" in sql
    assert "t.matrix" not in sql.split("WHERE")[1]                          # matris filtrelenmez, yalnızca seçilir


def satir_db(matris, hs, as_, surum="m2", kaynak="canli"):
    return {"fotmob_match_id": 1, "matrix": matris, "home_score": hs, "away_score": as_, "model_version": surum, "kaynak": kaynak}


def test_rapor_her_maci_pazarlara_acar_ve_puanlar():
    r = g.rapor([satir_db(m11({(2, 0): 1.0}), 2, 0), satir_db(m11({(0, 0): 1.0}), 3, 1)])
    assert r["mac"] == 2 and r["atlanan"] == 0
    toplam25 = [t for t in r["tablo"] if t["pazar"] == "toplam_ust_2.5"][0]
    assert toplam25["n"] == 2 and toplam25["ort_p"] == pytest.approx(0.0) and toplam25["gercek"] == pytest.approx(0.5)
    assert r["uyarilar"] == []                                              # n=2: uyarı için çok az


def test_rapor_bozuk_matrisi_atlar_ve_sayar():
    r = g.rapor([satir_db(m11({(1, 1): 1.0}), 1, 1), satir_db([[1.0]], 1, 0), satir_db(m11({(1, 1): 0.5}), 1, 0)])
    assert r["mac"] == 1 and r["atlanan"] == 2


def test_rapor_bos_girdi():
    assert g.rapor([]) == {"mac": 0, "atlanan": 0, "tablo": [], "uyarilar": []}
