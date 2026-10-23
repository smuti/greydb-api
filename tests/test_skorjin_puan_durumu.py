"""Skorjin `standings` grubu: maçın liginin puan durumu (iki takımın sırası/puanı + lig bağlamı). Bitmiş maçlardan hesaplanır; veritabanı yok."""
import json
from datetime import datetime

import pytest

from app.services import skorjin_prematch as sp
from app.services import skorjin_sezon as z

MAC_GUNU = datetime(2026, 10, 25, 17, 0)


def satir(gun, ev, dep, hs, as_, ay=9):
    return {"tarih": datetime(2026, ay, gun, 17, 0), "home_team_id": ev, "away_team_id": dep, "home_score": hs, "away_score": as_,
            "ev_ad": f"T{ev}", "dep_ad": f"T{dep}"}


# Dört takım, ilk devre: T1 7 puan, T2 4, T3 2, T4 2 (T3/T4 averajla ayrılır: T3 -1, T4 -2)
LIG = [satir(1, 1, 2, 2, 0), satir(2, 3, 4, 1, 1), satir(8, 1, 3, 1, 0), satir(9, 2, 4, 3, 1), satir(15, 1, 4, 0, 0), satir(16, 2, 3, 2, 2)]


def test_iki_takimin_sirasi_ve_puani():
    d = z.puan_durumu(LIG, 2, 4, MAC_GUNU)
    assert d["home"] == {"position": 2, "played": 3, "points": 4, "wins": 1, "draws": 1, "losses": 1, "gf": 5, "ga": 5, "gd": 0}
    assert d["away"] == {"position": 4, "played": 3, "points": 2, "wins": 0, "draws": 2, "losses": 1, "gf": 2, "ga": 4, "gd": -2}


def test_esit_puanda_averaj_siralar():
    assert z.puan_durumu(LIG, 3, 4, MAC_GUNU)["home"]["position"] == 3 and z.puan_durumu(LIG, 3, 4, MAC_GUNU)["away"]["position"] == 4


def test_lig_baglami_lider_son_takim_sayisi_oynanan_araligi():
    d = z.puan_durumu(LIG, 2, 4, MAC_GUNU)
    assert d["n_teams"] == 4 and d["leader"] == {"name": "T1", "points": 7} and d["last_points"] == 2
    assert (d["played_min"], d["played_max"]) == (3, 3)


def test_oynanan_sayisi_farkli_olunca_aralik_gorunur():
    d = z.puan_durumu(LIG + [satir(22, 1, 2, 1, 0)], 2, 4, MAC_GUNU)
    assert (d["played_min"], d["played_max"]) == (3, 4)


def test_yalniz_mac_gunnden_onceki_biten_skorlu_maclar_sayilir():
    rows = LIG + [satir(25, 1, 2, 5, 0, ay=10), satir(30, 1, 2, 5, 0, ay=10), {**satir(5, 1, 2, None, None), "home_score": None}]
    assert z.puan_durumu(rows, 2, 4, MAC_GUNU) == z.puan_durumu(LIG, 2, 4, MAC_GUNU)


def test_uc_maci_olmayan_takim_tarafi_null_iki_taraf_da_yoksa_grup_null():
    az = LIG[:3]                                       # T1 2 maç, T2 1, T3 2, T4 1
    d = z.puan_durumu(LIG + [satir(22, 1, 5, 1, 0)], 5, 4, MAC_GUNU)
    assert d["home"] is None and d["away"]["position"] == 4
    assert z.puan_durumu(az, 2, 4, MAC_GUNU) is None
    assert z.puan_durumu([], 2, 4, MAC_GUNU) is None


def test_girdi_degismez_ve_deterministik():
    kopya = [dict(r) for r in LIG]
    assert z.puan_durumu(LIG, 2, 4, MAC_GUNU) == z.puan_durumu(LIG, 2, 4, MAC_GUNU) and LIG == kopya


def test_sezon_gruplari_standings_icerir():
    g = z.sezon_gruplari(LIG, 2, 4, MAC_GUNU)
    assert g["standings"]["home"]["position"] == 2
    assert z.sezon_gruplari([], 2, 4, MAC_GUNU)["standings"] is None


# ---------------------------------------------------------------- tablo ve yazma
def test_tablo_ddl_ve_yazma():
    for parca in ("standings JSONB", "standings_at TIMESTAMPTZ", "ADD COLUMN IF NOT EXISTS standings JSONB", "ADD COLUMN IF NOT EXISTS standings_at TIMESTAMPTZ"):
        assert parca in sp.TABLO_DDL
    sql, params = sp.yazma_sorgusu("gunluk", {"fotmob_match_id": 1, "standings": {"n_teams": 4}, "standings_at": datetime(2026, 10, 22)})
    assert "standings = EXCLUDED.standings" in sql and json.loads(params["standings"]) == {"n_teams": 4}
    sql, _ = sp.yazma_sorgusu("oran", {"fotmob_match_id": 1, "odds": None, "odds_at": None, "standings": {"x": 1}})
    assert "standings" not in sql


def test_orkestrasyon_standings_alani_zaman_damgasiyla_yazilir():
    simdi = datetime(2026, 10, 22, 12, 0)
    mac = {"fotmob_match_id": 1001, "iddaa_event_id": None, "home_team_id": 2, "away_team_id": 4, "home_name": "T2", "away_name": "T4",
           "league_id": 4, "league_name": "Süper Lig", "match_date": MAC_GUNU}
    yazilan = []
    k = sp.Kaynaklar(maclar=lambda b, s: [mac], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {}, iddaa=lambda e: (None, {}, {}),
                     mevcut=lambda ids: set(), yaz=lambda c, s: yazilan.append(s), fotmob=lambda f: None,
                     sezon=lambda lig, ev, dep, t: z.sezon_gruplari(LIG, ev, dep, t))
    r = sp.hazirla("gunluk", False, k, simdi, "harman-v1")
    s = yazilan[0]
    assert s["standings"]["home"]["position"] == 2 and s["standings_at"] == simdi
    assert "standings" in r["maclar"][0]["alanlar"]
    k2 = sp.Kaynaklar(maclar=lambda b, s: [mac], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {}, iddaa=lambda e: (None, {}, {}),
                      mevcut=lambda ids: set(), yaz=lambda c, s: yazilan.append(s), fotmob=lambda f: None, sezon=lambda *a: z.sezon_gruplari([], 2, 4, MAC_GUNU))
    sp.hazirla("gunluk", False, k2, simdi, "harman-v1")
    assert yazilan[-1]["standings"] is None and yazilan[-1]["standings_at"] is None
