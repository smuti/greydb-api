"""Skorjin `oyuncu` grubu: takımın son 10 lig maçındaki oyuncu özetleri (maç başı 90 dk), eksik oyuncuların takım içi sırası ve payı. Veritabanı yok."""
from datetime import datetime

import pytest

from app.services import skorjin_oyuncu as z

MAC_GUNU = datetime(2026, 11, 8, 16, 0)


def satir(mid, gun, takim, ad, pid, mevki, dk=90, ay=10, **kw):
    s = {"mac_id": mid, "tarih": datetime(2026, ay, gun, 17, 0), "team_id": takim, "player_name": ad, "fotmob_player_id": pid, "usual_position": mevki, "minutes_played": dk,
         "rating": 7.0, "goals": 0, "assists": 0, "xg_plus_xa": 0.0, "total_shots": 0, "chances_created": 0, "accurate_passes": 40, "total_passes": 50,
         "defensive_actions": 0, "recoveries": 0, "duels_won": 3, "duels_lost": 3, "aerial_duels_won": 1, "aerial_duels_total": 2, "saves": None, "goals_conceded": None, "goals_prevented": None}
    s.update(kw)
    return s


def mac(mid, gun, takim=1, ay=10):
    """Bir takım-maçı: forvet F (üretken), orta saha M, defans D, kaleci K."""
    return [satir(mid, gun, takim, "F. Forvet", 11, 3, ay=ay, goals=1, xg_plus_xa=0.8, total_shots=4, chances_created=2),
            satir(mid, gun, takim, "M. Orta", 12, 2, ay=ay, xg_plus_xa=0.2, chances_created=3, recoveries=6, defensive_actions=5, duels_won=6, duels_lost=2),
            satir(mid, gun, takim, "D. Defans", 13, 1, ay=ay, xg_plus_xa=0.0, defensive_actions=9, recoveries=7, duels_won=7, duels_lost=1, aerial_duels_won=4, aerial_duels_total=5),
            satir(mid, gun, takim, "K. Kaleci", 14, 0, ay=ay, saves=4, goals_conceded=1, goals_prevented=0.6, rating=7.5)]


def maclar(n=4, takim=1):
    return [r for i in range(n) for r in mac(100 + i, 1 + i, takim)]


def test_oyuncu_ozeti_90_dakika_basi_ve_mevki():
    t = z.takim_oyunculari(maclar(), 1, MAC_GUNU)
    f = next(o for o in t["oyuncular"] if o["ad"] == "F. Forvet")
    assert t["n"] == 4 and f["mevki"] == "FW" and f["dk"] == 360 and f["mac"] == 4
    assert f["gol"] == 4 and f["xg_xa_90"] == 0.8 and f["sut_90"] == 4.0 and f["sans_90"] == 2.0
    d = next(o for o in t["oyuncular"] if o["ad"] == "D. Defans")
    assert d["mevki"] == "DF" and d["savunma_90"] == 9.0 and d["mucadele_pct"] == 87.5 and d["hava_pct"] == 80.0 and d["pas_isabet"] == 80.0


def test_kaleci_kurtaris_yenilen_ve_onlenen_gol_toplam():
    k = next(o for o in z.takim_oyunculari(maclar(), 1, MAC_GUNU)["oyuncular"] if o["mevki"] == "GK")
    assert k["kurtaris"] == 16 and k["yenilen_gol"] == 4 and k["onlenen_gol"] == pytest.approx(2.4) and k["puan"] == 7.5


def test_az_dakika_oyuncu_listeye_girmez_ve_puan_dakika_agirlikli():
    rows = maclar() + [satir(100, 1, 1, "Y. Yedek", 15, 2, dk=30), satir(101, 2, 1, "Y. Yedek", 15, 2, dk=30)]
    t = z.takim_oyunculari(rows, 1, MAC_GUNU)
    assert "Y. Yedek" not in [o["ad"] for o in t["oyuncular"]]                # 60 dk < 180
    r = [satir(100 + i, 1 + i, 1, "A. Oyuncu", 20, 2, dk=dk, rating=r_) for i, (dk, r_) in enumerate([(90, 8.0), (90, 6.0), (45, 9.0)])]
    o = z.takim_oyunculari(r, 1, MAC_GUNU)["oyuncular"][0]
    assert o["puan"] == pytest.approx((90 * 8 + 90 * 6 + 45 * 9) / 225, abs=0.01)


def test_yalniz_son_10_mac_ve_mac_gunnden_oncesi_ve_az_mac_none():
    eski = [r for i in range(5) for r in mac(200 + i, 1 + i, ay=8)]
    for r in eski:
        if r["player_name"] == "F. Forvet":
            r["xg_plus_xa"] = 5.0
    yeni = [r for i in range(10) for r in mac(100 + i, 1 + i, ay=9)]
    gelecek = [r for r in mac(900, 9, ay=11)]
    t = z.takim_oyunculari(eski + yeni + gelecek, 1, MAC_GUNU)
    assert t["n"] == 10 and next(o for o in t["oyuncular"] if o["ad"] == "F. Forvet")["xg_xa_90"] == 0.8
    assert z.takim_oyunculari(maclar(2), 1, MAC_GUNU) is None


def test_eksik_oyuncu_takim_ici_sira_ve_pay():
    t = z.takim_oyunculari(maclar(), 1, MAC_GUNU, [{"name": "F. Forvet", "status": "sakat"}, {"name": "Bilinmeyen Biri", "status": "cezali"}])
    e = {x["ad"]: x for x in t["eksik"]}
    assert e["F. Forvet"]["sira"] == 1 and e["F. Forvet"]["durum"] == "sakat" and e["F. Forvet"]["xg_xa_90"] == 0.8
    assert e["Bilinmeyen Biri"]["sira"] is None and e["Bilinmeyen Biri"]["durum"] == "cezali"
    assert t["eksik_pay"] == pytest.approx(0.8 / (0.8 + 0.2 + 0.0 + 0.0), abs=0.001)


def test_eksik_ad_eslemesi_buyuk_kucuk_ve_aksan_duyarsiz():
    rows = maclar()
    for r in rows:
        if r["player_name"] == "M. Orta":
            r["player_name"] = "Mikel Ortá"
    t = z.takim_oyunculari(rows, 1, MAC_GUNU, [{"name": "mikel orta", "status": "sakat"}])
    assert t["eksik"][0]["sira"] == 2


def test_oyuncu_grubu_iki_takim_ve_bos():
    rows = maclar(4, 1) + maclar(5, 2)
    g = z.oyuncu_grubu(rows, 1, 2, MAC_GUNU, None)
    assert g["home"]["n"] == 4 and g["away"]["n"] == 5
    assert z.oyuncu_grubu([], 1, 2, MAC_GUNU, None) is None


def test_sql_lig_sezon_ve_alanlar():
    s = z.OYUNCU_SQL
    assert "public.match_player_stats" in s and "m.league_id = :lig" in s and "m.finished" in s and "mps.minutes_played" in s and "mps.usual_position" in s


# ---------------------------------------------------------------- orkestrasyon (hazirla)
from app.services import skorjin_prematch as sp  # noqa: E402
from app.services.skorjin_prematch import Kaynaklar, hazirla  # noqa: E402

SIMDI = datetime(2026, 11, 7, 22, 30)


def ev_mac():
    return {"fotmob_match_id": 1001, "iddaa_event_id": None, "home_team_id": 1, "away_team_id": 2, "home_name": "Elche", "away_name": "Real Betis",
            "league_id": 4, "league_name": "LaLiga", "match_date": MAC_GUNU}


def kaynak_kur(oyuncu, yazilan, fotmob=lambda f: None):
    return Kaynaklar(maclar=lambda b, s: [ev_mac()], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {},
                     iddaa=lambda e: (None, {}, {}), mevcut=lambda ids: set(), yaz=lambda k, s: yazilan.append(s), fotmob=fotmob, oyuncu=oyuncu)


def test_gunluk_satiri_oyuncu_grubu_eksik_listesiyle_cagrilir():
    G = {"home": {"n": 10}, "away": None}
    cagrilar, yazilan = [], []
    detay = {"content": {"lineup": {"homeTeam": {"unavailable": [{"name": "Lemar", "unavailability": {"type": "injury", "expectedReturn": "x"}, "marketValue": 5}]},
                                    "awayTeam": {"unavailable": []}}}}
    r = hazirla("gunluk", False, kaynak_kur(lambda *a: cagrilar.append(a) or G, yazilan, fotmob=lambda f: detay), SIMDI, "harman-v1")
    s = yazilan[0]
    assert cagrilar[0][:4] == (4, 1, 2, MAC_GUNU) and cagrilar[0][4]["teams"][0]["players"][0]["name"] == "Lemar"
    assert s["oyuncu"] == G and s["oyuncu_at"] == SIMDI and "oyuncu" in r["maclar"][0]["alanlar"]


def test_oyuncu_hatasinda_anahtar_olmaz_ve_sayilir_kaynak_yoksa_cagrilmaz():
    def patlar(*a):
        raise RuntimeError("db")
    yazilan = []
    r = hazirla("gunluk", False, kaynak_kur(patlar, yazilan), SIMDI, "harman-v1")
    assert "oyuncu" not in yazilan[0] and r["oyuncu_hatasi"] == 1 and r["hazirlanan"] == 1
    yazilan = []
    hazirla("gunluk", False, kaynak_kur(None, yazilan), SIMDI, "harman-v1")
    assert "oyuncu" not in yazilan[0]


def test_tablo_ddl_ve_yazma_oyuncu_sutunlari():
    assert "ADD COLUMN IF NOT EXISTS oyuncu JSONB" in sp.TABLO_DDL
    sql, params = sp.yazma_sorgusu("gunluk", {"fotmob_match_id": 1001, "oyuncu": {"home": None}, "oyuncu_at": SIMDI})
    assert "CAST(:oyuncu AS jsonb)" in sql and params["oyuncu"] == '{"home": null}'
