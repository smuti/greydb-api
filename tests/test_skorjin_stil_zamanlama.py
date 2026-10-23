"""Skorjin `style` (oyun stili) ve `timing` (gol dakikaları) grupları: saf hesap + SQL metni. Veritabanı yok."""
from datetime import datetime

import pytest

from app.services import skorjin_sezon as z

MAC_GUNU = datetime(2026, 10, 25, 17, 0)


def satir(mid, gun, ev, dep, hs, as_, top=None, adv=None, ay=9):
    """top: (ev_top, dep_top); adv: dict ek sütunlar (ev_pas, dep_pas, ev_orta, ...)."""
    s = {"mac_id": mid, "tarih": datetime(2026, ay, gun, 17, 0), "home_team_id": ev, "away_team_id": dep, "home_score": hs, "away_score": as_,
         "ev_ad": f"T{ev}", "dep_ad": f"T{dep}"}
    if top:
        s["ev_top"], s["dep_top"] = top
    s.update(adv or {})
    return s


# ---------------------------------------------------------------- oyun stili
ADV = {"ev_pas": 85.0, "dep_pas": 78.0, "ev_orta": 20, "dep_orta": 10, "ev_ceza": 30, "dep_ceza": 18, "ev_dt_xg": 0.4, "dep_dt_xg": 0.2,
       "ev_oa_xg": 1.6, "dep_oa_xg": 0.8, "ev_ofsayt": 2, "dep_ofsayt": 3}


def stil_satirlari():
    # T1 evde 3 maç (top %60/40), T2 deplasmanda 3 maç; rakip T3 / T4
    return [satir(i, 1 + i, 1, 3, 2, 0, top=(60, 40), adv=ADV) for i in range(3)] + [satir(10 + i, 1 + i, 4, 2, 1, 1, top=(55, 45), adv={**ADV, "ev_pas": 80.0, "dep_pas": 90.0}) for i in range(3)]


def test_takim_stili_kendi_degerlerinin_ortalamasi():
    d = z.oyun_stili(stil_satirlari(), 1, 2, MAC_GUNU)
    assert d["home"] == {"n": 3, "possession": 60.0, "pass_accuracy": 85.0, "crosses": 20.0, "touches_in_box": 30.0, "set_piece_xg": 0.4,
                         "open_play_xg": 1.6, "offsides": 2.0}
    assert d["away"]["n"] == 3 and d["away"]["possession"] == 45.0 and d["away"]["pass_accuracy"] == 90.0 and d["away"]["offsides"] == 3.0


def test_lig_ortalamasi_tum_takim_maclarinin_havuzu():
    d = z.oyun_stili(stil_satirlari(), 1, 2, MAC_GUNU)
    assert d["league"]["n"] == 6 and d["league"]["possession"] == 50.0                 # her maçta iki taraf: ortalama %50
    assert d["league"]["crosses"] == 15.0 and d["league"]["pass_accuracy"] == pytest.approx((85 + 78) / 2 * 0.5 + (80 + 90) / 2 * 0.5, abs=0.01)


def test_eksik_alan_o_mactan_sayilmaz_ve_az_mac_null():
    rows = stil_satirlari()
    rows[0] = {**rows[0], "ev_ofsayt": None}
    d = z.oyun_stili(rows, 1, 2, MAC_GUNU)
    assert d["home"]["offsides"] == 2.0 and d["home"]["n"] == 3                         # 2 maç ortalaması, yine 2.0
    assert z.oyun_stili(stil_satirlari()[:2], 1, 99, MAC_GUNU) is None                    # 2 maç < eşik: ne takım ne lig


def test_ayni_veri_yoksa_none_gelmeyen_alanlar_none():
    rows = [satir(i, 1 + i, 1, 3, 1, 0, top=(55, 45)) for i in range(3)]                # gelişmiş istatistik yok
    d = z.oyun_stili(rows, 1, 3, MAC_GUNU)
    assert d["home"]["possession"] == 55.0 and d["home"]["crosses"] is None and d["home"]["set_piece_xg"] is None
    assert z.oyun_stili([], 1, 2, MAC_GUNU) is None
    assert z.oyun_stili([satir(1, 1, 1, 2, 1, 0)], 1, 2, MAC_GUNU) is None


def test_mac_gunnden_sonraki_maclar_sayilmaz():
    rows = stil_satirlari() + [satir(99, 25, 1, 3, 1, 0, top=(10, 90), adv=ADV, ay=10)]
    assert z.oyun_stili(rows, 1, 2, MAC_GUNU)["home"]["possession"] == 60.0


# ---------------------------------------------------------------- gol dakikaları
def olay(mid, takim, dk, ek=0):
    return {"mac_id": mid, "team_id": takim, "minute": dk, "added": ek}


def zaman_satirlari():
    # T1 evinde 3 maç; her maç sonucu = olay sayısı (kapsama kontrolü)
    return [satir(1, 1, 1, 2, 2, 1), satir(2, 8, 2, 1, 0, 1), satir(3, 15, 1, 3, 1, 0), satir(4, 15, 2, 3, 0, 0), satir(5, 20, 1, 2, 3, 0)]


OLAYLAR = [olay(1, 1, 8), olay(1, 1, 33, 2), olay(1, 2, 90, 3),            # maç1: T1 8', 33'+2 ; T2 90'+3 (son bölüm)
           olay(2, 2, 52),                                                  # maç2: T2 (evinde 2 değil: T2 ev?) -> ev=2 skor 0-1: gol T1 (dep)
           olay(3, 1, 77),                                                  # maç3: T1 gol
           olay(5, 1, 10), olay(5, 1, 46), olay(5, 1, 91)]                  # maç5: T1 3 gol, biri 91' (>90: kovaya girmez ama kapsamda sayılır)


def test_zaman_kovalari_ve_erken_10_dakika():
    olaylar = [olay(1, 1, 8), olay(1, 1, 33, 2), olay(1, 2, 90, 3), olay(2, 1, 52), olay(3, 1, 77), olay(5, 1, 10), olay(5, 1, 46), olay(5, 1, 80)]
    d = z.zamanlama(zaman_satirlari(), olaylar, 1, 2, MAC_GUNU)
    h = d["home"]
    assert h["n"] == 4 and h["scored"] == [2, 0, 1, 2, 0, 2] and h["conceded"] == [0, 0, 0, 0, 0, 1]
    assert (h["early10_scored"], h["early10_conceded"]) == (2, 0)


def test_golsuz_mac_kapsam_sayilir_olay_sayisi_skorla_uyusmayan_mac_sayilmaz():
    olaylar = [olay(1, 1, 8), olay(1, 1, 33, 2), olay(1, 2, 90, 3), olay(2, 1, 52), olay(3, 1, 77), olay(5, 1, 10)]       # maç5: skor 3-0 ama 1 olay -> kapsam yok
    assert z.zamanlama(zaman_satirlari(), olaylar, 1, 2, MAC_GUNU)["home"]["n"] == 3       # T1: maç1, maç2, maç3
    assert z.zamanlama(zaman_satirlari(), olaylar, 2, 1, MAC_GUNU)["home"]["n"] == 3       # T2: maç1, maç2, 0-0 maç4 (olay yok = kapsam var); maç5 yok


def test_90_ustu_dakika_kovaya_girmez_ama_kapsami_bozmaz():
    olaylar = [olay(5, 1, 10), olay(5, 1, 46), olay(5, 1, 91), olay(1, 1, 8), olay(1, 1, 33), olay(1, 2, 85), olay(2, 1, 52), olay(3, 1, 77)]
    d = z.zamanlama(zaman_satirlari(), olaylar, 1, 2, MAC_GUNU)
    assert d["home"]["n"] == 4 and sum(d["home"]["scored"]) == 7 - 1                    # 91' dışarıda


def test_uc_kapsamli_maci_olmayan_takim_null_ve_lig_ortalamasi_takim_macina_gore():
    olaylar = [olay(1, 1, 8), olay(1, 1, 33), olay(1, 2, 85), olay(2, 1, 52), olay(3, 1, 77), olay(5, 1, 10), olay(5, 1, 46), olay(5, 1, 80)]
    d = z.zamanlama(zaman_satirlari(), olaylar, 1, 77, MAC_GUNU)
    assert d["away"] is None and d["home"]["n"] == 4
    lg = d["league"]
    assert lg["n"] == 10                                                                 # 5 kapsamlı maç × 2 takım
    assert lg["scored"] == pytest.approx([0.2, 0.0, 0.1, 0.2, 0.0, 0.3]) and lg["early10"] == pytest.approx(0.2)
    assert z.zamanlama([], [], 1, 2, MAC_GUNU) is None


def test_zamanlama_girdiyi_degistirmez_ve_deterministik():
    rows, ev = zaman_satirlari(), [dict(o) for o in OLAYLAR]
    before = [dict(r) for r in rows]
    a = z.zamanlama(rows, ev, 1, 2, MAC_GUNU)
    assert a == z.zamanlama(rows, ev, 1, 2, MAC_GUNU) and rows == before


# ---------------------------------------------------------------- SQL
def test_sezon_sorgusu_mac_kimligi_possession_ve_gelismis_istatistikleri_getirir():
    q = z.SEZON_SQL
    for parca in ("m.id AS mac_id", "home_possession AS ev_top", "LEFT JOIN public.match_advanced_stats", "home_set_piece_xg AS ev_dt_xg",
                  "home_offsides AS ev_ofsayt", "home_touches_in_box AS ev_ceza", "home_crosses AS ev_orta", "home_pass_accuracy AS ev_pas"):
        assert parca in q


def test_olay_sorgusu_ligin_sezon_gol_olaylari():
    q = z.OLAY_SQL
    assert "event_type = 'GOAL'" in q and "m.league_id = :lig" in q and "m.finished" in q and "m.match_date >= :bas" in q and "m.match_date < :bit" in q
    assert "AS added" in q and "e.team_id" in q


def test_sezon_gruplari_style_ve_timing_icerir():
    rows = stil_satirlari()
    g = z.sezon_gruplari(rows, 1, 2, MAC_GUNU, olaylar=[])
    assert g["style"]["home"]["possession"] == 60.0 and "timing" in g
    assert z.sezon_gruplari([], 1, 2, MAC_GUNU)["style"] is None and z.sezon_gruplari([], 1, 2, MAC_GUNU)["timing"] is None
