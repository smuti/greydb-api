"""Skorjin sezon grupları (season, league_avg, season_matches): saf hesap, SQL ve orkestrasyon (veritabanı, ağ yok)."""
import json
from datetime import datetime

import pytest

from app.services import skorjin_prematch as sp
from app.services import skorjin_sezon as z
from app.services.skorjin_prematch import Kaynaklar, hazirla

MAC_GUNU = datetime(2026, 10, 25, 17, 0)


def yakin(x, abs=1e-3):
    """Değerler 3 ondalığa yuvarlanarak yazılır."""
    return pytest.approx(x, abs=abs)

YILDIZ, KUZEY = 1, 2


def satir(gun, ev, dep, hs, as_, st=None, xg=None, ay=9):
    """st: (ev_sut, dep_sut, ev_isabetli, dep_isabetli, ev_korner, dep_korner, ev_sari, dep_sari); xg: (ev, dep)"""
    s = {"tarih": datetime(2026, ay, gun, 17, 0), "home_team_id": ev, "away_team_id": dep, "home_score": hs, "away_score": as_,
         "ev_ad": f"T{ev}", "dep_ad": f"T{dep}",
         "ev_sut": None, "dep_sut": None, "ev_isabetli": None, "dep_isabetli": None, "ev_korner": None, "dep_korner": None,
         "ev_sari": None, "dep_sari": None, "xg_ev": None, "xg_dep": None}
    if st:
        s.update(dict(zip(("ev_sut", "dep_sut", "ev_isabetli", "dep_isabetli", "ev_korner", "dep_korner", "ev_sari", "dep_sari"), st)))
    if xg:
        s["xg_ev"], s["xg_dep"] = xg
    return s


def lig(n=6):
    """Yıldız(1) 3 maç: ev 2-0 (xG 1.8-0.4, şut verili), dep 1-1, ev 0-1; Kuzey(2) ve diğerleri ile."""
    return [
        satir(1, 1, 3, 2, 0, st=(14, 6, 6, 2, 7, 2, 1, 2), xg=(1.8, 0.4)),
        satir(8, 4, 1, 1, 1, st=(9, 10, 3, 4, 3, 5, 2, 2), xg=(1.0, 1.2)),
        satir(15, 1, 5, 0, 1, st=(8, 12, 2, 5, 4, 6, 3, 1)),
        satir(2, 2, 6, 3, 1),
        satir(9, 6, 2, 0, 0),
        satir(16, 2, 7, 1, 2),
    ][:n]


# ---------------------------------------------------------------- takım sezonu
def test_takim_sezonu_ev_ve_deplasman_ayri_puan_ve_gol():
    s = z.takim_sezonu(lig(), YILDIZ, MAC_GUNU)
    assert s["played"] == 3 and s["points"] == 4
    assert s["h"] == {"played": 2, "points": 3, "gf": 2, "ga": 1}
    assert s["a"] == {"played": 1, "points": 1, "gf": 1, "ga": 1}


def test_takim_sezonu_sut_korner_yalniz_verisi_olan_maclardan_ve_n_ile():
    s = z.takim_sezonu(lig(), YILDIZ, MAC_GUNU)
    assert (s["shots_for"], s["shots_against"]) == (14 + 10 + 8, 6 + 9 + 12)       # 2. maçta takım deplasmanda
    assert (s["sot_for"], s["sot_against"]) == (6 + 4 + 2, 2 + 3 + 5)
    assert (s["corners_for"], s["corners_against"]) == (7 + 5 + 4, 2 + 3 + 6)
    assert s["stats_n"] == 3


def test_xg_yalniz_xg_olan_maclarda_toplanir_n_ile_ve_yoksa_null():
    s = z.takim_sezonu(lig(), YILDIZ, MAC_GUNU)
    assert s["xg_for"] == yakin(1.8 + 1.2) and s["xg_against"] == yakin(0.4 + 1.0) and s["xg_n"] == 2
    bos = z.takim_sezonu(lig()[:0] + [satir(i, 1, 3, 1, 0) for i in (1, 2, 3)], YILDIZ, MAC_GUNU)
    assert bos["xg_for"] is None and bos["xg_against"] is None and bos["xg_n"] == 0
    assert bos["shots_for"] is None and bos["stats_n"] == 0                        # 0 uydurulmaz


def test_sifir_toplam_sut_korner_veri_yok_sayilir():
    s = z.takim_sezonu([satir(i, 1, 3, 1, 0, st=(0, 0, 0, 0, 0, 0, 0, 0)) for i in (1, 2, 3)], YILDIZ, MAC_GUNU)
    assert s["stats_n"] == 0 and s["shots_for"] is None


def test_en_az_uc_mac_yoksa_grup_null():
    assert z.takim_sezonu(lig(2)[:2], YILDIZ, MAC_GUNU) is None
    assert z.takim_sezonu([], YILDIZ, MAC_GUNU) is None


def test_yalniz_mac_gunnden_onceki_maclar_sayilir():
    rows = lig() + [satir(25, 1, 3, 5, 0, ay=10), satir(30, 1, 3, 5, 0, ay=10)]   # maç günü ve sonrası
    assert z.takim_sezonu(rows, YILDIZ, MAC_GUNU)["played"] == 3
    assert z.takim_sezonu(rows, YILDIZ, datetime(2026, 9, 10)) is None            # o güne kadar 2 maç: eşik altı


def test_skorsuz_maclar_sayilmaz():
    rows = lig() + [satir(20, 1, 3, None, None)]
    assert z.takim_sezonu(rows, YILDIZ, MAC_GUNU)["played"] == 3


# ---------------------------------------------------------------- lig ortalaması
def test_lig_ortalamasi_ev_ve_deplasman_taraflari_ayri_mac_basina():
    a = z.lig_ortalamasi(lig(), MAC_GUNU)
    assert a["n"] == 6
    ev = a["h"]
    assert ev["gf"] == yakin((2 + 1 + 0 + 3 + 0 + 1) / 6) and ev["ga"] == yakin((0 + 1 + 1 + 1 + 0 + 2) / 6)
    assert ev["points"] == yakin((3 + 1 + 0 + 3 + 1 + 0) / 6)
    assert a["a"]["gf"] == yakin(ev["ga"]) and a["a"]["points"] == yakin((0 + 1 + 3 + 0 + 1 + 3) / 6)


def test_lig_ortalamasi_sut_xg_yalniz_verisi_olan_maclarin_ortalamasi():
    a = z.lig_ortalamasi(lig(), MAC_GUNU)
    assert a["stats_n"] == 3 and a["xg_n"] == 2
    assert a["h"]["shots_for"] == yakin((14 + 9 + 8) / 3, abs=1e-3) and a["h"]["shots_against"] == yakin((6 + 10 + 12) / 3, abs=1e-3)
    assert a["h"]["xg_for"] == yakin((1.8 + 1.0) / 2) and a["a"]["xg_for"] == yakin((0.4 + 1.2) / 2)


def test_lig_ortalamasi_xg_yoksa_alan_null_ve_az_mac_null():
    a = z.lig_ortalamasi([satir(i, 1, 3, 1, 0) for i in (1, 2, 3)], MAC_GUNU)
    assert a["h"]["xg_for"] is None and a["h"]["shots_for"] is None and a["xg_n"] == 0
    assert z.lig_ortalamasi(lig(2)[:2], MAC_GUNU) is None


# ---------------------------------------------------------------- sezon maçları
def test_sezon_maclari_en_yeniden_eskiye_form_bicimi_xg_ve_istatistikle():
    m = z.sezon_maclari(lig(), YILDIZ, MAC_GUNU)
    assert [x["d"] for x in m] == ["2026-09-15", "2026-09-08", "2026-09-01"]
    assert m[0] == {"d": "2026-09-15", "v": "H", "gf": 0, "ga": 1, "opp": "T5", "st": {"shots": [8, 12], "sot": [2, 5], "corners": [4, 6], "yellow": [3, 1]}}
    assert m[1]["v"] == "A" and m[1]["xg_for"] == yakin(1.2) and m[1]["xg_against"] == yakin(1.0)
    assert m[1]["st"]["shots"] == [10, 9]
    assert "xg_for" not in m[0]


def test_sezon_maclari_maci_yoksa_bos_liste():
    assert z.sezon_maclari([], YILDIZ, MAC_GUNU) == []


# ---------------------------------------------------------------- bir arada
def test_gruplar_iki_takim_icin_ve_eksik_taraf_null():
    g = z.sezon_gruplari(lig(), YILDIZ, KUZEY, MAC_GUNU)
    assert g["season"]["home"]["played"] == 3 and g["season"]["away"] is not None
    assert g["league_avg"]["n"] == 6
    assert [x["d"] for x in g["season_matches"]["home"]][0] == "2026-09-15"
    az = z.sezon_gruplari(lig(), YILDIZ, 99, MAC_GUNU)
    assert az["season"]["away"] is None and az["season"]["home"] is not None             # yeni takım: tarafın değeri null
    assert az["season_matches"]["away"] == []


def test_hic_veri_yoksa_gruplar_null():
    g = z.sezon_gruplari([], YILDIZ, KUZEY, MAC_GUNU)
    assert g == {"season": None, "league_avg": None, "season_matches": None}


def test_sezon_araligi_mac_gununden_turetilir():
    assert z.sezon_icin(datetime(2026, 10, 25)) == 2026 and z.sezon_icin(datetime(2027, 3, 1)) == 2026


# ---------------------------------------------------------------- SQL ve tablo
def test_sezon_sorgusu_yalniz_ligin_bitmis_skorlu_sezon_maclari_ve_xg():
    q = z.SEZON_SQL
    assert "m.league_id = :lig" in q and "m.finished" in q and "NOT COALESCE(m.cancelled, false)" in q
    assert "m.match_date >= :bas" in q and "m.match_date < :bit" in q
    assert "LEFT JOIN public.match_stats" in q and "LEFT JOIN public.ms_match_xg" in q
    for kolon in ("xg_home AS xg_ev", "xg_away AS xg_dep", "home_shots AS ev_sut", "away_yellow_cards AS dep_sari"):
        assert kolon in q


def test_tablo_ddl_yeni_kolonlari_ekler_ve_mevcut_tabloyu_alter_eder():
    ddl = sp.TABLO_DDL
    for kolon in ("season JSONB", "season_at TIMESTAMPTZ", "league_avg JSONB", "league_avg_at TIMESTAMPTZ",
                  "season_matches JSONB", "season_matches_at TIMESTAMPTZ"):
        assert kolon in ddl
        assert f"ADD COLUMN IF NOT EXISTS {kolon}" in ddl


def test_gunluk_yazma_yeni_gruplari_json_olarak_yazar_oran_kapsami_dokunmaz():
    satir_ = {"fotmob_match_id": 1, "season": {"home": None}, "season_at": datetime(2026, 10, 22), "league_avg": None, "league_avg_at": None}
    sql, params = sp.yazma_sorgusu("gunluk", satir_)
    assert "season = EXCLUDED.season" in sql and "league_avg = EXCLUDED.league_avg" in sql and "season_matches" not in sql
    assert json.loads(params["season"]) == {"home": None} and params["league_avg"] is None
    sql, _ = sp.yazma_sorgusu("oran", {"fotmob_match_id": 1, "odds": None, "odds_at": None, "season": {"x": 1}})
    assert "season" not in sql


# ---------------------------------------------------------------- orkestrasyon
SIMDI = datetime(2026, 10, 22, 12, 0)


def mac():
    return {"fotmob_match_id": 1001, "iddaa_event_id": None, "home_team_id": YILDIZ, "away_team_id": KUZEY, "home_name": "Yıldız FK",
            "away_name": "Kuzey SK", "league_id": 4, "league_name": "Süper Lig", "match_date": MAC_GUNU}


def kaynaklar(yazilan, sezon, model=None):
    return Kaynaklar(maclar=lambda b, s: [mac()], form=lambda t, d: [], h2h=lambda a, b, d: [],
                     model=lambda ms: model or {}, iddaa=lambda e: (None, {}, {}), mevcut=lambda ids: set(),
                     yaz=lambda k, s: yazilan.append((k, s)), fotmob=lambda f: None, sezon=sezon)


def calistir(sezon, kapsam="gunluk"):
    yazilan = []
    return hazirla(kapsam, False, kaynaklar(yazilan, sezon), SIMDI, "harman-v1"), yazilan


def test_gunluk_satiri_sezon_gruplarini_zaman_damgasiyla_yazar():
    cagrilar = []
    def sezon(lig_id, ev, dep, tarih):
        cagrilar.append((lig_id, ev, dep, tarih))
        return z.sezon_gruplari(lig(), ev, dep, tarih)
    r, yazilan = calistir(sezon)
    satir_ = yazilan[0][1]
    assert cagrilar == [(4, YILDIZ, KUZEY, MAC_GUNU)]
    assert satir_["season"]["home"]["played"] == 3 and satir_["season_at"] == SIMDI
    assert satir_["league_avg"]["n"] == 6 and satir_["league_avg_at"] == SIMDI
    assert satir_["season_matches_at"] == SIMDI
    assert {"season", "league_avg", "season_matches"} <= set(r["maclar"][0]["alanlar"])


def test_sezon_verisi_yoksa_gruplar_null_zaman_damgasi_null():
    _, yazilan = calistir(lambda *a: {"season": None, "league_avg": None, "season_matches": None})
    s = yazilan[0][1]
    assert s["season"] is None and s["season_at"] is None and s["league_avg_at"] is None and s["season_matches_at"] is None


def test_sezon_kaynagi_hata_verirse_anahtarlar_olmaz_eski_veri_korunur_ve_is_surer():
    def patlar(*a):
        raise RuntimeError("db")
    r, yazilan = calistir(patlar)
    s = yazilan[0][1]
    assert "season" not in s and "season_at" not in s and "league_avg" not in s
    assert r["sezon_hatasi"] == 1 and r["hazirlanan"] == 1
    sql, _ = sp.yazma_sorgusu("gunluk", s)
    assert "season" not in sql


def test_sezon_kaynagi_verilmezse_eski_davranis():
    _, yazilan = calistir(None)
    assert "season" not in yazilan[0][1]


def test_oran_kapsami_sezon_kaynagini_cagirmaz():
    cagrilar = []
    yazilan = []
    k = Kaynaklar(maclar=lambda b, s: [mac()], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {},
                  iddaa=lambda e: (None, {}, {}), mevcut=lambda ids: {1001}, yaz=lambda c, s: yazilan.append(s),
                  fotmob=lambda f: None, sezon=lambda *a: cagrilar.append(a))
    hazirla("oran", False, k, SIMDI, "harman-v1")
    assert cagrilar == []


def test_gercek_kaynak_lig_sezonunu_bir_kez_ceker_ve_bellekler(monkeypatch):
    calls = []
    monkeypatch.setattr(z, "_sorgu", lambda sql, p: calls.append(p) or lig())
    fn = z.sezon_kaynagi()
    a = fn(4, YILDIZ, KUZEY, MAC_GUNU)
    b = fn(4, 1, 5, MAC_GUNU)
    assert len(calls) == 1 and calls[0]["lig"] == 4
    assert calls[0]["bas"] == datetime(2026, 7, 1) and calls[0]["bit"] == datetime(2027, 7, 1)
    assert a["season"] is not None and b["league_avg"] == a["league_avg"]
