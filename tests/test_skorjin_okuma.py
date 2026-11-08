"""Skorjin `okuma` grubu: takımların son 10 lig maçı profili + lig tabanı (saf hesap + SQL metni). Veritabanı yok."""
from datetime import datetime

import pytest

from app.services import skorjin_okuma as z

MAC_GUNU = datetime(2026, 11, 8, 16, 0)


def satir(mid, gun, ev, dep, hs, as_, ay=10, **kw):
    """Ev ve deplasman ölçüleri `ev_*` / `dep_*` önekiyle; verilmeyen ölçü yok sayılır (None)."""
    s = {"mac_id": mid, "tarih": datetime(2026, ay, gun, 17, 0), "home_team_id": ev, "away_team_id": dep, "home_score": hs, "away_score": as_}
    s.update(kw)
    return s


FULL = dict(ev_xg=1.5, dep_xg=1.0, ev_sut=14, dep_sut=10, ev_korner=6, dep_korner=4, ev_sari=2, dep_sari=3, ev_kirmizi=0, dep_kirmizi=1, ev_faul=11, dep_faul=13,
            ev_top=55.0, dep_top=45.0, ev_pas_kendi=300, dep_pas_kendi=250, ev_pas_rakip=200, dep_pas_rakip=150, ev_ceza=25, dep_ceza=15, ev_mudahale=17, dep_mudahale=14,
            ev_kontra=0.30, dep_kontra=0.10, ev_duran=0.50, dep_duran=0.20, sut_var=True)


def takim1_maclari(n=5):
    # takım 1 hep evde, rakipler 2..; sonuç 2-1 galibiyet
    return [satir(i, 1 + i, 1, 2 + i, 2, 1, **FULL) for i in range(n)]


def test_profil_son_maclarin_mac_basina_ortalamasi_ve_rakip_degerleri_yenilen():
    p = z.takim_profili(takim1_maclari(), 1, MAC_GUNU)
    assert p["n"] == 5
    assert p["xg"] == 1.5 and p["xg_yenilen"] == 1.0
    assert p["sut"] == 14.0 and p["sut_yenilen"] == 10.0 and p["korner"] == 6.0 and p["korner_yenilen"] == 4.0
    assert p["kontra_xg"] == 0.3 and p["kontra_xg_yenilen"] == 0.1 and p["duran_xg"] == 0.5 and p["duran_xg_yenilen"] == 0.2
    assert p["topla_oynama"] == 55.0 and p["rakip_ceza_dokunus"] == 25.0 and p["mudahale"] == 17.0 and p["sari"] == 2.0 and p["faul"] == 11.0


def test_rakip_yari_pas_payi_yuzde_ve_beraberlik_orani():
    p = z.takim_profili(takim1_maclari(), 1, MAC_GUNU)
    assert p["rakip_yari_pas_payi"] == pytest.approx(200 / (200 + 300) * 100, abs=0.01)
    assert p["beraberlik_orani"] == 0.0
    ber = takim1_maclari(4) + [satir(9, 20, 1, 9, 1, 1, **FULL)]
    assert z.takim_profili(ber, 1, MAC_GUNU)["beraberlik_orani"] == 0.2


def test_deplasman_maclarinda_taraflar_cevrilir():
    rows = [satir(i, 1 + i, 7, 1, 0, 2, **FULL) for i in range(4)]
    p = z.takim_profili(rows, 1, MAC_GUNU)
    assert p["xg"] == 1.0 and p["xg_yenilen"] == 1.5 and p["kontra_xg"] == 0.1 and p["kontra_xg_yenilen"] == 0.3 and p["topla_oynama"] == 45.0


def test_yalniz_son_10_mac_ve_mac_gunnden_oncesi():
    rows = [satir(100 + i, 1 + i, 1, 3, 1, 0, ay=8, **{**FULL, "ev_xg": 9.0}) for i in range(5)] + [satir(i, 1 + i, 1, 3, 1, 0, ay=9, **FULL) for i in range(10)]
    rows.append(satir(500, 9, 1, 3, 1, 0, ay=11, **{**FULL, "ev_xg": 99.0}))              # maç günü: aynı gün 17:00 > 16:00, sayılmaz
    p = z.takim_profili(rows, 1, MAC_GUNU)
    assert p["n"] == 10 and p["xg"] == 1.5                                                  # eski 5 maç (xG 9) pencerenin dışında


def test_eksik_olcu_o_mactan_sayilmaz_ve_az_mac_none():
    rows = takim1_maclari(4)
    rows[0] = {**rows[0], "ev_kontra": None, "dep_kontra": None, "sut_var": False}
    p = z.takim_profili(rows, 1, MAC_GUNU)
    assert p["n"] == 4 and p["kontra_xg"] == 0.3                                            # 3 maçtan ortalama
    assert z.takim_profili(takim1_maclari(2), 1, MAC_GUNU) is None                          # 3'ten az maç
    ham = [satir(i, 1 + i, 1, 3, 1, 0) for i in range(4)]                                   # hiç istatistik yok
    p2 = z.takim_profili(ham, 1, MAC_GUNU)
    assert p2["n"] == 4 and p2["xg"] is None and p2["kontra_xg"] is None and p2["beraberlik_orani"] == 0.0


def test_lig_tabani_mac_ortalamalari_ve_kontra_ust_ceyrek_esigi():
    rows = []
    for t in range(1, 9):                                      # 8 takım, her biri evinde 4 maç; kontra üretimi takım numarasıyla artar
        rows += [satir(t * 10 + i, 1 + i, t, 20 + t, 2, 1, **{**FULL, "ev_kontra": 0.05 * t, "dep_kontra": 0.02 * t}) for i in range(4)]
    l = z.lig_tabani(rows, MAC_GUNU)
    assert l["gol"] == 3.0 and l["kg"] == 1.0 and l["beraberlik"] == 0.0
    assert l["sut_toplam"] == 24.0 and l["korner_toplam"] == 10.0 and l["kart_puani_toplam"] == 2 + 3 + 0 + 2 * 1
    assert l["takim"]["xg"] == 1.25 and l["takim"]["duran_xg"] == 0.35
    ust = l["kontra_xg_p75"]
    assert ust is not None and ust > l["takim"]["kontra_xg"]                                # üst %25 eşiği ortalamanın üstünde
    assert l["kontra_xg_yenilen_p75"] is not None


def test_lig_tabani_az_mac_none():
    assert z.lig_tabani(takim1_maclari(3), MAC_GUNU) is None


def test_okuma_grubu_ev_dep_lig_ve_bos_veri():
    rows = takim1_maclari(6) + [satir(50 + i, 1 + i, 30, 1, 0, 1, **FULL) for i in range(4)]
    g = z.okuma_grubu(rows, 1, 30, MAC_GUNU)
    assert g["home"]["n"] == 10 and g["away"]["n"] == 4 and g["league"]["gol"] is not None
    assert z.okuma_grubu([], 1, 2, MAC_GUNU) is None


def test_sql_sut_durumlari_ve_yalniz_biten_lig_maclari():
    s = z.OKUMA_SQL
    assert "FastBreak" in s and "FromCorner" in s and "Penalty" not in s.split("FILTER")[0] or "'Penalty'" not in s
    assert "m.finished" in s and "m.league_id = :lig" in s and "shotmap" in s


# ---------------------------------------------------------------- orkestrasyon (hazirla)
from app.services import skorjin_prematch as sp  # noqa: E402
from app.services.skorjin_prematch import Kaynaklar, hazirla  # noqa: E402

SIMDI = datetime(2026, 11, 7, 22, 30)


def ev_mac():
    return {"fotmob_match_id": 1001, "iddaa_event_id": None, "home_team_id": 1, "away_team_id": 2, "home_name": "Elche", "away_name": "Real Betis",
            "league_id": 4, "league_name": "LaLiga", "match_date": MAC_GUNU}


def kaynak_kur(okuma, yazilan):
    return Kaynaklar(maclar=lambda b, s: [ev_mac()], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {},
                     iddaa=lambda e: (None, {}, {}), mevcut=lambda ids: set(), yaz=lambda k, s: yazilan.append(s), fotmob=lambda f: None, okuma=okuma)


def test_gunluk_satiri_okuma_grubu_zaman_damgasiyla():
    G = {"home": {"n": 10}, "away": {"n": 10}, "league": None}
    cagrilar, yazilan = [], []
    r = hazirla("gunluk", False, kaynak_kur(lambda lig, ev, dep, t: cagrilar.append((lig, ev, dep, t)) or G, yazilan), SIMDI, "harman-v1")
    s = yazilan[0]
    assert cagrilar == [(4, 1, 2, MAC_GUNU)] and s["okuma"] == G and s["okuma_at"] == SIMDI and "okuma" in r["maclar"][0]["alanlar"]


def test_okuma_verisi_yoksa_null_hata_olursa_anahtar_olmaz_ve_sayilir():
    yazilan = []
    hazirla("gunluk", False, kaynak_kur(lambda *a: None, yazilan), SIMDI, "harman-v1")
    assert yazilan[0]["okuma"] is None and yazilan[0]["okuma_at"] is None

    def patlar(*a):
        raise RuntimeError("db")
    yazilan = []
    r = hazirla("gunluk", False, kaynak_kur(patlar, yazilan), SIMDI, "harman-v1")
    assert "okuma" not in yazilan[0] and r["okuma_hatasi"] == 1 and r["hazirlanan"] == 1


def test_kaynak_verilmezse_ve_oran_kapsaminda_cagrilmaz():
    yazilan = []
    hazirla("gunluk", False, kaynak_kur(None, yazilan), SIMDI, "harman-v1")
    assert "okuma" not in yazilan[0]
    cagri = []
    k = kaynak_kur(lambda *a: cagri.append(a), yazilan)
    hazirla("oran", False, Kaynaklar(**{**k.__dict__, "mevcut": lambda ids: {1001}}), SIMDI, "harman-v1")
    assert cagri == []


def test_tablo_ddl_ve_yazma_okuma_sutunlari():
    assert "okuma JSONB" in sp.TABLO_DDL and "okuma_at TIMESTAMPTZ" in sp.TABLO_DDL
    sql, params = sp.yazma_sorgusu("gunluk", {"fotmob_match_id": 1001, "okuma": {"home": None}, "okuma_at": SIMDI})
    assert "okuma" in sql and "CAST(:okuma AS jsonb)" in sql and params["okuma"] == '{"home": null}'
