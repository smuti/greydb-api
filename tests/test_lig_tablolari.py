"""Lig tabloları: beklenen sonuç (Poisson), takım toplamları, üst/alt çizgileri, KG, kapsam sayaçları."""
import pytest

from app.services.lig_tablolari import beklenen_sonuc, hesapla


def mac(ev, dep, eg, dg, **ek):
    return {"ev_id": ev, "ev_ad": f"T{ev}", "dep_id": dep, "dep_ad": f"T{dep}", "ev_gol": eg, "dep_gol": dg, **ek}


def test_beklenen_sonuc_toplam_1_ve_yon():
    pw, pb, pl = beklenen_sonuc(2.1, 0.8)
    assert abs(pw + pb + pl - 1) < 1e-9 and pw > pl and pw > pb
    a, b, c = beklenen_sonuc(1.0, 1.0)
    assert abs(a - c) < 1e-9
    assert beklenen_sonuc(0.0, 0.0) == pytest.approx((0.0, 1.0, 0.0), abs=1e-9)


def test_skor_sayilari_ust_alt_kg_temiz_sayfa():
    t = {x["takim_id"]: x for x in hesapla([mac(1, 2, 3, 0), mac(2, 1, 1, 1), mac(1, 3, 2, 2)])}
    assert (t[1]["oynanan"], t[1]["puan"], t[1]["atilan"], t[1]["yenilen"]) == (3, 5, 6, 3)
    assert t[1]["temiz_sayfa"] == 1 and t[1]["kg_var"] == 2
    assert (t[1]["ust15"], t[1]["ust25"], t[1]["ust35"], t[1]["ust45"]) == (3, 2, 1, 0)         # toplam goller 3, 2, 4
    assert t[2]["kg_var"] == 1 and t[3]["kg_var"] == 1


def test_istatistik_yoksa_stat_mac_sifir_ve_olculer_bos():
    t = hesapla([mac(1, 2, 1, 0)])[0]
    assert t["stat_mac"] == 0 and t["xg_mac"] == 0 and t["xp"] == 0 and t["sut"] == 0


def test_istatistik_ve_xg_toplanir_beklenen_puan_gercege_yakin_yon():
    m = mac(1, 2, 2, 0, ev_xg=2.4, dep_xg=0.5, ev_sut=15, dep_sut=6, ev_korner=7, dep_korner=2, ev_sari=1, dep_sari=3, ev_kirmizi=0, dep_kirmizi=1)
    ev, dep = [x for x in hesapla([m])]
    assert ev["takim_id"] == 1 and ev["xg_mac"] == 1 and ev["xg_atilan"] == 2.4 and ev["xg_yenilen"] == 0.5
    assert ev["sut"] == 15 and ev["sut_yenilen"] == 6 and ev["korner"] == 7 and dep["sari"] == 3 and dep["kirmizi"] == 1
    assert ev["xp"] > 2.0 and dep["xp"] < 0.5 and abs(ev["xp"] + dep["xp"] - (3 * (1 - beklenen_sonuc(2.4, 0.5)[1]) + 2 * beklenen_sonuc(2.4, 0.5)[1])) < 0.02


def test_skorsuz_mac_sayilmaz_ve_siralama_puana_gore():
    r = hesapla([mac(1, 2, None, None), mac(3, 4, 1, 0), mac(5, 6, 0, 0)])
    assert [x["takim_id"] for x in r][:1] == [3] and all(x["oynanan"] == 1 for x in r) and len(r) == 4


def test_korner_puani_rakipten_cok_3_esit_1_az_0():
    maclar = [mac(1, 2, 1, 0, ev_korner=7, dep_korner=3), mac(2, 1, 0, 0, ev_korner=4, dep_korner=4), mac(1, 3, 0, 1, ev_korner=2, dep_korner=9), mac(1, 4, 1, 1)]
    t = {x["takim_id"]: x for x in hesapla(maclar)}
    assert (t[1]["korner_g"], t[1]["korner_b"], t[1]["korner_m"], t[1]["korner_puan"], t[1]["korner_mac"]) == (1, 1, 1, 4, 3)     # korner yazılmamış maç sayılmaz
    assert (t[2]["korner_puan"], t[2]["korner_mac"]) == (1, 2) and t[3]["korner_puan"] == 3 and t[4]["korner_mac"] == 0
    assert t[1]["korner"] == 7 + 4 + 2 and t[1]["korner_yenilen"] == 3 + 4 + 9
