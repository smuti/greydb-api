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


def test_kg_kirilimlari_golsuz_ic_dis_ve_son5():
    # T1: iç 3-0 (KG yok), dış 1-1 (KG), iç 2-2 (KG), iç 0-1 (gol yok), dış 1-2 (KG), iç 2-0 (KG yok)
    maclar = [mac(1, 2, 3, 0), mac(2, 1, 1, 1), mac(1, 3, 2, 2), mac(1, 4, 0, 1), mac(5, 1, 2, 1), mac(1, 6, 2, 0)]
    t = {x["takim_id"]: x for x in hesapla(maclar)}[1]
    assert t["golsuz"] == 1 and t["kg_var"] == 3
    assert (t["ic_oynanan"], t["ic_kg_var"], t["dis_oynanan"], t["dis_kg_var"]) == (4, 1, 2, 2)
    assert (t["son5_mac"], t["son5_kg_var"]) == (5, 3)                         # ilk maç (KG yok) son 5'in dışında
    assert t["ic_oynanan"] + t["dis_oynanan"] == t["oynanan"]


def test_son5_az_maclik_takim():
    t = {x["takim_id"]: x for x in hesapla([mac(1, 2, 1, 1), mac(2, 1, 0, 2)])}[2]
    assert (t["son5_mac"], t["son5_kg_var"], t["golsuz"], t["dis_oynanan"]) == (2, 1, 1, 1)


def test_ic_dis_son5_gol_ve_ust():
    maclar = [mac(1, 2, 3, 0), mac(2, 1, 1, 1), mac(1, 3, 2, 2), mac(1, 4, 0, 1), mac(5, 1, 2, 1), mac(1, 6, 2, 0)]
    t = {x["takim_id"]: x for x in hesapla(maclar)}[1]
    assert (t["ic_atilan"], t["ic_yenilen"], t["dis_atilan"], t["dis_yenilen"]) == (7, 3, 2, 3)
    assert (t["son5_atilan"], t["son5_yenilen"]) == (6, 6)                       # ilk maç (3-0) dışarıda
    assert (t["ic_ust25"], t["dis_ust25"], t["son5_ust25"]) == (2, 1, 2)          # toplamlar: iç 3,4,1,2 · dış 2,3 · son 5: 2,4,1,3,2


def test_korner_ve_kart_esikleri_ve_son5():
    maclar = [mac(1, 2, 1, 0, ev_korner=6, dep_korner=4, ev_sari=2, dep_sari=3, ev_kirmizi=0, dep_kirmizi=0),     # korner 10, kart 5
              mac(2, 1, 0, 0, ev_korner=3, dep_korner=5, ev_sari=1, dep_sari=1, ev_kirmizi=0, dep_kirmizi=1),     # korner 8, kart 3
              mac(1, 3, 2, 1)]                                                                                    # veri yok
    t = {x["takim_id"]: x for x in hesapla(maclar)}[1]
    assert (t["korner_mac"], t["korner_ust95"], t["kart_mac"], t["kart_ust45"]) == (2, 1, 2, 1)
    assert (t["kart_ust25"], t["kart_ust35"]) == (2, 1)                          # kartlar 5 ve 3
    assert (t["son5_korner_mac"], t["son5_korner_toplam"], t["son5_kart_mac"], t["son5_kart_toplam"]) == (2, 18, 2, 8)
