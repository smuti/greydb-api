"""Yayın paketi saf fonksiyonları: ham kaynak yanıtlarından (FotMob, kendi veritabanımız, istatistik servisi) sayfanın okuyacağı kart verisi.
Veri eksikse fonksiyon None döner; kart hiç üretilmez (yarım kart yok). Dış servise ve veritabanına dokunmaz."""
from app.services.yayin_paketi import (
    eksik_listesi, puan_karti, form_karti, h2h_karti, ic_dis_karti, gol_karti, savunma_karti, zaman_karti, tip_karti, kart_karti, mac_paketi,
)


def _kisi(ad, tip, deger=None):
    return {"id": 1, "name": ad, "marketValue": deger, "unavailability": {"type": tip, "expectedReturn": "Late October 2026"}}


def test_eksik_listesi_sakat_ve_cezali_piyasa_degerine_gore_azalan():
    ham = {"unavailable": [_kisi("A", "injury", 5), _kisi("B", "suspension", 50), _kisi("C", "injury", 20), _kisi("D", "injury")]}
    assert eksik_listesi(ham) == [
        {"ad": "B", "tur": "cezali", "deger": 50}, {"ad": "C", "tur": "sakat", "deger": 20},
        {"ad": "A", "tur": "sakat", "deger": 5}, {"ad": "D", "tur": "sakat"},   # değeri bilinmeyen sona
    ]


def test_eksik_listesi_bilinmeyen_tur_atlanir_ve_bos_girdi_bos_liste():
    assert eksik_listesi({"unavailable": [_kisi("A", "other", 5)]}) == []
    assert eksik_listesi({"unavailable": None}) == []
    assert eksik_listesi(None) == []
    assert eksik_listesi({}) == []


TABLO = [{"takim_id": 10, "sira": 3, "puan": 15, "averaj": 4}, {"takim_id": 20, "sira": 9, "puan": 11, "averaj": -2}]


def test_puan_karti_iki_takim_da_tabloda_olmali():
    assert puan_karti(TABLO, 10, 20) == {"ev": {"sira": 3, "puan": 15, "averaj": 4}, "dep": {"sira": 9, "puan": 11, "averaj": -2}}
    assert puan_karti(TABLO, 10, 99) is None
    assert puan_karti([], 10, 20) is None


def _form(dizi, atilan, yenilen):
    return {"maclar": [{}] * len(dizi), "ozet": {"dizi": dizi, "atilan": atilan, "yenilen": yenilen}}


def test_form_karti_eskiden_yeniye_siralar_ve_kucuk_harfe_cevirir():
    # kaynak dizi EN YENİDEN eskiye ("GBM": son maç G); yayında soldan sağa eskiden yeniye gösterilir
    k = form_karti(_form("GBMGG", 7, 4), _form("MMBGG", 3, 6))
    assert k["ev"] == {"sonuclar": ["g", "g", "m", "b", "g"], "atilan": 7, "yenilen": 4}
    assert k["dep"]["sonuclar"] == ["g", "g", "b", "m", "m"]


def test_form_karti_maci_olmayan_takimda_none():
    assert form_karti(_form("", 0, 0), _form("G", 1, 0)) is None


def test_h2h_karti_son_5_maci_bu_macin_evine_gore_sayar():
    maclar = [  # en yeniden eskiye; ev adı "Ev"
        {"ev": "Ev", "dep": "Dep", "ev_gol": 2, "dep_gol": 0},     # Ev kazandı
        {"ev": "Dep", "dep": "Ev", "ev_gol": 1, "dep_gol": 1},     # beraberlik
        {"ev": "Dep", "dep": "Ev", "ev_gol": 3, "dep_gol": 0},     # Dep kazandı
        {"ev": "Ev", "dep": "Dep", "ev_gol": 0, "dep_gol": 1},     # Dep kazandı
        {"ev": "Ev", "dep": "Dep", "ev_gol": 4, "dep_gol": 1},     # Ev kazandı
        {"ev": "Ev", "dep": "Dep", "ev_gol": 9, "dep_gol": 0},     # 6. maç: sayılmaz
    ]
    assert h2h_karti({"maclar": maclar}, "Ev") == {"evGalibiyet": 2, "beraberlik": 1, "depGalibiyet": 2}
    assert h2h_karti({"maclar": []}, "Ev") is None


def test_ic_dis_karti_evinde_ve_deplasmanda_gec_gol_farki_mac_basi():
    k = ic_dis_karti([(2, 0), (1, 1), (0, 1)], [(0, 3), (1, 1)])
    assert k["ev"] == {"g": 1, "b": 1, "m": 1, "farkMac": (2 - 0 + 0 + -1) / 3}
    assert k["dep"] == {"g": 0, "b": 1, "m": 1, "farkMac": (-3 + 0) / 2}
    assert ic_dis_karti([], [(1, 0)]) is None


STAT = {"match_count": 7, "stats": {"xg_per_match": 1.41, "xga_per_match": 1.52, "possession_avg": 48.7}, "totals": {"goals_scored": 17, "goals_conceded": 13}}


def test_gol_ve_savunma_karti_maca_gore_ortalama():
    assert gol_karti(STAT, STAT) == {"ev": {"gMac": 17 / 7, "xg": 1.41}, "dep": {"gMac": 17 / 7, "xg": 1.41}}
    s = savunma_karti(STAT, STAT, _form("GGGGG", 5, 2), _form("MMMMM", 1, 9))
    assert s["ev"] == {"yMac": 13 / 7, "xga": 1.52, "son5Yenilen": 2}
    assert s["dep"]["son5Yenilen"] == 9
    assert gol_karti(STAT, None) is None and gol_karti({"match_count": 0}, STAT) is None


GT = {"intervals": {"0-15": {"scored": 7, "conceded": 0}, "16-30": {"scored": 0, "conceded": 3}, "31-45": {"scored": 2, "conceded": 2},
                    "46-60": {"scored": 3, "conceded": 3}, "61-75": {"scored": 4, "conceded": 2}, "76-90": {"scored": 1, "conceded": 3}}}


def test_zaman_karti_alti_dilim_sirali():
    z = zaman_karti(GT, GT)
    assert z["ev"] == {"atilan": [7, 0, 2, 3, 4, 1], "yenilen": [0, 3, 2, 3, 2, 3]}
    assert zaman_karti({"intervals": {}}, GT) is None and zaman_karti(None, GT) is None


TIP = {"team_type": {"name": "Dengeli Orta Blok Takımı", "description": "Ortalama profil, adaptif oyun, risk-ödül dengesi"}}


def test_tip_karti_ad_aciklama_ve_topa_sahip_olma():
    t = tip_karti(STAT, STAT, TIP, TIP)
    assert t["ev"] == {"tip": "Dengeli Orta Blok Takımı", "karakter": "Ortalama profil, adaptif oyun, risk-ödül dengesi", "topaSahip": 48.7}
    assert tip_karti(STAT, STAT, None, TIP) is None


KARTP = {"overall": {"n_matches": 10, "red_for_avg": 0.1, "yellow_for_avg": 2.4}, "thresholds": {"over_3_5_rate": 90.0}}


def test_kart_karti_sari_ortalama_oran_ve_kirmizi_sayisi():
    assert kart_karti(KARTP, KARTP) == {"ev": {"sari": 2.4, "kart35": 90, "kirmizi": 1}, "dep": {"sari": 2.4, "kart35": 90, "kirmizi": 1}}
    assert kart_karti(KARTP, {"overall": {"n_matches": 0}}) is None and kart_karti(None, KARTP) is None


def test_mac_paketi_yalniz_dolu_kartlari_tasir():
    p = mac_paketi(3, {"ad": "Ev", "logo": "/team-logos/1.png", "renk": "#fff"}, {"ad": "Dep", "logo": None, "renk": "#000"},
                   lig="Süper Lig", hafta="9. Hafta", saat="20:00", tarih="2026-10-24",
                   puan={"ev": {"sira": 1, "puan": 2, "averaj": 3}, "dep": {"sira": 2, "puan": 1, "averaj": 0}}, form=None, h2h=None)
    assert p["sira"] == 3 and p["lig"] == "Süper Lig" and p["tarih"] == "2026-10-24"
    assert list(p["kartlar"]) == ["puan"]
