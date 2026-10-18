"""Kupon hesap çekirdeği: bacak sonucu (skordan), toplam oran, sistem kombinasyonları, kupon sonucu. Saf fonksiyonlar, veritabanı yok."""
import pytest

from app.services.kupon_hesap import (
    DESTEKLENMIYOR, VERI_YOK, bacak_sonucu, en_yuksek_odeme, kombinasyon_sayisi, kupon_sonucu, toplam_oran,
)


def M(h, a, **kw):
    """Maç verisi: skor + isteğe bağlı devre skoru (ht), istatistik, olaylar, oyuncular."""
    return {"home": h, "away": a, "home_name": "Galatasaray", "away_name": "Fenerbahçe", **kw}


def b(oran, sonuc="pending"):
    return {"odds": oran, "result": sonuc}


# ---- Bacak sonucu (iddaa market adlarıyla) ----
@pytest.mark.parametrize("pick,h,a,beklenen", [
    ("1", 2, 1, "won"), ("1", 1, 1, "lost"), ("0", 1, 1, "won"), ("0", 2, 1, "lost"), ("2", 0, 1, "won"), ("2", 1, 0, "lost"),
])
def test_mac_sonucu_iddaa_secimleriyle(pick, h, a, beklenen):
    assert bacak_sonucu("Maç Sonucu", pick, M(h, a)) == beklenen


def test_mac_sonucu_x_ve_metin_secimleri_de_calisir():
    assert bacak_sonucu("Maç Sonucu", "X", M(1, 1)) == "won"
    assert bacak_sonucu("Maç Sonucu", "Beraberlik", M(0, 2)) == "lost"


@pytest.mark.parametrize("market", ["Altı/Üstü 2,5", "Alt/Üst 2.5", "Alt/Üst 2,5"])
def test_alt_ust_virgullu_ve_noktali_cizgi(market):
    assert bacak_sonucu(market, "Üst", M(2, 1)) == "won"
    assert bacak_sonucu(market, "Üst", M(1, 1)) == "lost"
    assert bacak_sonucu(market, "Alt", M(1, 1)) == "won"
    assert bacak_sonucu(market, "Alt", M(2, 1)) == "lost"


def test_tam_sayili_cizgide_esitlik_iade():
    assert bacak_sonucu("Alt/Üst 2", "Üst", M(1, 1)) == "void"
    assert bacak_sonucu("Alt/Üst 2", "Alt", M(2, 0)) == "void"


def test_karsilikli_gol():
    assert bacak_sonucu("Karşılıklı Gol", "Var", M(1, 1)) == "won"
    assert bacak_sonucu("Karşılıklı Gol", "Var", M(2, 0)) == "lost"
    assert bacak_sonucu("Karşılıklı Gol", "Yok", M(2, 0)) == "won"


@pytest.mark.parametrize("pick,h,a,beklenen", [
    ("1X", 1, 1, "won"), ("1X", 0, 1, "lost"), ("12", 1, 1, "lost"), ("12", 2, 1, "won"), ("X2", 0, 0, "won"), ("X2", 1, 0, "lost"),
    ("1-0", 1, 1, "won"), ("1/2", 1, 1, "lost"), ("0-2", 0, 1, "won"),   # iddaa yazımı: 0 = beraberlik
])
def test_cifte_sans_farkli_yazimlar(pick, h, a, beklenen):
    assert bacak_sonucu("Çifte Şans", pick, M(h, a)) == beklenen


def test_bilinmeyen_market_desteklenmiyor():
    assert bacak_sonucu("Uydurma Market", "1", M(2, 1)) == DESTEKLENMIYOR


# ---- Toplam oran ----
def test_toplam_oran_carpim_ve_yuvarlama():
    assert toplam_oran([b(1.5), b(2.0), b(1.2)]) == 3.6
    assert toplam_oran([b(1.38)]) == 1.38


def test_toplam_oran_bos_ya_da_gecersiz_oran():
    assert toplam_oran([]) is None
    with pytest.raises(ValueError):
        toplam_oran([b(1.5), b(0)])
    with pytest.raises(ValueError):
        toplam_oran([b(None)])


# ---- Sistem ----
def test_kombinasyon_sayisi():
    assert kombinasyon_sayisi(4, 3) == 4
    assert kombinasyon_sayisi(5, 2) == 10
    assert kombinasyon_sayisi(3, 2) == 3


def test_en_yuksek_odeme_sistemde_hepsi_tutarsa_ortalama():
    # 3 maç, sistem 2/3: kombinasyonlar 2*3=6, 2*4=8, 3*4=12; ortalama (6+8+12)/3
    assert en_yuksek_odeme([b(2), b(3), b(4)], 2) == pytest.approx(26 / 3, abs=0.01)


def test_en_yuksek_odeme_kombinede_toplam_oran():
    assert en_yuksek_odeme([b(2), b(3)], None) == 6.0


# ---- Kupon sonucu: kombine ----
def test_kombine_bekleyen_varsa_bekliyor():
    assert kupon_sonucu([b(2, "won"), b(2)], None)["sonuc"] == "pending"


def test_kombine_biri_yattiysa_yatti_bekleyen_olsa_bile():
    assert kupon_sonucu([b(2, "lost"), b(2)], None)["sonuc"] == "lost"


def test_kombine_hepsi_tuttu():
    s = kupon_sonucu([b(2, "won"), b(1.5, "won")], None)
    assert s["sonuc"] == "won" and s["odeme"] == 3.0


def test_kombine_iptal_bacak_orani_bir_sayilir():
    s = kupon_sonucu([b(2, "won"), b(1.5, "void")], None)
    assert s["sonuc"] == "won" and s["odeme"] == 2.0


def test_kombine_hepsi_iptal_iade():
    s = kupon_sonucu([b(2, "void"), b(1.5, "void")], None)
    assert s["sonuc"] == "void" and s["odeme"] == 1.0


def test_tek_mac_kombinenin_ozel_hali():
    assert kupon_sonucu([b(1.8, "won")], None)["sonuc"] == "won"
    assert kupon_sonucu([b(1.8, "lost")], None)["sonuc"] == "lost"


# ---- Kupon sonucu: sistem ----
def test_sistem_2_3_bir_yatti_iki_tuttu():
    # tutan: A(2) B(3); yatan: C. Kombinasyonlar: AB=6 (tutar), AC=0, BC=0 → ödeme 6/3=2.0 > 1 → tuttu
    s = kupon_sonucu([b(2, "won"), b(3, "won"), b(4, "lost")], 2)
    assert s["sonuc"] == "won" and s["odeme"] == 2.0
    assert s["kombinasyon"] == {"toplam": 3, "tutan": 1}


def test_sistem_zarar_eden_kismi_tutma_yatti_sayilir():
    # 4 maç sistem 3/4: yalnız 1 kombinasyon tutar, oranı 1.5*1.4*1.3=2.73; ödeme 2.73/4=0.68 < 1 → yattı
    s = kupon_sonucu([b(1.5, "won"), b(1.4, "won"), b(1.3, "won"), b(2, "lost")], 3)
    assert s["sonuc"] == "lost"
    assert s["kombinasyon"] == {"toplam": 4, "tutan": 1}
    assert s["odeme"] == pytest.approx(0.6825, abs=1e-4)


def test_sistem_hicbiri_tutmadi():
    s = kupon_sonucu([b(2, "lost"), b(3, "lost"), b(4, "lost")], 2)
    assert s["sonuc"] == "lost" and s["odeme"] == 0 and s["kombinasyon"]["tutan"] == 0


def test_sistem_bekleyen_varsa_bekliyor():
    assert kupon_sonucu([b(2, "won"), b(3), b(4, "won")], 2)["sonuc"] == "pending"


def test_sistem_iptal_bacak_orani_bir_sayilir_kombinasyon_korunur():
    # A(2) won, B void(→1.0), C(3) won; 2/3: AB=2*1=2, AC=6, BC=3 → (2+6+3)/3
    s = kupon_sonucu([b(2, "won"), b(5, "void"), b(3, "won")], 2)
    assert s["odeme"] == pytest.approx(11 / 3, abs=1e-4) and s["sonuc"] == "won"


def test_sistem_hepsi_iptal_iade():
    s = kupon_sonucu([b(2, "void"), b(3, "void"), b(4, "void")], 2)
    assert s["sonuc"] == "void"


def test_sistem_basa_bas_iade_sayilir():
    # 3 maç 2/3, AB tutar oranı tam 3.0 → ödeme 3.0/3 = 1.0 (başa baş) → void
    s = kupon_sonucu([b(3, "won"), b(1, "won"), b(2, "lost")], 2)
    assert s["odeme"] == pytest.approx(1.0) and s["sonuc"] == "void"


@pytest.mark.parametrize("k", [0, 1, 3, 4])
def test_gecersiz_sistem_k_hata(k):
    # 3 bacak: k 2..n-1 arası olmalı (k=n kombine, k=1 tekli bahis sistemi yok)
    with pytest.raises(ValueError):
        kupon_sonucu([b(2, "won"), b(3, "won"), b(4, "won")], k)
