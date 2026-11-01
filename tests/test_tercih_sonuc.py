"""Tercih izleme: bir tercihin (kategori + seçim metni) maç sonucuna göre tuttu/tutmadı kararı ve iddaa olayından oran bulma. Saf fonksiyonlar, DB yok."""
import pytest

from app.services.tercih_sonuc import oran_bul, sonuclandir

EV, DEP = "Galatasaray", "Fenerbahçe"
SKOR = {"home": 2, "away": 1}
ISTAT = {"corners": {"home": 8, "away": 3}, "yellow_cards": {"home": 2, "away": 1}, "red_cards": {"home": 0, "away": 1},
         "shots_on_target": {"home": 5, "away": 2}}


def s(kategori, secim, skor=SKOR, istat=ISTAT):
    return sonuclandir(kategori, secim, EV, DEP, skor, istat)


@pytest.mark.parametrize("secim,beklenen", [(f"MS 1 ({EV})", True), ("MS 0 (Beraberlik)", False), (f"MS 2 ({DEP})", False)])
def test_mac_sonucu(secim, beklenen):
    assert s("Maç sonucu", secim) is beklenen


@pytest.mark.parametrize("secim,beklenen", [("Çifte şans 1-0", True), ("Çifte şans 0-2", False), ("Çifte şans 1-2", True)])
def test_cifte_sans(secim, beklenen):
    assert s("Çifte şans", secim) is beklenen


def test_karsilikli_gol():
    assert s("Karşılıklı gol", "KG Var") is True and s("Karşılıklı gol", "KG Yok") is False
    assert s("Karşılıklı gol", "KG Var", {"home": 2, "away": 0}) is False


@pytest.mark.parametrize("secim,beklenen", [("Gol Üst 2.5", True), ("Gol Alt 2.5", False), ("Gol Üst 3.5", False), ("Gol Alt 3.5", True)])
def test_toplam_gol(secim, beklenen):
    assert s("Toplam gol", secim) is beklenen


def test_takim_golu_ev_ve_deplasman():
    assert s("Takım golü", f"{EV} gol Üst 1.5") is True
    assert s("Takım golü", f"{DEP} gol Üst 1.5") is False
    assert s("Takım golü", f"{DEP} gol Alt 1.5") is True


@pytest.mark.parametrize("secim,beklenen", [(f"Kim daha çok korner: {EV}", True), ("Kim daha çok korner: Eşit", False), (f"Kim daha çok korner: {DEP}", False)])
def test_kim_daha_cok_korner(secim, beklenen):
    assert s("Korner: kim daha çok", secim) is beklenen


def test_korner_esit():
    esit = {**ISTAT, "corners": {"home": 4, "away": 4}}
    assert s("Korner: kim daha çok", "Kim daha çok korner: Eşit", istat=esit) is True


def test_takim_ve_toplam_korner():
    assert s("Takım korneri", f"{EV} korner Üst 7.5") is True
    assert s("Takım korneri", f"{DEP} korner Üst 3.5") is False
    assert s("Toplam korner", "Toplam korner Üst 10.5") is True       # 11 korner
    assert s("Toplam korner", "Toplam korner Alt 10.5") is False


def test_korner_handikap():
    assert s("Korner handikap", f"Korner handikap {EV} -4.5") is True        # 8-3 = +5 > 4.5
    assert s("Korner handikap", f"Korner handikap {DEP} +4.5") is False
    assert s("Korner handikap", f"Korner handikap {EV} -5.5") is False
    assert s("Korner handikap", f"Korner handikap {DEP} +5.5") is True


def test_kart_puani_sari_bir_kirmizi_iki():
    assert s("Toplam kart", "Toplam kart Üst 4.5") is True            # 2 + 1 sarı + 2*1 kırmızı = 5
    assert s("Toplam kart", "Toplam kart Üst 5.5") is False
    assert s("Kim daha çok kart", f"Kim daha çok kart: {DEP}") is True   # Fenerbahçe 1 + 2 = 3, Galatasaray 2
    assert s("Kim daha çok kart", f"Kim daha çok kart: {EV}") is False


def test_isabetli_sut():
    assert s("Takım isabetli şut", f"{EV} 4+") is True
    assert s("Takım isabetli şut", f"{DEP} 3+") is False
    assert s("Toplam isabetli şut", "Toplam isabetli şut 7+") is True       # 5 + 2
    assert s("Toplam isabetli şut", "Toplam isabetli şut 8+") is False


def test_faul_ve_bilinmeyen_sonuclanamaz():
    assert s("Toplam faul", "Toplam faul Üst 20.5") is None         # istatistikte faul yok
    assert s("Takım faul", f"{EV} 11+") is None
    assert s("Yeni kategori", "bir şey") is None


def test_eksik_istatistik_none():
    assert s("Toplam korner", "Toplam korner Üst 9.5", istat={}) is None
    assert s("Maç sonucu", f"MS 1 ({EV})", skor=None) is None


# ---- oran bulma: iddaa olayı (`m` listesi; st = piyasa kodu, sov = çizgi, o = seçenekler) ----
def pazar(st, secenekler, sov=None):
    p = {"st": st, "s": 1, "o": [{"n": n, "odd": o} for n, o in secenekler.items()]}
    if sov is not None:
        p["sov"] = str(sov)
    return p


OLAY = {"m": [pazar(1, {"1": 1.5, "0": 4.0, "2": 6.0}), pazar(92, {"1 ve 0": 1.15, "0 ve 2": 3.0, "1 ve 2": 1.2}), pazar(89, {"Var": 1.7, "Yok": 2.0}),
              pazar(101, {"Alt": 2.0, "Üst": 1.75}, 2.5), pazar(603, {"Alt": 2.2, "Üst": 1.6}, 1.5), pazar(604, {"Alt": 1.4, "Üst": 2.6}, 1.5),
              pazar(52, {"1": 1.3, "0": 7.2, "2": 4.0}), pazar(822, {"Alt": 2.0, "Üst": 1.7}, 7.5), pazar(48, {"Alt": 2.1, "Üst": 1.65}, 10.5),
              pazar(863, {"1": 1.9, "2": 1.85}, -4.5), pazar(658, {"Alt": 1.9, "Üst": 1.8}, 4.5), pazar(824, {"1": 2.3, "0": 5.0, "2": 2.5}),
              pazar(913, {f"{EV} 4+": 1.8, f"{DEP} 3+": 1.5, "Toplam 7+": 1.9, "İY Galatasaray 2+": 2.2})]}


def o(kategori, secim):
    return oran_bul(kategori, secim, EV, DEP, OLAY)


def test_oran_bul_temel_pazarlar():
    assert o("Maç sonucu", f"MS 1 ({EV})") == 1.5 and o("Maç sonucu", "MS 0 (Beraberlik)") == 4.0
    assert o("Çifte şans", "Çifte şans 1-0") == 1.15 and o("Çifte şans", "Çifte şans 0-2") == 3.0 and o("Çifte şans", "Çifte şans 1-2") == 1.2
    assert o("Karşılıklı gol", "KG Var") == 1.7
    assert o("Toplam gol", "Gol Üst 2.5") == 1.75 and o("Toplam gol", "Gol Alt 2.5") == 2.0


def test_oran_bul_takim_ve_korner():
    assert o("Takım golü", f"{EV} gol Üst 1.5") == 1.6 and o("Takım golü", f"{DEP} gol Üst 1.5") == 2.6
    assert o("Korner: kim daha çok", f"Kim daha çok korner: {EV}") == 1.3 and o("Korner: kim daha çok", "Kim daha çok korner: Eşit") == 7.2
    assert o("Takım korneri", f"{EV} korner Üst 7.5") == 1.7
    assert o("Toplam korner", "Toplam korner Alt 10.5") == 2.1
    assert o("Korner handikap", f"Korner handikap {EV} -4.5") == 1.9 and o("Korner handikap", f"Korner handikap {DEP} +4.5") == 1.85


def test_oran_bul_kart_ve_sut():
    assert o("Toplam kart", "Toplam kart Üst 4.5") == 1.8
    assert o("Kim daha çok kart", f"Kim daha çok kart: {DEP}") == 2.5
    assert o("Takım isabetli şut", f"{EV} 4+") == 1.8 and o("Takım isabetli şut", f"{DEP} 3+") == 1.5
    assert o("Toplam isabetli şut", "Toplam isabetli şut 7+") == 1.9
    assert o("Takım isabetli şut", "İY Galatasaray 2+") is None        # ilk yarı seçenekleri listede yok


def test_oran_bul_olmayan_ya_da_gecersiz_none():
    assert o("Toplam gol", "Gol Üst 3.5") is None                   # o çizgide pazar yok
    assert o("Yeni kategori", "x") is None
    assert oran_bul("Maç sonucu", f"MS 1 ({EV})", EV, DEP, None) is None
    assert oran_bul("Maç sonucu", f"MS 1 ({EV})", EV, DEP, {"m": []}) is None
