"""Bacak sonucu: iddaa market aileleri (B skordan, C yarı, D istatistik, E olay/oyuncu). Gerçek tahmin verisindeki seçim biçimleriyle."""
import pytest

from app.services.kupon_hesap import DESTEKLENMIYOR, VERI_YOK, bacak_sonucu


def M(h, a, **kw):
    return {"home": h, "away": a, "home_name": "Galatasaray", "away_name": "Fenerbahçe", **kw}


def gol(dk, takim, oyuncu=None, ek=None, asist=None, kk=False, pen=False):
    """olay_listesi biçimi: takim = olayı yapan, skor_takimi = skoru yazılan (kendi kalesine gol rakibe yazılır)."""
    skor = ("away" if takim == "home" else "home") if kk else takim
    return {"tur": "GOAL", "dakika": dk, "ek_dakika": ek, "takim": takim, "skor_takimi": skor, "oyuncu": oyuncu,
            "asist": asist, "kendi_kalesine": kk, "penalti": pen}


def kart(dk, takim, oyuncu, tur="YELLOW_CARD"):
    return {"tur": tur, "dakika": dk, "ek_dakika": None, "takim": takim, "skor_takimi": None, "oyuncu": oyuncu, "asist": None, "kendi_kalesine": False, "penalti": False}


def ist(**kw):
    """İstatistik: {anahtar: (ev, dep)}"""
    return kw


# =============== B. Skordan çıkanlar ===============
@pytest.mark.parametrize("market,pick,h,a,beklenen", [
    ("Maç Sonucu ve Alt/Üst 1.5", "1 ve Üst", 2, 0, "won"), ("Maç Sonucu ve Alt/Üst 1.5", "1 ve Üst", 1, 0, "lost"),
    ("Maç Sonucu ve Alt/Üst 4.5", "2 ve Alt", 0, 2, "won"), ("Maç Sonucu ve Alt/Üst 1.5", "0 ve Alt", 0, 0, "won"),
    ("Maç Sonucu ve Karşılıklı Gol", "1 ve Var", 2, 1, "won"), ("Maç Sonucu ve Karşılıklı Gol", "0 ve Yok", 0, 0, "won"),
    ("Alt/Üst ve Karşılıklı Gol 2.5", "Üst ve Var", 2, 1, "won"), ("Alt/Üst ve Karşılıklı Gol 2.5", "Üst ve Var", 3, 0, "lost"),
    ("Altı/Üstü 2,5 ve Karşılıklı Gol", "Alt ve Yok", 1, 0, "won"), ("Karşılıklı Gol ve Alt/Üst 2.5", "Alt ve Var", 1, 1, "won"),
])
def test_birlesik_marketler(market, pick, h, a, beklenen):
    assert bacak_sonucu(market, pick, M(h, a)) == beklenen


@pytest.mark.parametrize("market,pick,h,a,beklenen", [
    ("Ev Sahibi Alt/Üst 1.5", "Üst", 2, 0, "won"), ("Ev Sahibi Alt/Üst 1.5", "Üst", 1, 3, "lost"),
    ("Deplasman Alt/Üst 0.5", "Üst", 2, 1, "won"), ("Deplasman Alt/Üst 1.5", "Alt", 0, 1, "won"),
    ("Ev Sahibi Alt/Üst (Uzt.Dahil) 2.5", "Alt", 2, 0, "won"),
])
def test_takim_golu_alt_ust(market, pick, h, a, beklenen):
    assert bacak_sonucu(market, pick, M(h, a)) == beklenen


@pytest.mark.parametrize("pick,h,a,beklenen", [
    ("0-1 gol", 1, 0, "won"), ("0-1 gol", 1, 1, "lost"), ("2-3 gol", 2, 1, "won"), ("2-3 gol", 0, 0, "lost"),
    ("4-5 gol", 3, 2, "won"), ("6+ gol", 4, 3, "won"), ("6+ gol", 3, 3, "won"), ("6+ gol", 2, 2, "lost"),
])
def test_toplam_gol_araliklari(pick, h, a, beklenen):
    assert bacak_sonucu("Toplam Gol", pick, M(h, a)) == beklenen


def test_tek_cift_ve_skor_ve_fark():
    assert bacak_sonucu("Tek/Çift", "Tek", M(2, 1)) == "won"
    assert bacak_sonucu("Tek/Çift", "Çift", M(2, 1)) == "lost"
    assert bacak_sonucu("Maç Skoru", "2:1", M(2, 1)) == "won"
    assert bacak_sonucu("Maç Skoru", "2:1", M(1, 2)) == "lost"
    for pick, h, a, b in [("Ev 2", 3, 1, "won"), ("Ev 3+", 4, 0, "won"), ("Ev 1", 2, 1, "won"), ("Dep 1", 0, 1, "won"), ("Dep 3+", 0, 4, "won"), ("0", 1, 1, "won"), ("Ev 2", 2, 1, "lost")]:
        assert bacak_sonucu("Hangi Takım Kaç Farkla Kazanır", pick, M(h, a)) == b, (pick, h, a)


def test_beraberlikte_iade_ve_ev_deplasman():
    for market in ("Beraberlikte İade", "Ev/Deplasman"):
        assert bacak_sonucu(market, "1", M(1, 1)) == "void"
        assert bacak_sonucu(market, "1", M(2, 1)) == "won"
        assert bacak_sonucu(market, "2", M(2, 1)) == "lost"


def test_gol_yemeden_bitirir_ve_kazanir():
    assert bacak_sonucu("Ev Sahibi Gol Yemeden Bitirir", "Evet", M(2, 0)) == "won"
    assert bacak_sonucu("Ev Sahibi Gol Yemeden Bitirir", "Evet", M(2, 1)) == "lost"
    assert bacak_sonucu("Ev Sahibi Gol Yemeden Bitirir", "Hayır", M(2, 1)) == "won"
    assert bacak_sonucu("Deplasman Gol Yemeden Bitirir", "Evet", M(1, 0)) == "lost"   # deplasman gol yemedi mi: ev 1 attı
    assert bacak_sonucu("Deplasman Gol Yemeden Kazanır", "Evet", M(0, 1)) == "won"
    assert bacak_sonucu("Deplasman Gol Yemeden Kazanır", "Evet", M(1, 2)) == "lost"
    assert bacak_sonucu("Ev Sahibi Gol Yemeden Kazanır", "Evet", M(0, 0)) == "lost"


@pytest.mark.parametrize("market,pick,h,a,beklenen", [
    ("Handikaplı Maç Sonucu -1", "1", 3, 1, "won"), ("Handikaplı Maç Sonucu -1", "1", 2, 1, "lost"), ("Handikaplı Maç Sonucu -1", "0", 2, 1, "won"),
    ("Handikaplı Maç Sonucu -3", "2", 2, 0, "won"), ("Handikaplı Maç Sonucu 1:0", "1", 1, 1, "won"), ("Handikaplı Maç Sonucu 0:1", "2", 1, 1, "won"),
])
def test_handikapli_mac_sonucu(market, pick, h, a, beklenen):
    assert bacak_sonucu(market, pick, M(h, a)) == beklenen


def test_cifte_sans_iddaa_yazimi_ve_ve():
    assert bacak_sonucu("Çifte Şans", "1 ve 0", M(1, 1)) == "won"
    assert bacak_sonucu("Çifte Şans", "0 ve 2", M(2, 1)) == "lost"
    assert bacak_sonucu("Çifte Şans", "1 ve 2", M(1, 1)) == "lost"


# =============== C. Yarı marketleri (devre skoru) ===============
def test_devre_skoru_yoksa_veri_yok():
    assert bacak_sonucu("İlk Yarı Sonucu", "1", M(2, 0)) == VERI_YOK


@pytest.mark.parametrize("market,pick,ht,ft,beklenen", [
    ("İlk Yarı Sonucu", "1", (1, 0), (2, 0), "won"), ("İlk Yarı Sonucu", "0", (1, 0), (2, 0), "lost"),
    ("İlk Yarı Alt/Üst 0.5", "Üst", (1, 0), (1, 0), "won"), ("İlk Yarı Alt/Üst 0.5", "Üst", (0, 0), (2, 0), "lost"),
    ("1. Yarı Alt/Üst 1.5", "Alt", (1, 0), (3, 0), "won"),
    ("Ev Sahibi İlk Yarı Alt/Üst 0.5", "Üst", (1, 0), (1, 0), "won"), ("Deplasman 1. Yarı Altı/Üstü 0.5", "Üst", (1, 0), (1, 0), "lost"),
    ("İlk Yarı Karşılıklı Gol", "Var", (1, 1), (1, 1), "won"), ("İlk Yarı Çifte Şans", "1 ve 0", (0, 0), (1, 0), "won"),
    ("İlk Yarı Tek/Çift", "Tek", (1, 0), (1, 0), "won"), ("İlk Yarı Skoru", "1:0", (1, 0), (3, 0), "won"),
    ("İlk Yarı Toplam Gol", "2+", (1, 1), (1, 1), "won"),
    ("İlk Yarı/Maç Sonucu", "1/1", (1, 0), (2, 0), "won"), ("İlk Yarı / Maç Sonucu", "0/1", (0, 0), (2, 0), "won"), ("İlk Yarı/Maç Sonucu", "1/2", (1, 0), (1, 2), "won"),
    ("İlk Yarı/Maç Sonucu Skorları", "0-0 1-2", (0, 0), (1, 2), "won"),
    ("İkinci Yarı Sonucu", "1", (1, 1), (3, 1), "won"), ("İkinci Yarı Karşılıklı Gol", "Yok", (1, 1), (2, 1), "won"),
    ("İkinci Yarı Alt/Üst 1.5", "Üst", (1, 1), (3, 1), "won"),
    ("İlk Yarı Sonucu ve Altı/Üstü 1.5", "1 ve Alt", (1, 0), (2, 0), "won"), ("İlk Yarı Sonucu ve Karşılıklı Gol", "0 ve Var", (1, 1), (1, 1), "won"),
    ("Hangi Yarıda Daha Fazla Gol", "2.", (0, 0), (2, 0), "won"), ("Hangi Yarıda Daha Fazla Gol", "Eşit", (1, 0), (1, 1), "won"), ("Hangi Yarıda Daha Fazla Gol", "1.", (2, 0), (3, 0), "won"),
    ("Ev Sahibi Hangi Yarıda Daha Fazla Gol Atar", "1.", (2, 0), (3, 0), "won"),
    ("Ev Sahibi Yarı Kazanır", "Evet", (1, 0), (1, 1), "won"), ("Ev Sahibi Yarı Kazanır", "Hayır", (0, 0), (1, 1), "won"), ("Deplasman Yarı Kazanır", "Evet", (1, 0), (1, 0), "lost"), ("Deplasman Yarı Kazanır", "Evet", (1, 0), (1, 1), "won"),
    ("Ev Sahibi Her İki Yarıyı Kazanır", "Evet", (1, 0), (2, 0), "won"), ("Ev Sahibi Her İki Yarıyı Kazanır", "Evet", (1, 0), (1, 1), "lost"),
    ("Ev Sahibi Her İki Yarıda da Gol Atar", "Evet", (1, 0), (2, 0), "won"), ("Deplasman Her İki Yarıda da Gol Atar", "Evet", (0, 1), (0, 1), "lost"),
    ("Her İki Yarıda da Üst 1.5", "Evet", (2, 0), (4, 0), "won"), ("Her İki Yarıda Alt 1.5", "Evet", (1, 0), (2, 0), "won"),
    ("1. Yarı ve 2. Yarıda Karşılıklı Gol Olur", "Evet / Hayır", (1, 1), (1, 1), "won"), ("1. Yarı ve 2. Yarıda Karşılıklı Gol Olur", "Hayır / Hayır", (1, 0), (1, 0), "won"),
])
def test_yari_marketleri(market, pick, ht, ft, beklenen):
    assert bacak_sonucu(market, pick, M(ft[0], ft[1], ht=ht)) == beklenen, (market, pick)


def test_devre_skoru_olaylardan_turetilir():
    olaylar = [gol(20, "home", "A"), gol(50, "away", "B")]
    m = M(1, 1, events=olaylar)
    assert bacak_sonucu("İlk Yarı Sonucu", "1", m) == "won"        # devre 1-0
    assert bacak_sonucu("İkinci Yarı Sonucu", "2", m) == "won"     # ikinci yarı 0-1


def test_45_dakika_ek_sure_golu_ilk_yaridir():
    m = M(1, 0, events=[gol(45, "home", "A", ek=3)])
    assert bacak_sonucu("İlk Yarı Alt/Üst 0.5", "Üst", m) == "won"


def test_olaylar_skorla_tutmuyorsa_devre_guvenilmez():
    assert bacak_sonucu("İlk Yarı Sonucu", "1", M(2, 1, events=[gol(20, "home", "A")])) == VERI_YOK


def test_kendi_kalesine_gol_rakibe_yazilir_devre_skorunda():
    m = M(0, 1, events=[gol(10, "home", "A", kk=True)])   # ev oyuncusu kendi kalesine: skor deplasmana
    assert bacak_sonucu("İlk Yarı Sonucu", "2", m) == "won"


def test_ilk_gol_hangi_takim():
    m = M(1, 1, events=[gol(30, "away", "B"), gol(60, "home", "A")])
    assert bacak_sonucu("İlk Golü Hangi Takım Atar", "2", m) == "won"
    assert bacak_sonucu("İlk Golü Hangi Takım Atar", "1", m) == "lost"
    assert bacak_sonucu("Son Golü Hangi Takım Atar", "1", m) == "won"
    assert bacak_sonucu("İlk Golü Hangi Takım Atar", "Gol Olmaz", M(0, 0, events=[])) == "won"
    assert bacak_sonucu("İlk Golü Hangi Takım Atar", "1", M(2, 0)) == VERI_YOK   # olay yok


def test_kendi_kalesine_gol_ve_penalti():
    kk = M(0, 1, events=[gol(10, "home", "A", kk=True)])
    assert bacak_sonucu("Kendi Kalesine Gol Olur Mu", "Evet", kk) == "won"
    assert bacak_sonucu("Kendi Kalesine Gol Olur Mu", "Hayır", M(1, 0, events=[gol(10, "home", "A")])) == "won"
    assert bacak_sonucu("Penaltı Olur Mu", "Evet", M(1, 0, events=[gol(10, "home", "A", pen=True)])) == "won"
    # penaltı golü yoksa kaçan penaltı bilinmiyor: Hayır doğrulanamaz
    assert bacak_sonucu("Penaltı Olur Mu", "Hayır", M(1, 0, events=[gol(10, "home", "A")])) == VERI_YOK


# =============== D. İstatistik marketleri ===============
def test_istatistik_yoksa_veri_yok():
    assert bacak_sonucu("Toplam Korner Alt/Üst 9.5", "Üst", M(1, 0)) == VERI_YOK


@pytest.mark.parametrize("market,pick,corners,beklenen", [
    ("Toplam Korner Alt/Üst 9.5", "Üst", (6, 5), "won"), ("Toplam Korner Alt/Üst 9.5", "Alt", (6, 5), "lost"),
    ("Ev Sahibi Toplam Korner Alt/Üst 4.5", "Üst", (5, 3), "won"), ("Deplasman Toplam Korner Altı/Üstü 4.5", "Alt", (5, 3), "won"),
    ("Kim Daha Çok Korner Kullanır", "2", (3, 6), "won"), ("Kim Daha Çok Korner Kullanır", "0", (4, 4), "won"),
    ("Korner Tek/Çift", "Tek", (4, 3), "won"),
    ("Toplam Korner Aralığı", "9-11", (6, 4), "won"), ("Toplam Korner Aralığı", "12+", (7, 6), "won"), ("Toplam Korner Aralığı", "0-8", (4, 3), "won"),
    ("Korner Handikap -0.5", "1", (6, 5), "won"), ("Korner Handikap -0.5", "2", (5, 5), "won"),
])
def test_korner_marketleri(market, pick, corners, beklenen):
    assert bacak_sonucu(market, pick, M(0, 0, stats=ist(corners=corners))) == beklenen, (market, pick)


def test_ilk_yari_korner_veride_olmadigi_icin_veri_yok():
    m = M(0, 0, stats=ist(corners=(6, 5)))
    assert bacak_sonucu("İlk Yarı Toplam Korner Alt/Üst 3.5", "Üst", m) == VERI_YOK
    assert bacak_sonucu("İlk Yarı Korner Handikap 0.5", "1", m) == VERI_YOK


def test_kart_marketleri_kirmizi_varsa_belirsiz():
    temiz = M(0, 0, stats=ist(yellow=(2, 2), red=(0, 0)))
    assert bacak_sonucu("Toplam Kart Alt/Üst 3.5", "Üst", temiz) == "won"
    assert bacak_sonucu("Toplam Kart Alt/Üst 4.5", "Alt", temiz) == "won"
    assert bacak_sonucu("Kim Daha Çok Kart Görür", "0", temiz) == "won"
    kirmizili = M(0, 0, stats=ist(yellow=(2, 2), red=(0, 1)))
    assert bacak_sonucu("Toplam Kart Alt/Üst 3.5", "Üst", kirmizili) == VERI_YOK   # kırmızının kaç kart sayıldığı belirsiz
    assert bacak_sonucu("Kırmızı Kart Olur Mu", "Evet", kirmizili) == "won"
    assert bacak_sonucu("Kırmızı Kart Olur Mu", "Hayır", temiz) == "won"


@pytest.mark.parametrize("market,pick,anahtar,deger,beklenen", [
    ("Takım İsabetli Şut", "Galatasaray 7+", "sot", (8, 3), "won"), ("Takım İsabetli Şut", "Galatasaray 7+", "sot", (6, 9), "lost"),
    ("Takım İsabetli Şut", "Fenerbahçe 3+", "sot", (8, 3), "won"),
    ("Takım Şut", "Galatasaray 15+", "shots", (16, 8), "won"), ("Takım Faul", "Fenerbahçe 13+", "fouls", (10, 12), "lost"),
    ("Takım Ofsayt", "Galatasaray 2+", "offsides", (2, 0), "won"), ("Takım Topa Sahip Olma", "Galatasaray 55+", "possession", (57, 43), "won"),
])
def test_takim_istatistik_marketleri(market, pick, anahtar, deger, beklenen):
    assert bacak_sonucu(market, pick, M(0, 0, stats={anahtar: deger})) == beklenen


def test_takim_adi_farkli_yazilsa_da_eslesir_bulunamazsa_veri_yok():
    m = M(0, 0, home_name="Bayern München", away_name="Barcelona", stats={"sot": (8, 3)})
    assert bacak_sonucu("Takım İsabetli Şut", "Bayern Münih 7+", m) == "won"
    assert bacak_sonucu("Takım İsabetli Şut", "Real Madrid 7+", m) == VERI_YOK


def test_veride_olmayan_marketler_desteklenmiyor():
    for market in ("Hakem VAR Monitörüne Gider Alt/Üst 0.5", "Maç Sonu Tabelada Gösterilen Süre", "Takım Taç Atışı", "Takım Kale Vuruşu"):
        assert bacak_sonucu(market, "Üst", M(1, 0, stats={})) in (DESTEKLENMIYOR, VERI_YOK)


# =============== E. Oyuncu marketleri ===============
OYUNCULAR = [
    {"ad": "Mauro Icardi", "takim": "home", "gol": 2, "asist": 0, "sut": 5, "isabetli_sut": 3, "faul_yapma": 1, "faul_alma": 3, "kurtaris": 0},
    {"ad": "Dries Mertens", "takim": "home", "gol": 0, "asist": 1, "sut": 1, "isabetli_sut": 0, "faul_yapma": 0, "faul_alma": 0, "kurtaris": 0},
    {"ad": "Edin Dzeko", "takim": "away", "gol": 1, "asist": 0, "sut": 3, "isabetli_sut": 2, "faul_yapma": 2, "faul_alma": 1, "kurtaris": 0},
    {"ad": "Fernando Muslera", "takim": "home", "gol": 0, "asist": 0, "sut": 0, "isabetli_sut": 0, "faul_yapma": 0, "faul_alma": 0, "kurtaris": 5},
]
OLAYLAR = [gol(10, "home", "Mauro Icardi", asist="Dries Mertens"), gol(30, "away", "Edin Dzeko"), gol(70, "home", "Mauro Icardi"),
           kart(40, "away", "Edin Dzeko")]


def MO(**kw):
    return M(2, 1, events=OLAYLAR, players=OYUNCULAR, **kw)


def test_oyuncu_gol_atar_ve_ilk_son_gol():
    assert bacak_sonucu("Oyuncu Gol Atar", "Mauro Icardi", MO()) == "won"
    assert bacak_sonucu("Oyuncu Gol Atar", "Dries Mertens", MO()) == "lost"
    assert bacak_sonucu("Oyuncu İlk Golü Atar", "Mauro Icardi", MO()) == "won"
    assert bacak_sonucu("Oyuncu İlk Golü Atar", "Edin Dzeko", MO()) == "lost"
    assert bacak_sonucu("Oyuncu Son Golü Atar", "Mauro Icardi", MO()) == "won"
    assert bacak_sonucu("Oyuncu 2+ Gol Atar (Uzt.Dahil)", "Mauro Icardi", MO()) == "won"
    assert bacak_sonucu("Oyuncu 2+ Gol Atar (Uzt.Dahil)", "Edin Dzeko", MO()) == "lost"
    assert bacak_sonucu("Oyuncu Hat-trick Yapar (Uzt. Dahil)", "Mauro Icardi", MO()) == "lost"


def test_kendi_kalesine_gol_oyuncunun_golu_sayilmaz():
    m = M(0, 1, events=[gol(10, "home", "Mauro Icardi", kk=True)], players=OYUNCULAR)
    assert bacak_sonucu("Oyuncu Gol Atar", "Mauro Icardi", m) == "lost"


def test_oyuncu_asist_kart_ve_birlesikler():
    assert bacak_sonucu("Oyuncu Asist Yapar (Uzt. Dahil)", "Dries Mertens", MO()) == "won"
    assert bacak_sonucu("Oyuncu Gol Atar veya Asist Yapar (Uzt. Dahil)", "Dries Mertens", MO()) == "won"
    assert bacak_sonucu("Oyuncu Gol Atar ve Asist Yapar (Uzt. Dahil)", "Mauro Icardi", MO()) == "lost"
    assert bacak_sonucu("Oyuncu Kart Görür (Uzt. Dahil)", "Edin Dzeko", MO()) == "won"
    assert bacak_sonucu("Oyuncu Kart Görür (Uzt. Dahil)", "Mauro Icardi", MO()) == "lost"
    assert bacak_sonucu("Oyuncu Kırmızı Kart Görür (Uzt. Dahil)", "Edin Dzeko", MO()) == "lost"
    assert bacak_sonucu("Oyuncu Gol Atar ve Kart Görür (Uzt. Dahil)", "Edin Dzeko", MO()) == "won"


def test_oyuncu_istatistik_esikleri():
    assert bacak_sonucu("Oyuncu Şut Çeker (Uzt. Dahil)", "Mauro Icardi 5+", MO()) == "won"
    assert bacak_sonucu("Oyuncu Şut Çeker (Uzt. Dahil)", "Mauro Icardi 6+", MO()) == "lost"
    assert bacak_sonucu("Oyuncu Kaleyi Bulan Şut Çeker (Uzt. Dahil)", "Mauro Icardi 3+", MO()) == "won"
    assert bacak_sonucu("Oyuncu Faul Alır (Uzt. Dahil)", "Mauro Icardi 3+", MO()) == "won"
    assert bacak_sonucu("Oyuncu Faul Yapar (Uzt. Dahil)", "Edin Dzeko 2+", MO()) == "won"
    assert bacak_sonucu("Kaleci Kurtarışı (Uzt. Dahil)", "Fernando Muslera 4+", MO()) == "won"
    assert bacak_sonucu("Oyuncu 3+ Şut Çeker", "Dries Mertens", MO()) == "lost"   # eşik adda: seçim yalnız isim


def test_oyuncu_adi_eslesmesi():
    assert bacak_sonucu("Oyuncu Gol Atar", "Mauro İcardi", MO()) == "won"          # aksan/büyük harf farkı
    assert bacak_sonucu("Oyuncu Gol Atar", "Icardi", MO()) == "won"                # soyadı tek kişi
    assert bacak_sonucu("Oyuncu Gol Atar", "Bilinmeyen Oyuncu", MO()) == VERI_YOK
    iki = OYUNCULAR + [{"ad": "Arda Güler", "takim": "away", "gol": 0, "asist": 0}, {"ad": "Berkay Güler", "takim": "home", "gol": 0, "asist": 0}]
    assert bacak_sonucu("Oyuncu Gol Atar", "Güler", M(2, 1, events=OLAYLAR, players=iki)) == VERI_YOK   # belirsiz soyadı


def test_oyuncu_verisi_yoksa_veri_yok_ve_desteklenmeyenler():
    assert bacak_sonucu("Oyuncu Gol Atar", "Mauro Icardi", M(1, 0)) == VERI_YOK
    assert bacak_sonucu("Oyuncu Kafa İle Gol Atar (Uzt. Dahil)", "Mauro Icardi", MO()) == DESTEKLENMIYOR
    assert bacak_sonucu("Oyuncu Ofsayta Düşer (Uzt. Dahil)", "Mauro Icardi", MO()) == DESTEKLENMIYOR
