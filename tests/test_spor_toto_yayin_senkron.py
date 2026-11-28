"""Spor Toto yayın bağlantısı otomatik senkronu: yalnız ana analiz videosu, yalnız boş bağlantı, elle girilene dokunmaz."""
from app.services import spor_toto_yayin_senkron as y
from app.services import zamanlayici as Z


def entry(vid, baslik, tarih="2026-11-26"):
    return f"<entry><yt:videoId>{vid}</yt:videoId><title>{baslik}</title><published>{tarih}T10:00:00+00:00</published></entry>"


AKIS = "<feed>" + "".join([      # "SPOR TOTO MASASI" oynatma listesinin gerçek başlıkları
    entry("l-hkw1l7z08", "SPOR TOTO 18. OYUN PROGRAMI: 13 BİLDİK! BU KEZ 15 GELİR Mİ? | 15 Maç Analizi", "2026-11-26"),
    entry("cNQE49-eWWw", "SPOR TOTO 17. OYUN PROGRAMI: BU 3 MAÇ KUPONU YIKAR! | 15 Maç Analizi", "2026-11-23"),
    entry("_jZslx_US2w", "🚨⚽ SPOR TOTO 16. HAFTA CANLI! | 15 Maçta Kritik Analiz ve Tuzaklar", "2026-11-20"),
    entry("YUncd0tBz8o", "SPOR TOTO 16. HAFTA: BU 3 MAÇ KUPONLARI YIKAR! | 15 Maç Analizi", "2026-11-16"),
    entry("8P7TyPqnpmw", "SPOR TOTO 11. HAFTA TAHMİNLERİ 🔥 EKONOMİK KUPON &amp; TEK ATIŞ!", "2026-10-21"),
    entry("vCIw2jenJYc", "SPOR TOTO 11. HAFTA TAHMİNLERİ", "2026-10-21"),
    entry("0LQtl4GyER8", "SPOR TOTO 9. HAFTA TAHMİNLERİ: 4 KRİTİK MAÇA DİKKAT! | 15 MAÇ ANALİZİ", "2026-10-05"),
]) + "</feed>"


def test_liste_basliklari_haftaya_eslenir_emojili_ve_eski_bicim_dahil():
    v = y.hafta_videolari(y.akis_oku(AKIS))
    assert v[18]["id"] == "l-hkw1l7z08" and v[17]["id"] == "cNQE49-eWWw" and v[9]["id"] == "0LQtl4GyER8"
    assert set(v) == {9, 11, 16, 17, 18}


def test_ayni_hafta_birden_cok_videoda_ana_analiz_kazanir_canli_tekrari_degil():
    v = y.hafta_videolari(y.akis_oku(AKIS))
    assert v[16]["id"] == "YUncd0tBz8o"      # "CANLI" tekrarı (_jZslx_US2w) daha yeni olsa da ana analiz videosu seçilir
    assert v[11]["id"] == "8P7TyPqnpmw"      # analiz etiketi yok: aynı gün, akıştaki ilk


def test_alakasiz_baslik_eslesmez():
    xml = "<feed>" + entry("aaaaaaaaaaa", "Spor Toto 1960'tan Bugüne") + entry("bbbbbbbbbbb", "Süper Lig 12. hafta tahminleri") + "</feed>"
    assert y.hafta_videolari(y.akis_oku(xml)) == {}


def test_gecersiz_video_kimligi_atilir_ve_varliklar_cozulur():
    xml = "<feed>" + entry("kisa", "SPOR TOTO 18. OYUN PROGRAMI: x") + entry("ccccccccccc", "SPOR TOTO 18. OYUN PROGRAMI: A &amp; B") + "</feed>"
    v = y.akis_oku(xml)
    assert [x["id"] for x in v] == ["ccccccccccc"] and v[0]["baslik"].endswith("A & B")


def calistir(haftalar, dry_run=False, akis=AKIS):
    yazilan = []
    r = y.calistir(dry_run=dry_run, akis=lambda: akis, haftalar=lambda: haftalar, yaz=lambda h, l: yazilan.append((h, l)) or 1)
    return r, yazilan


def test_bos_hafta_doldurulur_elle_girilene_dokunulmaz():
    haftalar = [{"hafta_no": 1545, "hafta_etiketi": "18. Oyun Programı", "youtube_linki": None},
                {"hafta_no": 1544, "hafta_etiketi": "17. Oyun Programı", "youtube_linki": "https://youtu.be/ELLE_GIRILEN"},      # dolu: dokunma
                {"hafta_no": 1546, "hafta_etiketi": "19. Oyun Programı", "youtube_linki": ""}]                                      # video henüz yok
    r, yazilan = calistir(haftalar)
    assert yazilan == [(1545, "https://www.youtube.com/watch?v=l-hkw1l7z08")]
    assert [x["hafta"] for x in r["yazilan"]] == [18] and r["bekleyen"] == [19]


def test_kuru_calistirma_yazmaz_ama_raporlar():
    r, yazilan = calistir([{"hafta_no": 1545, "hafta_etiketi": "18. Oyun Programı", "youtube_linki": None}], dry_run=True)
    assert yazilan == [] and r["yazilan"][0]["video_id"] == "l-hkw1l7z08" and r["dry_run"] is True


def test_etiketsiz_haftada_hafta_no_kullanilir_ve_bos_akis_cokmez():
    r, yazilan = calistir([{"hafta_no": 18, "hafta_etiketi": None, "youtube_linki": None}])
    assert yazilan[0][0] == 18
    r2, y2 = calistir([{"hafta_no": 1545, "hafta_etiketi": "18. Oyun Programı", "youtube_linki": None}], akis="")
    assert y2 == [] and r2["bekleyen"] == [18]


def test_zamanlayicida_kayitli_ve_hata_disari_cikmaz(monkeypatch):
    tablo = {ad: (kw, is_) for ad, kw, is_ in Z.isler()}
    assert tablo["spor-toto-yayin-baglantisi"][0] == dict(hour="*", minute="12,42")
    monkeypatch.setattr(y, "calistir", lambda dry_run=True: (_ for _ in ()).throw(RuntimeError("youtube kapalı")))
    tablo["spor-toto-yayin-baglantisi"][1]()      # istisna dışarı çıkmamalı (zamanlayıcı iş parçacığı ölmesin)
