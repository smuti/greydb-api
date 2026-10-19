"""Veri özeti ucu: işlenen maç sayısı ve 'veri noktası' (dolu, teknik olmayan değerler). Sahte veritabanı ve sahte saat."""
from app.services import veri_ozeti as V


def sahte(cevaplar, hata_tablolari=()):
    """sorguda geçen parçaya göre satır döndürür; hata_tablolari'ndaki tablo için istisna fırlatır."""
    cagrilar = []

    def yurut(sql, params=None):
        cagrilar.append(sql)
        for t in hata_tablolari:
            if t in sql:
                raise RuntimeError("tablo yok")
        for parca, satirlar in cevaplar:
            if parca in sql:
                return satirlar
        return []
    yurut.cagrilar = cagrilar
    return yurut


def test_sorgu_teknik_sütunları_ve_boş_değerleri_saymaz():
    sql = V.deger_sorgusu("public.match_stats")
    assert "public.match_stats" in sql and "to_jsonb" in sql
    for teknik in ("id", "created_at", "updated_at"):
        assert f"'{teknik}'" in sql
    assert "_id" in sql          # *_id (kimlik) sütunları da veri noktası sayılmaz
    assert "'null'" in sql       # JSON null sayılmaz


def test_özet_toplar_ve_eksik_tabloyu_atlar():
    q = sahte(
        [("FROM public.matches", [{"toplam": 4000}]), ("public.match_stats", [{"satir": 100, "deger": 1500}]), ("public.match_events", [{"satir": 900, "deger": 5400}])],
        hata_tablolari=("public.match_lineups",),
    )
    o = V.ozet_olustur(q, tablolar=("public.match_stats", "public.match_events", "public.match_lineups"), simdi=1000.0)
    assert o["mac_sayisi"] == 4000
    assert o["veri_noktasi"] == 1500 + 5400
    assert o["tablo_sayisi"] == 2                                   # hata veren tablo sayılmadı, işi durdurmadı
    assert {t["tablo"] for t in o["tablolar"]} == {"match_stats", "match_events"}
    assert o["guncelleme"] == 1000.0


def test_önbellek_süre_dolana_kadar_veritabanına_gitmez():
    V.onbellegi_temizle()
    sayac = {"n": 0}

    def uret():
        sayac["n"] += 1
        return {"mac_sayisi": sayac["n"]}
    assert V.onbellekli(uret, simdi=100.0, ttl=600)["mac_sayisi"] == 1
    assert V.onbellekli(uret, simdi=500.0, ttl=600)["mac_sayisi"] == 1        # 400 sn sonra hâlâ önbellekte
    assert V.onbellekli(uret, simdi=701.0, ttl=600, arka_plan=lambda f: f())["mac_sayisi"] == 1    # süre doldu: eski değer hemen, yenisi arka planda
    assert V.onbellekli(uret, simdi=702.0, ttl=600)["mac_sayisi"] == 2        # arka plan işi bitti: taze değer
    V.onbellegi_temizle()


def test_üretim_hata_verirse_eski_değer_döner():
    V.onbellegi_temizle()
    V.onbellekli(lambda: {"mac_sayisi": 7}, simdi=0.0, ttl=10)

    def bozuk():
        raise RuntimeError("veritabanı kapalı")
    assert V.onbellekli(bozuk, simdi=50.0, ttl=10)["mac_sayisi"] == 7         # süresi dolmuş ama taze üretilemedi: eskisi
    V.onbellegi_temizle()


def test_suresi_dolmus_deger_hemen_doner_yenisi_arka_planda_hazirlanir():
    V.onbellegi_temizle()
    V.onbellekli(lambda: {"mac_sayisi": 1}, simdi=0.0, ttl=10)
    isler = []
    # süre doldu: kullanıcı beklemez, eski değer gelir; yenileme arka plana verilir
    o = V.onbellekli(lambda: {"mac_sayisi": 2}, simdi=50.0, ttl=10, arka_plan=isler.append)
    assert o["mac_sayisi"] == 1 and len(isler) == 1
    isler[0]()                                       # arka plan işi çalışınca önbellek tazelenir
    assert V.onbellekli(lambda: {"mac_sayisi": 3}, simdi=51.0, ttl=10, arka_plan=isler.append)["mac_sayisi"] == 2
    V.onbellegi_temizle()


def test_ayni_anda_tek_yenileme_baslar():
    V.onbellegi_temizle()
    V.onbellekli(lambda: {"mac_sayisi": 1}, simdi=0.0, ttl=10)
    isler = []
    for _ in range(3):
        V.onbellekli(lambda: {"mac_sayisi": 2}, simdi=50.0, ttl=10, arka_plan=isler.append)
    assert len(isler) == 1                           # kuyruk birikmez
    V.onbellegi_temizle()


def test_isinma_degeri_hazirlar_hata_verirse_sessiz_gecer():
    V.onbellegi_temizle()
    V.isin(lambda: {"mac_sayisi": 9}, simdi=0.0)
    assert V.onbellekli(lambda: {"mac_sayisi": 0}, simdi=1.0, ttl=100)["mac_sayisi"] == 9
    V.onbellegi_temizle()

    def bozuk():
        raise RuntimeError("db yok")
    V.isin(bozuk, simdi=0.0)                         # çökmez
    V.onbellegi_temizle()
