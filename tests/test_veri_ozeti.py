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
    assert V.onbellekli(uret, simdi=701.0, ttl=600)["mac_sayisi"] == 2        # süre doldu
    V.onbellegi_temizle()


def test_üretim_hata_verirse_eski_değer_döner():
    V.onbellegi_temizle()
    V.onbellekli(lambda: {"mac_sayisi": 7}, simdi=0.0, ttl=10)

    def bozuk():
        raise RuntimeError("veritabanı kapalı")
    assert V.onbellekli(bozuk, simdi=50.0, ttl=10)["mac_sayisi"] == 7         # süresi dolmuş ama taze üretilemedi: eskisi
    V.onbellegi_temizle()
