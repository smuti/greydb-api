"""Çelişen bacak sonucu düzeltme kararı: yalnızca motor kesin bir sonuç verdiyse ve mevcut sonuçtan farklıysa düzeltilir."""
from app.services.kupon_celiski import duzeltme_karari


def test_motor_farkli_kesin_sonuc_verirse_duzeltilir():
    assert duzeltme_karari("won", "lost") == "lost"
    assert duzeltme_karari("void", "lost") == "lost"
    assert duzeltme_karari("lost", "won") == "won"


def test_ayni_sonuc_degisiklik_yok():
    assert duzeltme_karari("won", "won") is None


def test_motor_emin_degilse_dokunmaz():
    for belirsiz in ("veri_yok", "desteklenmiyor", None, ""):
        assert duzeltme_karari("won", belirsiz) is None


def test_bekleyen_bacaga_dokunmaz_ve_gecersiz_deger_yok_sayilir():
    assert duzeltme_karari("pending", "lost") is None      # bekleyen sonuçlandırma işinin görevi
    assert duzeltme_karari(None, "lost") is None
    assert duzeltme_karari("won", "xyz") is None


def test_silme_yalniz_tahminden_tasinan_kuponlar_icin_izinli():
    from app.services.kupon_celiski import silinebilir_mi
    assert silinebilir_mi("tahmin:38") is True
    assert silinebilir_mi("kupon:12") is False      # eski kuponlardan taşınan
    assert silinebilir_mi(None) is False            # admin'in elle oluşturduğu kupon (kaynak yok): asla
    assert silinebilir_mi("") is False
