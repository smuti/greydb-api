"""Kupon modeli: doğrulama, oran hesabı, sonuç özeti, Bayi/Elit kilidi. Saf fonksiyonlar, veritabanı yok."""
import pytest

from app.services.kupon_model import KuponHatasi, dogrula_olustur, kilitle, kupon_ozeti, seviye_yeter


def bacak(ev="Galatasaray", dep="Fenerbahçe", oran=1.5, secim="1", tur="Maç Sonucu", **kw):
    return {"iddaa_event_id": kw.pop("iddaa_event_id", None), "fotmob_match_id": None, "ev": ev, "dep": dep, "lig": "Süper Lig",
            "mac_zamani": kw.pop("mac_zamani", "2026-10-25T17:00:00Z"), "bahis_turu": tur, "secim": secim, "oran": oran, **kw}


def girdi(**kw):
    return {"seviye": "acik", "bacaklar": [bacak()], "nedenler": ["Form iyi"], "ana_risk": "Erken gol", **kw}


# ---- Oluşturma ----
def test_kombine_toplam_oran_sunucuda_hesaplanir():
    k = dogrula_olustur(girdi(bacaklar=[bacak(oran=1.5), bacak(ev="A", dep="B", oran=2.0, iddaa_event_id=2, mac_zamani="2026-10-25T18:00:00Z")]))
    assert k["toplam_oran"] == 3.0 and k["en_yuksek_odeme"] == 3.0 and k["sistem_k"] is None and k["kombinasyon_sayisi"] == 1


def test_istemcinin_gonderdigi_oran_yok_sayilir():
    k = dogrula_olustur(girdi(toplam_oran=99.0))
    assert k["toplam_oran"] == 1.5


def test_sistem_kuponu_en_yuksek_odeme_ve_kombinasyon():
    b = [bacak(ev=str(i), dep="X", oran=o, iddaa_event_id=i) for i, o in enumerate((2, 3, 4), 1)]
    k = dogrula_olustur(girdi(bacaklar=b, sistem_k=2))
    assert k["kombinasyon_sayisi"] == 3 and k["en_yuksek_odeme"] == pytest.approx(8.67, abs=0.01) and k["toplam_oran"] is None


@pytest.mark.parametrize("hata_alani,degisiklik", [
    ("seviye", {"seviye": "gri"}), ("seviye", {"seviye": None}),
    ("bacaklar", {"bacaklar": []}), ("bacaklar", {"bacaklar": [bacak()] * 21}),
    ("sistem_k", {"sistem_k": 2}),                                            # 1 bacak: k geçersiz
    ("nedenler", {"nedenler": [1, 2]}), ("nedenler", {"nedenler": ["x"] * 11}), ("nedenler", {"nedenler": "tek metin"}),
    ("ana_risk", {"ana_risk": "x" * 501}), ("durum", {"durum": "silindi"}), ("zamanlama", {"zamanlama": "dün"}),
])
def test_gecersiz_girdi_hata_alani_belirtir(hata_alani, degisiklik):
    with pytest.raises(KuponHatasi) as e:
        dogrula_olustur(girdi(**degisiklik))
    assert e.value.alan == hata_alani


@pytest.mark.parametrize("degisiklik", [{"oran": 0}, {"oran": -1}, {"oran": None}, {"oran": "x"}, {"oran": 1001}, {"secim": " "}, {"bahis_turu": ""},
                                         {"ev": ""}, {"dep": ""}, {"mac_zamani": "yarın"}, {"mac_zamani": None}])
def test_gecersiz_bacak_alani(degisiklik):
    with pytest.raises(KuponHatasi) as e:
        dogrula_olustur(girdi(bacaklar=[bacak(**degisiklik)]))
    assert e.value.alan.startswith("bacaklar[0]")


def test_ayni_mactan_iki_bacak_olmaz():
    with pytest.raises(KuponHatasi) as e:
        dogrula_olustur(girdi(bacaklar=[bacak(iddaa_event_id=5), bacak(iddaa_event_id=5, secim="Üst", tur="Alt/Üst 2.5")]))
    assert e.value.alan == "bacaklar"
    with pytest.raises(KuponHatasi):     # kimlik yokken aynı takımlar ve saat
        dogrula_olustur(girdi(bacaklar=[bacak(), bacak(secim="X")]))


def test_metinler_kirpilir_ve_varsayilanlar():
    k = dogrula_olustur(girdi(bacaklar=[bacak(secim="  1  ", tur=" Maç Sonucu ")], nedenler=["  a  ", "", "b"]))
    assert k["bacaklar"][0]["secim"] == "1" and k["bacaklar"][0]["bahis_turu"] == "Maç Sonucu"
    assert k["nedenler"] == ["a", "b"]
    assert k["durum"] == "taslak" and k["zamanlama"] == "pre" and k["one_cikan"] is False


# ---- Sonuç özeti ----
def kb(sonuc, oran=2.0):
    return {"oran": oran, "sonuc": sonuc}


def test_kupon_ozeti_kombine():
    assert kupon_ozeti([kb("won"), kb("won", 1.5)], None)["sonuc"] == "won"
    assert kupon_ozeti([kb("won"), kb("pending")], None)["sonuc"] == "pending"
    assert kupon_ozeti([kb("lost"), kb("pending")], None)["sonuc"] == "lost"


def test_kupon_ozeti_sistem_odeme():
    o = kupon_ozeti([kb("won", 2), kb("won", 3), kb("lost", 4)], 2)
    assert o["sonuc"] == "won" and o["odeme"] == 2.0 and o["kombinasyon"] == {"toplam": 3, "tutan": 1}


# ---- Seviye ve kilit ----
@pytest.mark.parametrize("kupon,uye,beklenen", [
    ("acik", "acik", True), ("acik", "elit", True), ("bayi", "acik", False), ("bayi", "bayi", True), ("bayi", "elit", True),
    ("elit", "acik", False), ("elit", "bayi", False), ("elit", "elit", True), ("elit", None, False), ("acik", None, True),
])
def test_seviye_birikimli(kupon, uye, beklenen):
    assert seviye_yeter(kupon, uye) is beklenen


def tam_kupon(seviye="elit"):
    return {"id": 1, "seviye": seviye, "durum": "yayinda", "sistem_k": None, "toplam_oran": 3.0, "en_yuksek_odeme": 3.0, "kombinasyon_sayisi": 1,
            "sonuc": "won", "odeme": 3.0, "nedenler": ["gizli neden"], "ana_risk": "gizli risk", "yorum": "gizli yorum", "sinyal": "guclu", "risk_seviyesi": "orta",
            "bacaklar": [{"id": 9, "ev": "A", "dep": "B", "lig": "L", "mac_zamani": "2026-10-25T17:00:00Z", "iddaa_event_id": 3, "fotmob_match_id": 4,
                          "bahis_turu": "Maç Sonucu", "secim": "1", "oran": 1.5, "sonuc": "won"}]}


def test_yetkisiz_gorunum_secim_oran_neden_riski_gizler_maci_gosterir():
    g = kilitle(tam_kupon("elit"), "acik")
    assert g["kilitli"] is True
    for alan in ("nedenler", "ana_risk", "yorum", "toplam_oran", "en_yuksek_odeme", "odeme", "sinyal", "risk_seviyesi"):
        assert g[alan] is None, alan
    b = g["bacaklar"][0]
    assert (b["ev"], b["dep"], b["lig"], b["mac_zamani"]) == ("A", "B", "L", "2026-10-25T17:00:00Z")
    for alan in ("bahis_turu", "secim", "oran", "sonuc", "iddaa_event_id", "fotmob_match_id"):
        assert b[alan] is None, alan
    assert g["sonuc"] == "won"          # kuponun tuttu/yattı bilgisi herkese açık (şeffaflık)
    assert g["seviye"] == "elit" and g["bacak_sayisi"] == 1


def test_yetkili_gorunum_degismez():
    k = tam_kupon("bayi")
    g = kilitle(k, "elit")
    assert g["kilitli"] is False and g["bacaklar"][0]["secim"] == "1" and g["nedenler"] == ["gizli neden"]


def test_kilit_girdiyi_degistirmez():
    k = tam_kupon("elit")
    kilitle(k, "acik")
    assert k["bacaklar"][0]["secim"] == "1"


def test_taslak_kupon_yetkisizlere_hic_donmez():
    from app.services.kupon_model import gorunur_mu
    assert gorunur_mu({"durum": "taslak"}, "elit") is False
    assert gorunur_mu({"durum": "arsiv"}, "acik") is False
    assert gorunur_mu({"durum": "yayinda"}, None) is True


# ---- Yayın zorunlulukları (siki=True) ----
def yayin(**kw):
    return girdi(**{"durum": "yayinda", "sinyal": "guclu", "risk_seviyesi": "orta", **kw})


def test_siki_yayinda_ana_risk_sinyal_ve_risk_zorunlu_neden_degil():
    assert dogrula_olustur(yayin(), siki=True)["durum"] == "yayinda"
    assert dogrula_olustur(yayin(nedenler=[]), siki=True)["durum"] == "yayinda"       # neden artık zorunlu değil (formdan kaldırıldı)
    for degisiklik, alan in (({"ana_risk": None}, "ana_risk"), ({"ana_risk": " "}, "ana_risk"),
                             ({"sinyal": None}, "sinyal"), ({"risk_seviyesi": None}, "risk_seviyesi")):
        with pytest.raises(KuponHatasi) as e:
            dogrula_olustur(yayin(**degisiklik), siki=True)
        assert e.value.alan == alan, degisiklik


def test_siki_olmayan_ya_da_taslak_serbest():
    assert dogrula_olustur(girdi(durum="yayinda", nedenler=[], ana_risk=None))["durum"] == "yayinda"      # siki=False (eski kayıtların durum geçişi)
    assert dogrula_olustur(girdi(durum="taslak", nedenler=[], ana_risk=None), siki=True)["durum"] == "taslak"
    assert dogrula_olustur(girdi(durum="arsiv", nedenler=[], ana_risk=None), siki=True)["durum"] == "arsiv"


def test_sinyal_ve_risk_degerleri_gecerli_olmali():
    for kotu in ({"sinyal": "süper"}, {"risk_seviyesi": "korkunç"}):
        with pytest.raises(KuponHatasi):
            dogrula_olustur(girdi(**kotu))
    assert dogrula_olustur(girdi(sinyal="orta", risk_seviyesi="dusuk-orta"))["sinyal"] == "orta"


# ---- Maç başına kısa gerekçe (paylaşım görseli) ----
def test_kisa_nedenler_yoksa_bos_liste():
    assert dogrula_olustur(girdi())["bacaklar"][0]["kisa_nedenler"] == []


def test_kisa_nedenler_kirpilir_bos_satirlar_atilir():
    b = bacak(kisa_nedenler=["  Hücum gücü yüksek ", "", "   ", "Deplasmanda gol buluyor"])
    assert dogrula_olustur(girdi(bacaklar=[b]))["bacaklar"][0]["kisa_nedenler"] == ["Hücum gücü yüksek", "Deplasmanda gol buluyor"]


@pytest.mark.parametrize("kotu", [["a", "b", "c", "d"], ["x" * 81], "tek metin", [1, 2], {"a": 1}])
def test_gecersiz_kisa_nedenler_hata_alani_belirtir(kotu):
    with pytest.raises(KuponHatasi) as e:
        dogrula_olustur(girdi(bacaklar=[bacak(kisa_nedenler=kotu)]))
    assert e.value.alan == "bacaklar[0].kisa_nedenler"


def test_kisa_nedenler_en_fazla_3_ve_80_karakter_sinirda_gecer():
    assert len(dogrula_olustur(girdi(bacaklar=[bacak(kisa_nedenler=["x" * 80] * 3)]))["bacaklar"][0]["kisa_nedenler"]) == 3


def test_kilitli_kuponda_kisa_nedenler_sizmaz():
    k = tam_kupon("elit")
    k["bacaklar"][0]["kisa_nedenler"] = ["gizli kısa gerekçe"]
    assert kilitle(k, "acik")["bacaklar"][0]["kisa_nedenler"] is None
    assert kilitle(k, "elit")["bacaklar"][0]["kisa_nedenler"] == ["gizli kısa gerekçe"]
