"""Tercih izleme işleri: hangi pencerede kayıt alınır, matchforge listesi maça nasıl eşlenir, kayıt ve sonuçlandırma akışı (sahte kaynaklarla; DB ve ağ yok)."""
from datetime import datetime, timedelta

import pytest

from app.services.tercih_izleme import Kaynaklar, kaydet, pencere_sec, satirlari_esle, sonuclandir_bekleyenler

SIMDI = datetime(2026, 11, 2, 10, 0)


# ---- pencere seçimi: kickoff'a kalan süre kaç saat, hangi pencere hâlâ yazılmadı ----
@pytest.mark.parametrize("kalan,beklenen", [(30.0, None), (24.0, "acilis"), (12.0, "acilis"), (6.0, "orta"), (3.0, "orta"), (1.0, "kapanis"), (0.2, "kapanis"),
                                            (0.0, None), (-1.0, None)])
def test_pencere_kalan_saate_gore(kalan, beklenen):
    assert pencere_sec(kalan, set()) == beklenen


def test_pencere_yazilmissa_atlanir_ve_geri_doldurulmaz():
    assert pencere_sec(3.0, {"orta"}) is None                      # aynı pencere ikinci kez yazılmaz
    assert pencere_sec(3.0, {"acilis"}) == "orta"
    assert pencere_sec(0.5, {"acilis", "orta"}) == "kapanis"
    assert pencere_sec(0.5, set()) == "kapanis"                    # önceki pencereler kaçırıldıysa yalnız şimdikini yaz


# ---- liste satırlarını maça eşleme ----
def mac(fid, ev, dep, saat, eid=None):
    return {"fotmob_match_id": fid, "iddaa_event_id": eid, "home_name": ev, "away_name": dep, "league_name": "Premier League", "match_date": saat}


def satir(m, kategori, secim, p, oran):
    return {"mac": m, "kategori": kategori, "secim": secim, "olasilik": p, "oran": oran, "guvenilirlik": "g", "not_": ""}


MACLAR = [mac(1, "Chelsea", "Manchester United", SIMDI + timedelta(hours=3), 111), mac(2, "Tottenham Hotspur", "Crystal Palace", SIMDI + timedelta(hours=0.5), 222)]


def test_esleme_ad_ve_pencere():
    satirlar = [satir("Chelsea – Manchester United", "Maç sonucu", "MS 1 (Chelsea)", 0.6, 1.8), satir("Tottenham – Crystal Palace", "Toplam gol", "Gol Üst 2.5", 0.7, None)]
    esle, eslesmeyen = satirlari_esle(MACLAR, {1: "orta", 2: "kapanis"}, satirlar)
    assert eslesmeyen == []
    assert [(r["fotmob_match_id"], r["pencere"], r["kategori"], r["oran"]) for r in esle] == [(1, "orta", "Maç sonucu", 1.8), (2, "kapanis", "Toplam gol", None)]
    assert esle[0]["ev"] == "Chelsea" and esle[0]["dep"] == "Manchester United" and esle[0]["iddaa_match_id"] == 111


def test_esleme_bulunamayan_ayri_raporlanir():
    esle, eslesmeyen = satirlari_esle(MACLAR, {1: "orta"}, [satir("Bilinmeyen – Takım", "Maç sonucu", "MS 1 (Bilinmeyen)", 0.5, 2.0)])
    assert esle == [] and eslesmeyen == ["Bilinmeyen – Takım"]


def test_esleme_kapanis_oran_bir_ve_altini_oransiz_yazar():
    esle, _ = satirlari_esle(MACLAR, {1: "orta"}, [satir("Chelsea – Manchester United", "Maç sonucu", "MS 1 (Chelsea)", 0.95, 1.0)])
    assert esle[0]["oran"] == 1.0                                  # ham oran olduğu gibi saklanır; "1.00 çıkar" kararı rapor katmanında


# ---- kaydet ----
class Kay:
    """Sahte kaynaklar: çağrıları kaydeder."""
    def __init__(self, maclar=MACLAR, mevcut=None, liste=None):
        self.cagri = {"liste": [], "yaz": []}
        self.mevcut_ = mevcut or {}
        self.maclar_ = maclar
        self.liste_ = liste

    def kaynaklar(self):
        def liste(ids):
            self.cagri["liste"].append(list(ids))
            return self.liste_ if self.liste_ is not None else [satir("Chelsea – Manchester United", "Maç sonucu", "MS 1 (Chelsea)", 0.6, 1.8),
                                                                satir("Tottenham – Crystal Palace", "Maç sonucu", "MS 1 (Tottenham)", 0.7, 1.4)]
        return Kaynaklar(maclar=lambda bas, son: self.maclar_, mevcut=lambda ids: self.mevcut_, liste=liste, yaz=lambda r: self.cagri["yaz"].append(r) or len(r),
                         bekleyen=lambda bas: [], sonuc_yaz=lambda *a: None, mac_sonucu=lambda i: None)


def test_kaydet_pencereye_giren_maclari_yazar():
    k = Kay()
    rapor = kaydet(SIMDI, k.kaynaklar(), dry_run=False)
    assert k.cagri["liste"] == [[1, 2]]                           # tek toplu çağrı
    yazilan = k.cagri["yaz"][0]
    assert {(r["fotmob_match_id"], r["pencere"]) for r in yazilan} == {(1, "orta"), (2, "kapanis")}
    assert rapor["aday"] == 2 and rapor["yazilan"] == 2 and rapor["eslesmeyen"] == []


def test_kaydet_zaten_yazilmis_pencereyi_atlar():
    k = Kay(mevcut={1: {"orta"}})
    kaydet(SIMDI, k.kaynaklar(), dry_run=False)
    assert k.cagri["liste"] == [[2]]


def test_kaydet_kuru_calistirma_yazmaz():
    k = Kay()
    rapor = kaydet(SIMDI, k.kaynaklar(), dry_run=True)
    assert k.cagri["yaz"] == [] and rapor["yazilan"] == 0 and rapor["aday"] == 2 and rapor["dry_run"] is True


def test_kaydet_pencere_disindaki_ve_baslamis_maclari_almaz():
    uzak = mac(3, "A", "B", SIMDI + timedelta(hours=40), 3)
    baslamis = mac(4, "C", "D", SIMDI - timedelta(minutes=5), 4)
    k = Kay(maclar=[uzak, baslamis])
    rapor = kaydet(SIMDI, k.kaynaklar(), dry_run=False)
    assert k.cagri["liste"] == [] and rapor["aday"] == 0


def test_kaydet_otuzluk_gruplar():
    cok = [mac(100 + i, f"E{i}", f"D{i}", SIMDI + timedelta(hours=3)) for i in range(65)]
    k = Kay(maclar=cok, liste=[])
    kaydet(SIMDI, k.kaynaklar(), dry_run=False)
    assert [len(g) for g in k.cagri["liste"]] == [30, 30, 5]


def test_kaydet_liste_hatasi_digerlerini_durdurmaz():
    k = Kay()
    ks = k.kaynaklar()
    cagrilar = []

    def liste(ids):
        cagrilar.append(list(ids))
        if len(cagrilar) == 1:
            raise RuntimeError("matchforge yanıt vermedi")
        return []
    import dataclasses
    ks = dataclasses.replace(ks, liste=liste)
    rapor = kaydet(SIMDI, ks, dry_run=False)
    assert rapor["hata"] and rapor["yazilan"] == 0                # hata raporlanır, istisna dışarı sızmaz


# ---- sonuçlandırma ----
def bekleyen(id_, fid, kategori, secim, kickoff=SIMDI - timedelta(hours=5)):
    return {"id": id_, "fotmob_match_id": fid, "ev": "Galatasaray", "dep": "Fenerbahçe", "kategori": kategori, "secim": secim, "kickoff": kickoff}


BITMIS = {"finished": True, "cancelled": False, "score": {"home": 2, "away": 1}, "stats": {"corners": {"home": 8, "away": 3}, "yellow_cards": {"home": 1, "away": 2},
                                                                                         "red_cards": {"home": 0, "away": 0}, "shots_on_target": {"home": 5, "away": 2}}}


def test_sonuclandir_bekleyenler_yazar():
    yazilan = []
    ks = Kaynaklar(maclar=None, mevcut=None, liste=None, yaz=None,
                   bekleyen=lambda bas: [bekleyen(1, 9, "Maç sonucu", "MS 1 (Galatasaray)"), bekleyen(2, 9, "Toplam faul", "Toplam faul Üst 20.5")],
                   sonuc_yaz=lambda id_, sonuc, an: yazilan.append((id_, sonuc)), mac_sonucu=lambda fid: BITMIS)
    rapor = sonuclandir_bekleyenler(SIMDI, ks, dry_run=False)
    assert yazilan == [(1, True), (2, None)]                       # faul istatistiği yok: sonuçlanamadı (None) ama işlendi
    assert rapor["islenen"] == 2 and rapor["tuttu"] == 1 and rapor["sonuclanamayan"] == 1


def test_sonuclandir_bitmemis_maci_atlar():
    yazilan = []
    ks = Kaynaklar(maclar=None, mevcut=None, liste=None, yaz=None, bekleyen=lambda bas: [bekleyen(1, 9, "Maç sonucu", "MS 1 (Galatasaray)")],
                   sonuc_yaz=lambda *a: yazilan.append(a), mac_sonucu=lambda fid: {**BITMIS, "finished": False})
    rapor = sonuclandir_bekleyenler(SIMDI, ks, dry_run=False)
    assert yazilan == [] and rapor["bitmemis"] == 1


def test_sonuclandir_iptal_edilen_maci_isler_ama_sonucsuz():
    yazilan = []
    ks = Kaynaklar(maclar=None, mevcut=None, liste=None, yaz=None, bekleyen=lambda bas: [bekleyen(1, 9, "Maç sonucu", "MS 1 (Galatasaray)")],
                   sonuc_yaz=lambda id_, sonuc, an: yazilan.append((id_, sonuc)), mac_sonucu=lambda fid: {**BITMIS, "finished": True, "cancelled": True})
    sonuclandir_bekleyenler(SIMDI, ks, dry_run=False)
    assert yazilan == [(1, None)]


def test_sonuclandir_kuru_calistirma_yazmaz():
    yazilan = []
    ks = Kaynaklar(maclar=None, mevcut=None, liste=None, yaz=None, bekleyen=lambda bas: [bekleyen(1, 9, "Maç sonucu", "MS 1 (Galatasaray)")],
                   sonuc_yaz=lambda *a: yazilan.append(a), mac_sonucu=lambda fid: BITMIS)
    rapor = sonuclandir_bekleyenler(SIMDI, ks, dry_run=True)
    assert yazilan == [] and rapor["tuttu"] == 1


def test_sonuclandir_ayni_maci_bir_kez_ceker():
    cekilen = []
    ks = Kaynaklar(maclar=None, mevcut=None, liste=None, yaz=None,
                   bekleyen=lambda bas: [bekleyen(i, 9, "Maç sonucu", f"MS {t} (x)") for i, t in ((1, 1), (2, 0), (3, 2))],
                   sonuc_yaz=lambda *a: None, mac_sonucu=lambda fid: cekilen.append(fid) or BITMIS)
    sonuclandir_bekleyenler(SIMDI, ks, dry_run=False)
    assert cekilen == [9]
