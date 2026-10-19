"""DB'de olmayan maçlar için FotMob (matchforge günlük liste) yedeği: yalnızca MAÇ SONU skoru; olay ve istatistik yok → onları isteyen bahisler 'veri_yok' kalır, yanlış sonuç üretilmez."""
from datetime import datetime

from app.services.kupon_fotmob import fotmob_mac_bul, fotmob_mac_sozlugu, fotmob_takim_kimlikleri
from app.services.kupon_hesap import bacak_sonucu

GUN = [
    {"id": 9, "home": {"id": 8371, "name": "Osasuna", "score": None}, "away": {"id": 8696, "name": "Racing Santander", "score": None}, "finished": False, "cancelled": False, "status_id": 1, "kickoff": "2026-10-25T12:00:00.000Z"},
    {"id": 5868097, "home": {"id": 8371, "name": "Osasuna", "score": 2}, "away": {"id": 8696, "name": "Racing Santander", "score": 0}, "finished": True, "cancelled": False, "status_id": 6, "kickoff": "2026-10-18T12:00:00.000Z"},
    {"id": 5881697, "home": {"name": "Dynamo Dresden", "score": 2}, "away": {"name": "Magdeburg", "score": 0}, "finished": True, "cancelled": False, "status_id": 6, "kickoff": "2026-10-18T11:30:00.000Z"},
    {"id": 6361398, "home": {"name": "Union Rochefortoise", "score": 3}, "away": {"name": "Gent", "score": 3}, "finished": True, "cancelled": False, "status_id": 13, "kickoff": "2026-10-18T11:30:00.000Z"},
    {"id": 1, "home": {"name": "Belgium", "score": 3}, "away": {"name": "Turkiye", "score": 0}, "finished": True, "cancelled": False, "status_id": 6, "kickoff": "2026-10-02T18:45:00.000Z"},
    {"id": 2, "home": {"name": "Sevilla", "score": None}, "away": {"name": "Betis", "score": None}, "finished": False, "cancelled": False, "status_id": 1, "kickoff": "2026-10-18T19:00:00.000Z"},
]


def getir(tarih):          # sahte günlük liste: her maç yalnız kendi gününün listesinde (gerçek davranış)
    getir.cagrilar.append(tarih)
    return [m for m in GUN if m["kickoff"][:10].replace("-", "") == tarih]
getir.cagrilar = []


def bacak(ev, dep, z="2026-10-18T12:00:00", **o):
    return {"ev": ev, "dep": dep, "mac_zamani": datetime.fromisoformat(z), **o}


def test_ad_farki_olsa_bile_bulur_ve_mac_sonu_skorunu_verir():
    m = fotmob_mac_bul(getir, bacak("Osasuna", "Santander"))
    assert (m["home"], m["away"]) == (2, 0) and m["fotmob_id"] == 5868097


def test_ulke_adlari_turkceden_cevrilir():
    m = fotmob_mac_bul(getir, bacak("Belçika", "Türkiye", "2026-10-02T18:45:00"))
    assert (m["home"], m["away"]) == (3, 0)


def test_bitmemis_ya_da_normal_sure_disinda_biten_mac_alinmaz():
    assert fotmob_mac_bul(getir, bacak("Sevilla", "Betis", "2026-10-18T19:00:00")) is None       # henüz başlamadı
    assert fotmob_mac_bul(getir, bacak("Union Rochefortoise", "Gent")) is None                    # penaltılar: 90 dk sonucu belirsiz


def test_bulunamayan_mac_none_ve_sozluk_yalniz_skor_icerir():
    assert fotmob_mac_bul(getir, bacak("Olmayan FK", "Yok SK")) is None
    d = fotmob_mac_sozlugu(GUN[0])
    assert d["events"] is None and d["stats"] == {} and d["players"] == [] and d["_dis"] is True


def test_skorla_cozulen_bahisler_sonuclanir_olay_gerektirenler_veri_yok_kalir():
    d = fotmob_mac_sozlugu(GUN[1])                      # 2-0
    assert bacak_sonucu("Çifte Şans", "0 ve 2", d) == "lost"
    assert bacak_sonucu("Çifte Şans", "1 ve 0", d) == "won"
    assert bacak_sonucu("Karşılıklı Gol", "Var", d) == "lost"
    assert bacak_sonucu("Alt/Üst 2.5", "Üst", d) == "lost"
    assert bacak_sonucu("İlk Yarı Sonucu", "1", d) == "veri_yok"            # olay yok: yanlış sonuç uydurulmaz
    assert bacak_sonucu("Toplam Korner Alt/Üst 8.5", "Üst", d) == "veri_yok"


def test_gun_listesi_hata_verirse_none_doner_cokmez():
    def bozuk(t):
        raise RuntimeError("ağ yok")
    assert fotmob_mac_bul(bozuk, bacak("Osasuna", "Santander")) is None


def test_logo_icin_takim_kimlikleri_bitmemis_maclarda_da_bulunur():
    assert fotmob_takim_kimlikleri(getir, bacak("Osasuna", "Santander", "2026-10-25T12:00:00")) == (8371, 8696, 9)
    assert fotmob_takim_kimlikleri(getir, bacak("Osasuna", "Santander")) == (8371, 8696, 5868097)
    assert fotmob_takim_kimlikleri(getir, bacak("Olmayan", "Yok")) == (None, None, None)


def test_logo_aramasinda_iki_aday_varsa_tahmin_yok():
    ikili = [{"id": 1, "home": {"id": 10, "name": "Osasuna", "score": None}, "away": {"id": 11, "name": "Racing Santander", "score": None}, "finished": False, "cancelled": False, "status_id": 1},
             {"id": 2, "home": {"id": 12, "name": "Osasuna", "score": None}, "away": {"id": 13, "name": "Racing Santander", "score": None}, "finished": False, "cancelled": False, "status_id": 1}]
    assert fotmob_takim_kimlikleri(lambda t: ikili, bacak("Osasuna", "Santander")) == (None, None, None)
