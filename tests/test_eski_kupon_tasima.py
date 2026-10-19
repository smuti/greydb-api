"""Eski greydb.coupons + coupon_matches → yeni kupon modeli (saf). Dönüştürülemeyenler nedeniyle atlanır (Tasinamaz)."""
from datetime import datetime
from decimal import Decimal

import pytest

from app.services.eski_kupon_tasima import Tasinamaz, eski_kupon, tarih_coz

OLUSTURMA = datetime(2026, 10, 12, 9, 0)


def kup(**kw):
    return {"id": 18, "type": "acik_radar", "status": "active", "total_odds": Decimal("1.31"), "winnings": None, "created_by_email": "sabri@x",
            "created_at": OLUSTURMA, "updated_at": OLUSTURMA, **kw}


def mac(**kw):
    return {"id": 1, "home_team": "Leipzig", "away_team": "PSV", "league": "UEFA Şampiyonlar Ligi", "prediction": "Üst", "market_name": "Alt/Üst 2.5",
            "odds": Decimal("1.31"), "match_date": "13 Eki 22:00", **kw}


def test_tarih_turkce_gun_ay_saat_tsi_den_utc():
    assert tarih_coz("13 Eki 22:00", OLUSTURMA) == datetime(2026, 10, 13, 19, 0)       # 22:00 TSİ = 19:00 UTC
    assert tarih_coz("1 Oca 00:30", datetime(2025, 12, 30)) == datetime(2025, 12, 31, 21, 30)   # yıl sonu: sonraki yıl, TSİ→UTC bir önceki gün


def test_tarih_cozulemeyenler():
    for s in ("19:00", "", None, "Bugün 19:00", "32 Eki 19:00", "13 Xxx 19:00"):
        with pytest.raises(Tasinamaz):
            tarih_coz(s, OLUSTURMA)


def test_turler_ve_durumlar():
    for tur, seviye in (("acik_radar", "acik"), ("bayi", "bayi")):
        k = eski_kupon(kup(type=tur), [mac()])
        assert k["seviye"] == seviye and k["kaynak"] == "kupon:18"
    for durum, beklenen in (("active", "yayinda"), ("archived", "arsiv"), ("draft", "taslak")):
        assert eski_kupon(kup(status=durum), [mac()])["durum"] == beklenen


@pytest.mark.parametrize("tur", ["premium", "banko", "populer", "yukselen", "uyem", "editor", "bilinmeyen"])
def test_eski_pazarlama_turleri_tasinmaz(tur):
    with pytest.raises(Tasinamaz) as e:
        eski_kupon(kup(type=tur), [mac()])
    assert "tür" in str(e.value)


def test_bacak_eslemesi():
    k = eski_kupon(kup(total_odds=Decimal("1.72")), [mac(), mac(id=2, home_team="Inter", away_team="Club Brugge", market_name="Maç Sonucu", prediction="1", odds=Decimal("1.31"))])
    b = k["bacaklar"][0]
    assert (b["ev"], b["dep"], b["lig"], b["bahis_turu"], b["secim"], b["oran"], b["sonuc"]) == ("Leipzig", "PSV", "UEFA Şampiyonlar Ligi", "Alt/Üst 2.5", "Üst", 1.31, "pending")
    assert b["mac_zamani"] == "2026-10-13T19:00:00Z"
    assert k["toplam_oran"] == 1.72 and k["olusturan_email"] == "sabri@x"


def test_cozulemeyen_lig_adi_atilir():
    assert eski_kupon(kup(total_odds=Decimal("1.31")), [mac(league="League 22353")])["bacaklar"][0]["lig"] is None
    assert eski_kupon(kup(total_odds=Decimal("1.31")), [mac(league="")])["bacaklar"][0]["lig"] is None


def test_marketi_bos_bacak_tasinmaz():
    with pytest.raises(Tasinamaz) as e:
        eski_kupon(kup(total_odds=Decimal("1.31")), [mac(market_name=None)])
    assert "market" in str(e.value)


def test_tarihi_bos_bacak_tasinmaz():
    with pytest.raises(Tasinamaz):
        eski_kupon(kup(total_odds=Decimal("1.31")), [mac(match_date="19:00")])


def test_toplam_oran_tutarsizsa_tasinmaz():
    with pytest.raises(Tasinamaz) as e:
        eski_kupon(kup(total_odds=Decimal("5.19")), [mac(), mac(id=2, home_team="A", away_team="B")])
    assert "oran" in str(e.value)


def test_sistem_yazili_kupon_tasinmaz():
    with pytest.raises(Tasinamaz) as e:
        eski_kupon(kup(winnings="Sistem 2/3", total_odds=Decimal("1.31")), [mac()])
    assert "sistem" in str(e.value)


def test_bacaksiz_ve_ayni_mac_iki_kez():
    with pytest.raises(Tasinamaz):
        eski_kupon(kup(), [])
    with pytest.raises(Tasinamaz):
        eski_kupon(kup(total_odds=Decimal("1.72")), [mac(), mac(id=2, prediction="Alt")])
