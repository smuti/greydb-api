"""Günlük ön maç hazırlığı yaklaşan 10 günü kapsar (Spor Toto programının son maçı ~9 gün ileride olabilir); oran kapsamı 4 gün (96 saat): iddaa eşleştirmesiyle aynı pencere, maç iddaa'ya girince oranı en geç bir sonraki oran koşusunda gelir."""
from datetime import datetime, timedelta

from app.services.skorjin_prematch import Kaynaklar, ORAN_UFKU_SAAT, UFUK_GUN, hazirla

SIMDI = datetime(2026, 11, 14, 3, 30)


def _kaynaklar(cagrilar):
    return Kaynaklar(maclar=lambda b, s: cagrilar.append((b, s)) or [], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {},
                     iddaa=lambda e: (None, {}, {}), mevcut=lambda ids: set(), yaz=lambda k, s: None, fotmob=lambda f: None)


def test_gunluk_pencere_10_gun():
    cagrilar = []
    hazirla("gunluk", True, _kaynaklar(cagrilar), SIMDI, "harman-v1")
    assert UFUK_GUN == 10 and cagrilar == [(SIMDI, SIMDI + timedelta(days=10))]


def test_oran_penceresi_4_gun():
    # 48 saatte, iddaa'nın günlük koşudan sonra listelediği 3-4 gün ilerideki maçlar (Spor Toto haftası) ertesi sabaha kadar oransız kalıyordu
    cagrilar = []
    hazirla("oran", True, _kaynaklar(cagrilar), SIMDI, "harman-v1")
    assert ORAN_UFKU_SAAT == 96 and cagrilar == [(SIMDI, SIMDI + timedelta(hours=96))]
