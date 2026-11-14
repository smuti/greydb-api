"""Günlük ön maç hazırlığı yaklaşan 10 günü kapsar (Spor Toto programının son maçı ~9 gün ileride olabilir); oran kapsamı 48 saat kalır."""
from datetime import datetime, timedelta

from app.services.skorjin_prematch import Kaynaklar, UFUK_GUN, hazirla

SIMDI = datetime(2026, 11, 14, 3, 30)


def _kaynaklar(cagrilar):
    return Kaynaklar(maclar=lambda b, s: cagrilar.append((b, s)) or [], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {},
                     iddaa=lambda e: (None, {}, {}), mevcut=lambda ids: set(), yaz=lambda k, s: None, fotmob=lambda f: None)


def test_gunluk_pencere_10_gun():
    cagrilar = []
    hazirla("gunluk", True, _kaynaklar(cagrilar), SIMDI, "harman-v1")
    assert UFUK_GUN == 10 and cagrilar == [(SIMDI, SIMDI + timedelta(days=10))]


def test_oran_penceresi_48_saat_kalir():
    cagrilar = []
    hazirla("oran", True, _kaynaklar(cagrilar), SIMDI, "harman-v1")
    assert cagrilar == [(SIMDI, SIMDI + timedelta(hours=48))]
