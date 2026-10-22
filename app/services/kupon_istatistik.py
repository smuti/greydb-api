"""Sonuçlar sayfası istatistiği: sonuçlanmış kuponlardan özet, form, seri, aylık ve günlük dağılım (saf fonksiyon).

Birim kâr: her kupona 1 birim yatırılır; kâr = ödeme - 1 (kombinede tuttu: oran-1, yattı: -1; sistemde kısmi ödeme kesirli, iade 0).
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from math import sqrt
from typing import Any, Dict, List


def _wilson(basari: int, n: int, z: float = 1.96):
    if n == 0:
        return None, None
    p = basari / n
    payda = 1 + z * z / n
    merkez = (p + z * z / (2 * n)) / payda
    yari = z * sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / payda
    return round(max(0.0, merkez - yari), 4), round(min(1.0, merkez + yari), 4)


def _tarih(k: Dict[str, Any]):
    """Aylık/günlük dağılımın tarihi: kuponun ilk maçının zamanı (sonuçlar listesiyle aynı). Maç zamanı yoksa sonuçlanma zamanı."""
    zamanlar = [b["mac_zamani"] for b in k.get("bacaklar") or [] if b.get("mac_zamani")]
    return min(zamanlar) if zamanlar else k.get("sonuc_zamani")


def sonuc_istatistik(kuponlar: List[Dict[str, Any]], simdi: datetime) -> Dict[str, Any]:
    bitenler = sorted([k for k in kuponlar if k.get("sonuc") in ("won", "lost", "void")], key=lambda k: k["sonuc_zamani"] or "", reverse=True)
    sayac = Counter(k["sonuc"] for k in bitenler)
    kararli = [k for k in bitenler if k["sonuc"] in ("won", "lost")]
    karar = sayac["won"] + sayac["lost"]
    low, high = _wilson(sayac["won"], karar)
    oran = lambda k: k["toplam_oran"] if k.get("toplam_oran") is not None else k.get("en_yuksek_odeme")   # noqa: E731
    form = [k["sonuc"] for k in kararli]
    seri = {"type": None, "count": 0}
    if form:
        seri = {"type": form[0], "count": next((i for i, s in enumerate(form) if s != form[0]), len(form))}
    aylik, gunluk = defaultdict(Counter), defaultdict(Counter)
    sinir = (simdi - timedelta(days=90)).date().isoformat()
    for k in bitenler:
        z = _tarih(k)
        if not z:
            continue
        aylik[z[:7]][k["sonuc"]] += 1
        if z[:10] >= sinir:
            gunluk[z[:10]][k["sonuc"]] += 1
    satir = lambda anahtar, ad, c: {anahtar: ad, "won": c["won"], "lost": c["lost"], "void": c["void"]}   # noqa: E731
    return {
        "stats": {
            "settled": len(bitenler), "decided": karar, "won": sayac["won"], "lost": sayac["lost"], "void": sayac["void"],
            "win_rate": round(sayac["won"] / karar, 4) if karar else None, "win_rate_low": low, "win_rate_high": high,
            "avg_odds": round(sum(oran(k) for k in kararli) / karar, 4) if karar else None,
            "unit_profit": round(sum((k["odeme"] if k["odeme"] is not None else 0) - 1 for k in bitenler), 4),
            "form": form[:10], "streak": seri,
        },
        "series": {
            "monthly": [satir("month", a, aylik[a]) for a in sorted(aylik)],
            "daily": [satir("date", g, gunluk[g]) for g in sorted(gunluk)],
            "last_updated": max((k["sonuc_zamani"] for k in bitenler if k["sonuc_zamani"]), default=None),
        },
    }
