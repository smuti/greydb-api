"""Kupon hesap çekirdeği (saf fonksiyonlar): bacak sonucu, toplam oran, sistem kombinasyonları, kupon sonucu.

Kupon iki biçimde: kombine (sistem_k=None; tek maç da kombine) ya da sistem k/n (n bacaktan k'li tüm kombinasyonlar).
Sistemde 1 birim yatırılır ve kombinasyonlara eşit bölünür: ödeme = tutan kombinasyonların oranları toplamı / kombinasyon sayısı.
İptal bacağın oranı 1,00 sayılır. Sistem sonucu: ödeme > 1 tuttu, = 1 iade (void), < 1 yattı.
"""
import re
from itertools import combinations
from math import comb, prod
from typing import Any, Dict, List, Optional

_ESIK = 1e-9


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _cizgi(s: str) -> Optional[float]:
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*$", s)
    return float(m.group(1).replace(",", ".")) if m else None


def bacak_sonucu(market_name: str, pick: str, home: int, away: int) -> Optional[str]:
    """Maç sonu skorundan bacak sonucu: 'won' | 'lost' | 'void' | None (desteklenmeyen bahis türü: elle işaretlenir).
    Yalnızca TAM MAÇ skoruyla belirlenebilen marketler; yarı/handikap/kombine marketler None döner."""
    ad, p = _norm(market_name), _norm(pick)
    if not ad or any(x in ad for x in ("yarı", "handikap", " ve ", "ilk gol", "skor")):
        return None
    toplam = home + away

    if ad in ("maç sonucu", "1x2"):
        sec = {"1": "1", "ev sahibi": "1", "home": "1", "0": "0", "x": "0", "beraberlik": "0", "draw": "0", "2": "2", "deplasman": "2", "away": "2"}.get(p)
        if sec is None:
            return None
        gercek = "1" if home > away else "2" if away > home else "0"
        return "won" if sec == gercek else "lost"

    if re.match(r"^(altı/üstü|alt/üst|over/under)\s*\d", ad):
        cizgi = _cizgi(ad)
        if cizgi is None or p not in ("alt", "üst", "under", "over"):
            return None
        ust = p in ("üst", "over")
        if toplam == cizgi:
            return "void"
        return "won" if (toplam > cizgi) == ust else "lost"

    if ad in ("karşılıklı gol", "both teams to score", "kg"):
        if p not in ("var", "yok", "yes", "no"):
            return None
        ikisi = home > 0 and away > 0
        return "won" if ikisi == (p in ("var", "yes")) else "lost"

    if ad in ("çifte şans", "double chance"):
        sec = re.sub(r"[^0-9xX12]", "", pick or "").upper().replace("0", "X")
        gercek = "1" if home > away else "2" if away > home else "X"
        if sec not in ("1X", "12", "X2"):
            return None
        return "won" if gercek in sec else "lost"

    if ad in ("toplam gol", "total goals"):
        m = re.fullmatch(r"(\d+)\s*[-–]\s*(\d+)", p) or None
        if m:
            return "won" if int(m.group(1)) <= toplam <= int(m.group(2)) else "lost"
        m = re.fullmatch(r"(\d+)\s*\+", p)
        if m:
            return "won" if toplam >= int(m.group(1)) else "lost"
        if p.isdigit():
            return "won" if toplam == int(p) else "lost"
        return None
    return None


def _oran(b: Dict[str, Any]) -> float:
    o = b.get("odds")
    if not isinstance(o, (int, float)) or isinstance(o, bool) or o <= 0:
        raise ValueError("Her bacağın oranı 0'dan büyük olmalı")
    return float(o)


def toplam_oran(bacaklar: List[Dict[str, Any]]) -> Optional[float]:
    """Kombine toplam oran = bacak oranlarının çarpımı (2 ondalık). Bacak yoksa None."""
    if not bacaklar:
        return None
    return round(prod(_oran(b) for b in bacaklar), 2)


def kombinasyon_sayisi(n: int, k: int) -> int:
    return comb(n, k)


def _k_dogrula(n: int, k: Optional[int]) -> None:
    if k is not None and not (2 <= k <= n - 1):
        raise ValueError(f"Sistem k, 2 ile n-1 arasında olmalı (n={n}, k={k})")


def en_yuksek_odeme(bacaklar: List[Dict[str, Any]], sistem_k: Optional[int]) -> Optional[float]:
    """Hepsi tutarsa ödeme: kombinede toplam oran, sistemde kombinasyon oranlarının ortalaması."""
    if not bacaklar:
        return None
    if sistem_k is None:
        return toplam_oran(bacaklar)
    _k_dogrula(len(bacaklar), sistem_k)
    oranlar = [_oran(b) for b in bacaklar]
    kombiler = [prod(c) for c in combinations(oranlar, sistem_k)]
    return round(sum(kombiler) / len(kombiler), 2)


def kupon_sonucu(bacaklar: List[Dict[str, Any]], sistem_k: Optional[int]) -> Dict[str, Any]:
    """Bacak sonuçlarından kupon sonucu. Dönen: {sonuc: pending|won|lost|void, odeme (1 birim için), kombinasyon?: {toplam, tutan}}."""
    n = len(bacaklar)
    if n == 0:
        raise ValueError("Kuponda en az bir bacak olmalı")
    _k_dogrula(n, sistem_k)
    sonuclar = [b.get("result") or "pending" for b in bacaklar]

    if sistem_k is None:   # kombine
        if "lost" in sonuclar:
            return {"sonuc": "lost", "odeme": 0}
        if "pending" in sonuclar:
            return {"sonuc": "pending", "odeme": None}
        if all(s == "void" for s in sonuclar):
            return {"sonuc": "void", "odeme": 1.0}
        odeme = prod(1.0 if s == "void" else _oran(b) for b, s in zip(bacaklar, sonuclar))
        return {"sonuc": "won", "odeme": round(odeme, 4)}

    if "pending" in sonuclar:
        return {"sonuc": "pending", "odeme": None, "kombinasyon": {"toplam": comb(n, sistem_k), "tutan": None}}
    if all(s == "void" for s in sonuclar):
        return {"sonuc": "void", "odeme": 1.0, "kombinasyon": {"toplam": comb(n, sistem_k), "tutan": 0}}
    etkin = [1.0 if s == "void" else _oran(b) for b, s in zip(bacaklar, sonuclar)]
    toplam = comb(n, sistem_k)
    tutan, kazanc = 0, 0.0
    for idx in combinations(range(n), sistem_k):
        if any(sonuclar[i] == "lost" for i in idx):
            continue
        tutan += 1
        kazanc += prod(etkin[i] for i in idx)
    odeme = kazanc / toplam
    sonuc = "void" if abs(odeme - 1.0) < _ESIK else ("won" if odeme > 1.0 else "lost")
    return {"sonuc": sonuc, "odeme": round(odeme, 4), "kombinasyon": {"toplam": toplam, "tutan": tutan}}
