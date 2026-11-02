"""Farklı ülke liglerinden takımların maçında (Avrupa kupaları) piyasa oranından 1-0-2 ihtimali.

Neden: istatistiksel model (matchforge /api/ms/predict) yalnızca iç ligleri görür; Fenerbahçe'nin Süper Lig gücü ile Liverpool'un PL gücü aynı
ölçekte değildir (kupa maçları eğitime girmiyor). Oran ise lig farkını zaten fiyatlar. Bu modül iddaa "Maç Sonucu" pazarından komisyonu çıkarıp
matchforge sonucuyla AYNI şekilde (p1/p0/p2 + decision + facts) bir tahmin döndürür; böylece kayıt, ekran ve Skorjin akışı değişmeden çalışır.
"""
from typing import Any, Dict, List, Optional

from app.services.iddaa_eslestir import takim_benzer

SINGLE_MIN = 0.50   # matchforge ms_model/config.py ile aynı eşikler: >= tek (banko)
DOUBLE_MIN = 0.43   # >= favori + beraberlik, altı üçlü
KAYNAK = "piyasa-iddaa"

# Spor Toto turnuva kimliği -> ad. Haritada olmayan kimlikte ad UYDURULMAZ (çağıran genel ad kullanır).
TURNUVALAR = {218: "Şampiyonlar Ligi", 237: "Avrupa Ligi", 236: "Konferans Ligi"}
GENEL_TURNUVA = "Avrupa Kupası"


# public.leagues: 1 Champions League, 2 Europa League, 39 Conference League. Bu "lige" kayıtlı takımın iç ligi yoktur (Shakhtar, Sporting...):
# iki takım da aynı kupa kimliğinde görünse bile maç farklı ülke liglerindendir, model uygun değil.
AVRUPA_KUPA_LIG_IDLERI = {1, 2, 39}


def farkli_lig_mi(ev_lig_id: Optional[int], dep_lig_id: Optional[int]) -> bool:
    if ev_lig_id in AVRUPA_KUPA_LIG_IDLERI or dep_lig_id in AVRUPA_KUPA_LIG_IDLERI:
        return True
    return ev_lig_id != dep_lig_id


def turnuva_adi(turnuva_id: Optional[int]) -> Optional[str]:
    return TURNUVALAR.get(turnuva_id) if turnuva_id is not None else None


def ms_pazari(olay: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """iddaa olayındaki 'Maç Sonucu' (t=1, st=1) pazarının 1/0/2 oranları. 'Handikaplı MS' (st=100) ve diğer 1-0-2 pazarları KARIŞTIRILMAZ."""
    for p in olay.get("m") or []:
        if p.get("t") == 1 and p.get("st") == 1:
            oran = {str(o.get("n")): o.get("odd") for o in p.get("o") or []}
            if all(isinstance(oran.get(k), (int, float)) and oran[k] > 1 for k in ("1", "0", "2")):
                return {k: float(oran[k]) for k in ("1", "0", "2")}
    return None


def ihtimaller(oran: Dict[str, float]) -> Dict[str, float]:
    """Oranların tersi, toplam 1'e normalize edilir (bahis komisyonu çıkar)."""
    ters = {k: 1.0 / v for k, v in oran.items()}
    toplam = sum(ters.values())
    return {k: v / toplam for k, v in ters.items()}


def olay_bul(olaylar: List[Dict[str, Any]], ev: str, dep: str) -> Optional[Dict[str, Any]]:
    """Ev VE deplasman adı benzeyen olay. iddaa listesinde aynı maç bazen oransız boş bir kayıtla ikinci kez geçer (canlıda Fenerbahçe–Liverpool):
    yalnızca Maç Sonucu oranı olan adaylar sayılır; oranları birbirinden FARKLI birden çok aday varsa yanlış bağlamak yerine None."""
    aday = [e for e in olaylar if e.get("hn") and e.get("an") and takim_benzer(ev, e["hn"]) and takim_benzer(dep, e["an"]) and ms_pazari(e)]
    return aday[0] if aday and all(ms_pazari(e) == ms_pazari(aday[0]) for e in aday) else None


def _guven(p: float, n: int) -> str:
    """matchforge ms_model/explain.py coverage_language ile aynı dil."""
    if n == 3:
        return "tam kapsama"
    if n == 1:
        return "güçlü ihtimal" if p >= 0.65 else "ağırlıklı ihtimal" if p >= 0.50 else "gerçekçi ihtimal" if p >= 0.35 else "sürpriz ihtimali" if p >= 0.20 else "zor ihtimal"
    return "güvenli kapanış" if p >= 0.80 else ("geniş kapsama" if p >= 0.70 else "temkinli kapsama")


def karar(p1: float, p0: float, p2: float) -> Dict[str, Any]:
    """matchforge ms_model/decision.py decide ile aynı kural: beraberlik favori seçilmez, en yüksek ihtimale göre tek / çift / üçlü."""
    p = (p1, p0, p2)
    kod = ("1", "0", "2")
    top = max(range(3), key=lambda i: p[i])
    if top == 1 or p[top] < DOUBLE_MIN:
        return dict(tier="uclu", senaryo="1-0-2", secimler=["1", "0", "2"], kolon=3, banko=False)
    if p[top] >= SINGLE_MIN:
        return dict(tier="tek", senaryo=kod[top], secimler=[kod[top]], kolon=1, banko=True)
    return dict(tier="cift", senaryo="1-0" if top == 0 else "0-2", secimler=[kod[top], "0"], kolon=2, banko=False)


_PARCALAR = {"1": ("1",), "0": ("0",), "2": ("2",), "1-0": ("1", "0"), "0-2": ("0", "2"), "1-0-2": ("1", "0", "2")}


def piyasa_tahmini(olaylar: List[Dict[str, Any]], ev_adi: str, dep_adi: str) -> Optional[Dict[str, Any]]:
    """matchforge /api/ms/predict sonucuyla aynı şekilde sözlük; iddaa'da maç ya da Maç Sonucu pazarı yoksa None."""
    olay = olay_bul(olaylar, ev_adi, dep_adi)
    oran = ms_pazari(olay) if olay else None
    if not oran:
        return None
    ih = ihtimaller(oran)
    p1, p0, p2 = ih["1"], ih["0"], ih["2"]
    dec = karar(p1, p0, p2)
    senaryolar = []
    for kod, parca in _PARCALAR.items():
        p = sum(ih[k] for k in parca)
        senaryolar.append(dict(senaryo=kod, ihtimal=p, guven_dili=_guven(p, len(parca)), onerilen=(kod == dec["senaryo"])))
    facts = dict(
        ev=ev_adi, deplasman=dep_adi, ihtimaller=dict(p1=p1, p0=p0, p2=p2),
        favori_taraf="ev" if p1 >= p2 else "deplasman",
        kademe=dec["tier"], onerilen=dec["senaryo"], banko=dec["banko"], kolon=dec["kolon"], senaryolar=senaryolar,
        uyarilar=["Bu ihtimaller iddaa Maç Sonucu oranlarından (bahis payı çıkarılarak) hesaplandı; farklı ülke liglerinden takımlar karşılaştığı için istatistiksel modelin yerine kullanıldı.",
                  "Kadro, sakatlık, hakem ve hava durumu hesaba katılmadı; tahmin maçtan günler önce üretilir."],
        model_version=KAYNAK, as_of=None, kaynak="piyasa", oranlar=oran)
    return dict(home_team_id=None, away_team_id=None, p1=p1, p0=p0, p2=p2, decision=dec, facts=facts)
