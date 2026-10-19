"""iddaa oynanma dağılımı (saf dönüşüm). iddaa'nın açık uçlarından gelen veriyi 'en çok oynanan seçimler' ve 'market içi yüzde'ye çevirir.

Girdiler: maç detayı (market listesi: i=örnek kimliği, t, st, sov, o=[{no, odd, n}]), outcome-play-percentages içindeki bu maça ait
{market_ornek_id: {secim_no: yüzde}}, played-event-percentage içindeki maç payı, get_market_config (anahtar '<t>_<st>').
Yüzde: o seçimi içeren kuponların, tüm kuponlar içindeki payı. Bir marketin seçimleri toplamı maçın payına eşittir.
"""
from typing import Any, Dict, List, Optional


def _ad(market: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    c = cfg.get(f"{market.get('t')}_{market.get('st')}")
    if not c or not c.get("n"):
        return None
    sov = market.get("sov")
    ad = " ".join(c["n"].split())
    if sov is not None:
        ad = ad.replace("{0}", str(sov)).replace("{h}", str(sov))
    return ad


def dagilim_olustur(olay: Dict[str, Any], yuzdeler: Dict[str, Dict[str, float]], mac_payi: Optional[float], cfg: Dict[str, Any], ilk: int = 10) -> Dict[str, Any]:
    secimler: List[Dict[str, Any]] = []
    marketler: List[Dict[str, Any]] = []
    for m in olay.get("m", []):
        y = yuzdeler.get(str(m.get("i")))
        ad = _ad(m, cfg)
        if not y or not ad:
            continue
        liste = []
        for o in m.get("o", []):
            p = y.get(str(o.get("no")))
            if p is None:
                continue
            liste.append({"bahis_turu": ad, "secim": o.get("n"), "oran": o.get("odd"), "yuzde": float(p)})
        if not liste:
            continue
        secimler += liste
        toplam = sum(x["yuzde"] for x in liste)
        marketler.append({
            "bahis_turu": ad, "toplam_yuzde": round(toplam, 2),
            "secimler": [{"secim": x["secim"], "oran": x["oran"], "yuzde": x["yuzde"], "market_ici_yuzde": round(100 * x["yuzde"] / toplam, 1) if toplam else None}
                         for x in sorted(liste, key=lambda x: -x["yuzde"])],
        })
    en_cok = sorted(secimler, key=lambda x: -x["yuzde"])[:ilk]      # sorted kararlıdır: eşitlikte kaynak sırası korunur
    return {"mac_payi": mac_payi, "en_cok_oynanan": en_cok, "marketler": sorted(marketler, key=lambda m: -m["toplam_yuzde"])}
