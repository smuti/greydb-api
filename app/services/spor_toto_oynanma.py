"""iddaa Spor Toto sayfasındaki oynanma yüzdelerinin (1/0/2) zaman damgalı kaydı. Geçmişi yok: bugünden itibaren birikir (kalabalık × sonuç ölçümü için)."""
from typing import Any, Dict, List

import httpx

IDDAA_SPOR_TOTO = "https://sportotov2.iddaa.com/SporToto"


def satirlar(veri: Dict[str, Any]) -> List[Dict[str, Any]]:
    """iddaa cevabının `data` kısmından kayıt satırları. Yüzdesi olmayan maç atlanır."""
    cikti = []
    for e in veri.get("events") or []:
        p = (e.get("homePercentage"), e.get("drawPercentage"), e.get("awayPercentage"))
        if any(x is None for x in p):
            continue
        ad = e.get("eventName") or ""
        ev, _, dep = ad.partition("-")
        cikti.append({"donem_no": veri["gameCycleNo"], "mac_sira": e["eventNo"], "ev": ev or None, "dep": dep or None, "mac_tarihi": e.get("eventDate"),
                      "kapanis": veri.get("payinEndDate"), "yuzde_1": p[0], "yuzde_0": p[1], "yuzde_2": p[2]})
    return cikti


def cek() -> Dict[str, Any]:
    r = httpx.get(IDDAA_SPOR_TOTO, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    r.raise_for_status()
    j = r.json()
    if not j.get("isSuccess") or not j.get("data"):
        raise RuntimeError("iddaa Spor Toto cevabı başarısız")
    return j["data"]
