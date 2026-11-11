"""Spor Toto arşivi: resmî API'den (webapi.sportoto.gov.tr) her haftanın 15/14/13/12 bilen sayıları, ikramiyeleri ve 15 maçın sonuçları.

Saf kısım (ayrıştırma + istatistik) burada, I/O (istemci, depo) enjekte edilir: testlerde sahte, canlıda `HttpIstemci` + SQL deposu.
"İstatistiklere" yalnız `temiz` haftalar girer: 15 maçı tam, noter çekilişi yok, sonucu yayınlanmış.
Bu tabloların kaynağı herkese açık resmî sonuçlardır; Fuat'ın tercihleri, yorumları ve modeli burada YOKTUR.
"""
import math
import re
import statistics
import time
from typing import Any, Dict, List, Optional, Tuple

SPORTOTO_BASE = "https://webapi.sportoto.gov.tr"
BEKLENEN_MAC = 15
SONUCLAR = {1: "1", 0: "0", 2: "2"}


def oyun_no(ad: Optional[str]) -> Optional[int]:
    """"14. Oyun Programı" / "7. Hafta" -> sayı."""
    m = re.match(r"\s*(\d+)\.", ad or "")
    return int(m.group(1)) if m else None


def _mac_satiri(round_id: int, sira: int, x: Dict[str, Any]) -> Dict[str, Any]:
    m = x.get("match") or {}
    s = m.get("score") or {}
    sonuc = m.get("fullTimeWin")
    return {"round_id": round_id, "sira": sira, "tarih": m.get("date"), "ev": (m.get("homeTeam") or {}).get("name"), "dep": (m.get("awayTeam") or {}).get("name"),
            "skor_ev": s.get("homeRegular"), "skor_dep": s.get("awayRegular"), "sonuc": SONUCLAR.get(sonuc), "noter": m.get("noterWin") == 1}


def hafta_kaydi(round_id: int, sezon: str, ad: str, sonuc: Optional[Dict[str, Any]], maclar: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Resmî sonuç (yoksa None) ve maç listesinden: (hafta satırı, maç satırları). Sıra, API'nin döndürdüğü sıradır (1..)."""
    satirlar = [_mac_satiri(round_id, i, x) for i, x in enumerate(maclar, 1)]
    ev = sum(1 for m in satirlar if m["sonuc"] == "1")
    ber = sum(1 for m in satirlar if m["sonuc"] == "0")
    dep = sum(1 for m in satirlar if m["sonuc"] == "2")
    noter = sum(1 for m in satirlar if m["noter"])
    r = sonuc or {}
    temiz = bool(sonuc) and len(satirlar) == BEKLENEN_MAC and ev + ber + dep == BEKLENEN_MAC and noter == 0
    hafta = {"round_id": round_id, "sezon": sezon, "oyun_no": oyun_no(ad), "ad": ad, "kapanis": r.get("gameRoundCloseDate"),
             "bilen_15": r.get("fifteenWinCount"), "bilen_14": r.get("fourteenWinCount"), "bilen_13": r.get("thirteenWinCount"), "bilen_12": r.get("twelveWinCount"),
             "ikramiye_15": r.get("fifteenWinPrize"), "ikramiye_14": r.get("fourteenWinPrize"), "ikramiye_13": r.get("thirteenWinPrize"), "ikramiye_12": r.get("twelveWinPrize"),
             "mac_sayisi": len(satirlar), "ev": ev, "beraberlik": ber, "deplasman": dep, "noter_sayisi": noter, "temiz": temiz}
    return hafta, satirlar


# ---------------- istatistik ----------------
def temiz_haftalar(haftalar):
    return [h for h in haftalar if h.get("temiz") and h.get("bilen_15") is not None]


_temiz = temiz_haftalar


def sezon_ozeti(haftalar: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cikti = []
    for sezon in sorted({h["sezon"] for h in haftalar}):
        H = [h for h in _temiz(haftalar) if h["sezon"] == sezon]
        if not H:
            continue
        mac = BEKLENEN_MAC * len(H)
        en = max(H, key=lambda h: h["bilen_15"])
        cikti.append({"sezon": sezon, "hafta": len(H),
                      "oran": {"ev": round(100 * sum(h["ev"] for h in H) / mac), "beraberlik": round(100 * sum(h["beraberlik"] for h in H) / mac), "deplasman": round(100 * sum(h["deplasman"] for h in H) / mac)},
                      "ort_beraberlik": round(sum(h["beraberlik"] for h in H) / len(H), 1),
                      "medyan_15": statistics.median(h["bilen_15"] for h in H), "medyan_14": statistics.median(h["bilen_14"] for h in H),
                      "devir_hafta": sum(1 for h in H if h["bilen_15"] == 0), "en_cok_15": {"oyun_no": en["oyun_no"], "sayi": en["bilen_15"]}})
    return cikti


def zorluk_tablosu(haftalar: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Haftadaki beraberlik sayısına göre: hafta, 15 ve 14 bilen medyanı, devir yüzdesi. 7 ve üstü tek satır."""
    gruplar: Dict[int, List[Dict[str, Any]]] = {}
    for h in _temiz(haftalar):
        gruplar.setdefault(min(h["beraberlik"], 7), []).append(h)
    return [{"beraberlik": k, "etiket": "7+" if k == 7 else str(k), "hafta": len(H), "medyan_15": statistics.median(h["bilen_15"] for h in H),
             "medyan_14": statistics.median(h["bilen_14"] for h in H), "devir_yuzde": round(100 * sum(1 for h in H if h["bilen_15"] == 0) / len(H))}
            for k, H in sorted(gruplar.items())]


def _ozet(H):
    return {"medyan_15": statistics.median(h["bilen_15"] for h in H), "medyan_14": statistics.median(h["bilen_14"] for h in H),
            "devir_yuzde": round(100 * sum(1 for h in H if h["bilen_15"] == 0) / len(H))}


def benzer_haftalar(haftalar: List[Dict[str, Any]], ev: float, ber: float, dep: float, k: int = 20) -> Dict[str, Any]:
    """Sonuç dağılımı (ev/beraberlik/deplasman) bu dağılıma en yakın geçmiş haftalar. Eşit uzaklıkta yeni hafta önce. Girdi toplamı 15 olmalı (beklenen sayılar olabilir)."""
    if abs(ev + ber + dep - BEKLENEN_MAC) > 0.5:
        raise ValueError("ev + beraberlik + deplasman 15 olmalı")
    H = _temiz(haftalar)
    sirali = sorted(H, key=lambda h: (math.sqrt((h["ev"] - ev) ** 2 + (h["beraberlik"] - ber) ** 2 + (h["deplasman"] - dep) ** 2), -h["round_id"]))
    en = sirali[:k]
    return {"haftalar": [{**{x: h[x] for x in ("round_id", "sezon", "oyun_no", "ev", "beraberlik", "deplasman", "bilen_15", "bilen_14")},
                          "uzaklik": round(math.sqrt((h["ev"] - ev) ** 2 + (h["beraberlik"] - ber) ** 2 + (h["deplasman"] - dep) ** 2), 2)} for h in en],
            "ozet": {**_ozet(en), "k": len(en)}, "genel": {**_ozet(H), "hafta": len(H)}}


def _korelasyon(a, b):
    ma, mb = sum(a) / len(a), sum(b) / len(b)
    pay = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    payda = math.sqrt(sum((x - ma) ** 2 for x in a) * sum((y - mb) ** 2 for y in b))
    return pay / payda if payda else 0.0


def korelasyon(haftalar: List[Dict[str, Any]]) -> Dict[str, float]:
    """log10(15 bilen + 1) ile haftadaki ev / beraberlik / deplasman sayısı arasındaki Pearson korelasyonu."""
    H = _temiz(haftalar)
    y = [math.log10(h["bilen_15"] + 1) for h in H]
    return {"ev": round(_korelasyon([h["ev"] for h in H], y), 2), "beraberlik": round(_korelasyon([h["beraberlik"] for h in H], y), 2),
            "deplasman": round(_korelasyon([h["deplasman"] for h in H], y), 2)}


# ---------------- senkron ----------------
class HttpIstemci:
    """Resmî API; 522/zaman aşımı sık: üssel bekleme ile yeniden dener."""

    def __init__(self, deneme: int = 5, bekle=time.sleep, get=None):
        self.deneme, self.bekle = deneme, bekle
        self._get_fn = get

    def _get(self, yol: str, **params):
        import httpx
        son = None
        for t in range(self.deneme):
            try:
                r = (self._get_fn or (lambda u, p: httpx.get(u, params=p, headers={"User-Agent": "Mozilla/5.0"}, timeout=40)))(SPORTOTO_BASE + yol, params)
                r.raise_for_status()
                return r.json()
            except Exception as e:  # noqa: BLE001 — her hata (522, zaman aşımı, bozuk JSON) yeniden denenir
                son = e
                self.bekle(2 + 3 * t)
        raise RuntimeError(f"resmî API çekilemedi: {yol}: {type(son).__name__}")

    def yillar(self) -> List[str]:
        return [x["year"] for x in self._get("/api/GameRound/GetGameRoundYears")["object"]]

    def adlar(self, sezon: str) -> List[Dict[str, Any]]:
        return self._get("/api/GameRound/GetGameRoundNamesByYear", year=sezon)["object"] or []

    def sonuc(self, rid: int) -> Optional[Dict[str, Any]]:
        return self._get("/api/GameResult/GetGameResultByGameRoundId", id=rid)["object"]

    def maclar(self, rid: int) -> List[Dict[str, Any]]:
        return self._get("/api/GameMatch/GetGameMatches/", gameRoundId=rid)["object"] or []


def senkronla(istemci, depo, sezonlar: Optional[List[str]] = None, bekle=time.sleep, aralik: float = 0.0) -> Dict[str, int]:
    """Eksik haftaları ve sonucu henüz yayınlanmamış (ya da temiz olmayan) haftaları çeker; temiz kayıtlı haftaya dokunmaz. Tek haftanın hatası diğerlerini durdurmaz."""
    sezonlar = sezonlar if sezonlar is not None else istemci.yillar()
    mevcut = depo.mevcut_haftalar()
    rapor = {"sezon": len(sezonlar), "hafta": 0, "yazilan": 0, "atlanan": 0, "hata": 0}
    for sezon in sezonlar:
        for x in sorted(istemci.adlar(sezon), key=lambda x: x["id"]):
            rapor["hafta"] += 1
            if mevcut.get(x["id"]):
                rapor["atlanan"] += 1
                continue
            try:
                sonuc = istemci.sonuc(x["id"])
                hafta, maclar = hafta_kaydi(x["id"], sezon, x["name"], sonuc, istemci.maclar(x["id"]))
                depo.kaydet(hafta, maclar)
                rapor["yazilan"] += 1
            except Exception:  # noqa: BLE001 — bir haftanın hatası diğerlerini ve zamanlayıcıyı durdurmasın
                rapor["hata"] += 1
            if aralik:
                bekle(aralik)
    return rapor
