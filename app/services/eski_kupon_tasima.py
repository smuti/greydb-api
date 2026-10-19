"""Eski greydb.coupons + coupon_matches kaydını yeni kupon girdisine çevirir (saf). Dönüştürülemeyen Tasinamaz(neden) ile atlanır ve eski tabloda kalır.

Neden dönüştürülemez: eski pazarlama türleri (banko, premium, populer, yukselen, uyem, editor) yeni seviyelerle (Açık/Bayi/Elit) eşlenmiyor; bahis türü
boş; maç tarihi çözülemiyor (yılsız "13 Eki 22:00" yazısı, yalnız saat ya da "Bugün"); sistem yazılı ama bacaklar tutmuyor; toplam oran bacakların çarpımıyla tutmuyor.
"""
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List

from app.services.kupon_model import KuponHatasi, dogrula_olustur

_TUR = {"acik_radar": "acik", "bayi": "bayi"}
_DURUM = {"active": "yayinda", "archived": "arsiv", "draft": "taslak"}
_AY = {"oca": 1, "şub": 2, "mar": 3, "nis": 4, "may": 5, "haz": 6, "tem": 7, "ağu": 8, "eyl": 9, "eki": 10, "kas": 11, "ara": 12}


class Tasinamaz(Exception):
    pass


def tarih_coz(metin: Any, olusturma: datetime) -> datetime:
    """"13 Eki 22:00" (TSİ, yılsız) → UTC. Yıl kuponun oluşturulma tarihinden; çok geriye düşerse sonraki yıl (yıl sonu)."""
    m = re.fullmatch(r"\s*(\d{1,2})\s+([A-Za-zÇĞİÖŞÜçğıöşü]{3})\s+(\d{1,2}):(\d{2})\s*", metin or "")
    ay = _AY.get(m.group(2).lower().replace("İ", "i")) if m else None
    if not m or not ay:
        raise Tasinamaz(f"maç tarihi çözülemedi: {metin!r}")
    try:
        yerel = datetime(olusturma.year, ay, int(m.group(1)), int(m.group(3)), int(m.group(4)))
        if yerel < olusturma - timedelta(days=60):
            yerel = yerel.replace(year=olusturma.year + 1)
    except ValueError:
        raise Tasinamaz(f"maç tarihi geçersiz: {metin!r}")
    return yerel - timedelta(hours=3)


def eski_kupon(k: Dict[str, Any], bacaklar: List[Dict[str, Any]]) -> Dict[str, Any]:
    seviye = _TUR.get(k["type"])
    if seviye is None:
        raise Tasinamaz(f"eski kupon türü ({k['type']}) yeni seviyelerle eşlenmiyor")
    if not bacaklar:
        raise Tasinamaz("bacak yok")
    if k.get("winnings") and "sistem" in str(k["winnings"]).lower():
        raise Tasinamaz(f"sistem kuponu ({k['winnings']}) ama bacaklar tutmuyor")
    yeni = []
    for b in bacaklar:
        if not (b.get("market_name") or "").strip():
            raise Tasinamaz("bahis türü (market) boş")
        lig = (b.get("league") or "").strip()
        yeni.append({
            "iddaa_event_id": None, "fotmob_match_id": None, "ev": b["home_team"], "dep": b["away_team"],
            "lig": None if (not lig or re.fullmatch(r"League \d+", lig)) else lig,
            "mac_zamani": tarih_coz(b.get("match_date"), k["created_at"]).isoformat() + "Z",
            "bahis_turu": b["market_name"].strip(), "secim": b["prediction"], "oran": float(b["odds"]), "sonuc": "pending",
        })
    carpim = 1.0
    for b in yeni:
        carpim *= b["oran"]
    if abs(carpim - float(k["total_odds"])) > 0.02 * float(k["total_odds"]):
        raise Tasinamaz(f"toplam oran ({k['total_odds']}) bacak oranlarının çarpımıyla ({carpim:.2f}) tutmuyor")
    try:
        g = dogrula_olustur({"seviye": seviye, "durum": _DURUM.get(k["status"], "taslak"), "zamanlama": "pre", "bacaklar": yeni, "nedenler": []})
    except KuponHatasi as e:
        raise Tasinamaz(f"{e.alan}: {e.mesaj}")
    g["kaynak"] = f"kupon:{k['id']}"
    g["olusturan_email"] = k["created_by_email"]
    g["olusturma"], g["guncelleme"] = k["created_at"], k["updated_at"]
    return g
