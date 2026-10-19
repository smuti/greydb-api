"""DB'de olmayan maçlar için FotMob yedeği (matchforge günlük maç listesi): yalnızca MAÇ SONU (90 dk) skoru. Olay ve istatistik yoktur: onları isteyen bahisler 'veri_yok' kalır."""
import re
import unicodedata
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional

import httpx

from app.services.iddaa_eslestir import takim_benzer
from app.services.skorjin_match_analysis import translate_team_name

MATCHFORGE = "https://web-production-0ce73.up.railway.app"
_NORMAL_BITIS = 6        # FotMob status_id: 6 = normal süre sonunda bitti (13 penaltılar vb. 90 dk sonucu belirsiz)
_onbellek: Dict[str, List[Dict[str, Any]]] = {}


def gun_getir(tarih: str) -> List[Dict[str, Any]]:
    """tarih: YYYYMMDD. Aynı gün için süreç içinde önbellekli."""
    if tarih not in _onbellek:
        r = httpx.get(f"{MATCHFORGE}/api/matches/by-date", params={"date": tarih}, timeout=25.0)
        r.raise_for_status()
        _onbellek[tarih] = r.json().get("matches") or []
    return _onbellek[tarih]


def _ad_esit(iddaa_adi: str, fotmob_adi: str) -> bool:
    cevrik = translate_team_name(fotmob_adi) or fotmob_adi
    if takim_benzer(iddaa_adi, cevrik) or takim_benzer(iddaa_adi, fotmob_adi):
        return True
    # "Santander" ~ "Racing Santander": iddaa adı FotMob adının sonunda/başında tam kelime olarak geçiyorsa (en az 5 harf)
    def temiz(s: str) -> str:
        return re.sub(r"[^a-z0-9 ]", "", unicodedata.normalize("NFD", s.lower()).encode("ascii", "ignore").decode()).strip()
    a, b = temiz(iddaa_adi), temiz(fotmob_adi)
    return len(a) >= 5 and (f" {a} " in f" {b} ")


def fotmob_mac_sozlugu(m: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "home": m["home"]["score"], "away": m["away"]["score"], "home_name": m["home"]["name"], "away_name": m["away"]["name"],
        "events": None, "stats": {}, "players": [], "fotmob_id": m.get("id"), "_dis": True,
    }


def _adaylar(getir, bacak, bitmis_gerekli: bool) -> List[Dict[str, Any]]:
    z = bacak["mac_zamani"]
    bulunan = []
    for gun in (z - timedelta(days=1), z, z + timedelta(days=1)):
        try:
            liste = getir(gun.strftime("%Y%m%d"))
        except Exception:
            continue
        for m in liste:
            if m.get("cancelled"):
                continue
            if bitmis_gerekli and (not m.get("finished") or m.get("status_id") != _NORMAL_BITIS or m["home"].get("score") is None or m["away"].get("score") is None):
                continue
            if _ad_esit(bacak["ev"], m["home"]["name"]) and _ad_esit(bacak["dep"], m["away"]["name"]):
                bulunan.append(m)
    return list({m["id"]: m for m in bulunan}.values())


def fotmob_takim_kimlikleri(getir, bacak) -> tuple:
    """Logo için FotMob takım kimlikleri ve maç kimliği: (ev_id, dep_id, mac_id) ya da (None, None, None). Bitmemiş maç da olur; tek aday olmalı."""
    a = _adaylar(getir, bacak, bitmis_gerekli=False)
    if len(a) != 1:
        return None, None, None
    m = a[0]
    return m["home"].get("id"), m["away"].get("id"), m.get("id")


def fotmob_mac_bul(getir: Callable[[str], List[Dict[str, Any]]], bacak: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Bacağın maçını FotMob günlük listesinde takım adlarıyla bulur (tarih ±1 gün). Bitmiş (normal süre), iptal olmamış ve skoru olan maç; tek aday olmalı."""
    adaylar = _adaylar(getir, bacak, bitmis_gerekli=True)
    benzersiz = {m["id"]: m for m in adaylar}
    return fotmob_mac_sozlugu(next(iter(benzersiz.values()))) if len(benzersiz) == 1 else None
