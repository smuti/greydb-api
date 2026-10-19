"""Kupon bacağının iki takımının FotMob takım kimlikleri (logo için). Bitmiş olma şartı yok: başlamamış maçın logosu da gerekir.
Sıra: FotMob maç kimliği → iddaa kimliği (sütun yoksa atlanır) → takım adı + tarih (tek aday olmalı). Yarım sonuç döndürmez."""
from datetime import timedelta
from typing import Any, Dict, Optional, Tuple

from app.services.iddaa_eslestir import takim_benzer

_TABLOLAR = ("public.matches", "public.upcoming_matches")


def _sorgu(tablo: str, kosul: str) -> str:
    return (f"SELECT th.fotmob_team_id AS ev_id, ta.fotmob_team_id AS dep_id, th.name AS ev_ad, ta.name AS dep_ad "
            f"FROM {tablo} m JOIN public.teams th ON th.id = m.home_team_id JOIN public.teams ta ON ta.id = m.away_team_id WHERE {kosul}")


def _cift(satir: Optional[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    if satir and satir.get("ev_id") is not None and satir.get("dep_id") is not None:
        return int(satir["ev_id"]), int(satir["dep_id"])
    return None, None


def takim_kimlikleri(execute_query, bacak: Dict[str, Any], pencere_saat: int = 36) -> Tuple[Optional[int], Optional[int]]:
    for tablo in _TABLOLAR:
        if bacak.get("fotmob_match_id"):
            s = execute_query(_sorgu(tablo, "m.fotmob_match_id = :f"), {"f": bacak["fotmob_match_id"]})
            if s and _cift(s[0]) != (None, None):
                return _cift(s[0])
        if bacak.get("iddaa_event_id"):
            try:
                s = execute_query(_sorgu(tablo, "m.iddaa_event_id = :i"), {"i": bacak["iddaa_event_id"]})
            except Exception:      # iddaa_event_id sütunu henüz yok
                s = []
            if s and _cift(s[0]) != (None, None):
                return _cift(s[0])
    tarih = bacak.get("mac_zamani")
    if not tarih:
        return None, None
    uyan = []
    for tablo in _TABLOLAR:
        adaylar = execute_query(_sorgu(tablo, "m.match_date BETWEEN :bas AND :son"),
                                {"bas": tarih - timedelta(hours=pencere_saat), "son": tarih + timedelta(hours=pencere_saat)})
        uyan += [a for a in adaylar if takim_benzer(bacak["ev"], a["ev_ad"]) and takim_benzer(bacak["dep"], a["dep_ad"])]
    ciftler = {_cift(a) for a in uyan}      # aynı maç iki tabloda da olabilir (yaklaşandan bitmişe geçerken): çift olarak tekilleştirilir
    return ciftler.pop() if len(ciftler) == 1 and _cift(uyan[0]) != (None, None) else (None, None)


def doldur(bacaklar, bul, yaz, kuru: bool = True) -> Dict[str, Any]:
    """Takım kimliği eksik bacakları doldurur. bul(bacak) -> (ev_id, dep_id); yaz(bacak_id, ev_id, dep_id). Kuru çalışmada yazmaz.
    Hata veren bacak tüm işi durdurmaz; raporda sayılır."""
    bulunan, bulunamayan, hata, ornek = 0, 0, 0, []
    for b in bacaklar:
        try:
            ev_id, dep_id = bul(b)
        except Exception:
            hata += 1
            continue
        if ev_id is None or dep_id is None:
            bulunamayan += 1
            if len(ornek) < 15:
                ornek.append(f"{b['ev']} - {b['dep']} ({b['mac_zamani']:%d.%m.%Y})")
            continue
        bulunan += 1
        if not kuru:
            yaz(b["id"], ev_id, dep_id)
    return {"kuru_calisma": kuru, "incelenen": len(bacaklar), "bulunan": bulunan, "bulunamayan": bulunamayan, "hata": hata, "bulunamayan_ornekler": ornek}
