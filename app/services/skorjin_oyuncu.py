"""Skorjin anlık görüntüsünün `oyuncu` grubu (maç okuması, yönetici): her takımın son 10 lig maçındaki oyuncu özetleri, eksik oyuncuların takım içi sırası ve payı.
Biçim sözleşmesi Skorjin deposunda: docs/prematch-snapshot.md "Oyuncu grubu".

Kaynak `public.match_player_stats` (FotMob'un bütün oyuncu alanları, onarılmış tablo). Kurallar: yalnızca maçın ligindeki, mevcut sezonun ve maç gününden ÖNCE biten maçlar;
pencere takımın son OYUNCU_PENCERE maçı; listeye en az MIN_DK dakika oynayan oyuncular girer. Değerler 90 dakika başınadır; verisi olmayan alan null (0 uydurulmaz).
Eksik oyuncular FotMob adıyla (büyük/küçük harf ve aksan duyarsız) eşlenir; bulunamayan ya da yeterli süresi olmayan oyuncunun sırası null.
"""
import unicodedata
from collections import Counter
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.services.puan_durumu import sezon_araligi
from app.services.skorjin_sezon import SEZON_MIN, sezon_icin

OYUNCU_PENCERE = 10
MIN_DK = 180
MAKS_OYUNCU = 20
KONUM = {0: "GK", 1: "DF", 2: "MF", 3: "FW"}

OYUNCU_SQL = """
SELECT m.id AS mac_id, m.match_date AS tarih, mps.team_id, mps.player_name, mps.fotmob_player_id, mps.usual_position, mps.minutes_played, mps.rating,
       mps.goals, mps.assists, mps.xg_plus_xa, mps.total_shots, mps.chances_created, mps.accurate_passes, mps.total_passes, mps.defensive_actions, mps.recoveries,
       mps.duels_won, mps.duels_lost, mps.aerial_duels_won, mps.aerial_duels_total, mps.saves, mps.goals_conceded, mps.goals_prevented
FROM public.match_player_stats mps
JOIN public.matches m ON m.id = mps.match_id
WHERE m.league_id = :lig AND m.finished AND NOT COALESCE(m.cancelled, false)
  AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND m.match_date >= :bas AND m.match_date < :bit
"""


def _ad_anahtari(ad: Any) -> str:
    s = unicodedata.normalize("NFKD", str(ad or "")).casefold()
    return " ".join("".join(c for c in s if not unicodedata.combining(c)).split())


def _topla(satirlar: List[Dict[str, Any]], alan: str) -> Optional[float]:
    v = [float(s[alan]) for s in satirlar if s.get(alan) is not None]
    return sum(v) if v else None


def _90(toplam: Optional[float], dk: float) -> Optional[float]:
    return None if toplam is None or dk <= 0 else round(toplam / dk * 90.0, 2)


def _yuzde(pay: Optional[float], payda: Optional[float]) -> Optional[float]:
    return None if pay is None or payda is None or payda <= 0 else round(pay / payda * 100.0, 1)


def _mevki(satirlar: List[Dict[str, Any]]) -> Optional[str]:
    k = Counter(int(s["usual_position"]) for s in satirlar if s.get("usual_position") is not None)
    return KONUM.get(k.most_common(1)[0][0]) if k else None


def _oyuncu(satirlar: List[Dict[str, Any]]) -> Dict[str, Any]:
    dk = sum(float(s.get("minutes_played") or 0) for s in satirlar)
    puanli = [s for s in satirlar if s.get("rating") is not None and s.get("minutes_played")]
    puan = round(sum(float(s["rating"]) * float(s["minutes_played"]) for s in puanli) / sum(float(s["minutes_played"]) for s in puanli), 2) if puanli else None
    kazan, kaybet = _topla(satirlar, "duels_won"), _topla(satirlar, "duels_lost")
    toplam_duello = None if kazan is None or kaybet is None else kazan + kaybet
    saves, yenilen, onlenen = _topla(satirlar, "saves"), _topla(satirlar, "goals_conceded"), _topla(satirlar, "goals_prevented")
    return {
        "ad": satirlar[0]["player_name"], "id": satirlar[0].get("fotmob_player_id"), "mevki": _mevki(satirlar), "dk": int(dk), "mac": len({s["mac_id"] for s in satirlar}), "puan": puan,
        "gol": int(_topla(satirlar, "goals") or 0), "asist": int(_topla(satirlar, "assists") or 0), "xg_xa_90": _90(_topla(satirlar, "xg_plus_xa"), dk),
        "sut_90": _90(_topla(satirlar, "total_shots"), dk), "sans_90": _90(_topla(satirlar, "chances_created"), dk),
        "pas_isabet": _yuzde(_topla(satirlar, "accurate_passes"), _topla(satirlar, "total_passes")), "savunma_90": _90(_topla(satirlar, "defensive_actions"), dk),
        "geri_kazanim_90": _90(_topla(satirlar, "recoveries"), dk), "mucadele_pct": _yuzde(kazan, toplam_duello),
        "hava_pct": _yuzde(_topla(satirlar, "aerial_duels_won"), _topla(satirlar, "aerial_duels_total")),
        "kurtaris": None if saves is None else int(saves), "yenilen_gol": None if yenilen is None else int(yenilen), "onlenen_gol": None if onlenen is None else round(onlenen, 2),
    }


def takim_oyunculari(satirlar: List[Dict[str, Any]], takim: Any, tarih: datetime, eksikler: Optional[List[Dict[str, Any]]] = None,
                     pencere: int = OYUNCU_PENCERE) -> Optional[Dict[str, Any]]:
    """Takımın son `pencere` maçındaki oyuncular (en az MIN_DK dk), eksiklerin sırası ve payı. En az SEZON_MIN maç yoksa None."""
    kendi = [s for s in satirlar if s["team_id"] == takim and s["tarih"] < tarih]
    tarihler: Dict[Any, datetime] = {}
    for s in kendi:
        tarihler[s["mac_id"]] = s["tarih"]
    son = set(sorted(tarihler, key=lambda m: tarihler[m], reverse=True)[:pencere])
    if len(son) < SEZON_MIN:
        return None
    gruplar: Dict[Any, List[Dict[str, Any]]] = {}
    for s in kendi:
        if s["mac_id"] in son:
            gruplar.setdefault(s.get("fotmob_player_id") or _ad_anahtari(s["player_name"]), []).append(s)
    ozetler = [_oyuncu(g) for g in gruplar.values()]
    uygun = sorted((o for o in ozetler if o["dk"] >= MIN_DK), key=lambda o: (-o["dk"], o["ad"]))
    sira = {o["ad"]: i + 1 for i, o in enumerate(sorted((o for o in uygun if o["xg_xa_90"] is not None), key=lambda o: (-o["xg_xa_90"], -o["dk"], o["ad"])))}
    takim_xgxa = sum(float(s["xg_plus_xa"]) for g in gruplar.values() for s in g if s.get("xg_plus_xa") is not None)
    bulunan = {_ad_anahtari(g[0]["player_name"]): (_oyuncu(g), g) for g in gruplar.values()}
    eksik_cikti, eksik_xgxa = [], 0.0
    for e in eksikler or []:
        o, rows = bulunan.get(_ad_anahtari(e.get("name")), (None, []))
        tam = o is not None and o["dk"] >= MIN_DK
        eksik_cikti.append({"ad": e.get("name"), "durum": e.get("status"), "dk": o["dk"] if o else None, "mevki": o["mevki"] if o else None,
                            "xg_xa_90": o["xg_xa_90"] if tam else None, "sira": sira.get(o["ad"]) if tam else None})
        eksik_xgxa += sum(float(s["xg_plus_xa"]) for s in rows if s.get("xg_plus_xa") is not None)
    return {"n": len(son), "oyuncular": uygun[:MAKS_OYUNCU], "eksik": eksik_cikti,
            "eksik_pay": round(eksik_xgxa / takim_xgxa, 3) if eksikler and takim_xgxa > 0 else None}


def oyuncu_grubu(satirlar: List[Dict[str, Any]], ev: Any, dep: Any, tarih: datetime, eksik: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """`eksik`: Skorjin `absences` JSON'u ({"teams": [{"name", "players": [{"name", "status"}]}]}); takım sırası ev, dep."""
    takimlar = (eksik or {}).get("teams") or []
    ev_e = (takimlar[0].get("players") if len(takimlar) > 0 else None) or []
    dep_e = (takimlar[1].get("players") if len(takimlar) > 1 else None) or []
    home, away = takim_oyunculari(satirlar, ev, tarih, ev_e), takim_oyunculari(satirlar, dep, tarih, dep_e)
    return {"home": home, "away": away} if (home or away) else None


def oyuncu_kaynagi() -> Callable[[Any, Any, Any, datetime, Optional[Dict[str, Any]]], Optional[Dict[str, Any]]]:
    """(lig_id, ev_id, dep_id, maç_tarihi, eksik) -> oyuncu grubu. Her lig-sezon için sorgu BİR kez çalışır (koşu boyunca bellekte)."""
    onbellek: Dict[Tuple[Any, int], List[Dict[str, Any]]] = {}

    def kaynak(lig: Any, ev: Any, dep: Any, tarih: datetime, eksik: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        sezon = sezon_icin(tarih)
        if (lig, sezon) not in onbellek:
            from app.services.db import execute_query
            bas, bit = sezon_araligi(sezon)
            onbellek[(lig, sezon)] = execute_query(OYUNCU_SQL, {"lig": lig, "bas": bas, "bit": bit})
        return oyuncu_grubu(onbellek[(lig, sezon)], ev, dep, tarih, eksik)

    return kaynak
