"""Skorjin anlık görüntüsünün sezon grupları: `season` (takım başına sezon özeti), `league_avg` (maç başına lig ortalaması),
`season_matches` (takımın sezon maç listesi). Saf hesaplar + tek lig sorgusu. Biçim sözleşmesi Skorjin deposunda:
docs/prematch-snapshot.md.

Kurallar: yalnızca maçın ligindeki, mevcut sezonun (Temmuz-Temmuz) ve maç gününden ÖNCE biten maçlar sayılır (kupa/Avrupa dışarıda).
Şut/korner/xG yalnızca verisi olan maçlardan toplanır ve kaç maçtan geldiği (`stats_n`, `xg_n`) yazılır; veri yoksa alan null (0 uydurulmaz).
Sezon özeti ve lig ortalaması için en az SEZON_MIN maç gerekir, aksi halde null.
"""
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.services.puan_durumu import guncel_sezon, hesapla, sezon_araligi
from app.services.skorjin_prematch import _istatistik, _skorlu, takim_maclari

SEZON_MIN = 3
_TOPLAM = ("shots", "sot", "corners")
_SEZON_SQL_ALANLARI = (
    "st.home_shots AS ev_sut, st.away_shots AS dep_sut, st.home_shots_on_target AS ev_isabetli, st.away_shots_on_target AS dep_isabetli,\n"
    "       st.home_corners AS ev_korner, st.away_corners AS dep_korner, st.home_yellow_cards AS ev_sari, st.away_yellow_cards AS dep_sari,\n"
    "       x.xg_home AS xg_ev, x.xg_away AS xg_dep"
)
SEZON_SQL = f"""
SELECT m.match_date AS tarih, m.home_team_id, m.away_team_id, m.home_score, m.away_score, ev.name AS ev_ad, dep.name AS dep_ad,
       {_SEZON_SQL_ALANLARI}
FROM public.matches m
JOIN public.teams ev ON ev.id = m.home_team_id
JOIN public.teams dep ON dep.id = m.away_team_id
LEFT JOIN public.match_stats st ON st.match_id = m.id
LEFT JOIN public.ms_match_xg x ON x.match_id = m.id
WHERE m.league_id = :lig AND m.finished AND NOT COALESCE(m.cancelled, false)
  AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND m.match_date >= :bas AND m.match_date < :bit
ORDER BY m.match_date DESC
"""


def sezon_icin(tarih: datetime) -> int:
    return guncel_sezon(tarih.date())


def _oncesi(satirlar: List[Dict[str, Any]], tarih: datetime) -> List[Dict[str, Any]]:
    return [s for s in satirlar if _skorlu(s) and s["tarih"] < tarih]


def _puan(gf: int, ga: int) -> int:
    return 3 if gf > ga else (1 if gf == ga else 0)


def _xg(s: Dict[str, Any], ev_mi: bool) -> Optional[Tuple[float, float]]:
    ev, dep = s.get("xg_ev"), s.get("xg_dep")
    if ev is None or dep is None:
        return None
    return (float(ev), float(dep)) if ev_mi else (float(dep), float(ev))


def _yuvarla(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(x, 3)


def takim_sezonu(satirlar: List[Dict[str, Any]], takim: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
    """Takımın maç gününe kadarki sezon özeti: toplam, ev ("h") ve deplasman ("a") ayrı; şut/korner/xG toplamları."""
    maclar = [s for s in _oncesi(satirlar, tarih) if takim in (s["home_team_id"], s["away_team_id"])]
    if len(maclar) < SEZON_MIN:
        return None
    taraf = {"h": {"played": 0, "points": 0, "gf": 0, "ga": 0}, "a": {"played": 0, "points": 0, "gf": 0, "ga": 0}}
    toplam = {f"{m}_{y}": 0 for m in _TOPLAM for y in ("for", "against")}
    xg_for = xg_against = 0.0
    stats_n = xg_n = 0
    for s in maclar:
        ev_mi = s["home_team_id"] == takim
        gf, ga = (s["home_score"], s["away_score"]) if ev_mi else (s["away_score"], s["home_score"])
        t = taraf["h" if ev_mi else "a"]
        t["played"] += 1
        t["points"] += _puan(gf, ga)
        t["gf"] += gf
        t["ga"] += ga
        st = _istatistik(s, ev_mi)
        if st:
            stats_n += 1
            for ad, anahtar in (("shots", "shots"), ("sot", "sot"), ("corners", "corners")):
                toplam[f"{ad}_for"] += st[anahtar][0]
                toplam[f"{ad}_against"] += st[anahtar][1]
        xg = _xg(s, ev_mi)
        if xg:
            xg_n += 1
            xg_for += xg[0]
            xg_against += xg[1]
    return {
        "played": taraf["h"]["played"] + taraf["a"]["played"], "points": taraf["h"]["points"] + taraf["a"]["points"],
        "h": taraf["h"], "a": taraf["a"],
        "xg_for": _yuvarla(xg_for) if xg_n else None, "xg_against": _yuvarla(xg_against) if xg_n else None, "xg_n": xg_n,
        **{k: (v if stats_n else None) for k, v in toplam.items()}, "stats_n": stats_n,
    }


def _ort(toplam: float, n: int) -> Optional[float]:
    return _yuvarla(toplam / n) if n else None


def lig_ortalamasi(satirlar: List[Dict[str, Any]], tarih: datetime) -> Optional[Dict[str, Any]]:
    """Maç başına lig ortalaması: evdeki takımın ("h") ve deplasmandaki takımın ("a") ortalama gol/puan/şut/korner/xG değerleri."""
    maclar = _oncesi(satirlar, tarih)
    n = len(maclar)
    if n < SEZON_MIN:
        return None
    taraflar = {}
    stats_n = sum(1 for s in maclar if _istatistik(s, True))
    xg_n = sum(1 for s in maclar if _xg(s, True))
    for ad, ev_mi in (("h", True), ("a", False)):
        gf = sum(s["home_score"] if ev_mi else s["away_score"] for s in maclar)
        ga = sum(s["away_score"] if ev_mi else s["home_score"] for s in maclar)
        puan = sum(_puan(*((s["home_score"], s["away_score"]) if ev_mi else (s["away_score"], s["home_score"]))) for s in maclar)
        d = {"gf": _ort(gf, n), "ga": _ort(ga, n), "points": _ort(puan, n)}
        xg = [_xg(s, ev_mi) for s in maclar]
        xg = [x for x in xg if x]
        d["xg_for"], d["xg_against"] = _ort(sum(x[0] for x in xg), len(xg)), _ort(sum(x[1] for x in xg), len(xg))
        st = [x for x in (_istatistik(s, ev_mi) for s in maclar) if x]
        for metrik in _TOPLAM:
            d[f"{metrik}_for"] = _ort(sum(x[metrik][0] for x in st), len(st))
            d[f"{metrik}_against"] = _ort(sum(x[metrik][1] for x in st), len(st))
        taraflar[ad] = d
    return {"n": n, "stats_n": stats_n, "xg_n": xg_n, **taraflar}


def sezon_maclari(satirlar: List[Dict[str, Any]], takim: Any, tarih: datetime) -> List[Dict[str, Any]]:
    """Takımın maç gününe kadarki sezon maçları, en yeniden eskiye (form biçimi + varsa xG)."""
    maclar = sorted((s for s in _oncesi(satirlar, tarih) if takim in (s["home_team_id"], s["away_team_id"])),
                    key=lambda s: s["tarih"], reverse=True)
    cikti = []
    for s in maclar:
        m = takim_maclari([s], takim)[0]
        xg = _xg(s, s["home_team_id"] == takim)
        if xg:
            m["xg_for"], m["xg_against"] = round(xg[0], 3), round(xg[1], 3)
        cikti.append(m)
    return cikti


def puan_durumu(satirlar: List[Dict[str, Any]], ev: Any, dep: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
    """Maçın liginin maç gününe kadarki puan durumu (bitmiş maçlardan; sıralama: puan, averaj, atılan gol, ad). Yalnız iki takımın satırı ve
    lig bağlamı yazılır: sıra, oynanan, puan, G/B/M, atılan/yenen, averaj; takım sayısı, lider, son sıranın puanı, oynanan maç aralığı
    (`played_min/max`: ertelenen maçlar yüzünden takımların oynadığı sayı farklı olabilir). Takımın 3'ten az maçı varsa o taraf None;
    iki taraf da yoksa None. Resmi sıralamada ikili averaj, puan silme gibi kurallar farklı olabilir."""
    maclar = [{"tarih": s["tarih"], "ev_id": s["home_team_id"], "ev_ad": s["ev_ad"], "dep_id": s["away_team_id"], "dep_ad": s["dep_ad"],
               "ev_gol": s["home_score"], "dep_gol": s["away_score"]} for s in _oncesi(satirlar, tarih)]
    tablo = hesapla(maclar)
    if not tablo:
        return None
    by_id = {t["takim_id"]: t for t in tablo}

    def taraf(kimlik: Any) -> Optional[Dict[str, Any]]:
        t = by_id.get(kimlik)
        if t is None or t["oynanan"] < SEZON_MIN:
            return None
        return {"position": t["sira"], "played": t["oynanan"], "points": t["puan"], "wins": t["galibiyet"], "draws": t["beraberlik"],
                "losses": t["maglubiyet"], "gf": t["atilan"], "ga": t["yenilen"], "gd": t["averaj"]}

    home, away = taraf(ev), taraf(dep)
    if home is None and away is None:
        return None
    oynanan = [t["oynanan"] for t in tablo]
    return {"n_teams": len(tablo), "leader": {"name": tablo[0]["takim"], "points": tablo[0]["puan"]}, "last_points": tablo[-1]["puan"],
            "played_min": min(oynanan), "played_max": max(oynanan), "home": home, "away": away}


def sezon_gruplari(satirlar: List[Dict[str, Any]], ev: Any, dep: Any, tarih: datetime) -> Dict[str, Any]:
    """Üç grup; veri yoksa grup None. Takım tarafı yetersizse (yeni takım, <3 maç) o taraf None."""
    sezon = {"home": takim_sezonu(satirlar, ev, tarih), "away": takim_sezonu(satirlar, dep, tarih)}
    maclar = {"home": sezon_maclari(satirlar, ev, tarih), "away": sezon_maclari(satirlar, dep, tarih)}
    return {
        "season": sezon if (sezon["home"] or sezon["away"]) else None,
        "league_avg": lig_ortalamasi(satirlar, tarih),
        "season_matches": maclar if (maclar["home"] or maclar["away"]) else None,
        "standings": puan_durumu(satirlar, ev, dep, tarih),
    }


def _sorgu(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    return execute_query(sql, params)


def sezon_kaynagi() -> Callable[[Any, Any, Any, datetime], Dict[str, Any]]:
    """(lig_id, ev_id, dep_id, maç_tarihi) -> sezon grupları. Her lig-sezon için sorgu BİR kez çalışır (koşu boyunca bellekte)."""
    onbellek: Dict[Tuple[Any, int], List[Dict[str, Any]]] = {}

    def kaynak(lig: Any, ev: Any, dep: Any, tarih: datetime) -> Dict[str, Any]:
        sezon = sezon_icin(tarih)
        if (lig, sezon) not in onbellek:
            bas, bit = sezon_araligi(sezon)
            onbellek[(lig, sezon)] = _sorgu(SEZON_SQL, {"lig": lig, "bas": bas, "bit": bit})
        return sezon_gruplari(onbellek[(lig, sezon)], ev, dep, tarih)

    return kaynak
