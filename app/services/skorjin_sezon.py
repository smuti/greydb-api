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
    "       x.xg_home AS xg_ev, x.xg_away AS xg_dep,\n"
    "       st.home_possession AS ev_top, st.away_possession AS dep_top,\n"
    "       a.home_pass_accuracy AS ev_pas, a.away_pass_accuracy AS dep_pas, a.home_crosses AS ev_orta, a.away_crosses AS dep_orta,\n"
    "       a.home_touches_in_box AS ev_ceza, a.away_touches_in_box AS dep_ceza, a.home_set_piece_xg AS ev_dt_xg, a.away_set_piece_xg AS dep_dt_xg,\n"
    "       a.home_open_play_xg AS ev_oa_xg, a.away_open_play_xg AS dep_oa_xg, a.home_offsides AS ev_ofsayt, a.away_offsides AS dep_ofsayt"
)
SEZON_SQL = f"""
SELECT m.id AS mac_id, m.match_date AS tarih, m.home_team_id, m.away_team_id, m.home_score, m.away_score, ev.name AS ev_ad, dep.name AS dep_ad,
       {_SEZON_SQL_ALANLARI}
FROM public.matches m
JOIN public.teams ev ON ev.id = m.home_team_id
JOIN public.teams dep ON dep.id = m.away_team_id
LEFT JOIN public.match_stats st ON st.match_id = m.id
LEFT JOIN public.ms_match_xg x ON x.match_id = m.id
LEFT JOIN public.match_advanced_stats a ON a.match_id = m.id
WHERE m.league_id = :lig AND m.finished AND NOT COALESCE(m.cancelled, false)
  AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND m.match_date >= :bas AND m.match_date < :bit
ORDER BY m.match_date DESC
"""

OLAY_SQL = """
SELECT e.match_id AS mac_id, e.team_id, e.minute, COALESCE(e.added_time, 0) AS added
FROM public.match_events e
JOIN public.matches m ON m.id = e.match_id
WHERE e.event_type = 'GOAL' AND m.league_id = :lig AND m.finished AND NOT COALESCE(m.cancelled, false)
  AND m.match_date >= :bas AND m.match_date < :bit
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
    lig bağlamı yazılır: sıra, oynanan, puan, G/B/M, atılan/yenen, averaj; takım sayısı, lider ve son sıra (adı + tam satır), oynanan maç aralığı
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

    def satir(t: Dict[str, Any]) -> Dict[str, Any]:
        """Lider ve son sıra için TAM satır: Skorjin tabloyu bu iki takımla birlikte yukarıdan aşağı sıralı gösterir."""
        return {"name": t["takim"], "position": t["sira"], "played": t["oynanan"], "points": t["puan"], "wins": t["galibiyet"], "draws": t["beraberlik"],
                "losses": t["maglubiyet"], "gf": t["atilan"], "ga": t["yenilen"], "gd": t["averaj"]}

    home, away = taraf(ev), taraf(dep)
    if home is None and away is None:
        return None
    oynanan = [t["oynanan"] for t in tablo]
    return {"n_teams": len(tablo), "leader": satir(tablo[0]), "last": satir(tablo[-1]), "last_points": tablo[-1]["puan"],
            "played_min": min(oynanan), "played_max": max(oynanan), "home": home, "away": away}


# ---------------------------------------------------------------- oyun stili (topla oynama, orta, ceza sahası, duran top, ofsayt)
_STIL = (("possession", "top"), ("pass_accuracy", "pas"), ("crosses", "orta"), ("touches_in_box", "ceza"),
         ("set_piece_xg", "dt_xg"), ("open_play_xg", "oa_xg"), ("offsides", "ofsayt"))


def _ortalama(degerler: List[float], basamak: int = 2) -> Optional[float]:
    return round(sum(degerler) / len(degerler), basamak) if degerler else None


def _stil_ozet(degerler_satirlari: List[Dict[str, Optional[float]]], esik: int = SEZON_MIN) -> Optional[Dict[str, Any]]:
    """Satır başı değerlerden ortalama; `n` = topla oynama verisi olan satır sayısı; eşik altında None."""
    n = sum(1 for d in degerler_satirlari if d["top"] is not None)
    if n < esik:
        return None
    return {"n": n, **{ad: _ortalama([d[k] for d in degerler_satirlari if d[k] is not None]) for ad, k in _STIL}}


def oyun_stili(satirlar: List[Dict[str, Any]], ev: Any, dep: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
    """Takımın sezon oyun stili (kendi değerleri, maç başı ortalama) ve ligin havuzlanmış ortalaması. Verisi olan maçlardan ortalama
    (`n` = topla oynama verisi olan maç); 3'ten az maç ya da veri yoksa o parça None."""
    maclar = _oncesi(satirlar, tarih)

    def taraf(takim: Any) -> Optional[Dict[str, Any]]:
        deger = []
        for s in maclar:
            if takim not in (s["home_team_id"], s["away_team_id"]):
                continue
            on = "ev_" if s["home_team_id"] == takim else "dep_"
            deger.append({k: s.get(on + k) for _, k in _STIL})
        return _stil_ozet(deger)

    havuz = [{k: s.get(on + k) for _, k in _STIL} for s in maclar for on in ("ev_", "dep_")]
    lig = _stil_ozet(havuz, 2 * SEZON_MIN)           # havuzda her maç iki satır
    if lig is not None:
        lig["n"] = lig["n"] // 2          # maç sayısı (havuzda her maç iki satır)
    home, away = taraf(ev), taraf(dep)
    return {"home": home, "away": away, "league": lig} if (home or away or lig) else None


# ---------------------------------------------------------------- gol dakikaları
KOVA_UST = (15, 30, 45, 60, 75, 90)      # 0-15, 16-30, 31-45, 46-60, 61-75, 76-90 (uzatma dakikaları dahil, 45 ve 90'a yazılır)
ERKEN_DK = 10


def _kova(dakika: int) -> Optional[int]:
    if dakika < 1 or dakika > 90:
        return None
    return next(i for i, ust in enumerate(KOVA_UST) if dakika <= ust)


def zamanlama(satirlar: List[Dict[str, Any]], olaylar: List[Dict[str, Any]], ev: Any, dep: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
    """Gol dakikaları: takım başına atılan/yenen gol sayısı altı 15 dakikalık dilimde ve ilk 10 dakikada. Yalnız olay kaydı skorla uyuşan
    ("kapsamlı") maçlar sayılır (olay sayısı = toplam gol; golsüz maç da kapsamlıdır). Olay `team_id` yararlanan takımdır (öz gol dahil).
    90. dakikadan sonraki goller dilimlere girmez ama maçın kapsamını bozmaz. Lig ortalaması takım-maçı başınadır. 3'ten az kapsamlı maçta None."""
    maclar = _oncesi(satirlar, tarih)
    olay_mac: Dict[Any, List[Dict[str, Any]]] = {}
    for o in olaylar:
        olay_mac.setdefault(o["mac_id"], []).append(o)
    takimlar: Dict[Any, Dict[str, Any]] = {}
    lig = {"n": 0, "scored": [0] * 6, "early10": 0}

    def kayit(takim: Any) -> Dict[str, Any]:
        return takimlar.setdefault(takim, {"n": 0, "scored": [0] * 6, "conceded": [0] * 6, "early10_scored": 0, "early10_conceded": 0})

    kapsamli = 0
    for m in maclar:
        mid = m.get("mac_id")
        if mid is None:
            continue
        golleri = olay_mac.get(mid, [])
        if len(golleri) != m["home_score"] + m["away_score"]:
            continue
        kapsamli += 1
        for takim in (m["home_team_id"], m["away_team_id"]):
            kayit(takim)["n"] += 1
            lig["n"] += 1
        for o in golleri:
            atan = o["team_id"]
            yiyen = m["away_team_id"] if atan == m["home_team_id"] else m["home_team_id"]
            k = _kova(o["minute"])
            if k is not None:
                kayit(atan)["scored"][k] += 1
                kayit(yiyen)["conceded"][k] += 1
                lig["scored"][k] += 1
            if 1 <= o["minute"] <= ERKEN_DK:
                kayit(atan)["early10_scored"] += 1
                kayit(yiyen)["early10_conceded"] += 1
                lig["early10"] += 1

    def taraf(takim: Any) -> Optional[Dict[str, Any]]:
        t = takimlar.get(takim)
        return dict(t) if t and t["n"] >= SEZON_MIN else None

    home, away = taraf(ev), taraf(dep)
    league = None
    if kapsamli >= SEZON_MIN:
        n = lig["n"]
        league = {"n": n, "scored": [round(x / n, 3) for x in lig["scored"]], "early10": round(lig["early10"] / n, 3)}
    return {"home": home, "away": away, "league": league} if (home or away or league) else None


def sezon_gruplari(satirlar: List[Dict[str, Any]], ev: Any, dep: Any, tarih: datetime,
                   olaylar: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Üç grup; veri yoksa grup None. Takım tarafı yetersizse (yeni takım, <3 maç) o taraf None."""
    sezon = {"home": takim_sezonu(satirlar, ev, tarih), "away": takim_sezonu(satirlar, dep, tarih)}
    maclar = {"home": sezon_maclari(satirlar, ev, tarih), "away": sezon_maclari(satirlar, dep, tarih)}
    return {
        "season": sezon if (sezon["home"] or sezon["away"]) else None,
        "league_avg": lig_ortalamasi(satirlar, tarih),
        "season_matches": maclar if (maclar["home"] or maclar["away"]) else None,
        "standings": puan_durumu(satirlar, ev, dep, tarih),
        "style": oyun_stili(satirlar, ev, dep, tarih),
        "timing": zamanlama(satirlar, olaylar or [], ev, dep, tarih),
    }


def _sorgu(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    return execute_query(sql, params)


def sezon_kaynagi() -> Callable[[Any, Any, Any, datetime], Dict[str, Any]]:
    """(lig_id, ev_id, dep_id, maç_tarihi) -> sezon grupları. Her lig-sezon için sorgu BİR kez çalışır (koşu boyunca bellekte)."""
    onbellek: Dict[Tuple[Any, int], List[Dict[str, Any]]] = {}
    olay_onbellek: Dict[Tuple[Any, int], List[Dict[str, Any]]] = {}

    def kaynak(lig: Any, ev: Any, dep: Any, tarih: datetime) -> Dict[str, Any]:
        sezon = sezon_icin(tarih)
        if (lig, sezon) not in onbellek:
            bas, bit = sezon_araligi(sezon)
            onbellek[(lig, sezon)] = _sorgu(SEZON_SQL, {"lig": lig, "bas": bas, "bit": bit})
        if (lig, sezon) not in olay_onbellek:
            bas, bit = sezon_araligi(sezon)
            olay_onbellek[(lig, sezon)] = _sorgu(OLAY_SQL, {"lig": lig, "bas": bas, "bit": bit})
        return sezon_gruplari(onbellek[(lig, sezon)], ev, dep, tarih, olay_onbellek[(lig, sezon)])

    return kaynak
