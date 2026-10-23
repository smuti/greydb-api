"""Skorjin anlık görüntüsünün `referee` grubu: hakemin adı FotMob'dan (yaklaşan maçın matchDetails'inde, maçtan 1-2 gün önce açıklanır),
istatistiği bizim geçmişimizden. Saf hesaplar + iki toplu sorgu. Biçim sözleşmesi Skorjin deposunda: docs/prematch-snapshot.md.

İstatistik: hakemin bu maçtan ÖNCE yönettiği, bitmiş ve istatistiği olan tüm maçlar (ligler arası; ad eşleşmesi `match_context.referee`).
Lig karşılaştırması: aynı ligin aynı tarihe kadarki tüm maçları. En az HAKEM_MIN maçı olmayan hakemde sayı yazılmaz (yalnız ad ve `n`);
lig ortalaması LIG_MIN maçtan azsa `league` yoktur. Veri yoksa alan null (0 uydurulmaz). Penaltı = penaltıdan atılan gol + kaçırılan penaltı olayı.
"""
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple

HAKEM_MIN = 5
LIG_MIN = 30
KAYNAK = "FotMob"

_STAT = """
SELECT m.id, s.home_yellow_cards + s.away_yellow_cards AS sari, s.home_red_cards + s.away_red_cards AS kirmizi,
       s.home_fouls + s.away_fouls AS faul, m.home_score + m.away_score AS gol, (m.home_score > m.away_score) AS ev_galibiyet
FROM public.matches m
{birlesim}
JOIN public.match_stats s ON s.match_id = m.id
WHERE m.finished AND NOT COALESCE(m.cancelled, false) AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND m.match_date < :tarih AND {filtre}
"""
_OZET = """
SELECT count(*) AS n, avg(mm.sari) AS sari, avg(mm.kirmizi) AS kirmizi, avg(mm.faul) AS faul, avg(mm.gol) AS gol,
       avg(mm.ev_galibiyet::int) AS ev_galibiyet, COALESCE(sum(pen.n), 0)::float / NULLIF(count(*), 0) AS penalti
FROM mm LEFT JOIN pen ON pen.match_id = mm.id
"""
_PEN = ("pen AS (SELECT e.match_id, count(*) AS n FROM public.match_events e WHERE e.match_id IN (SELECT id FROM mm) "
        "AND (e.is_penalty OR e.event_type = 'MISSEDPENALTY') GROUP BY 1)")
HAKEM_SQL = ("WITH mm AS (" + _STAT.format(birlesim="JOIN public.match_context c ON c.match_id = m.id", filtre="c.referee = :ad") + "), "
             + _PEN + _OZET)
LIG_SQL = "WITH mm AS (" + _STAT.format(birlesim="", filtre="m.league_id = :lig") + "), " + _PEN + _OZET


def hakem_bilgisi(detay: Optional[Dict[str, Any]]) -> Optional[Dict[str, Optional[str]]]:
    """FotMob matchDetails -> {"name", "country"}; hakem açıklanmadıysa None."""
    ref = (((detay or {}).get("content") or {}).get("matchFacts") or {}).get("infoBox", {})
    ref = ref.get("Referee") if isinstance(ref, dict) else None
    if not isinstance(ref, dict):
        return None
    ad = (ref.get("text") or "").strip()
    if not ad:
        return None
    ulke = (ref.get("country") or "").strip() or None
    return {"name": ad, "country": ulke}


def _r(x: Any, n: int = 2) -> Optional[float]:
    return None if x is None else round(float(x), n)


def hakem_alani(bilgi: Optional[Dict[str, Any]], ist: Optional[Dict[str, Any]], lig: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Grubun JSON'u. İstatistik yoksa ya da HAKEM_MIN'den azsa yalnız ad (+ `n` biliniyorsa)."""
    if not bilgi:
        return None
    alan: Dict[str, Any] = {"name": bilgi["name"], "country": bilgi.get("country"), "source": KAYNAK}
    if not ist:
        return alan
    alan["n"] = int(ist["n"])
    if alan["n"] < HAKEM_MIN:
        return alan
    alan.update(yellow_avg=_r(ist["sari"]), red_avg=_r(ist["kirmizi"]), foul_avg=_r(ist["faul"]), penalty_avg=_r(ist["penalti"]),
                goals_avg=_r(ist["gol"]), home_win_rate=_r(ist["ev_galibiyet"], 3))
    if lig and int(lig["n"]) >= LIG_MIN:
        alan["league"] = {"n": int(lig["n"]), "yellow_avg": _r(lig["sari"]), "red_avg": _r(lig["kirmizi"]), "foul_avg": _r(lig["faul"]),
                          "penalty_avg": _r(lig["penalti"]), "goals_avg": _r(lig["gol"])}
    return alan


def _sorgu(sql: str, params: Dict[str, Any]):
    from app.services.db import execute_query
    return execute_query(sql, params)


def _satir(rows) -> Optional[Dict[str, Any]]:
    return dict(rows[0]) if rows and rows[0].get("n") else None


def hakem_kaynagi() -> Callable[[str, Any, datetime], Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]]:
    """(hakem_adı, lig_id, maç_tarihi) -> (hakem istatistiği, lig istatistiği). Koşu boyunca hakem ve lig başına tek sorgu."""
    hakemler: Dict[str, Optional[Dict[str, Any]]] = {}
    ligler: Dict[Any, Optional[Dict[str, Any]]] = {}

    def kaynak(ad: str, lig: Any, tarih: datetime):
        if ad not in hakemler:
            hakemler[ad] = _satir(_sorgu(HAKEM_SQL, {"ad": ad, "tarih": tarih}))
        if lig not in ligler:
            ligler[lig] = _satir(_sorgu(LIG_SQL, {"lig": lig, "tarih": tarih}))
        return hakemler[ad], ligler[lig]

    return kaynak
