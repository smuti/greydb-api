"""Skorjin anlık görüntüsünün `schedule` (dinlenme, yoğun program, Avrupa) ve `rotation` (ilk 11 değişimi) grupları. Saf hesaplar + takım
başına tek sorgu. Biçim sözleşmesi Skorjin deposunda: docs/prematch-snapshot.md.

Kapsam: yalnız veritabanımızdaki müsabakalar (lig, Şampiyonlar Ligi, Avrupa Ligi, Uluslar Ligi); yurt içi kupa maçları YOKTUR, yani
"son maçtan beri geçen gün" arada oynanan bir kupa maçını göremeyebilir. Maçın ilk 11'i `match_lineups.is_starter` ile; 11 kişi
olmayan ya da hiç olmayan kadrolar rotasyona girmez.
"""
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

AVRUPA_LIGLERI = frozenset({10, 11})        # Şampiyonlar Ligi, Avrupa Ligi (public.leagues)
EN_FAZLA_GUN = 45                            # son maç bundan eskiyse (sezon başı) program anlamsız: None
KISA_DINLENME_GUN = 4                        # bu kadar ya da daha az gün arayla oynanan maç "kısa dinlenme"
ROTASYON_PENCERE = 10                        # son 10 geçiş
ROTASYON_MIN = 3                             # ortalama için en az geçiş
GRUP_MIN = 2                                 # kısa/normal dinlenme ortalaması için en az geçiş

TAKIM_SQL = """
WITH son AS (
    SELECT m.id, m.match_date, m.league_id, m.home_team_id, m.away_team_id, m.home_score, m.away_score
    FROM public.matches m
    WHERE (m.home_team_id = :t OR m.away_team_id = :t) AND m.finished AND NOT COALESCE(m.cancelled, false)
      AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL AND m.match_date < :tarih
    ORDER BY m.match_date DESC LIMIT 12
)
SELECT s.id AS mac_id, s.match_date AS tarih, s.league_id, l.name AS lig_ad, s.home_team_id, s.away_team_id, s.home_score, s.away_score,
       opp.name AS rakip, array_agg(ml.player_name ORDER BY ml.player_name) FILTER (WHERE ml.is_starter) AS ilk11
FROM son s
JOIN public.teams opp ON opp.id = CASE WHEN s.home_team_id = :t THEN s.away_team_id ELSE s.home_team_id END
LEFT JOIN public.leagues l ON l.id = s.league_id
LEFT JOIN public.match_lineups ml ON ml.match_id = s.id AND ml.team_id = :t
GROUP BY s.id, s.match_date, s.league_id, l.name, s.home_team_id, s.away_team_id, s.home_score, s.away_score, opp.name
ORDER BY s.match_date DESC
"""


def _oncesi(satirlar: Sequence[Dict[str, Any]], tarih: datetime) -> List[Dict[str, Any]]:
    return sorted((s for s in satirlar if s["tarih"] < tarih), key=lambda s: s["tarih"], reverse=True)


def program(satirlar: Sequence[Dict[str, Any]], takim: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
    """Maç gününe kadarki dinlenme ve yoğunluk: son maçtan beri gün, son maç, son 7/14 günde maç sayısı, 7 günde Avrupa maçı var mı."""
    maclar = _oncesi(satirlar, tarih)
    if not maclar:
        return None
    son = maclar[0]
    gun = (tarih.date() - son["tarih"].date()).days
    if gun > EN_FAZLA_GUN:
        return None
    ev_mi = son["home_team_id"] == takim
    gf, ga = (son["home_score"], son["away_score"]) if ev_mi else (son["away_score"], son["home_score"])
    yedi, on_dort = timedelta(days=7), timedelta(days=14)
    return {
        "days_since_last": gun,
        "last": {"d": son["tarih"].date().isoformat(), "competition": son.get("lig_ad"), "competition_id": son.get("league_id"),
                 "venue": "H" if ev_mi else "A", "opp": son.get("rakip"), "gf": gf, "ga": ga},
        "matches_7d": sum(1 for s in maclar if tarih - s["tarih"] <= yedi),
        "matches_14d": sum(1 for s in maclar if tarih - s["tarih"] <= on_dort),
        "european_7d": any(s.get("league_id") in AVRUPA_LIGLERI and tarih - s["tarih"] <= yedi for s in maclar),
    }


def rotasyon(satirlar: Sequence[Dict[str, Any]], takim: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
    """Ardışık iki maçın ilk 11'i arasında kaç oyuncu değişti (11 − ortak oyuncu), kısa ve normal dinlenme sonrası ayrı ortalamayla.
    Geçerli (tam 11 kişilik) kadrosu olmayan maçlar atlanır; son `ROTASYON_PENCERE` geçiş."""
    gecerli = [s for s in reversed(_oncesi(satirlar, tarih)) if isinstance(s.get("ilk11"), (list, tuple)) and len(set(s["ilk11"])) == 11]
    gecerli = gecerli[-(ROTASYON_PENCERE + 1):]
    gecisler: List[Dict[str, Any]] = []
    for onceki, simdiki in zip(gecerli, gecerli[1:]):
        gecisler.append({"d": simdiki["tarih"].date().isoformat(), "competition": simdiki.get("lig_ad"),
                         "days_rest": (simdiki["tarih"].date() - onceki["tarih"].date()).days,
                         "changes": 11 - len(set(simdiki["ilk11"]) & set(onceki["ilk11"]))})
    if not gecisler:
        return None
    yeni_eski = list(reversed(gecisler))

    def ort(parca: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        return {"n": len(parca), "avg_changes": round(sum(g["changes"] for g in parca) / len(parca), 2)} if len(parca) >= GRUP_MIN else None

    return {
        "n": len(gecisler),
        "avg_changes": round(sum(g["changes"] for g in gecisler) / len(gecisler), 2) if len(gecisler) >= ROTASYON_MIN else None,
        "last_changes": yeni_eski[0]["changes"],
        "short_rest": ort([g for g in gecisler if g["days_rest"] <= KISA_DINLENME_GUN]),
        "normal_rest": ort([g for g in gecisler if g["days_rest"] > KISA_DINLENME_GUN]),
        "matches": yeni_eski,
    }


def _sorgu(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    return execute_query(sql, params)


def program_kaynagi() -> Callable[[Any, datetime], Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]]:
    """(takım_id, maç_tarihi) -> (schedule, rotation). Takım ve tarih başına tek sorgu (koşu boyunca bellekte)."""
    onbellek: Dict[Tuple[Any, datetime], Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]] = {}

    def kaynak(takim: Any, tarih: datetime):
        if (takim, tarih) not in onbellek:
            satirlar = _sorgu(TAKIM_SQL, {"t": takim, "tarih": tarih})
            onbellek[(takim, tarih)] = (program(satirlar, takim, tarih), rotasyon(satirlar, takim, tarih))
        return onbellek[(takim, tarih)]

    return kaynak
