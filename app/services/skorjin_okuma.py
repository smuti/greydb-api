"""Skorjin anlık görüntüsünün `okuma` grubu (maç okuması, yönetici): her takımın son 10 lig maçının maç başına profili ve ligin tabanı.
Biçim sözleşmesi Skorjin deposunda: src/skorjin/prematch/okuma_girdi.py (anahtarlar `TakimProfil` / `LigTabani` alanlarıyla aynı).

Kurallar: yalnızca maçın ligindeki, mevcut sezonun ve maç gününden ÖNCE biten maçlar; pencere takımın son OKUMA_PENCERE maçı.
Ölçüler yalnızca verisi olan maçlardan ortalanır (`n` toplam maç, ölçü başına veri yoksa null; 0 uydurulmaz). Takım için en az SEZON_MIN maç gerekir.
Kontra = şut haritasında durum FastBreak; duran top = FromCorner, FreeKick, ThrowInSetPiece, SetPiece (penaltı hariç). `*_yenilen` rakibin aynı ölçüsüdür.
"""
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.services.puan_durumu import sezon_araligi
from app.services.skorjin_prematch import _skorlu
from app.services.skorjin_sezon import SEZON_MIN, sezon_icin

OKUMA_PENCERE = 10

OKUMA_SQL = """
SELECT m.id AS mac_id, m.match_date AS tarih, m.home_team_id, m.away_team_id, m.home_score, m.away_score,
       COALESCE(x.xg_home, st.home_xg) AS ev_xg, COALESCE(x.xg_away, st.away_xg) AS dep_xg,
       st.home_shots AS ev_sut, st.away_shots AS dep_sut, st.home_corners AS ev_korner, st.away_corners AS dep_korner,
       st.home_yellow_cards AS ev_sari, st.away_yellow_cards AS dep_sari, st.home_red_cards AS ev_kirmizi, st.away_red_cards AS dep_kirmizi,
       st.home_fouls AS ev_faul, st.away_fouls AS dep_faul, st.home_possession AS ev_top, st.away_possession AS dep_top,
       a.home_passes_own_half AS ev_pas_kendi, a.away_passes_own_half AS dep_pas_kendi, a.home_passes_opp_half AS ev_pas_rakip, a.away_passes_opp_half AS dep_pas_rakip,
       a.home_touches_in_box AS ev_ceza, a.away_touches_in_box AS dep_ceza, a.home_tackles AS ev_mudahale, a.away_tackles AS dep_mudahale,
       sh.n > 0 AS sut_var, sh.ev_kontra, sh.dep_kontra, sh.ev_duran, sh.dep_duran
FROM public.matches m
JOIN public.teams th ON th.id = m.home_team_id
LEFT JOIN public.match_stats st ON st.match_id = m.id
LEFT JOIN public.ms_match_xg x ON x.match_id = m.id
LEFT JOIN public.match_advanced_stats a ON a.match_id = m.id
LEFT JOIN LATERAL (
  SELECT COUNT(*) AS n,
         SUM(q.xg) FILTER (WHERE q.ev AND q.durum = 'FastBreak') AS ev_kontra, SUM(q.xg) FILTER (WHERE NOT q.ev AND q.durum = 'FastBreak') AS dep_kontra,
         SUM(q.xg) FILTER (WHERE q.ev AND q.durum IN ('FromCorner', 'FreeKick', 'ThrowInSetPiece', 'SetPiece')) AS ev_duran,
         SUM(q.xg) FILTER (WHERE NOT q.ev AND q.durum IN ('FromCorner', 'FreeKick', 'ThrowInSetPiece', 'SetPiece')) AS dep_duran
  FROM (SELECT COALESCE((s->>'expectedGoals')::numeric, 0) AS xg, s->>'situation' AS durum, ((s->>'teamId')::bigint = th.fotmob_team_id) AS ev
        FROM jsonb_array_elements(CASE WHEN jsonb_typeof(m.raw_match_details->'content'->'shotmap'->'shots') = 'array'
                                       THEN m.raw_match_details->'content'->'shotmap'->'shots' ELSE '[]'::jsonb END) AS s) q
) sh ON true
WHERE m.league_id = :lig AND m.finished AND NOT COALESCE(m.cancelled, false)
  AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND m.match_date >= :bas AND m.match_date < :bit
ORDER BY m.match_date DESC
"""

_ORT = ("sut", "xg", "korner", "sari", "faul", "top", "ceza", "mudahale")      # ev_/dep_ öneki ile doğrudan ortalanan ölçüler


def _ort(degerler: List[float], basamak: int = 2) -> Optional[float]:
    return round(sum(degerler) / len(degerler), basamak) if degerler else None


def _deger(s: Dict[str, Any], on: str, ad: str) -> Optional[float]:
    v = s.get(on + ad)
    return None if v is None else float(v)


def _taraf(s: Dict[str, Any], takim: Any) -> Tuple[str, str]:
    """(kendi öneki, rakip öneki)"""
    return ("ev_", "dep_") if s["home_team_id"] == takim else ("dep_", "ev_")


def _sut_var(s: Dict[str, Any]) -> bool:
    return bool(s.get("sut_var"))


def _kart_puani(s: Dict[str, Any], on: str) -> Optional[float]:
    sari, kirmizi = s.get(on + "sari"), s.get(on + "kirmizi")
    return None if sari is None else float(sari) + 2.0 * float(kirmizi or 0)


def _pas_payi(s: Dict[str, Any], on: str) -> Optional[float]:
    kendi, rakip = s.get(on + "pas_kendi"), s.get(on + "pas_rakip")
    return None if kendi is None or rakip is None or (kendi + rakip) == 0 else rakip / (kendi + rakip) * 100.0


def _oncesi(satirlar: List[Dict[str, Any]], tarih: datetime) -> List[Dict[str, Any]]:
    return [s for s in satirlar if _skorlu(s) and s["tarih"] < tarih]


def _ozet(maclar: List[Dict[str, Any]], takim: Any) -> Dict[str, Any]:
    """Maç listesinden (herhangi sırada) takım profili; ölçü başına verisi olan maçların ortalaması."""
    def al(f: Callable[[Dict[str, Any], str, str], Optional[float]], rakip: bool = False) -> Optional[float]:
        vals = []
        for s in maclar:
            kendi, karsi = _taraf(s, takim)
            v = f(s, karsi if rakip else kendi, "")
            if v is not None:
                vals.append(v)
        return _ort(vals)

    def kalem(ad: str, rakip: bool = False, sadece_sut: bool = False) -> Optional[float]:
        vals = []
        for s in maclar:
            if sadece_sut and not _sut_var(s):
                continue
            kendi, karsi = _taraf(s, takim)
            v = _deger(s, karsi if rakip else kendi, ad)
            if v is not None:
                vals.append(v)
        return _ort(vals)

    berabere = sum(1 for s in maclar if s["home_score"] == s["away_score"])
    return {
        "n": len(maclar), "sut": kalem("sut"), "sut_yenilen": kalem("sut", True), "xg": kalem("xg"), "xg_yenilen": kalem("xg", True),
        "kontra_xg": kalem("kontra", sadece_sut=True), "kontra_xg_yenilen": kalem("kontra", True, True),
        "duran_xg": kalem("duran", sadece_sut=True), "duran_xg_yenilen": kalem("duran", True, True),
        "topla_oynama": kalem("top"), "rakip_yari_pas_payi": al(lambda s, on, _: _pas_payi(s, on)), "rakip_ceza_dokunus": kalem("ceza"),
        "mudahale": kalem("mudahale"), "korner": kalem("korner"), "korner_yenilen": kalem("korner", True), "sari": kalem("sari"), "faul": kalem("faul"),
        "beraberlik_orani": round(berabere / len(maclar), 3),
    }


def takim_profili(satirlar: List[Dict[str, Any]], takim: Any, tarih: datetime, pencere: int = OKUMA_PENCERE) -> Optional[Dict[str, Any]]:
    maclar = sorted((s for s in _oncesi(satirlar, tarih) if takim in (s["home_team_id"], s["away_team_id"])), key=lambda s: s["tarih"], reverse=True)[:pencere]
    return _ozet(maclar, takim) if len(maclar) >= SEZON_MIN else None


def _yuzdelik(degerler: List[float], q: float) -> Optional[float]:
    """Doğrusal aradeğerlemeli yüzdelik (q 0-1)."""
    if not degerler:
        return None
    v = sorted(degerler)
    pos = (len(v) - 1) * q
    alt = int(pos)
    ust = min(alt + 1, len(v) - 1)
    return round(v[alt] + (v[ust] - v[alt]) * (pos - alt), 3)


def lig_tabani(satirlar: List[Dict[str, Any]], tarih: datetime) -> Optional[Dict[str, Any]]:
    """Ligin maç başı tabanı. Takım ortalaması = her takım-maçının (ev ve dep) ortalaması; kontra eşikleri takımların son 10 maç ortalamalarının üst %25'i."""
    maclar = _oncesi(satirlar, tarih)
    if len(maclar) < 2 * SEZON_MIN:
        return None
    n = len(maclar)
    gol = _ort([s["home_score"] + s["away_score"] for s in maclar])
    kg = round(sum(1 for s in maclar if s["home_score"] > 0 and s["away_score"] > 0) / n, 3)
    ber = round(sum(1 for s in maclar if s["home_score"] == s["away_score"]) / n, 3)

    def toplam(ad: str) -> Optional[float]:
        return _ort([float(s["ev_" + ad]) + float(s["dep_" + ad]) for s in maclar if s.get("ev_" + ad) is not None and s.get("dep_" + ad) is not None])

    def kart() -> Optional[float]:
        return _ort([_kart_puani(s, "ev_") + _kart_puani(s, "dep_") for s in maclar if _kart_puani(s, "ev_") is not None and _kart_puani(s, "dep_") is not None])

    # takım-maçı havuzu: her maçı iki kez (ev gözünden, dep gözünden) say
    tm = []
    for s in maclar:
        tm.append(_ozet([s], s["home_team_id"]))
        tm.append(_ozet([s], s["away_team_id"]))

    def tak(ad: str) -> Optional[float]:
        return _ort([t[ad] for t in tm if t[ad] is not None])
    takim = {ad: tak(ad) for ad in ("sut", "sut_yenilen", "xg", "xg_yenilen", "kontra_xg", "kontra_xg_yenilen", "duran_xg", "duran_xg_yenilen", "topla_oynama",
                                    "rakip_yari_pas_payi", "rakip_ceza_dokunus", "mudahale", "korner", "korner_yenilen", "sari", "faul")}
    takim["n"] = n
    profiller = [p for p in (takim_profili(satirlar, t, tarih) for t in sorted({s["home_team_id"] for s in maclar} | {s["away_team_id"] for s in maclar})) if p]
    return {"n": n, "gol": gol, "kg": kg, "beraberlik": ber, "sut_toplam": toplam("sut"), "korner_toplam": toplam("korner"), "kart_puani_toplam": kart(), "takim": takim,
            "kontra_xg_p75": _yuzdelik([p["kontra_xg"] for p in profiller if p["kontra_xg"] is not None], 0.75),
            "kontra_xg_yenilen_p75": _yuzdelik([p["kontra_xg_yenilen"] for p in profiller if p["kontra_xg_yenilen"] is not None], 0.75)}


def okuma_grubu(satirlar: List[Dict[str, Any]], ev: Any, dep: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
    home, away, league = takim_profili(satirlar, ev, tarih), takim_profili(satirlar, dep, tarih), lig_tabani(satirlar, tarih)
    return {"home": home, "away": away, "league": league} if (home or away) else None


def okuma_kaynagi() -> Callable[[Any, Any, Any, datetime], Optional[Dict[str, Any]]]:
    """(lig_id, ev_id, dep_id, maç_tarihi) -> okuma grubu. Her lig-sezon için sorgu BİR kez çalışır (koşu boyunca bellekte)."""
    onbellek: Dict[Tuple[Any, int], List[Dict[str, Any]]] = {}

    def kaynak(lig: Any, ev: Any, dep: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
        sezon = sezon_icin(tarih)
        if (lig, sezon) not in onbellek:
            from app.services.db import execute_query
            bas, bit = sezon_araligi(sezon)
            onbellek[(lig, sezon)] = execute_query(OKUMA_SQL, {"lig": lig, "bas": bas, "bit": bit})
        return okuma_grubu(onbellek[(lig, sezon)], ev, dep, tarih)

    return kaynak
