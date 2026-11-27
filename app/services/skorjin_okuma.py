"""Skorjin anlık görüntüsünün `okuma` grubu (maç okuması, yönetici): her takımın son 10 lig maçının maç başına profili ve ligin tabanı.
Biçim sözleşmesi Skorjin deposunda: src/skorjin/prematch/okuma_girdi.py (anahtarlar `TakimProfil` / `LigTabani` alanlarıyla aynı).

Kurallar: yalnızca maçın ligindeki, mevcut sezonun ve maç gününden ÖNCE biten maçlar; pencere takımın son OKUMA_PENCERE maçı.
Ölçüler yalnızca verisi olan maçlardan ortalanır (`n` toplam maç, ölçü başına veri yoksa null; 0 uydurulmaz). Takım için en az SEZON_MIN maç gerekir.
Kapsam ayrımı (`europe`, `prev_league`, `prev_europe`): aynı P biçimi, ama lig ve Avrupa AYRI kümelerden hesaplanır, hiçbir ortalama ikisini karıştırmaz.
Kaynak skorjin_stat.takim_mac (tur 'lig' | 'avrupa'); Avrupa'da lig süzgeci yoktur, geçen sezon ligi maçın ligiyle sınırlıdır (terfi eden takımda null). Sezon Temmuz-Temmuz,
yalnız maç gününden ÖNCE biten maçlar; en az SEZON_MIN maç yoksa null. Kontra xG bu kaynakta yok: null kalır (0 uydurulmaz).
Kontra = şut haritasında durum FastBreak; duran top = FromCorner, FreeKick, ThrowInSetPiece, SetPiece (penaltı hariç). `*_yenilen` rakibin aynı ölçüsüdür.
"""
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.services.puan_durumu import sezon_araligi
from app.services.skorjin_prematch import _skorlu
from app.services.skorjin_sezon import SEZON_MIN, sezon_icin

logger = logging.getLogger(__name__)

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


def okuma_grubu(satirlar: List[Dict[str, Any]], ev: Any, dep: Any, tarih: datetime, kapsam: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    home, away, league = takim_profili(satirlar, ev, tarih), takim_profili(satirlar, dep, tarih), lig_tabani(satirlar, tarih)
    if not (home or away):
        return None
    grup: Dict[str, Any] = {"home": home, "away": away, "league": league}
    grup.update({k: v for k, v in (kapsam or {}).items() if v})      # kapsam alanları yalnız doluysa yazılır: eski tüketici ve eski görüntü aynı kalır
    return grup


# ---------------------------------------------------------------- Kapsam ayrımı: bu sezon Avrupa, geçen sezon lig, geçen sezon Avrupa
KAPSAM_SQL = """
SELECT k.mac_id, k.tarih, k.tur, k.turnuva, k.lig_id, k.takim_id, k.rakip_id, k.saha, k.gf, k.ga, k.xg_f, k.xg_a, k.sut_f, k.sut_a, k.korner_f, k.korner_a,
       k.sari_f, k.sari_a, k.kirmizi_f, k.kirmizi_a, k.faul_f, k.faul_a, k.top_f, k.top_a, k.pas_kendi_f, k.pas_kendi_a, k.pas_rakip_f, k.pas_rakip_a,
       k.kutu_f, k.kutu_a, k.mudahale_f, k.mudahale_a, k.duran_xg_f, k.duran_xg_a
FROM skorjin_stat.takim_mac k
WHERE k.takim_id = :takim AND k.tarih >= :bas AND k.tarih < :bit AND k.gf IS NOT NULL AND k.ga IS NOT NULL
  AND (k.tur = 'avrupa' OR (k.tur = 'lig' AND k.lig_id = :lig))
ORDER BY k.tarih DESC
"""
AVRUPA_TABAN_SQL = KAPSAM_SQL.replace("WHERE k.takim_id = :takim AND", "WHERE k.tur = 'avrupa' AND k.turnuva = :turnuva AND k.saha = 'E' AND") \
                             .replace("  AND (k.tur = 'avrupa' OR (k.tur = 'lig' AND k.lig_id = :lig))\n", "")
_OLCULER = (("xg", "xg"), ("sut", "sut"), ("korner", "korner"), ("sari", "sari"), ("kirmizi", "kirmizi"), ("faul", "faul"), ("top", "top"),
            ("pas_kendi", "pas_kendi"), ("pas_rakip", "pas_rakip"), ("ceza", "kutu"), ("mudahale", "mudahale"))


def kapsam_satiri(r: Dict[str, Any]) -> Dict[str, Any]:
    """takim_mac satırını (takım gözünden: *_f kendi, *_a rakip) OKUMA_SQL'in maç satırı biçimine çevirir; saha 'E' = takım evde."""
    ev_mi = r["saha"] == "E"
    ev_son, dep_son = ("_f", "_a") if ev_mi else ("_a", "_f")      # ev ve deplasman gözünden son ek
    s: Dict[str, Any] = {"mac_id": r["mac_id"], "tarih": r["tarih"], "home_team_id": r["takim_id"] if ev_mi else r["rakip_id"], "away_team_id": r["rakip_id"] if ev_mi else r["takim_id"],
                         "home_score": r["gf"] if ev_mi else r["ga"], "away_score": r["ga"] if ev_mi else r["gf"], "tur": r["tur"], "turnuva": r.get("turnuva")}
    for ad, kaynak in _OLCULER:
        s["ev_" + ad], s["dep_" + ad] = r.get(kaynak + ev_son), r.get(kaynak + dep_son)
    ev_dt, dep_dt = (r.get("duran_xg_f"), r.get("duran_xg_a")) if ev_mi else (r.get("duran_xg_a"), r.get("duran_xg_f"))
    s.update(ev_duran=ev_dt, dep_duran=dep_dt, ev_kontra=None, dep_kontra=None, sut_var=ev_dt is not None)      # kontra xG bu kaynakta yok: null
    return s


def _tur_sezon(satirlar: List[Dict[str, Any]], tur: str, sezon: int, tarih: datetime) -> List[Dict[str, Any]]:
    """Sezon sınırı Temmuz-Temmuz (30 Haziran sezonun son günü, 1 Temmuz yeni sezon); yalnız maç gününden önce biten, skorlu maçlar."""
    bas, bit = sezon_araligi(sezon)
    return [s for s in satirlar if s["tur"] == tur and bas <= s["tarih"] < bit and s["tarih"] < tarih and _skorlu(s)]


def _kapsam_profil(satirlar: List[Dict[str, Any]], takim: Any, tur: str, sezon: int, tarih: datetime, pencere: int) -> Optional[Dict[str, Any]]:
    return takim_profili(_tur_sezon(satirlar, tur, sezon, tarih), takim, tarih, pencere)


def kapsam_gruplari(ev_satirlari: List[Dict[str, Any]], dep_satirlari: List[Dict[str, Any]], ev: Any, dep: Any, tarih: datetime,
                    avrupa_tabani: Optional[Callable[[str, int], List[Dict[str, Any]]]] = None) -> Dict[str, Any]:
    """{europe, prev_league, prev_europe}: her biri {"home": P | null, "away": P | null} ya da (hiç profil yoksa) None.
    Bu sezon Avrupa: son OKUMA_PENCERE maç; geçen sezon: sezonun tamamı. `europe` ayrıca `turnuva` (takımın bu sezonki en son Avrupa turnuvası) ve `base`
    (aynı sezonun AYNI turnuvasındaki takım-maçı ortalaması; lig tabanıyla kıyaslanmaz) taşır. `avrupa_tabani(turnuva, sezon)` -> o turnuvanın maç satırları."""
    sezon = sezon_icin(tarih)
    ev_s, dep_s = [kapsam_satiri(r) for r in ev_satirlari], [kapsam_satiri(r) for r in dep_satirlari]

    def cift(tur: str, s: int, pencere: int) -> Optional[Dict[str, Any]]:
        h, a = _kapsam_profil(ev_s, ev, tur, s, tarih, pencere), _kapsam_profil(dep_s, dep, tur, s, tarih, pencere)
        return {"home": h, "away": a} if (h or a) else None
    avrupa = cift("avrupa", sezon, OKUMA_PENCERE)
    if avrupa:
        turnuva, base = {}, {}
        for taraf, satirlar, takim in (("home", ev_s, ev), ("away", dep_s, dep)):
            son = next(iter(sorted((s for s in _tur_sezon(satirlar, "avrupa", sezon, tarih) if takim in (s["home_team_id"], s["away_team_id"])), key=lambda s: s["tarih"], reverse=True)), None)
            turnuva[taraf] = son["turnuva"] if son else None
            base[taraf] = lig_tabani(_tur_sezon(avrupa_tabani(son["turnuva"], sezon), "avrupa", sezon, tarih), tarih) if son and avrupa_tabani else None
        avrupa = {**avrupa, "turnuva": turnuva, "base": base}
    return {"europe": avrupa, "prev_league": cift("lig", sezon - 1, 10 ** 6), "prev_europe": cift("avrupa", sezon - 1, 10 ** 6)}


def okuma_kaynagi() -> Callable[[Any, Any, Any, datetime], Optional[Dict[str, Any]]]:
    """(lig_id, ev_id, dep_id, maç_tarihi) -> okuma grubu. Her lig-sezon için sorgu BİR kez çalışır (koşu boyunca bellekte)."""
    onbellek: Dict[Tuple[Any, int], List[Dict[str, Any]]] = {}
    taban_onbellek: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}

    def kapsam(lig: Any, ev: Any, dep: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
        """Kapsam alanları ek bilgidir: kaynak okunamazsa ya da erişim yoksa yalnız onlar düşer, okuma grubunun geri kalanı yazılır."""
        try:
            from app.services.db import execute_query
            sezon = sezon_icin(tarih)
            bas, bit = sezon_araligi(sezon - 1)[0], tarih

            def al(takim: Any) -> List[Dict[str, Any]]:
                return execute_query(KAPSAM_SQL, {"takim": takim, "lig": lig, "bas": bas, "bit": bit})

            def taban(turnuva: str, sz: int) -> List[Dict[str, Any]]:
                if (turnuva, sz) not in taban_onbellek:
                    b, t = sezon_araligi(sz)
                    taban_onbellek[(turnuva, sz)] = [kapsam_satiri(r) for r in execute_query(AVRUPA_TABAN_SQL, {"turnuva": turnuva, "bas": b, "bit": t})]
                return taban_onbellek[(turnuva, sz)]
            return kapsam_gruplari(al(ev), al(dep), ev, dep, tarih, taban)
        except Exception:
            logger.exception("okuma kapsam alanları okunamadı (lig=%s ev=%s dep=%s)", lig, ev, dep)
            return None

    def kaynak(lig: Any, ev: Any, dep: Any, tarih: datetime) -> Optional[Dict[str, Any]]:
        sezon = sezon_icin(tarih)
        if (lig, sezon) not in onbellek:
            from app.services.db import execute_query
            bas, bit = sezon_araligi(sezon)
            onbellek[(lig, sezon)] = execute_query(OKUMA_SQL, {"lig": lig, "bas": bas, "bit": bit})
        return okuma_grubu(onbellek[(lig, sezon)], ev, dep, tarih, kapsam(lig, ev, dep, tarih))

    return kaynak
