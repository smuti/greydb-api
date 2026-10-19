"""Yayın paketi toplayıcı (dış dünyayla konuşan katman): bir maç için FotMob (eksikler), kendi veritabanımız (puan, form, geçmiş, iç/dış saha)
ve istatistik servisinden (xG, gol zamanlaması, takım tipi, kart) veri toplar; kartları yayin_paketi saf fonksiyonlarıyla üretir.

Ağ ve veritabanı çağrıları küçük işlevlerde tutulur; testlerde değiştirilir. Hiçbir adım tüm paketi düşürmez: bir kaynak hata verirse yalnızca o kart atlanır."""
import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.services import yayin_paketi as P
from app.services.db import execute_insert, execute_query
from app.services.mac_baglam import form_ozeti, h2h_ozeti
from app.services.puan_durumu import guncel_sezon, hesapla, sezon_araligi
from app.services.takim_renk import NOTR, baskin_renk

log = logging.getLogger("yayin")
ISTATISTIK = "https://web-production-0ce73.up.railway.app"
FOTMOB_DETAY = "https://www.fotmob.com/api/data/matchDetails"
LOGO_URL = "https://images.fotmob.com/image_resources/logo/teamlogo/{id}.png"
UA = {"User-Agent": "Mozilla/5.0"}
_TSI = 3  # Türkiye saati UTC+3 (yaz saati yok)


def _ad(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").lower().replace("ı", "i"))
    return re.sub(r"[^a-z0-9]+", " ", "".join(c for c in s if not unicodedata.combining(c))).strip()


# ---------------- Ağ ----------------
def _get(url: str, **params) -> Optional[Any]:
    try:
        r = httpx.get(url, params=params or None, headers=UA, timeout=25.0)
        return r.json() if r.status_code == 200 else None
    except Exception as e:  # noqa: BLE001
        log.warning("istek hatası %s: %s", url, e)
        return None


def fotmob_detay(mac_id: int) -> Optional[Dict[str, Any]]:
    d = _get(FOTMOB_DETAY, matchId=mac_id)
    return d if isinstance(d, dict) and d.get("general") else None


_liglerin_takimlari: Dict[int, List[Dict[str, Any]]] = {}
_ligler: Optional[List[Dict[str, Any]]] = None


def _analiz_ligleri() -> List[Dict[str, Any]]:
    global _ligler
    if _ligler is None:
        _ligler = (_get(f"{ISTATISTIK}/api/leagues/") or {}).get("leagues") or []
    return _ligler


def _lig_takimlari(lig_id: int) -> List[Dict[str, Any]]:
    if lig_id not in _liglerin_takimlari:
        veri = _get(f"{ISTATISTIK}/api/leagues/{lig_id}/teams") or {}
        _liglerin_takimlari[lig_id] = (veri.get("teams") if isinstance(veri, dict) else veri) or []
    return _liglerin_takimlari[lig_id]


def analiz_takimlari(lig_adi: str, ev_ad: str, dep_ad: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Maçın liginin istatistik servisindeki karşılığında iki takımı adından bulur (aynı adlı birden çok lig varsa ikisini de içereni seçer)."""
    adaylar = [l for l in _analiz_ligleri() if _ad(l.get("name", "")) == _ad(lig_adi)]
    for lig in adaylar:
        takimlar = _lig_takimlari(lig["id"])
        bul = lambda ad: next((t for t in takimlar if _ad(t["name"]) == _ad(ad) or _ad(ad) in _ad(t["name"]) or _ad(t["name"]) in _ad(ad)), None)  # noqa: E731
        e, d = bul(ev_ad), bul(dep_ad)
        if e and d:
            return e, d
    return None, None


def takim_istatistigi(takim_id: int) -> Tuple[Optional[Dict], Optional[Dict], Optional[Dict]]:
    """Gol/savunma/xG, takım tipi ve gol zamanlaması: bu sezon TÜM turnuvalardaki maçlar (all_comps=1; istatistik servisi eski sürümse parametreyi yok sayar, kendi ligine düşer)."""
    t = f"{ISTATISTIK}/api/leagues/teams/{takim_id}"
    iyi = lambda x: x if isinstance(x, dict) and "error" not in x else None  # noqa: E731
    return iyi(_get(f"{t}/stats", all_comps=1)), iyi(_get(f"{t}/type", all_comps=1)), iyi(_get(f"{t}/goal-timing", all_comps=1))


def kart_profili(takim_adi: str) -> Optional[Dict[str, Any]]:
    p = _get(f"{ISTATISTIK}/api/matches/cards/team-profile/{takim_adi}", n_matches=10)
    return p if isinstance(p, dict) and "error" not in p else None


# ---------------- Veritabanı ----------------
_TAKIM_IC = "SELECT id FROM public.teams WHERE fotmob_team_id = :f LIMIT 1"
_ORTAK = """SELECT m.match_date AS tarih, m.home_team_id, m.away_team_id, m.home_score, m.away_score, ev.name AS ev_ad, dep.name AS dep_ad, l.name AS lig_ad
            FROM public.matches m JOIN public.teams ev ON ev.id = m.home_team_id JOIN public.teams dep ON dep.id = m.away_team_id
            LEFT JOIN public.leagues l ON l.id = m.league_id"""
_BITMIS = "AND m.finished AND NOT COALESCE(m.cancelled, false) AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL AND m.match_date < :tarih"
_FORM = f"{_ORTAK} WHERE (m.home_team_id = :t OR m.away_team_id = :t) {_BITMIS} ORDER BY m.match_date DESC LIMIT 5"
_H2H = f"{_ORTAK} WHERE ((m.home_team_id = :a AND m.away_team_id = :b) OR (m.home_team_id = :b AND m.away_team_id = :a)) {_BITMIS} ORDER BY m.match_date DESC LIMIT 10"
_SEZON_EV = "SELECT m.home_score AS a, m.away_score AS y FROM public.matches m JOIN public.leagues l ON l.id = m.league_id WHERE m.home_team_id = :t AND l.fotmob_league_id = :lig AND m.finished AND NOT COALESCE(m.cancelled,false) AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL AND m.match_date >= :bas AND m.match_date < :tarih"
_SEZON_DEP = "SELECT m.away_score AS a, m.home_score AS y FROM public.matches m JOIN public.leagues l ON l.id = m.league_id WHERE m.away_team_id = :t AND l.fotmob_league_id = :lig AND m.finished AND NOT COALESCE(m.cancelled,false) AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL AND m.match_date >= :bas AND m.match_date < :tarih"
_TABLO = """SELECT m.match_date AS tarih, m.home_score AS ev_gol, m.away_score AS dep_gol, ev.fotmob_team_id AS ev_id, ev.name AS ev_ad, dep.fotmob_team_id AS dep_id, dep.name AS dep_ad
            FROM public.matches m JOIN public.leagues l ON l.id = m.league_id JOIN public.teams ev ON ev.id = m.home_team_id JOIN public.teams dep ON dep.id = m.away_team_id
            WHERE l.fotmob_league_id = :lig AND m.finished AND NOT COALESCE(m.cancelled,false) AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
              AND m.match_date >= :bas AND m.match_date < :bit ORDER BY m.match_date"""


def _ic_id(fotmob_team_id: int) -> Optional[int]:
    r = execute_query(_TAKIM_IC, {"f": fotmob_team_id})
    return r[0]["id"] if r else None


# ---------------- Takım rengi ve logo ----------------
def takim_bilgisi(fotmob_team_id: int, ad: str, logo_adresi: str) -> Dict[str, Any]:
    """Rengi (elle seçilen varsa o) ve logo adresini döner. İlk kez görülen takımın logosu FotMob'dan bir kez alınıp saklanır."""
    r = execute_query("SELECT renk, renk_elle, (logo IS NOT NULL) AS logo_var FROM greydb.yayin_takimlari WHERE fotmob_team_id = :f", {"f": fotmob_team_id})
    if r and r[0]["logo_var"]:
        return {"ad": ad, "logo": logo_adresi, "renk": r[0]["renk_elle"] or r[0]["renk"]}
    png = None
    try:
        resp = httpx.get(LOGO_URL.format(id=fotmob_team_id), headers=UA, timeout=20.0)
        png = resp.content if resp.status_code == 200 and resp.content else None
    except Exception as e:  # noqa: BLE001
        log.warning("logo alınamadı %s: %s", fotmob_team_id, e)
    renk = baskin_renk(png) if png else NOTR
    execute_insert(
        """INSERT INTO greydb.yayin_takimlari (fotmob_team_id, ad, renk, logo) VALUES (:f, :ad, :renk, :logo)
           ON CONFLICT (fotmob_team_id) DO UPDATE SET ad = EXCLUDED.ad, renk = EXCLUDED.renk, logo = COALESCE(EXCLUDED.logo, greydb.yayin_takimlari.logo),
                                                      guncelleme = (now() AT TIME ZONE 'utc')""",
        {"f": fotmob_team_id, "ad": ad, "renk": renk, "logo": png},
    )
    return {"ad": ad, "logo": logo_adresi if png else None, "renk": (r[0]["renk_elle"] if r and r[0]["renk_elle"] else renk)}


# ---------------- Bir maçın paketi ----------------
def _saat_tarih(utc_iso: str) -> Tuple[str, str]:
    d = datetime.fromisoformat(utc_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    yerel = datetime.fromtimestamp(d.timestamp() + _TSI * 3600, tz=timezone.utc)
    return yerel.strftime("%H:%M"), yerel.strftime("%Y-%m-%d")


def _guvenli(f, *a):
    try:
        return f(*a)
    except Exception as e:  # noqa: BLE001
        log.warning("kart atlandı (%s): %s", getattr(f, "__name__", f), e)
        return None


def mac_hazirla(sira: int, fotmob_match_id: int, logo_taban: str) -> Optional[Dict[str, Any]]:
    """Bir maçın yayın paketi; FotMob maç ayrıntısı alınamazsa None (maç hiç eklenmez)."""
    d = fotmob_detay(fotmob_match_id)
    if not d:
        return None
    g = d["general"]
    ev, dep = g["homeTeam"], g["awayTeam"]
    saat, tarih = _saat_tarih(g["matchTimeUTCDate"])
    hafta = g.get("leagueRoundName") or g.get("matchRound")
    hafta_yazi = f"{hafta}. Hafta" if str(hafta or "").isdigit() else (str(hafta) if hafta else None)
    ev_id, dep_id = int(ev["id"]), int(dep["id"])
    evb = takim_bilgisi(ev_id, ev["name"], f"{logo_taban}/{ev_id}")
    depb = takim_bilgisi(dep_id, dep["name"], f"{logo_taban}/{dep_id}")
    lineup = (d.get("content") or {}).get("lineup") or {}
    eksik = {"ev": P.eksik_listesi(lineup.get("homeTeam")), "dep": P.eksik_listesi(lineup.get("awayTeam"))}

    kartlar: Dict[str, Any] = {"eksik": eksik}
    ev_ic, dep_ic = _ic_id(ev_id), _ic_id(dep_id)
    tarih_dt = datetime.fromisoformat(g["matchTimeUTCDate"].replace("Z", "+00:00")).replace(tzinfo=None)
    form_ev = form_dep = None
    lig_fm = g.get("parentLeagueId") or g.get("leagueId")   # maçın yarıştığı turnuva (iç/dış saha ve puan durumu bununla sınırlı)
    if ev_ic and dep_ic:
        form_ev = {"maclar": [], **form_ozeti(execute_query(_FORM, {"t": ev_ic, "tarih": tarih_dt}), ev_ic)}
        form_dep = {"maclar": [], **form_ozeti(execute_query(_FORM, {"t": dep_ic, "tarih": tarih_dt}), dep_ic)}
        kartlar["form"] = P.form_karti(form_ev, form_dep)
        kartlar["h2h"] = P.h2h_karti(h2h_ozeti(execute_query(_H2H, {"a": ev_ic, "b": dep_ic, "tarih": tarih_dt}), ev_ic, dep_ic), ev["name"])
        bas, _ = sezon_araligi(guncel_sezon(tarih_dt.date()))
        kartlar["icDis"] = P.ic_dis_karti(
            [(r["a"], r["y"]) for r in execute_query(_SEZON_EV, {"t": ev_ic, "lig": int(lig_fm or 0), "bas": bas, "tarih": tarih_dt})],
            [(r["a"], r["y"]) for r in execute_query(_SEZON_DEP, {"t": dep_ic, "lig": int(lig_fm or 0), "bas": bas, "tarih": tarih_dt})],
        )
    if lig_fm:
        bas, bit = sezon_araligi(guncel_sezon(tarih_dt.date()))
        tablo = hesapla(execute_query(_TABLO, {"lig": int(lig_fm), "bas": bas, "bit": bit}))
        kartlar["puan"] = P.puan_karti(tablo, ev_id, dep_id)

    ae, ad = analiz_takimlari(g.get("leagueName") or "", ev["name"], dep["name"])
    if ae and ad:
        se, te, ge = takim_istatistigi(ae["id"])
        sd, td, gd = takim_istatistigi(ad["id"])
        kartlar["gol"] = _guvenli(P.gol_karti, se, sd)
        if form_ev and form_dep:
            kartlar["savunma"] = _guvenli(P.savunma_karti, se, sd, form_ev, form_dep)
        kartlar["zaman"] = _guvenli(P.zaman_karti, ge, gd)
        kartlar["tip"] = _guvenli(P.tip_karti, se, sd, te, td)
        kartlar["kart"] = _guvenli(P.kart_karti, kart_profili(ae["name"]), kart_profili(ad["name"]))
    return P.mac_paketi(sira, evb, depb, lig=g.get("leagueName") or "", hafta=hafta_yazi, saat=saat, tarih=tarih, **kartlar)
