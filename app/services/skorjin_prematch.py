"""Skorjin maç öncesi anlık görüntüsü (greydb.match_prematch): saf dönüşümler.

Skorjin (maç öncesi cevap servisi) veritabanını salt-okunur okur ve bu tablodan cevap kurar; sayıları kendisi uydurmaz.
Burası ham girdileri (maç satırları, iddaa olayı, matchforge sonucu) tablonun JSON biçimine çevirir. I/O yoktur.
Biçim sözleşmesi Skorjin deposunda: docs/prematch-snapshot.md.
"""
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from app.services.iddaa_dagilim import _ad as market_adi

MS_MARKET = "Maç Sonucu"
ALT_UST_25 = "Alt/Üst 2.5"
_MS_SECIM = {"1": "home", "0": "draw", "2": "away"}


def _gun(tarih) -> str:
    return tarih.date().isoformat()


def _skorlu(s: Dict[str, Any]) -> bool:
    return s.get("home_score") is not None and s.get("away_score") is not None


def takim_maclari(satirlar: List[Dict[str, Any]], takim_ic_id: Any) -> List[Dict[str, Any]]:
    """Takımın bitmiş maçları, takımın penceresinden (v: H ev / A deplasman). Sıra girdideki gibi (en yeniden eskiye)."""
    cikti = []
    for s in satirlar:
        if not _skorlu(s):
            continue
        ev_mi = s["home_team_id"] == takim_ic_id
        cikti.append({
            "d": _gun(s["tarih"]), "v": "H" if ev_mi else "A",
            "gf": s["home_score"] if ev_mi else s["away_score"], "ga": s["away_score"] if ev_mi else s["home_score"],
            "opp": s["dep_ad"] if ev_mi else s["ev_ad"],
        })
    return cikti


def h2h_listesi(satirlar: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """İki takım arasındaki bitmiş maçlar, oynandığı gibi (ev/deplasman adları o maçtakiyle)."""
    return [{"d": _gun(s["tarih"]), "home": s["ev_ad"], "away": s["dep_ad"], "hg": s["home_score"], "ag": s["away_score"]}
            for s in satirlar if _skorlu(s)]


def _market(olay: Optional[Dict[str, Any]], cfg: Dict[str, Any], ad: str) -> Optional[Dict[str, Any]]:
    for m in (olay or {}).get("m", []):
        if market_adi(m, cfg) == ad:
            return m
    return None


def iddaa_oranlari(olay: Optional[Dict[str, Any]], cfg: Dict[str, Any]) -> Optional[Dict[str, Optional[float]]]:
    """iddaa olayından Maç Sonucu (1-0-2) ve 2.5 Alt/Üst oranları. Maç Sonucu yoksa ya da oran geçersizse None (uydurma yok)."""
    ms = _market(olay, cfg, MS_MARKET)
    if ms is None:
        return None
    oran = {_MS_SECIM[o["n"]]: o["odd"] for o in ms.get("o", []) if o.get("n") in _MS_SECIM}
    if set(oran) != {"home", "draw", "away"} or any(v is None or v < 1.0 for v in oran.values()):
        return None
    au = _market(olay, cfg, ALT_UST_25)
    secim = {o["n"]: o["odd"] for o in (au or {}).get("o", [])}
    ust, alt = secim.get("Üst"), secim.get("Alt")
    gecerli = ust is not None and alt is not None and ust >= 1.0 and alt >= 1.0
    return {**oran, "over25": ust if gecerli else None, "under25": alt if gecerli else None}


def iddaa_kalabalik(olay: Optional[Dict[str, Any]], yuzdeler: Dict[str, Dict[str, float]], cfg: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """Maç Sonucu seçimlerinin oynanma yüzdesi, market içinde normalize (toplam 1). Veri yoksa ya da toplam 0 ise None."""
    ms = _market(olay, cfg, MS_MARKET)
    y = (yuzdeler or {}).get(str(ms.get("i"))) if ms else None
    if not y:
        return None
    ham = {}
    for o in ms.get("o", []):
        taraf = _MS_SECIM.get(o.get("n"))
        if taraf is not None and y.get(str(o.get("no"))) is not None:
            ham[taraf] = float(y[str(o["no"])])
    toplam = sum(ham.values())
    if set(ham) != {"home", "draw", "away"} or toplam <= 0:
        return None
    return {k: round(v / toplam, 4) for k, v in ham.items()}


def model_verisi(sonuc: Optional[Dict[str, Any]], model_surumu: str) -> Optional[Dict[str, Any]]:
    """matchforge /api/ms/predict sonucu (include_matrix) -> skor matrisi + harman 1-X-2. Matris yoksa None."""
    if not sonuc or not sonuc.get("score_matrix"):
        return None
    return {"matrix": sonuc["score_matrix"], "blend": {"home": sonuc["p1"], "draw": sonuc["p0"], "away": sonuc["p2"]},
            "model_version": sonuc.get("model_version") or model_surumu}


# ---------------------------------------------------------------- orkestrasyon (kaynaklar enjekte edilir)
KAPSAMLAR = ("gunluk", "oran")
MODEL_LIGLERI = {4, 5, 6, 7, 8, 9, 12, 20, 21}     # matchforge ms_model kapsamı (SCOPE + EXTRA ligler)


@dataclass(frozen=True)
class Kaynaklar:
    maclar: Callable[[datetime, datetime], List[Dict[str, Any]]]                  # yaklaşan maçlar
    form: Callable[[Any, datetime], List[Dict[str, Any]]]                          # takımın maç öncesi bitmiş maçları (yeniden eskiye)
    h2h: Callable[[Any, Any, datetime], List[Dict[str, Any]]]
    model: Callable[[List[Dict[str, Any]]], Dict[int, Dict[str, Any]]]             # fotmob_match_id -> matchforge sonucu
    iddaa: Callable[[int], Tuple[Optional[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]]   # (olay, yüzdeler, market yapılandırması)
    mevcut: Callable[[List[int]], Set[int]]                                        # tabloda satırı olan maç kimlikleri
    yaz: Callable[[str, Dict[str, Any]], None]


def _ad(m: Dict[str, Any]) -> str:
    return f"{m['home_name']} - {m['away_name']}"


def _model_kullanilabilir(m: Dict[str, Any], sonuc: Optional[Dict[str, Any]]) -> bool:
    return bool(sonuc) and not sonuc.get("new_team_home") and not sonuc.get("new_team_away")


def _oran_alanlari(m: Dict[str, Any], k: Kaynaklar, simdi: datetime, rapor: Dict[str, Any]) -> Dict[str, Any]:
    """Başarılı çekimde piyasa yoksa None (artık yok); iddaa hatasında ANAHTARLAR OLMAZ (eski değer korunur)."""
    try:
        olay, yuzdeler, cfg = k.iddaa(m["iddaa_event_id"])
    except Exception:  # noqa: BLE001 — iddaa erişilemezse mevcut veri bozulmaz
        rapor["iddaa_hatasi"] += 1
        return {}
    oran, kalabalik = iddaa_oranlari(olay, cfg), iddaa_kalabalik(olay, yuzdeler, cfg)
    return {"odds": oran, "odds_at": simdi if oran else None, "crowd": kalabalik, "crowd_at": simdi if kalabalik else None}


def hazirla(kapsam: str, dry_run: bool, k: Kaynaklar, simdi: datetime, model_surumu: str,
            ufuk_gun: int = 7, oran_ufku_saat: int = 48) -> Dict[str, Any]:
    """Yaklaşan maçlar için greydb.match_prematch satırlarını hazırlar. `gunluk`: tüm alanlar, `oran`: yalnız oran/oynanma yüzdesi."""
    from datetime import timedelta
    if kapsam not in KAPSAMLAR:
        raise ValueError(f"kapsam gunluk ya da oran olmalı: {kapsam}")
    ufuk = simdi + (timedelta(days=ufuk_gun) if kapsam == "gunluk" else timedelta(hours=oran_ufku_saat))
    maclar = k.maclar(simdi, ufuk)
    rapor: Dict[str, Any] = {"kapsam": kapsam, "dry_run": dry_run, "aday": len(maclar), "hazirlanan": 0, "model_yok": 0,
                             "iddaa_hatasi": 0, "satiri_yok": 0, "iddaa_kimligi_yok": 0, "hatalar": [], "maclar": []}

    modeller: Dict[int, Dict[str, Any]] = {}
    if kapsam == "gunluk":
        kapsamdakiler = [m for m in maclar if m["league_id"] in MODEL_LIGLERI]
        if kapsamdakiler:
            try:
                modeller = k.model(kapsamdakiler)
            except Exception as e:  # noqa: BLE001 — model olmadan da form/h2h/oran hazırlanır
                rapor["hatalar"].append({"fotmob_match_id": None, "hata": f"model: {type(e).__name__}"})
    var = k.mevcut([m["fotmob_match_id"] for m in maclar]) if kapsam == "oran" else set()

    for m in maclar:
        fid = m["fotmob_match_id"]
        try:
            if kapsam == "oran":
                if fid not in var:
                    rapor["satiri_yok"] += 1
                    continue
                if not m.get("iddaa_event_id"):
                    rapor["iddaa_kimligi_yok"] += 1
                    continue
                alanlar = _oran_alanlari(m, k, simdi, rapor)
                if not alanlar:
                    continue
                satir = {"fotmob_match_id": fid, **alanlar}
            else:
                satir = _gunluk_satir(m, k, modeller.get(fid), simdi, model_surumu, rapor)
            if not dry_run:
                k.yaz(kapsam, satir)
            rapor["hazirlanan"] += 1
            rapor["maclar"].append({"fotmob_match_id": fid, "mac": _ad(m), "alanlar": sorted(
                a for a in ("form", "h2h", "scoreline", "odds", "crowd") if satir.get(a))})
        except Exception as e:  # noqa: BLE001 — tek maçın hatası işi durdurmaz
            rapor["hatalar"].append({"fotmob_match_id": fid, "hata": f"{type(e).__name__}: {e}"})
    return rapor


def _gunluk_satir(m, k, sonuc, simdi, model_surumu, rapor) -> Dict[str, Any]:
    tarih = m["match_date"]
    ev, dep = m["home_team_id"], m["away_team_id"]
    form = {"home": takim_maclari(k.form(ev, tarih), ev), "away": takim_maclari(k.form(dep, tarih), dep)}
    form = form if (form["home"] or form["away"]) else None
    h2h = h2h_listesi(k.h2h(ev, dep, tarih)) or None
    scoreline = model_verisi(sonuc, model_surumu) if _model_kullanilabilir(m, sonuc) else None
    if scoreline is None:
        rapor["model_yok"] += 1
    oran = _oran_alanlari(m, k, simdi, rapor) if m.get("iddaa_event_id") else {}
    return {
        "fotmob_match_id": m["fotmob_match_id"], "iddaa_event_id": m.get("iddaa_event_id"),
        "home_team_id": ev, "away_team_id": dep, "home_name": m["home_name"], "away_name": m["away_name"],
        "league_id": m["league_id"], "league_name": m["league_name"], "kickoff": tarih,
        "form": form, "form_at": simdi if form else None,
        "h2h": h2h, "h2h_at": simdi if h2h else None,
        "scoreline": scoreline, "scoreline_at": simdi if scoreline else None,
        "odds": oran.get("odds"), "odds_at": oran.get("odds_at"), "crowd": oran.get("crowd"), "crowd_at": oran.get("crowd_at"),
    }


# ---------------------------------------------------------------- tablo, sorgular, gerçek kaynaklar
TABLO_DDL = """
CREATE TABLE IF NOT EXISTS greydb.match_prematch (
    fotmob_match_id BIGINT PRIMARY KEY,
    iddaa_event_id BIGINT,
    home_team_id INTEGER, away_team_id INTEGER,
    home_name TEXT NOT NULL, away_name TEXT NOT NULL,
    league_id INTEGER, league_name TEXT,
    kickoff TIMESTAMPTZ NOT NULL,
    form JSONB, form_at TIMESTAMPTZ,
    h2h JSONB, h2h_at TIMESTAMPTZ,
    scoreline JSONB, scoreline_at TIMESTAMPTZ,
    odds JSONB, odds_at TIMESTAMPTZ,
    crowd JSONB, crowd_at TIMESTAMPTZ,
    absences JSONB, absences_at TIMESTAMPTZ, absences_source TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_match_prematch_kickoff ON greydb.match_prematch (kickoff);
"""

_KIMLIK = ("iddaa_event_id", "home_team_id", "away_team_id", "home_name", "away_name", "league_id", "league_name", "kickoff")
_ZAMANLI = ("form", "h2h", "scoreline", "odds", "crowd")
_JSON = set(_ZAMANLI)
_KOLONLAR = {
    "gunluk": _KIMLIK + tuple(c for g in _ZAMANLI for c in (g, f"{g}_at")),
    "oran": ("odds", "odds_at", "crowd", "crowd_at"),
}


def _js(x):
    return None if x is None else json.dumps(x, ensure_ascii=False)


def yazma_sorgusu(kapsam: str, satir: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """gunluk: satırı oluşturur/günceller (eksik oyuncu alanlarına dokunmaz); oran: yalnız oran alanlarını günceller, satır oluşturmaz."""
    kolonlar = _KOLONLAR[kapsam]
    params = {"fotmob_match_id": satir["fotmob_match_id"], **{c: (_js(satir.get(c)) if c in _JSON else satir.get(c)) for c in kolonlar}}
    deger = lambda c: f"CAST(:{c} AS jsonb)" if c in _JSON else f":{c}"   # noqa: E731
    if kapsam == "oran":
        atama = ", ".join(f"{c} = {deger(c)}" for c in kolonlar)
        return f"UPDATE greydb.match_prematch SET {atama}, updated_at = now() WHERE fotmob_match_id = :fotmob_match_id", params
    sutunlar = ", ".join(("fotmob_match_id",) + kolonlar)
    degerler = ", ".join(["CAST(:fotmob_match_id AS BIGINT)"] + [deger(c) for c in kolonlar])
    guncelle = ", ".join(f"{c} = EXCLUDED.{c}" for c in kolonlar)
    return (f"INSERT INTO greydb.match_prematch ({sutunlar}, updated_at) VALUES ({degerler}, now()) "
            f"ON CONFLICT (fotmob_match_id) DO UPDATE SET {guncelle}, updated_at = now()"), params


# Maça kadar, bitmiş ve skorlu maçlar (match_context ile aynı süzgeç: gelecek sonuç sızmaz)
_ORTAK = """
SELECT m.match_date AS tarih, m.home_team_id, m.away_team_id, m.home_score, m.away_score, ev.name AS ev_ad, dep.name AS dep_ad
FROM public.matches m
JOIN public.teams ev ON ev.id = m.home_team_id
JOIN public.teams dep ON dep.id = m.away_team_id
"""
_FILTRE = "AND m.finished AND NOT COALESCE(m.cancelled, false) AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL AND m.match_date < :tarih"
FORM_SQL = f"{_ORTAK} WHERE (m.home_team_id = :t OR m.away_team_id = :t) {_FILTRE} ORDER BY m.match_date DESC LIMIT 20"
H2H_SQL = (f"{_ORTAK} WHERE ((m.home_team_id = :a AND m.away_team_id = :b) OR (m.home_team_id = :b AND m.away_team_id = :a)) "
           f"{_FILTRE} ORDER BY m.match_date DESC LIMIT 10")
MACLAR_SQL = """
SELECT u.fotmob_match_id, u.iddaa_event_id, u.home_team_id, u.away_team_id, ev.name AS home_name, dep.name AS away_name,
       u.league_id, l.name AS league_name, u.match_date
FROM public.upcoming_matches u
JOIN public.teams ev ON ev.id = u.home_team_id
JOIN public.teams dep ON dep.id = u.away_team_id
LEFT JOIN public.leagues l ON l.id = u.league_id
WHERE u.fotmob_match_id IS NOT NULL AND u.match_date > :bas AND u.match_date <= :son
ORDER BY u.match_date
"""


def tablo_hazirla() -> None:
    from app.services.db import execute_insert
    for parca in TABLO_DDL.split(";"):
        if parca.strip():
            execute_insert(parca)


def _yaklasan(bas: datetime, son: datetime) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    tekil: Dict[Any, Dict[str, Any]] = {}
    for m in execute_query(MACLAR_SQL, {"bas": bas, "son": son}):      # aynı maç iki kez gelirse en güncel saatli olan
        if m["fotmob_match_id"] not in tekil or m["match_date"] > tekil[m["fotmob_match_id"]]["match_date"]:
            tekil[m["fotmob_match_id"]] = m
    return list(tekil.values())


def _model(maclar: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """matchforge /api/ms/predict (tek toplu çağrı, skor matrisiyle). as_of = en erken maç günü: gelecek sonuç sızmaz."""
    import httpx
    from app.services.skorjin_match_analysis import MATCHFORGE_BASE
    yuk = {"include_matrix": True, "matches": [
        {"home_team_id": int(m["home_team_id"]), "away_team_id": int(m["away_team_id"]), "league_id": m["league_id"],
         "match_date": m["match_date"].isoformat(), "home_name": m["home_name"], "away_name": m["away_name"]} for m in maclar]}
    yanit = httpx.post(f"{MATCHFORGE_BASE}/api/ms/predict", json=yuk, timeout=120.0)
    yanit.raise_for_status()
    govde = yanit.json()
    return {m["fotmob_match_id"]: {**r, "model_version": govde.get("model_version")} for m, r in zip(maclar, govde.get("results", []))}


def _iddaa(event_id: int):
    from app.routers.iddaa import iddaa_dagilim_verisi
    olay, yuzdeler, _pay, cfg = iddaa_dagilim_verisi(event_id)
    return olay, yuzdeler, cfg


def _mevcut(ids: List[int]) -> Set[int]:
    from app.services.db import execute_query
    if not ids:
        return set()
    try:
        return {r["fotmob_match_id"] for r in execute_query(
            "SELECT fotmob_match_id FROM greydb.match_prematch WHERE fotmob_match_id = ANY(:ids)", {"ids": list(ids)})}
    except Exception:  # noqa: BLE001 — tablo henüz yoksa (kuru çalıştırma) hiçbir satır yok
        return set()


def _yaz(kapsam: str, satir: Dict[str, Any]) -> None:
    from app.services.db import execute_insert
    sql, params = yazma_sorgusu(kapsam, satir)
    execute_insert(sql, params)


def gercek_kaynaklar() -> Kaynaklar:
    from app.services.db import execute_query
    return Kaynaklar(
        maclar=_yaklasan,
        form=lambda takim, tarih: execute_query(FORM_SQL, {"t": takim, "tarih": tarih}),
        h2h=lambda a, b, tarih: execute_query(H2H_SQL, {"a": a, "b": b, "tarih": tarih}),
        model=_model, iddaa=_iddaa, mevcut=_mevcut, yaz=_yaz,
    )


from contextlib import contextmanager  # noqa: E402

LOCK_ANAHTARI = 7_340_001     # pg_advisory_lock kimliği (Skorjin anlık görüntü işi)


@contextmanager
def tek_kosu(adi: str = "skorjin_prematch"):
    """Aynı anda tek koşu: oturum düzeyinde advisory lock. Kilit alınamazsa False verir (çağıran 409 döner)."""
    from sqlalchemy import text
    from app.services.db import engine
    conn = engine.connect()
    try:
        alindi = bool(conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_ANAHTARI}).scalar())
        try:
            yield alindi
        finally:
            if alindi:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_ANAHTARI})
    finally:
        conn.close()
