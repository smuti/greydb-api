"""Skorjin anlık görüntüsünün `referee` grubu: hakemin adı FotMob'dan (yaklaşan maçın matchDetails'inde, maçtan 1-2 gün önce açıklanır),
istatistiği bizim geçmişimizden. Saf hesaplar + iki toplu sorgu. Biçim sözleşmesi Skorjin deposunda: docs/prematch-snapshot.md.

İstatistik: hakemin bu maçtan ÖNCE yönettiği, bitmiş ve istatistiği olan tüm maçlar (ligler arası; ad eşleşmesi `match_context.referee`).
Lig karşılaştırması: aynı ligin aynı tarihe kadarki tüm maçları. En az HAKEM_MIN maçı olmayan hakemde sayı yazılmaz (yalnız ad ve `n`);
lig ortalaması LIG_MIN maçtan azsa `league` yoktur. Veri yoksa alan null (0 uydurulmaz). Penaltı = penaltıdan atılan gol + kaçırılan penaltı olayı.
"""
import unicodedata
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

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
HAKEM_SQL = ("WITH mm AS (" + _STAT.format(birlesim="JOIN public.match_context c ON c.match_id = m.id", filtre="c.referee = ANY(:adlar)") + "), "
             + _PEN + _OZET)
ADLAR_SQL = ("SELECT referee AS ad, COALESCE(referee_country, '') AS ulke, count(*) AS n FROM public.match_context "
             "WHERE NULLIF(referee, '') IS NOT NULL GROUP BY 1, 2")
ALIAS_DDL = """
CREATE TABLE IF NOT EXISTS greydb.referee_alias (
    variant TEXT NOT NULL,
    country TEXT NOT NULL DEFAULT '',
    canonical TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'auto',
    status TEXT NOT NULL DEFAULT 'onayli',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (variant, country)
)
"""
ALIAS_OKU_SQL = "SELECT variant, country, canonical, status, source FROM greydb.referee_alias"
ALIAS_UPSERT_SQL = ("INSERT INTO greydb.referee_alias (variant, country, canonical, source, status, updated_at) "
                    "VALUES (:variant, :country, :canonical, 'auto', :status, now()) "
                    "ON CONFLICT (variant, country) DO UPDATE SET canonical = EXCLUDED.canonical, status = EXCLUDED.status, updated_at = now() "
                    "WHERE greydb.referee_alias.source = 'auto'")
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
                          "penalty_avg": _r(lig["penalti"]), "goals_avg": _r(lig["gol"]),
                          "home_win_rate": _r(lig["ev_galibiyet"], 3)}
    return alan


def _fold(metin: Optional[str]) -> str:
    """Aksan ve büyük/küçük harf farkını yok sayan karşılaştırma anahtarı."""
    d = unicodedata.normalize("NFKD", metin or "")
    return " ".join("".join(ch for ch in d if not unicodedata.combining(ch)).casefold().split())


def _altdizi(kisa: Sequence[str], uzun: Sequence[str]) -> bool:
    it = iter(uzun)
    return all(s in it for s in kisa)


def esdeger_ayrintili(ad: str, ulke: Optional[str], adaylar: Sequence[Tuple[str, str]]) -> Tuple[List[str], bool]:
    """Aynı hakemin kayıtlardaki yazımları ve eşleşmenin BELİRSİZ olup olmadığı. Aksan/harf farkı yok sayılarak ad aynıysa ya da (en az iki
    sözcüklü) FotMob adının sözcükleri kayıtlı adda SIRAYLA geçiyor ve ilk/son sözcük aynıysa ("Cláudio Pereira" ~ "Cláudio Filipe Ruivo
    Pereira"; "Bruno Pires Costa" ~ "Bruno José Ribeiro Pires Costa" ama "Bruno Costa" ya da "Bruno Miguel Ribeiro Costa" değil). Birden çok
    uzun aday birbiriyle uyumsuzsa (aynı ilk/son adlı farklı kişiler olabilir) belirsizdir: yalnız tam eşleşme sayılır. İkisinin de ülkesi
    biliniyorsa ülke aynı olmalı. Tek sözcüklü adlar yalnız tam eşleşir."""
    t = _fold(ad).split()
    ulke_k = _fold(ulke)
    tam: List[str] = []
    uzun: List[Tuple[str, List[str]]] = []
    for aday, aday_ulke in adaylar:
        if aday in tam or any(aday == u for u, _ in uzun):
            continue
        if ulke_k and _fold(aday_ulke) and _fold(aday_ulke) != ulke_k:
            continue
        a = _fold(aday).split()
        if a == t:
            tam.append(aday)
        elif len(t) >= 2 and len(a) > len(t) and a[0] == t[0] and a[-1] == t[-1] and _altdizi(t, a):
            uzun.append((aday, a))
    uyumlu = all(_altdizi(x, y) or _altdizi(y, x) for _, x in uzun for _, y in uzun)
    return tam + ([u for u, _ in uzun] if uyumlu else []), not uyumlu


def esdeger_adlar(ad: str, ulke: Optional[str], adaylar: Sequence[Tuple[str, str]]) -> List[str]:
    return esdeger_ayrintili(ad, ulke, adaylar)[0]


# ---------------------------------------------------------------- alias tablosu (greydb.referee_alias, issue #34)
def alias_plani(adaylar: Sequence[Tuple[str, str]], sayilar: Optional[Dict[str, int]] = None) -> List[Dict[str, str]]:
    """Kayıtlı yazımlardan alias satırları: kısa/aksansız yazım -> kanonik ad (en çok sözcüklü, eşitte en çok maçlı, sonra en çok aksanlı harf içeren, sonra alfabetik).
    Tek başına ve belirsizlik içermeyen ad için satır yazılmaz; belirsiz adlar kendine işaret eden `inceleme` satırı alır (birleştirilmez)."""
    sayilar = sayilar or {}
    benzersiz = list(dict.fromkeys(adaylar))
    satirlar: Dict[Tuple[str, str], Dict[str, str]] = {}
    kanonikler: List[str] = []
    for ad, ulke in benzersiz:
        adlar, belirsiz = esdeger_ayrintili(ad, ulke, benzersiz)
        if belirsiz:
            satirlar[(ad, ulke)] = {"variant": ad, "country": ulke, "canonical": ad, "status": "inceleme"}
            continue
        kume = list(dict.fromkeys([ad] + adlar))
        if len(kume) == 1:
            continue
        kanonik = min(kume, key=lambda x: (-len(_fold(x).split()), -sayilar.get(x, 0), -sum(1 for ch in x if not ch.isascii()), x))
        satirlar[(ad, ulke)] = {"variant": ad, "country": ulke, "canonical": kanonik, "status": "onayli"}
        kanonikler.append(kanonik)
    for k in dict.fromkeys(kanonikler):                                    # kanonik adın kendisi de gruba satır olarak girer
        for ad, ulke in benzersiz:
            if ad == k and (ad, ulke) not in satirlar:
                satirlar[(ad, ulke)] = {"variant": ad, "country": ulke, "canonical": k, "status": "onayli"}
    return list(satirlar.values())


def _ulke_uyumlu(satir_ulke: Optional[str], ulke: Optional[str]) -> bool:
    return not _fold(satir_ulke) or not _fold(ulke) or _fold(satir_ulke) == _fold(ulke)


def alias_cozumle(ad: str, ulke: Optional[str], alias_satirlari: Sequence[Dict[str, Any]], adaylar: Sequence[Tuple[str, str]]) -> List[str]:
    """Hakemin tüm kayıtlı yazımları. Alias tablosunda satırı varsa kanonik grubu (onaylı ya da elle girilmiş satırlar); otomatik `inceleme`
    satırı birleştirmez; satır yoksa kural (`esdeger_adlar`) kullanılır."""
    uyan = [r for r in alias_satirlari if _fold(r["variant"]) == _fold(ad) and _ulke_uyumlu(r.get("country"), ulke)]
    if not uyan:
        return esdeger_adlar(ad, ulke, adaylar) or [ad]
    satir = next((r for r in uyan if r.get("source") == "manual"), uyan[0])
    if satir.get("source") != "manual" and satir.get("status") != "onayli":
        return [ad]
    kanonik = satir["canonical"]
    grup = [r["variant"] for r in alias_satirlari
            if r["canonical"] == kanonik and (r.get("status") == "onayli" or r.get("source") == "manual") and _ulke_uyumlu(r.get("country"), ulke)]
    return list(dict.fromkeys(grup + [kanonik, ad]))


def _sorgu(sql: str, params: Dict[str, Any]):
    from app.services.db import execute_query
    return execute_query(sql, params)


def _yaz(sql: str, params: Dict[str, Any]) -> None:
    from app.services.db import execute_insert
    execute_insert(sql, params)


def _alias_oku() -> List[Dict[str, Any]]:
    try:
        return [dict(r) for r in _sorgu(ALIAS_OKU_SQL, {})]
    except Exception:  # noqa: BLE001 — tablo henüz yoksa (ilk koşu) alias yok sayılır, kural devrede
        return []


def alias_senkronla(dry_run: bool = True) -> Dict[str, Any]:
    """Kayıtlı hakem adlarından alias tablosunu günceller (yalnız `source='auto'` satırlar; elle satırlar ezilmez, değişmeyen satır yazılmaz).
    Kuru çalışmada yazmaz: plan ve `inceleme` listesini (belirsiz adlar) raporlar."""
    kayitlar = _sorgu(ADLAR_SQL, {})
    adaylar = [(r["ad"], r["ulke"]) for r in kayitlar]
    sayilar: Dict[str, int] = {}
    for r in kayitlar:
        sayilar[r["ad"]] = sayilar.get(r["ad"], 0) + int(r.get("n") or 0)
    plan = alias_plani(adaylar, sayilar)
    mevcut = {(r["variant"], r["country"]): r for r in _alias_oku()}
    yazilacak = []
    for satir in plan:
        eski = mevcut.get((satir["variant"], satir["country"]))
        if eski and (eski.get("source") == "manual" or (eski["canonical"] == satir["canonical"] and eski["status"] == satir["status"])):
            continue
        yazilacak.append(satir)
    yazilan = 0
    if not dry_run:
        for satir in yazilacak:
            _yaz(ALIAS_UPSERT_SQL, satir)
            yazilan += 1
    return {"plan": len(plan), "degisecek": len(yazilacak), "yazilan": yazilan,
            "inceleme": sorted(r["variant"] for r in plan if r["status"] == "inceleme")}


def _satir(rows) -> Optional[Dict[str, Any]]:
    return dict(rows[0]) if rows and rows[0].get("n") else None


def hakem_kaynagi() -> Callable[[str, Optional[str], Any, datetime], Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]]:
    """(hakem_adı, hakem_ülkesi, lig_id, maç_tarihi) -> (hakem istatistiği, lig istatistiği). Kayıtlı yazımlar bir kez çekilir;
    hakem ve lig başına tek sorgu (koşu boyunca bellekte). Aynı hakem farklı yazımlarla kayıtlıysa hepsi birlikte sayılır."""
    adaylar: List[Optional[List[Tuple[str, str]]]] = [None]
    alias: List[Optional[List[Dict[str, Any]]]] = [None]
    hakemler: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}
    ligler: Dict[Any, Optional[Dict[str, Any]]] = {}

    def kaynak(ad: str, ulke: Optional[str], lig: Any, tarih: datetime):
        anahtar = (_fold(ad), _fold(ulke))
        if anahtar not in hakemler:
            if adaylar[0] is None:
                adaylar[0] = [(r["ad"], r["ulke"]) for r in _sorgu(ADLAR_SQL, {})]
            if alias[0] is None:
                alias[0] = _alias_oku()
            adlar = alias_cozumle(ad, ulke, alias[0], adaylar[0]) or [ad]
            hakemler[anahtar] = _satir(_sorgu(HAKEM_SQL, {"adlar": adlar, "tarih": tarih}))
        if lig not in ligler:
            ligler[lig] = _satir(_sorgu(LIG_SQL, {"lig": lig, "tarih": tarih}))
        return hakemler[anahtar], ligler[lig]

    return kaynak
