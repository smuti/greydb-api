"""Sitenin 'veri özeti': işlenen maç sayısı ve veri noktası (maç verisi tablolarındaki dolu, teknik olmayan değerlerin sayısı).
Veri noktası = her tablo satırındaki, kimlik/zaman damgası olmayan ve boş (null) olmayan alan sayısı. Yaklaşık değil, sayılmış değerdir; ağır olduğu için önbelleğe alınır."""
import time
from typing import Any, Callable, Dict, Optional, Sequence

# Maç verisi tutan tablolar. Olmayan tablo atlanır. ML/model, takım, lig gibi tablolar "maç verisi" sayılmadığı için dışarıda.
VERI_TABLOLARI = (
    "public.matches", "public.match_stats", "public.match_advanced_stats", "public.match_events", "public.match_player_stats", "public.match_lineups",
    "public.match_formations", "public.match_context", "public.iddaa_odds", "public.odds_snapshots", "public.live_match_snapshots", "public.team_form",
    "public.h2h_stats", "public.season_standings", "public.referee_stats", "public.ms_match_xg", "public.player_availability", "public.scenario_observations",
)
_TEKNIK = ("id", "created_at", "updated_at")


def deger_sorgusu(tablo: str) -> str:
    teknik = ", ".join(f"'{t}'" for t in _TEKNIK)
    return (
        "SELECT count(*) AS satir, COALESCE(sum(("
        "SELECT count(*) FROM jsonb_each(to_jsonb(t)) e "
        f"WHERE e.key NOT IN ({teknik}) AND e.key NOT LIKE '%\\_id' AND e.value <> 'null'::jsonb AND e.value <> '\"\"'::jsonb"
        f")), 0) AS deger FROM {tablo} t"
    )


def ozet_olustur(execute_query, tablolar: Sequence[str] = VERI_TABLOLARI, simdi: Optional[float] = None) -> Dict[str, Any]:
    mac = execute_query("SELECT count(*) AS toplam FROM public.matches WHERE finished = true AND home_score IS NOT NULL")
    ayrinti, toplam = [], 0
    for tablo in tablolar:
        try:
            s = execute_query(deger_sorgusu(tablo))
        except Exception:        # tablo bu veritabanında yok ya da okunamadı: işi durdurmaz
            continue
        if s:
            ayrinti.append({"tablo": tablo.split(".")[-1], "satir": int(s[0]["satir"]), "deger": int(s[0]["deger"])})
            toplam += int(s[0]["deger"])
    return {
        "mac_sayisi": int(mac[0]["toplam"]) if mac else 0,
        "veri_noktasi": toplam,
        "tablo_sayisi": len(ayrinti),
        "tablolar": sorted(ayrinti, key=lambda a: -a["deger"]),
        "guncelleme": time.time() if simdi is None else simdi,
    }


_onbellek: Dict[str, Any] = {}


def onbellegi_temizle() -> None:
    _onbellek.clear()


def onbellekli(uret: Callable[[], Dict[str, Any]], simdi: Optional[float] = None, ttl: float = 3600.0) -> Dict[str, Any]:
    """TTL dolana kadar aynı sonucu verir. Süre dolduğunda taze üretilemezse (veritabanı hatası) eski değeri döner; hiç değer yoksa hata yükselir."""
    simdi = time.time() if simdi is None else simdi
    kayit = _onbellek.get("ozet")
    if kayit and simdi - kayit[0] < ttl:
        return kayit[1]
    try:
        deger = uret()
    except Exception:
        if kayit:
            return kayit[1]
        raise
    _onbellek["ozet"] = (simdi, deger)
    return deger
