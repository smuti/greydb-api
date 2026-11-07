"""Oyuncu maç istatistiği: FotMob'un bütün oyuncu alanları (Önemli, Hücum, Pas, Savunma, Mücadele ve Kaleci sekmeleri) tek yerde ayıklanır.

Ham veri `matches.raw_match_details.content.playerStats`: her oyuncunun `stats` grupları içinde değerler başlıkla DEĞİL sabit `key` ile okunur
(başlıklar dile ve sürüme göre değişir; eski kayıt kodu başlık eşlediği için 25 sütunun yarısını yanlış, geri kalanını hiç yazmıyordu).
Aynı ayıklayıcı hem yeni biten maç kaydında (`match_saver.save_match_player_stats`) hem geçmiş maçların ham veriden onarımında kullanılır.
"""
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("oyuncu-stat")

KONUM = {0: "GK", 1: "DF", 2: "MF", 3: "FW"}         # FotMob usualPosition: kaleci, defans, orta saha, forvet

# (sütun, ham anahtar, parça: "value" ya da "total", yoksa varsayılan). Varsayılan 0 olanlar eski kaydın zaten 0 yazdığı sayaçlardır (tüketiciler bunlara güvenir).
_ALANLAR = [
    ("rating", "rating_title", "value", None), ("minutes_played", "minutes_played", "value", None),
    ("goals", "goals", "value", 0), ("assists", "assists", "value", 0),
    ("xg", "expected_goals", "value", None), ("xa", "expected_assists", "value", None), ("xg_plus_xa", "xg_and_xa", "value", None),
    ("xgot", "expected_goals_on_target_variant", "value", None), ("xg_non_penalty", "expected_goals_non_penalty", "value", None),
    ("total_shots", "total_shots", "value", 0), ("shots_on_target", "ShotsOnTarget", "value", 0), ("shots_off_target", "ShotsOffTarget", "value", None),
    ("blocked_shots", "blocked_shots", "value", None), ("shots_woodwork", "shots_woodwork", "value", None),
    ("touches_in_box", "touches_opp_box", "value", None), ("successful_dribbles", "dribbles_succeeded", "value", None), ("dribbles_total", "dribbles_succeeded", "total", None),
    ("offsides", "Offsides", "value", None), ("corners_taken", "corners", "value", None),
    ("touches", "touches", "value", 0), ("accurate_passes", "accurate_passes", "value", 0), ("total_passes", "accurate_passes", "total", 0),
    ("chances_created", "chances_created", "value", None), ("passes_into_final_third", "passes_into_final_third", "value", None), ("line_breaking_passes", "line_breaking_passes", "value", None),
    ("big_chances_created", "big_chance_created_team_title", "value", None), ("big_chances_missed", "big_chance_missed_title", "value", None),
    ("accurate_crosses", "accurate_crosses", "value", None), ("crosses_total", "accurate_crosses", "total", None),
    ("accurate_long_balls", "long_balls_accurate", "value", None), ("long_balls_total", "long_balls_accurate", "total", None),
    ("dispossessed", "dispossessed", "value", None),
    ("defensive_actions", "defensive_actions", "value", None), ("recoveries", "recoveries", "value", None),
    ("tackles", "matchstats.headers.tackles", "value", 0), ("interceptions", "interceptions", "value", 0), ("blocks", "shot_blocks", "value", None),
    ("clearances", "clearances", "value", 0), ("aerial_clearances", "headed_clearance", "value", None), ("clearance_off_the_line", "clearance_off_the_line", "value", None),
    ("was_dribbled", "dribbled_past", "value", None), ("last_man_tackles", "last_man_tackle", "value", None), ("errors_led_to_goal", "errors_led_to_goal", "value", None),
    ("duels_won", "duel_won", "value", 0), ("duels_lost", "duel_lost", "value", 0),
    ("ground_duels_won", "ground_duels_won", "value", None), ("ground_duels_total", "ground_duels_won", "total", None),
    ("aerial_duels_won", "aerials_won", "value", None), ("aerial_duels_total", "aerials_won", "total", None),
    ("fouls_committed", "fouls", "value", 0), ("fouls_won", "was_fouled", "value", 0),
    ("conceded_penalties", "conceded_penalties", "value", None), ("penalties_won", "penalties_won", "value", None), ("missed_penalty", "missed_penalty", "value", None),
    ("saved_penalties", "saved_penalties", "value", None), ("own_goals", "owngoal", "value", None),
    ("saves_inside_box", "saves_inside_box", "value", None), ("xgot_faced", "expected_goals_on_target_faced", "value", None), ("goals_prevented", "goals_prevented", "value", None),
    ("high_claims", "keeper_high_claim", "value", None), ("punches", "punches", "value", None), ("keeper_sweeper", "keeper_sweeper", "value", None),
    ("keeper_diving_saves", "keeper_diving_save", "value", None), ("throws", "player_throws", "value", None),
    ("top_speed", "physical_metrics_topspeed", "value", None), ("distance_covered", "physical_metrics_distance_covered", "value", None),
    ("running_distance", "physical_metrics_running", "value", None), ("sprinting_distance", "physical_metrics_sprinting", "value", None),
    ("number_of_sprints", "physical_metrics_number_of_sprints", "value", None), ("fantasy_points", "fantasy_points", "value", None),
]
_TURETILEN = ["saves", "goals_conceded", "duels_total", "duels_won_pct", "pass_accuracy", "key_passes", "position", "usual_position", "fotmob_player_id"]
TUM_SUTUNLAR: List[str] = [a[0] for a in _ALANLAR] + _TURETILEN

# Tabloda önceden olan sütunlar (match_player_stats, 52 sütunun veri kısmı); gerisi ilk yazımda eklenir.
_ESKI = {"rating", "minutes_played", "position", "goals", "assists", "xg", "xa", "xg_plus_xa", "xgot", "total_shots", "shots_on_target", "successful_dribbles", "big_chances_missed",
         "touches_in_box", "touches", "total_passes", "accurate_passes", "pass_accuracy", "key_passes", "chances_created", "accurate_crosses", "crosses_total", "accurate_long_balls",
         "defensive_actions", "tackles", "interceptions", "blocks", "clearances", "aerial_clearances", "was_dribbled", "recoveries", "duels_won", "duels_lost", "duels_total",
         "duels_won_pct", "ground_duels_won", "ground_duels_total", "aerial_duels_won", "aerial_duels_total", "fouls_committed", "fouls_won", "saves", "saves_inside_box",
         "goals_conceded", "xgot_faced", "goals_prevented", "high_claims", "punches"}
YENI_SUTUNLAR: List[str] = [c for c in TUM_SUTUNLAR if c not in _ESKI]
_ONDALIKLI = {"xg_non_penalty", "top_speed", "distance_covered", "running_distance", "sprinting_distance", "fantasy_points"}


def _tip(sutun: str) -> str:
    return "BIGINT" if sutun == "fotmob_player_id" else "SMALLINT" if sutun == "usual_position" else "DOUBLE PRECISION" if sutun in _ONDALIKLI else "INTEGER"


def _sayi(v: Any) -> Optional[float]:
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def ayikla(p: Dict[str, Any]) -> Dict[str, Any]:
    """Bir oyuncunun ham FotMob nesnesinden tablo sütunlarına karşılık gelen değerler. Bozuk ya da eksik girdi çökertmez."""
    ham: Dict[str, Dict[str, Any]] = {}
    for grup in (p.get("stats") if isinstance(p, dict) else None) or []:
        for e in ((grup or {}).get("stats") or {}).values():
            if isinstance(e, dict) and e.get("key") and isinstance(e.get("stat"), dict) and e["key"] not in ham:
                ham[e["key"]] = e["stat"]
    r: Dict[str, Any] = {}
    for sutun, anahtar, parca, varsayilan in _ALANLAR:
        v = _sayi(ham.get(anahtar, {}).get(parca))
        r[sutun] = varsayilan if v is None else v
    kaleci = bool(p.get("isGoalkeeper"))
    for sutun, anahtar in (("saves", "saves"), ("goals_conceded", "goals_conceded")):
        v = _sayi(ham.get(anahtar, {}).get("value"))
        r[sutun] = v if v is not None else (0 if kaleci else None)
    kazanilan, kaybedilen = r["duels_won"] or 0, r["duels_lost"] or 0
    r["duels_total"] = kazanilan + kaybedilen
    r["duels_won_pct"] = round(100.0 * kazanilan / r["duels_total"], 1) if r["duels_total"] else None
    r["pass_accuracy"] = round(100.0 * r["accurate_passes"] / r["total_passes"], 1) if r["total_passes"] else None
    r["key_passes"] = r["chances_created"] if r["chances_created"] is not None else 0          # FotMob'da "kilit pas" ayrı alan değil; karşılığı yaratılan gol fırsatı
    konum = p.get("usualPosition")
    r["usual_position"] = konum if isinstance(konum, int) and konum in KONUM else None
    r["position"] = KONUM.get(konum) if isinstance(konum, int) else None
    pid = p.get("id")
    r["fotmob_player_id"] = pid if isinstance(pid, int) else None
    return r


# ---------------- tablo ----------------
_HAZIR = False


def sutunlari_hazirla() -> None:
    """Yeni sütunlar yoksa ekler (yeniden çalıştırılabilir; dağıtım ile migration sırası önemli olmasın)."""
    global _HAZIR
    if _HAZIR:
        return
    from app.services.db import execute_insert
    execute_insert("ALTER TABLE public.match_player_stats " + ", ".join(f"ADD COLUMN IF NOT EXISTS {c} {_tip(c)}" for c in YENI_SUTUNLAR), {})
    _HAZIR = True


EKLE_SQL = ("INSERT INTO public.match_player_stats (match_id, team_id, player_name, " + ", ".join(TUM_SUTUNLAR) + ") VALUES (:match_id, :team_id, :player_name, "
            + ", ".join(f":{c}" for c in TUM_SUTUNLAR) + ")")
GUNCELLE_SQL = "UPDATE public.match_player_stats SET " + ", ".join(f"{c} = :{c}" for c in TUM_SUTUNLAR) + " WHERE id = :id"


def takim_eslemesi(icerik: Dict[str, Any], ev_id: int, dep_id: int) -> Dict[Any, int]:
    """FotMob takım kimliği -> iç takım kimliği (içerikteki kadro ev/dep kimlikleriyle)."""
    kadro = (icerik or {}).get("lineup") or {}
    ev, dep = (kadro.get("homeTeam") or {}).get("id"), (kadro.get("awayTeam") or {}).get("id")
    return {k: v for k, v in ((ev, ev_id), (dep, dep_id)) if k is not None}


# ---------------- geçmiş onarımı ----------------
@dataclass(frozen=True)
class Kaynaklar:
    maclar: Callable[[int, int], List[Dict[str, Any]]]                       # (imleç, adet) -> {id, home_team_id, away_team_id, raw} (id > imleç, id sıralı)
    mevcut: Callable[[int], List[Dict[str, Any]]]                            # maç -> tablodaki {id, team_id, player_name}
    guncelle: Callable[[int, Dict[str, Any]], None]
    ekle: Callable[[int, int, Dict[str, Any]], None]


def geri_doldur(sonra: int, adet: int, dry_run: bool, k: Kaynaklar) -> Dict[str, Any]:
    """Bir sonraki `adet` bitmiş maçın oyuncu satırlarını ham veriden onarır: varsa günceller, yoksa ekler.
    Aynı takımda aynı adlı birden çok satır varsa o oyuncu atlanır (belirsiz). `sonraki`: bir sonraki çağrının imleci."""
    rapor: Dict[str, Any] = dict(dry_run=dry_run, islenen_mac=0, guncellenen=0, eklenen=0, belirsiz=0, oyuncusuz_mac=0, hata=[], sonraki=sonra, bitti=False)
    maclar = k.maclar(sonra, adet)
    for m in maclar:
        rapor["islenen_mac"] += 1
        rapor["sonraki"] = m["id"]
        try:
            icerik = (m.get("raw") or {}).get("content") or {}
            oyuncular = icerik.get("playerStats") or {}
            if not oyuncular:
                rapor["oyuncusuz_mac"] += 1
                continue
            esle = takim_eslemesi(icerik, m["home_team_id"], m["away_team_id"])
            mevcut: Dict[Any, List[int]] = {}
            for s in k.mevcut(m["id"]):
                mevcut.setdefault((s["team_id"], s["player_name"]), []).append(s["id"])
            for p in oyuncular.values():
                takim = esle.get(p.get("teamId"))
                if takim is None or not p.get("name"):
                    continue
                satirlar = mevcut.get((takim, p["name"]), [])
                if len(satirlar) > 1:
                    rapor["belirsiz"] += 1
                    continue
                degerler = ayikla(p)
                if satirlar:
                    rapor["guncellenen"] += 1
                    if not dry_run:
                        k.guncelle(satirlar[0], degerler)
                else:
                    rapor["eklenen"] += 1
                    if not dry_run:
                        k.ekle(m["id"], takim, {"player_name": p["name"], **degerler})
        except Exception as e:  # noqa: BLE001 — bir maç hata verse de diğerleri onarılır
            log.warning("oyuncu onarımı: maç %s: %s", m.get("id"), e)
            rapor["hata"].append(f"{m.get('id')}: {str(e)[:150]}")
    rapor["bitti"] = len(maclar) < adet
    return rapor


def _maclar(sonra: int, adet: int) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    return execute_query("SELECT id, home_team_id, away_team_id, raw_match_details AS raw FROM public.matches WHERE finished AND NOT COALESCE(cancelled, false) "
                         "AND raw_match_details IS NOT NULL AND id > :sonra ORDER BY id LIMIT :adet", {"sonra": sonra, "adet": adet})


def _mevcut(mac_id: int) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    return execute_query("SELECT id, team_id, player_name FROM public.match_player_stats WHERE match_id = :m", {"m": mac_id})


def _guncelle(satir_id: int, degerler: Dict[str, Any]) -> None:
    from app.services.db import execute_insert
    execute_insert(GUNCELLE_SQL, {**{c: degerler.get(c) for c in TUM_SUTUNLAR}, "id": satir_id})


def _ekle(mac_id: int, takim_id: int, degerler: Dict[str, Any]) -> None:
    from app.services.db import execute_insert
    execute_insert(EKLE_SQL, {"match_id": mac_id, "team_id": takim_id, "player_name": degerler["player_name"], **{c: degerler.get(c) for c in TUM_SUTUNLAR}})


def gercek_kaynaklar() -> Kaynaklar:
    return Kaynaklar(maclar=_maclar, mevcut=_mevcut, guncelle=_guncelle, ekle=_ekle)


def calistir(sonra: int = 0, adet: int = 100, dry_run: bool = True) -> Dict[str, Any]:
    """Uç buradan çağırır. Kuru çalışmada tablo değişmez (sütun eklenmez, satır yazılmaz); gerçek çalışmada önce yeni sütunlar hazırlanır."""
    if not dry_run:
        sutunlari_hazirla()
    return geri_doldur(sonra, adet, dry_run, gercek_kaynaklar())
