"""
Skorjin "henüz başlamamış maç" analizi için veri toplama ve LLM çağrısı.

apps/web'in MatchDetail.tsx'teki "Form & Karşılaşma Geçmişi" bölümünün
kullandığı AYNI veri kaynaklarını (matchforge + team-analyzer, ikisi de
aynı Railway servisinde) sunucu tarafında tekrar toplar — frontend'in
gönderdiği keyfi metne değil, backend'in kendi çektiği veriye güvenmek
için (aynı maça her zaman aynı girdi -> cache tutarlılığı).
"""
import re
import urllib.parse
from typing import Any, Dict, Optional

import httpx

MATCHFORGE_BASE = "https://web-production-0ce73.up.railway.app"
SKORJIN_BASE = "https://skorjin-production.up.railway.app"

HTTP_TIMEOUT = 20.0

# apps/web/src/lib/teamAnalyzerLeagues.ts ile birebir aynı tutulmalı.
LEAGUE_NAME_TO_ID = {
    "süper lig": 4, "super lig": 4, "türkiye süper lig": 4, "turkiye super lig": 4,
    "premier league": 5, "premier lig": 5, "ingiltere premier lig": 5,
    "laliga": 6, "la liga": 6, "ispanya la liga": 6, "i̇spanya la liga": 6, "spain la liga": 6,
    "bundesliga": 7, "almanya bundesliga": 7,
    "serie a": 8, "italya serie a": 8,
    "ligue 1": 9, "fransa ligue 1": 9,
    "champions league": 10, "şampiyonlar ligi": 10, "sampiyonlar ligi": 10, "uefa champions league": 10,
    "europa league": 11, "avrupa ligi": 11, "uefa europa league": 11,
    "eredivisie": 12, "hollanda eredivisie": 12,
    "scottish premiership": 13, "iskoçya premier lig": 13,
    "primera división": 17, "primera division": 17, "arjantin ligi": 17,
    "primeira liga": 20, "portekiz ligi": 20,
    "first division a": 21, "jupiler pro league": 21, "belçika ligi": 21,
    "uefa nations league a": 24, "nations league a": 24,
    "uefa nations league b": 25, "nations league b": 25,
    "uefa nations league c": 26, "nations league c": 26,
    "uefa nations league d": 27, "nations league d": 27,
}

_GROUP_SUFFIX = re.compile(r"\s*grp\.?\s*\d+\s*$", re.IGNORECASE)
_TR_MAP = str.maketrans({"İ": "i", "ı": "i", "ü": "u", "ö": "o", "ş": "s", "ç": "c", "ğ": "g"})


def _normalize(name: str) -> str:
    return (name or "").lower().translate(_TR_MAP).strip()


# apps/web/src/lib/teamNames.ts ile birebir aynı tutulmalı. FotMob milli takım
# adları İngilizce geliyor — LLM'e gönderilen JSON'da hem üst seviye hem de
# team-analyzer'ın iç içe geçmiş "team_name" alanlarında ham İngilizce kalırsa
# model bazen "Croatia" bazen "Hırvatistan" yazıp tutarsız oluyor (gözlemlendi).
# Bu yüzden JSON'a HİÇ İngilizce isim girmesin diye kaynak noktasında çeviriyoruz.
NATIONAL_TEAM_NAMES_TR = {
    "Albania": "Arnavutluk", "Andorra": "Andorra", "Armenia": "Ermenistan",
    "Austria": "Avusturya", "Azerbaijan": "Azerbaycan", "Belarus": "Belarus",
    "Belgium": "Belçika", "Bosnia and Herzegovina": "Bosna Hersek",
    "Bulgaria": "Bulgaristan", "Croatia": "Hırvatistan", "Cyprus": "Kıbrıs",
    "Czechia": "Çekya", "Czech Republic": "Çekya", "Denmark": "Danimarka",
    "England": "İngiltere", "Estonia": "Estonya", "Faroe Islands": "Faroe Adaları",
    "Finland": "Finlandiya", "France": "Fransa", "Georgia": "Gürcistan",
    "Germany": "Almanya", "Gibraltar": "Cebelitarık", "Greece": "Yunanistan",
    "Hungary": "Macaristan", "Iceland": "İzlanda", "Israel": "İsrail",
    "Italy": "İtalya", "Kazakhstan": "Kazakistan", "Kosovo": "Kosova",
    "Latvia": "Letonya", "Liechtenstein": "Lihtenştayn", "Lithuania": "Litvanya",
    "Luxembourg": "Lüksemburg", "Malta": "Malta", "Moldova": "Moldova",
    "Montenegro": "Karadağ", "Netherlands": "Hollanda", "North Macedonia": "Kuzey Makedonya",
    "Northern Ireland": "Kuzey İrlanda", "Norway": "Norveç", "Poland": "Polonya",
    "Portugal": "Portekiz", "Republic of Ireland": "İrlanda", "Ireland": "İrlanda",
    "Romania": "Romanya", "Russia": "Rusya", "San Marino": "San Marino",
    "Scotland": "İskoçya", "Serbia": "Sırbistan", "Slovakia": "Slovakya",
    "Slovenia": "Slovenya", "Spain": "İspanya", "Sweden": "İsveç",
    "Switzerland": "İsviçre", "Turkiye": "Türkiye", "Turkey": "Türkiye",
    "Ukraine": "Ukrayna", "Wales": "Galler",
}


def translate_team_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return name
    return NATIONAL_TEAM_NAMES_TR.get(name.strip(), name)


_TEAM_NAME_KEYS = {"team_name", "home_team_name", "away_team_name", "opponent"}


def _translate_names_in_place(obj: Any) -> None:
    """match_data içindeki her 'team_name' türevi alanı (nested dict/list fark
    etmeksizin) Türkçeleştirir — LLM'in gördüğü HER yerde tutarlı isim olsun."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in _TEAM_NAME_KEYS and isinstance(value, str):
                obj[key] = translate_team_name(value)
            else:
                _translate_names_in_place(value)
    elif isinstance(obj, list):
        for item in obj:
            _translate_names_in_place(item)


def get_league_id_by_name(name: str) -> Optional[int]:
    normalized = _normalize(name)
    if normalized in LEAGUE_NAME_TO_ID:
        return LEAGUE_NAME_TO_ID[normalized]
    without_group = _GROUP_SUFFIX.sub("", normalized).strip()
    return LEAGUE_NAME_TO_ID.get(without_group)


class MatchAnalysisError(Exception):
    """Kullanıcıya gösterilebilecek, beklenen hata (400'e çevrilmesi beklenir)."""


def _get(client: httpx.Client, url: str) -> Optional[dict]:
    try:
        resp = client.get(url, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            return None
        return resp.json()
    except httpx.HTTPError:
        return None


def _pick(d: Optional[dict], *keys: str) -> Optional[dict]:
    if not d:
        return None
    return {k: d.get(k) for k in keys if k in d}


def build_match_analysis_data(fotmob_match_id: int) -> Dict[str, Any]:
    """Maç + takım verilerini toplar.

    Maç bulunamazsa veya 'upcoming' değilse MatchAnalysisError fırlatır
    (çağıran taraf 400'e çevirmeli). Lig desteklenmiyorsa veya takımlar
    eşleşmezse hata fırlatmaz — sadece temel maç bilgisiyle döner, prompt
    zaten "veri yoksa uydurma" kuralını içeriyor.
    """
    with httpx.Client() as client:
        live = _get(client, f"{MATCHFORGE_BASE}/api/matches/live?match_id={fotmob_match_id}")
        if not live or live.get("error"):
            raise MatchAnalysisError("Maç verisi bulunamadı.")

        match = live.get("match") or {}
        if match.get("status") != "upcoming":
            raise MatchAnalysisError("Bu analiz sadece henüz başlamamış maçlar için üretilir.")

        home_team_name = match.get("home_team")
        away_team_name = match.get("away_team")
        league_name = match.get("league")

        # data içinde SADECE Türkçe isim kullanılır — team_list eşleştirmesi ve
        # korner/kart URL'leri için ham home_team_name/away_team_name (İngilizce
        # FotMob adı) ayrı tutuluyor, aşağıda hiç değiştirilmiyor.
        data: Dict[str, Any] = {
            "ev_sahibi": translate_team_name(home_team_name),
            "deplasman": translate_team_name(away_team_name),
            "lig": league_name,
            "mac_tarihi": match.get("kickoff"),
        }

        league_id = get_league_id_by_name(league_name) if league_name else None
        if not league_id or not home_team_name or not away_team_name:
            return data

        teams_resp = _get(client, f"{MATCHFORGE_BASE}/api/leagues/{league_id}/teams")
        team_list = (teams_resp or {}).get("teams") or []

        def find_team(name: str) -> Optional[dict]:
            normalized = _normalize(name)
            for t in team_list:
                if _normalize(t.get("name", "")) == normalized:
                    return t
                if t.get("short_name") and _normalize(t["short_name"]) == normalized:
                    return t
            return None

        home_team = find_team(home_team_name)
        away_team = find_team(away_team_name)
        if not home_team or not away_team:
            return data

        home_id, away_id = home_team["id"], away_team["id"]
        home_q = urllib.parse.quote(home_team_name, safe="")
        away_q = urllib.parse.quote(away_team_name, safe="")

        home_stats = _get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{home_id}/stats?last_n=5")
        away_stats = _get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{away_id}/stats?last_n=5")
        home_type = _get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{home_id}/type?last_n=5")
        away_type = _get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{away_id}/type?last_n=5")
        home_goal_timing = _get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{home_id}/goal-timing?last_n=5")
        away_goal_timing = _get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{away_id}/goal-timing?last_n=5")
        h2h = _get(client, f"{MATCHFORGE_BASE}/api/leagues/h2h?team1_id={home_id}&team2_id={away_id}&last_n=5&home_team_id={home_id}")
        # Korner/kart profili şu an sadece bazı liglerde dolu (ör. backfill yapılmış
        # Uluslar Ligi) — eksikse sessizce atlanır, uydurulmaz.
        home_corners = _get(client, f"{MATCHFORGE_BASE}/api/matches/corners/team-profile/{home_q}?n_matches=10")
        away_corners = _get(client, f"{MATCHFORGE_BASE}/api/matches/corners/team-profile/{away_q}?n_matches=10")
        home_cards = _get(client, f"{MATCHFORGE_BASE}/api/matches/cards/team-profile/{home_q}?n_matches=10")
        away_cards = _get(client, f"{MATCHFORGE_BASE}/api/matches/cards/team-profile/{away_q}?n_matches=10")

        if home_stats and away_stats:
            data["son_5_mac_istatistikleri"] = {
                "ev_sahibi": _pick(home_stats, "stats", "totals", "match_count"),
                "deplasman": _pick(away_stats, "stats", "totals", "match_count"),
            }
        if home_type and away_type:
            data["oyun_tarzi"] = {
                "ev_sahibi": _pick(home_type, "team_type", "z_scores"),
                "deplasman": _pick(away_type, "team_type", "z_scores"),
            }
        if home_goal_timing and away_goal_timing:
            data["gol_zamanlamasi_son_5"] = {
                "ev_sahibi": _pick(home_goal_timing, "intervals", "totals"),
                "deplasman": _pick(away_goal_timing, "intervals", "totals"),
            }
        if h2h and h2h.get("matches"):
            data["karsilikli_gecmis"] = _pick(h2h, "match_count", "matches")
        if home_corners and away_corners:
            data["korner_profili_son_10"] = {
                "ev_sahibi": _pick(home_corners, "overall", "thresholds"),
                "deplasman": _pick(away_corners, "overall", "thresholds"),
            }
        if home_cards and away_cards:
            data["kart_profili_son_10"] = {
                "ev_sahibi": _pick(home_cards, "overall", "thresholds"),
                "deplasman": _pick(away_cards, "overall", "thresholds"),
            }

        # H2H maç listesindeki home_team_name/away_team_name gibi alanlar hâlâ
        # İngilizce — burada tek seferde Türkçeleştiriliyor.
        _translate_names_in_place(data)
        return data


def generate_analysis(match_data: Dict[str, Any]) -> str:
    """skorjin servisinin /api/v1/match-analysis ucunu çağırır, ham markdown döner."""
    with httpx.Client() as client:
        try:
            resp = client.post(
                f"{SKORJIN_BASE}/api/v1/match-analysis",
                json={"match_data": match_data},
                timeout=90.0,  # LLM çağrısı — ilk istek yavaş olabilir
            )
        except httpx.HTTPError as e:
            raise MatchAnalysisError(f"Skorjin analiz servisine ulaşılamadı: {e}")

    if resp.status_code != 200:
        raise MatchAnalysisError(f"Skorjin analiz servisinden hata: {resp.status_code}")

    body = resp.json()
    analysis = body.get("analysis")
    if not analysis:
        raise MatchAnalysisError("Skorjin boş analiz döndürdü.")
    return analysis
