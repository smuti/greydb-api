"""
Spor Toto haftalik yorum otomasyonu - veri toplama katmani.

Akis: Sportoto.gov.tr'nin kendi (WAF'siz) REST API'sinden haftalik 15 mac
cekilir -> her takim greydb'nin kendi veritabanindaki public.teams tablosuna
isim eslestirmesiyle baglanir (sonuc greydb.spor_toto_team_map'te kalici
olarak saklanir, bir dahaki hafta tekrar denenmez) -> iki katmanli veri
toplanir (teamAnalyzer'in 17 ligi icin zengin HTTP verisi, disinda kalan
ligler icin ayni Postgres'teki ham public.* tablolarindan basit agregatlar)
-> Skorjin'in yeni /api/v1/spor-toto-scenarios ucuna gonderilip 6 senaryo
uretilir.
"""
import re
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd

from app.services.db import query_to_df, execute_insert
from app.services.skorjin_match_analysis import (
    MATCHFORGE_BASE,
    _get as _mf_get,
    _pick,
    translate_team_name,
    _translate_names_in_place,
)

SPORTOTO_BASE = "https://webapi.sportoto.gov.tr"
SKORJIN_BASE = "https://skorjin-production.up.railway.app"
HTTP_TIMEOUT = 20.0
MS_PREDICT_TIMEOUT = 120.0   # matchforge günün ilk isteğinde modeli kurar (~6 sn); payı geniş tutuldu

_TR_MAP = str.maketrans({"İ": "i", "ı": "i", "ü": "u", "ö": "o", "ş": "s", "ç": "c", "ğ": "g"})


def _normalize(name: str) -> str:
    """Eşleştirme için ad: küçük harf, Türkçe harfler sadeleşir, tire boşluk olur, "st" = "saint" ("Paris St Germain" = "Paris Saint-Germain")."""
    temiz = re.sub(r"[-.]", " ", (name or "").lower().translate(_TR_MAP))
    return re.sub(r"\s+", " ", re.sub(r"\bst\b", "saint", temiz)).strip()


# Spor Toto'nun (Türkçe medya) kullandığı ad ile veritabanındaki ad farklıysa: normalize edilmiş Spor Toto adı -> normalize edilmiş veritabanı adı.
# Yalnız canlıda doğrulanmış olanlar yazılır ("Athletic Bilbao" = "Athletic Club", "Bayern Münih" = "Bayern München"); yeni eşleşmeyen takım çıkarsa buraya eklenir.
TAKIM_ADI_ESLEMELERI = {"athletic bilbao": "athletic club", "bayern munih": "bayern munchen"}


class SporTotoFetchError(Exception):
    """Kullanıcıya gösterilebilecek, beklenen hata."""


# ==================== 1. Sportoto.gov.tr ====================

def list_available_rounds(year: str = "2026/2027") -> List[Dict[str, Any]]:
    """Yayinlanmis Sportoto round'larini doner (en guncel en basta)."""
    with httpx.Client() as client:
        resp = client.get(
            f"{SPORTOTO_BASE}/api/GameRound",
            params={"year": year, "isPublished": "true"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    rounds = data.get("object", data) if isinstance(data, dict) else data
    if not isinstance(rounds, list):
        raise SporTotoFetchError("Sportoto round listesi beklenmeyen formatta döndü.")
    return sorted(rounds, key=lambda r: r.get("id", 0), reverse=True)


def fetch_round_matches(game_round_id: int) -> List[Dict[str, Any]]:
    """Bir round'un (hafta) 15 maçını Sportoto'dan çeker."""
    with httpx.Client() as client:
        resp = client.get(
            f"{SPORTOTO_BASE}/api/GameMatch/GetGameMatches/",
            params={"gameRoundId": game_round_id},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    raw_matches = data.get("object", data) if isinstance(data, dict) else data
    if not isinstance(raw_matches, list):
        raise SporTotoFetchError("Sportoto maç listesi beklenmeyen formatta döndü.")

    matches = []
    for i, item in enumerate(raw_matches, start=1):
        m = item.get("match") or {}
        home = m.get("homeTeam") or {}
        away = m.get("awayTeam") or {}
        matches.append({
            "mac_no": i,
            "ev_takim": home.get("name"),
            "deplasman_takim": away.get("name"),
            "sportoto_home_id": home.get("id"),
            "sportoto_away_id": away.get("id"),
            "lig_haftasi": (m.get("round") or {}).get("name") or (m.get("stage") or {}).get("name"),
            "mac_tarihi": m.get("date"),
        })
    return matches


# ==================== 2. Takım eşleştirme (kalıcı harita) ====================

def resolve_team(sportoto_team_id: int, sportoto_team_name: str) -> Optional[Dict[str, Any]]:
    """Sportoto takımını public.teams'e bağlar. Sonuç spor_toto_team_map'te
    kalıcı olarak saklanır — aynı takım için bir dahaki hafta tekrar
    isim-eşleştirmesi denenmez."""
    cached = query_to_df(
        "SELECT matchforge_team_id FROM greydb.spor_toto_team_map WHERE sportoto_team_id = %s",
        (sportoto_team_id,),
    )
    if not cached.empty:
        team_id = cached.iloc[0]["matchforge_team_id"]
        if not pd.isna(team_id):
            return {"team_id": int(team_id)}
        # Eşleşmemiş kayıt kalıcı "yok" sayılmaz: eşleştirme kuralları iyileşmiş olabilir, yeniden denenir (elle bağlama ise doludur ve korunur)

    all_teams = query_to_df("SELECT id, fotmob_team_id, name FROM public.teams")
    normalized_target = _normalize(sportoto_team_name)
    normalized_target = TAKIM_ADI_ESLEMELERI.get(normalized_target, normalized_target)

    match = None
    # 1. geçiş: tam eşleşme
    for _, row in all_teams.iterrows():
        if _normalize(row["name"]) == normalized_target:
            match = {"team_id": int(row["id"]), "fotmob_team_id": row["fotmob_team_id"], "name": row["name"]}
            break

    # 2. geçiş: sponsor önekli isimlerde DB'deki temiz ad, Sportoto adının
    # içinde geçer ("Arca Çorum FK" içinde "Çorum FK") — en uzun eşleşen
    # tercih edilir, yanlışlıkla kısa bir isme takılmayı önlemek için.
    if not match:
        best = None
        best_len = 0
        for _, row in all_teams.iterrows():
            normalized_db = _normalize(row["name"])
            if normalized_db and normalized_db in normalized_target and len(normalized_db) > best_len:
                best = {"team_id": int(row["id"]), "fotmob_team_id": row["fotmob_team_id"], "name": row["name"]}
                best_len = len(normalized_db)
        match = best

    execute_insert(
        """
        INSERT INTO greydb.spor_toto_team_map (sportoto_team_id, sportoto_team_name, matchforge_team_id)
        VALUES (:sportoto_team_id, :sportoto_team_name, :matchforge_team_id)
        ON CONFLICT (sportoto_team_id) DO UPDATE SET matchforge_team_id = COALESCE(greydb.spor_toto_team_map.matchforge_team_id, EXCLUDED.matchforge_team_id)
        """,
        {
            "sportoto_team_id": sportoto_team_id,
            "sportoto_team_name": sportoto_team_name,
            "matchforge_team_id": match["team_id"] if match else None,
        },
    )
    return match


def manual_link_team(sportoto_team_id: int, sportoto_team_name: str, matchforge_team_id: int) -> None:
    """Admin'in arama kutusundan elle onayladığı eşleşmeyi kaydeder/günceller."""
    execute_insert(
        """
        INSERT INTO greydb.spor_toto_team_map (sportoto_team_id, sportoto_team_name, matchforge_team_id)
        VALUES (:sportoto_team_id, :sportoto_team_name, :matchforge_team_id)
        ON CONFLICT (sportoto_team_id) DO UPDATE SET matchforge_team_id = EXCLUDED.matchforge_team_id
        """,
        {
            "sportoto_team_id": sportoto_team_id,
            "sportoto_team_name": sportoto_team_name,
            "matchforge_team_id": matchforge_team_id,
        },
    )


def search_teams(query: str, limit: int = 15) -> List[Dict[str, Any]]:
    """Admin'in elle eşleştirme için kullandığı serbest metin takım araması."""
    df = query_to_df(
        "SELECT id, fotmob_team_id, name, league_id FROM public.teams WHERE name ILIKE %s ORDER BY name LIMIT %s",
        (f"%{query}%", limit),
    )
    return [
        {"team_id": int(r["id"]), "fotmob_team_id": r["fotmob_team_id"], "name": r["name"], "league_id": r["league_id"]}
        for _, r in df.iterrows()
    ]


# ==================== 2b. Form özeti (LLM'in ev/deplasman ve "son maç" yorumunu yanlış yapmaması için kod hesaplar) ====================

_SONUC = {"W": "galibiyet", "D": "beraberlik", "L": "yenilgi"}


def _count_text(matches: List[Dict[str, Any]]) -> Optional[str]:
    parts = [f"{sum(1 for m in matches if m.get('sonuc') == k)} {v}" for k, v in _SONUC.items() if any(m.get("sonuc") == k for m in matches)]
    return ", ".join(parts) or None


def _gol_ayir(skor: Any) -> Optional[tuple]:
    """"3-2" -> (3, 2): takımın kendi golü önce yazılır; (attığı, yediği). Okunamazsa None."""
    m = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", str(skor or ""))
    return (int(m.group(1)), int(m.group(2))) if m else None


def _gol_notu(skor: Any) -> str:
    g = _gol_ayir(skor)
    return f" ({g[0]} gol attı, {g[1]} gol yedi)" if g else ""


def summarize_form(maclar: Optional[List[Dict[str, Any]]], takim: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """son_5_mac_formu.maclar (EN YENİ BAŞTA; skor = takımın kendi golü önce; saha H=ev, A=deplasman) -> hazır cümleler + sayılar.
    Takım/rakip adları çekimsiz yazılır ("Alanyaspor 1-2 yenilgi"); LLM kendi ev/deplasman sayımı yapmak yerine bunları alıntılar.
    `takim` verilirse her cümle o takımın adıyla başlar ("Athletic Club: Son 5 lig maçı: ...") ve skorlar "(3 gol attı, 2 gol yedi)" diye açık yazılır:
    LLM bir takımın maçını rakibine ya da öbür takıma atfetmesin, skorun yönünü karıştırmasın."""
    if not maclar:
        return None
    onek = f"{takim}: " if takim else ""
    maclar = list(maclar)
    home = [m for m in maclar if m.get("saha") == "H"]
    away = [m for m in maclar if m.get("saha") == "A"]
    out: Dict[str, Any] = {"son_5": _count_text(maclar), "evde": _count_text(home), "deplasman": _count_text(away)}
    cumleler = []
    if out["son_5"]:
        cumleler.append(f"{onek}Son {len(maclar)} lig maçı: {out['son_5']}.")
    if home and out["evde"]:
        cumleler.append(f"{onek}Evinde oynadığı son {len(home)} maç: {out['evde']}." if len(home) > 1 else f"{onek}Evinde oynadığı son maç: {out['evde']}.")
    if away and out["deplasman"]:
        cumleler.append(f"{onek}Deplasmanda oynadığı son {len(away)} maç: {out['deplasman']}." if len(away) > 1 else f"{onek}Deplasmanda oynadığı son maç: {out['deplasman']}.")
    last = maclar[0]
    out["son_mac"] = {"rakip": last.get("rakip"), "saha": "ev" if last.get("saha") == "H" else "deplasman", "skor": last.get("skor"), "sonuc": _SONUC.get(last.get("sonuc"))}
    if _gol_ayir(last.get("skor")):
        out["son_mac"]["atilan"], out["son_mac"]["yenilen"] = _gol_ayir(last.get("skor"))
    if out["son_mac"]["sonuc"]:
        cumleler.append(f"{onek}Son maçı: {last.get('rakip')} ({out['son_mac']['saha']}) {last.get('skor')} {out['son_mac']['sonuc']}{_gol_notu(last.get('skor'))}.")
    for key, group, label in (("evde_son_mac", home, "Evinde oynadığı en son maç"), ("deplasman_son_mac", away, "Deplasmanda oynadığı en son maç")):
        if group:
            m = group[0]
            out[key] = {"rakip": m.get("rakip"), "skor": m.get("skor"), "sonuc": _SONUC.get(m.get("sonuc"))}
            if len(group) > 1 and m is not last and out[key]["sonuc"]:
                cumleler.append(f"{onek}{label}: {m.get('rakip')} {m.get('skor')} {out[key]['sonuc']}{_gol_notu(m.get('skor'))}.")
    streak = 1
    while streak < len(maclar) and maclar[streak].get("sonuc") == maclar[0].get("sonuc"):
        streak += 1
    if streak >= 2 and _SONUC.get(maclar[0].get("sonuc")):
        cumleler.append(f"{onek}Üst üste {streak} {_SONUC[maclar[0].get('sonuc')]}.")
    out["cumleler"] = cumleler
    return out


# ==================== 3. İki katmanlı veri toplama ====================

def _fetch_rich_team_data(team_id: int, team_name: str) -> Dict[str, Any]:
    """teamAnalyzer'ın 17 ligini kapsayan zengin veri. Fuat'ın GERÇEK konuşma tarzı
    soyut metriklerden (xG, possession) çok somut maç sonuçlarına dayanıyor
    ("son 5 maçta 3 galibiyet aldı", "4-0 kazandılar") — bu yüzden son_5_mac_formu
    (W/D/L + rakip + skor) EN ÖNEMLİ alan, diğerleri destekleyici."""
    data: Dict[str, Any] = {}
    with httpx.Client() as client:
        form = _mf_get(client, f"{MATCHFORGE_BASE}/api/teams/{team_id}/form?last_n=5")
        stats = _mf_get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{team_id}/stats?last_n=5")
        team_type = _mf_get(client, f"{MATCHFORGE_BASE}/api/leagues/teams/{team_id}/type?last_n=5")
        corners = _mf_get(client, f"{MATCHFORGE_BASE}/api/matches/corners/team-profile/{team_name}?n_matches=10")
        cards = _mf_get(client, f"{MATCHFORGE_BASE}/api/matches/cards/team-profile/{team_name}?n_matches=10")
    if form:
        data["son_5_mac_formu"] = {
            "maclar": [
                {"rakip": m.get("opponent"), "saha": m.get("venue"), "skor": m.get("score"), "sonuc": m.get("result")}
                for m in (form.get("form") or [])
            ],
            "ozet": _pick(form.get("summary") or {}, "form_string", "points", "goals_for", "goals_against"),
        }
    if stats:
        data["sezon_istatistikleri"] = _pick(stats, "stats", "totals")
    if team_type:
        data["takim_tipi"] = _pick(team_type, "team_type")
    if corners:
        data["korner_profili"] = _pick(corners, "overall", "thresholds")
    if cards:
        data["kart_profili"] = _pick(cards, "overall", "thresholds")
    return data


def _fetch_fallback_team_data(team_id: int) -> Dict[str, Any]:
    """teamAnalyzer kapsamı dışındaki ligler için ham public.* tablolarından
    basit agregatlar — hâlâ gerçek veri, sadece daha az zengin."""
    data: Dict[str, Any] = {}

    df = query_to_df(
        "SELECT last5_avg_shots, last5_avg_sot, last5_avg_xg, last5_avg_goals, last5_avg_conceded_shots "
        "FROM public.team_recent_form WHERE team_id = %s",
        (team_id,),
    )
    if not df.empty:
        r = df.iloc[0]
        data["son_5_mac_ortalamalari"] = {
            "sut": r["last5_avg_shots"], "isabetli_sut": r["last5_avg_sot"],
            "xg": r["last5_avg_xg"], "gol": r["last5_avg_goals"], "yedigi_sut": r["last5_avg_conceded_shots"],
        }

    df = query_to_df(
        "SELECT season_matches, season_avg_goals, season_avg_conceded_goals, season_avg_xg "
        "FROM public.team_stats_season WHERE team_id = %s",
        (team_id,),
    )
    if not df.empty:
        r = df.iloc[0]
        data["sezon_ortalamalari"] = {
            "mac_sayisi": r["season_matches"], "gol": r["season_avg_goals"],
            "yedigi_gol": r["season_avg_conceded_goals"], "xg": r["season_avg_xg"],
        }

    df = query_to_df(
        "SELECT position, played, wins, draws, losses, points FROM public.season_standings "
        "WHERE team_id = %s ORDER BY last_updated DESC LIMIT 1",
        (team_id,),
    )
    if not df.empty:
        r = df.iloc[0]
        data["lig_durumu"] = {
            "sira": r["position"], "oynanan": r["played"], "galibiyet": r["wins"],
            "beraberlik": r["draws"], "maglubiyet": r["losses"], "puan": r["points"],
        }

    return data


def _clean_for_json(obj: Any) -> Any:
    """pandas/numpy tiplerini (int64, NaN...) JSON-uyumlu düz Python tiplerine çevirir."""
    import math

    if isinstance(obj, dict):
        return {k: _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_for_json(v) for v in obj]
    if isinstance(obj, float):
        return None if math.isnan(obj) else obj
    if hasattr(obj, "item"):  # numpy skaler (int64, float64, vb.)
        val = obj.item()
        return None if isinstance(val, float) and math.isnan(val) else val
    return obj


# teamAnalyzer'ın kapsadığı 17 ligin ID'leri — skorjin_match_analysis.py'deki
# LEAGUE_NAME_TO_ID map'in DEĞERLERİ. public.leagues.id ile AYNI ID uzayını
# paylaştığı doğrulandı (ör. "Super Lig" hem public.leagues'te hem teamAnalyzer'da id=4).
_RICH_TIER_LEAGUE_IDS = {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 17, 20, 21, 24, 25, 26, 27}


def get_team_league(team_id: int) -> Dict[str, Any]:
    """Takımın gerçek ligini public.teams + public.leagues'ten çeker — Sportoto'nun
    verdiği 'round' adı ("8. Hafta") GERÇEK lig adı DEĞİL, bu yüzden lig ancak
    takım eşleştikten sonra buradan öğrenilebiliyor."""
    df = query_to_df(
        "SELECT l.id, l.name FROM public.teams t JOIN public.leagues l ON t.league_id = l.id WHERE t.id = %s",
        (team_id,),
    )
    if df.empty:
        return {"league_id": None, "league_name": None}
    return {"league_id": int(df.iloc[0]["id"]), "league_name": df.iloc[0]["name"]}


def build_match_data(home: Dict[str, Any], away: Dict[str, Any], sportoto_round_label: Optional[str]) -> Dict[str, Any]:
    """home/away: {"team_id", "name"} — resolve_team'den gelen sonuç.
    sportoto_round_label sadece gerçek lig bulunamazsa yedek görüntüleme etiketi olarak kullanılır."""
    home_league = get_team_league(home["team_id"]) if home.get("team_id") else {}
    away_league = get_team_league(away["team_id"]) if away.get("team_id") else {}
    league_id = home_league.get("league_id") or away_league.get("league_id")
    league_name = home_league.get("league_name") or away_league.get("league_name") or sportoto_round_label

    data: Dict[str, Any] = {
        "ev_sahibi": translate_team_name(home.get("name")),
        "deplasman": translate_team_name(away.get("name")),
        "lig": league_name,
    }

    use_rich = league_id in _RICH_TIER_LEAGUE_IDS

    for side, team in (("ev_sahibi_verisi", home), ("deplasman_verisi", away)):
        team_id = team.get("team_id")
        if not team_id:
            continue
        team_data = {}
        if use_rich:
            try:
                team_data = _fetch_rich_team_data(team_id, team.get("name", ""))
            except Exception:
                team_data = {}
        if not team_data:
            team_data = _fetch_fallback_team_data(team_id)
        if team_data:
            maclar = (team_data.get("son_5_mac_formu") or {}).get("maclar")
            for mac in maclar or []:        # skorun yönü açık olsun: LLM "kim kaç gol yedi"yi skor sırasından çıkarmasın
                g = _gol_ayir(mac.get("skor"))
                if g:
                    mac["atilan"], mac["yenilen"] = g
            ozet = summarize_form(maclar, translate_team_name(team.get("name")))
            if ozet:
                team_data["form_ozeti"] = ozet
            data[side] = team_data

    _translate_names_in_place(data)
    return _clean_for_json(data)


# ==================== 3b. İstatistiksel model (matchforge /api/ms/predict) ====================

def fetch_model_prediction(home: Dict[str, Any], away: Dict[str, Any], mac_tarihi: Any, league_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Maç için model olasılıkları + karar (tek/çift/üçlü) + LLM'e verilecek 'facts'. Herhangi bir sorunda None döner:
    model olmadan eski akış (yalnızca LLM) çalışmaya devam eder, üretim ASLA bunun yüzünden düşmez."""
    if not home.get("team_id") or not away.get("team_id"):
        return None
    # Tarih eksik/bozuksa modeli ATLAMAK yerine bugünün tarihiyle çalıştır (Spor Toto maçları üretim anında gelecek maçtır; bugün <= maç günü,
    # yani sızıntı olmaz). Sessiz atlama, tarih kaydı hatasını aylarca gizlerdi.
    try:
        missing = mac_tarihi is None or (isinstance(mac_tarihi, str) and not mac_tarihi.strip()) or pd.isna(mac_tarihi)
        date = pd.Timestamp(mac_tarihi).isoformat() if not missing else None
    except (ValueError, TypeError):
        date = None
    if date is None:
        date = pd.Timestamp.utcnow().tz_localize(None).normalize().isoformat()
        print(f"[spor_toto] maç tarihi eksik/geçersiz ({mac_tarihi!r}); model bugünün tarihiyle çalıştırıldı: {date}")
    payload = {"matches": [{
        "home_team_id": int(home["team_id"]), "away_team_id": int(away["team_id"]), "league_id": league_id,
        "match_date": date, "home_name": translate_team_name(home.get("name")), "away_name": translate_team_name(away.get("name")),
    }]}
    try:
        with httpx.Client() as client:
            resp = client.post(f"{MATCHFORGE_BASE}/api/ms/predict", json=payload, timeout=MS_PREDICT_TIMEOUT)
        if resp.status_code != 200:
            print(f"[spor_toto] model çağrısı {resp.status_code}: {str(resp.text)[:200]}")
            return None
        results = resp.json().get("results") or []
        return results[0] if results else None
    except Exception as e:  # noqa: BLE001 — model opsiyonel, hata üretimi durdurmamalı
        print(f"[spor_toto] model çağrısı başarısız: {e}")
        return None


def apply_model_choice(scenarios: List[Dict[str, Any]], model: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Model varsa 'önerilen' kartı model belirler (Skorjin eski sürümde kalsa bile). Girdiyi değiştirmez."""
    if not model:
        return scenarios
    pick = (model.get("decision") or {}).get("senaryo")
    if not pick:
        return scenarios
    return [{**s, "onerilen": s["senaryo"] == pick} for s in scenarios]


# ==================== 4. Skorjin çağrısı ====================

def generate_scenarios(match_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    with httpx.Client() as client:
        try:
            resp = client.post(
                f"{SKORJIN_BASE}/api/v1/spor-toto-scenarios",
                json={"match_data": match_data},
                timeout=90.0,
            )
        except httpx.HTTPError as e:
            raise SporTotoFetchError(f"Skorjin spor-toto-scenarios ucuna ulaşılamadı: {e}")

    if resp.status_code != 200:
        raise SporTotoFetchError(f"Skorjin'den hata: {resp.status_code} {resp.text[:300]}")

    body = resp.json()
    scenarios = body.get("scenarios")
    if not scenarios:
        raise SporTotoFetchError("Skorjin boş senaryo listesi döndürdü.")
    return scenarios
