"""Bitmiş maç detayı: veritabanı satırlarını sayfanın kullanacağı biçime çevirir (saf fonksiyonlar)."""
from typing import Any, Dict, List, Optional

_ATLA = {"match_id", "created_at", "id"}


def cift_istatistik(satir: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """home_xg / away_xg gibi sütun çiftlerini {xg: {home, away}} yapar; iki tarafı da boş olanı atar."""
    cikti: Dict[str, Dict[str, Any]] = {}
    for anahtar, deger in satir.items():
        if anahtar in _ATLA or not anahtar.startswith("home_"):
            continue
        ad = anahtar[len("home_"):]
        dep = satir.get(f"away_{ad}")
        if deger is None and dep is None:
            continue
        cikti[ad] = {"home": deger, "away": dep}
    return cikti


def _taraf(team_id: Any, ev_ic_id: Any, dep_ic_id: Any) -> Optional[str]:
    return "home" if team_id == ev_ic_id else "away" if team_id == dep_ic_id else None


def _skor_takimi(s: Dict[str, Any], taraf: Optional[str]) -> Optional[str]:
    """Golün hangi takımın skoruna yazıldığı: kendi kalesine golde rakibin."""
    if s.get("event_type") != "GOAL" or taraf is None:
        return None
    if s.get("is_own_goal"):
        return "away" if taraf == "home" else "home"
    return taraf


def olay_listesi(satirlar: List[Dict[str, Any]], ev_ic_id: Any, dep_ic_id: Any) -> List[Dict[str, Any]]:
    sirali = sorted(satirlar, key=lambda s: (s.get("minute") or 0, s.get("added_time") or 0, s.get("id") or 0))
    return [{
        "dakika": s.get("minute"), "ek_dakika": s.get("added_time"), "tur": s.get("event_type"),
        "takim": _taraf(s.get("team_id"), ev_ic_id, dep_ic_id),
        "skor_takimi": _skor_takimi(s, _taraf(s.get("team_id"), ev_ic_id, dep_ic_id)), "oyuncu": s.get("player_name"),
        "asist": s.get("assisted_by"), "giren": s.get("player_in"), "cikan": s.get("player_out"),
        "kendi_kalesine": bool(s.get("is_own_goal")), "penalti": bool(s.get("is_penalty")),
    } for s in sirali]


def kadro_listesi(satirlar: List[Dict[str, Any]], ev_ic_id: Any, dep_ic_id: Any, dizilis: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    kadro = {
        "home": {"dizilis": (dizilis or {}).get("home_formation"), "ilk11": [], "yedekler": []},
        "away": {"dizilis": (dizilis or {}).get("away_formation"), "ilk11": [], "yedekler": []},
    }
    for s in satirlar:
        taraf = _taraf(s.get("team_id"), ev_ic_id, dep_ic_id)
        if taraf is None:
            continue
        oyuncu = {"ad": s.get("player_name"), "numara": s.get("shirt_number"), "pozisyon": s.get("position"), "yas": s.get("age"), "puan": s.get("rating")}
        kadro[taraf]["ilk11" if s.get("is_starter") else "yedekler"].append(oyuncu)
    return kadro
