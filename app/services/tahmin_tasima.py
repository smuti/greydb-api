"""Eski greydb.predictions satırını yeni kupon girdisine çevirir (saf). Her tahmin tek bacaklı kombine kupon olur.

Dikkat: eski satırdaki match_fotmob_id adına rağmen İDDAA maç kimliğini tutuyor (admin formu iddaa listesinden dolduruyordu).
Tümünde show_on_homepage varsayılan olarak doğru geldiği için 'öne çıkan' sayılmaz.
"""
from typing import Any, Dict

from app.services.kupon_model import dogrula_olustur

_DURUM = {"active": "yayinda", "draft": "taslak", "archived": "arsiv"}


def tahmin_kuponu(t: Dict[str, Any]) -> Dict[str, Any]:
    sonuc = t.get("result") if t.get("result") in ("won", "lost", "void") else "pending"
    k = dogrula_olustur({
        "seviye": "acik", "durum": _DURUM.get(t.get("status"), "taslak"), "zamanlama": "pre", "one_cikan": False,
        "nedenler": [x for x in (t.get("reasons") or []) if isinstance(x, str)], "ana_risk": t.get("main_risk"), "yorum": t.get("content"),
        "sinyal": t.get("signal_strength"), "risk_seviyesi": t.get("risk_level"),
        "bacaklar": [{
            "iddaa_event_id": t.get("match_fotmob_id"), "fotmob_match_id": None, "ev": t["home_team"], "dep": t["away_team"], "lig": t.get("league"),
            "mac_zamani": t["match_date"].isoformat() + "Z", "bahis_turu": t["market_name"], "secim": t["pick"], "oran": float(t["odds"]), "sonuc": sonuc,
        }],
    })
    k["kaynak"] = f"tahmin:{t['id']}"
    k["olusturan_email"] = t["created_by_email"]
    k["olusturma"], k["guncelleme"] = t.get("created_at"), t.get("updated_at")
    return k
