"""Skorjin kuponlarının bacaklarını maç bitince sonuçlandırır (won / lost / void). Motor sitenin normal kuponlarındakiyle aynı: kupon_hesap.bacak_sonucu.

Maç başladıktan 2 saat sonra bakılır. Maç bulunamazsa, veri eksikse ya da pazar tanınmıyorsa bacağa HİÇBİR ŞEY yazılmaz (beklemede kalır) ve rapora konur.
Yalnız bacaklar yazılır; kuponun başlığı, durumu ve yayın zamanı değişmez. Yayında ve arşivdeki kuponlar taranır (arşiv de geçmiş karnesi için).
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.services.kupon_hesap import DESTEKLENMIYOR, VERI_YOK, bacak_sonucu

BEKLEME = timedelta(hours=2)


def _utc(zaman: str) -> datetime:
    z = datetime.fromisoformat(str(zaman).replace("Z", "+00:00"))
    return z.replace(tzinfo=None) if z.tzinfo is None else z.astimezone(timezone.utc).replace(tzinfo=None)


def sonuclandir(kuponlar: List[Dict[str, Any]], mac_bul: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
                mac_yukle: Callable[[Dict[str, Any]], Dict[str, Any]], simdi: datetime) -> Tuple[Dict[int, List[Dict[str, Any]]], Dict[str, Any]]:
    """Dönen: {kupon_id: güncel bacak listesi} (yalnız değişenler) ve rapor."""
    degisen: Dict[int, List[Dict[str, Any]]] = {}
    rapor: Dict[str, Any] = {"sonuclanan": 0, "erken": 0, "mac_yok": 0, "veri_yok": 0, "desteklenmiyor": 0, "ayrinti": []}
    for k in kuponlar:
        bacaklar = [dict(b) for b in k["bacaklar"]]
        degisti = False
        for b in bacaklar:
            if b.get("sonuc"):
                continue
            zaman = _utc(b["macZamani"])
            if simdi - zaman < BEKLEME:
                rapor["erken"] += 1
                continue
            ad = f"{b['ev']} - {b['dep']} · {b['marketAd']} / {b['secim']}"
            mac = mac_bul({"ev": b["ev"], "dep": b["dep"], "mac_zamani": zaman, "fotmob_match_id": None, "iddaa_event_id": b.get("macId")})
            if mac is None:
                rapor["mac_yok"] += 1
                rapor["ayrinti"].append({"kupon_id": k["id"], "durum": "mac_yok", "bacak": ad})
                continue
            s = bacak_sonucu(b["marketAd"], b["secim"], mac_yukle(mac))
            if s in (VERI_YOK, DESTEKLENMIYOR):
                rapor["veri_yok" if s == VERI_YOK else "desteklenmiyor"] += 1
                rapor["ayrinti"].append({"kupon_id": k["id"], "durum": s, "bacak": ad})
                continue
            b["sonuc"], b["sonucKaynak"] = s, "otomatik"
            rapor["sonuclanan"] += 1
            rapor["ayrinti"].append({"kupon_id": k["id"], "durum": s, "bacak": ad})
            degisti = True
        if degisti:
            degisen[int(k["id"])] = bacaklar
    return degisen, rapor


def calistir(dry_run: bool = True) -> Dict[str, Any]:
    from app.routers import skorjin_kuponlari as K
    from app.routers.kupon_sonuclari import mac_bul_yedekli, mac_yukle_yedekli
    kuponlar = [k for k in K.db_liste(False) if k["durum"] in ("yayinda", "arsiv")]
    degisen, rapor = sonuclandir(kuponlar, mac_bul_yedekli, mac_yukle_yedekli, datetime.now(timezone.utc).replace(tzinfo=None))
    if not dry_run:
        for kid, bacaklar in degisen.items():
            K.db_bacaklar_yaz(kid, bacaklar)
    return {"dry_run": dry_run, **rapor}
