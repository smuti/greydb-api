"""Bekleyen bacakları, maç bittikten sonra kendi veritabanımızdaki maç verisiyle otomatik sonuçlandırır (orkestrasyon; saf, bağımlılıklar dışarıdan).

Maç başladıktan 2 saat sonra bakılır (uzatma/ertelenme payı). Maç bulunamazsa, veri eksikse ya da market tanınmıyorsa YAZMAZ: rapor eder, admin elle işaretler.
"""
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.services.kupon_hesap import DESTEKLENMIYOR, VERI_YOK, bacak_sonucu

BEKLEME = timedelta(hours=2)


def sonuclandir(bekleyen: List[Dict[str, Any]], mac_bul: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
                mac_yukle: Callable[[Dict[str, Any]], Dict[str, Any]], simdi: datetime) -> Tuple[List[Tuple[int, int, str]], Dict[str, Any]]:
    guncellemeler: List[Tuple[int, int, str]] = []
    rapor: Dict[str, Any] = {"sonuclanan": 0, "erken": 0, "mac_yok": 0, "veri_yok": 0, "desteklenmiyor": 0, "ayrinti": []}

    def not_et(b, durum):
        rapor[durum] += 1
        rapor["ayrinti"].append({"kupon_id": b["kupon_id"], "bacak_id": b["id"], "durum": durum, "bahis": f"{b['bahis_turu']} / {b['secim']}"})

    for b in bekleyen:
        if simdi - b["mac_zamani"] < BEKLEME:
            rapor["erken"] += 1
            continue
        mac = mac_bul(b)
        if mac is None:
            not_et(b, "mac_yok")
            continue
        s = bacak_sonucu(b["bahis_turu"], b["secim"], mac_yukle(mac))
        if s == VERI_YOK:
            not_et(b, "veri_yok")
        elif s == DESTEKLENMIYOR:
            not_et(b, "desteklenmiyor")
        else:
            guncellemeler.append((b["kupon_id"], b["id"], s))
            rapor["sonuclanan"] += 1
    return guncellemeler, rapor
