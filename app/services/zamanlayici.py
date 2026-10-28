"""Uygulama içi zamanlayıcı: Skorjin ön maç işi ve iddaa eşleştirme (GitHub Actions cron'u saatlerce gecikiyor/atlıyordu).

ZAMANLAYICI=1 ortam değişkeniyle açılır (varsayılan kapalı; testler ve yerel çalışma etkilenmez). Saatler UTC.
Ön maç işi zaten veritabanı kilidiyle (pg advisory lock) tek koşu; birden fazla kopya çalışsa bile ikinci koşu atlanır.
"""
import logging
import os
from typing import Callable, List, Optional, Tuple

log = logging.getLogger("zamanlayici")


def _prematch(kapsam: str) -> Callable[[], None]:
    def is_():
        from app.routers.skorjin_prematch import zamanli_prematch
        log.info("prematch %s: %s", kapsam, zamanli_prematch(kapsam))
    return is_


def _eslestir() -> None:
    from app.routers.iddaa import eslestir_calistir
    r = eslestir_calistir(dry_run=False)
    log.info("iddaa eşleştir: eşleşen %s / %s", r.get("eslesen"), r.get("toplam_mac"))


def isler() -> List[Tuple[str, dict, Callable[[], None]]]:
    """(kimlik, cron alanları, iş). GitHub iş akışlarının eski planıyla aynı saatler."""
    return [
        ("prematch-gunluk", dict(hour="3", minute="30"), _prematch("gunluk")),
        ("prematch-oran", dict(hour="5-22", minute="0,30"), _prematch("oran")),
        ("iddaa-eslestir", dict(hour="*/4", minute="20"), _eslestir),
    ]


def baslat(calistir: bool = True):
    if os.environ.get("ZAMANLAYICI") != "1":
        return None
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    s = BackgroundScheduler(timezone="UTC")
    for ad, alanlar, is_ in isler():
        s.add_job(is_, CronTrigger(timezone="UTC", **alanlar), id=ad, max_instances=1, coalesce=True, misfire_grace_time=600)
    if calistir:
        s.start()
        log.info("zamanlayıcı başladı: %s", ", ".join(j.id for j in s.get_jobs()))
    return s


def durdur(s) -> None:
    if s is not None and s.running:
        s.shutdown(wait=False)
