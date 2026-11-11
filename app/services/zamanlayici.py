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


def _tercih(is_adi: str) -> Callable[[], None]:
    def is_():
        from app.services import tercih_izleme as t
        try:
            log.info("tercih %s: %s", is_adi, t.calistir(is_adi, dry_run=False))
        except Exception:  # noqa: BLE001 — bir koşunun hatası zamanlayıcı iş parçacığını ya da diğer işleri durdurmasın
            log.exception("tercih %s koşusu hata verdi", is_adi)
    return is_


def _canli_korner_sonuc() -> None:
    from app.services import canli_korner_izleme as c
    try:
        log.info("canlı korner sonuç: %s", c.calistir(dry_run=False))
    except Exception:  # noqa: BLE001 — bir koşunun hatası zamanlayıcı iş parçacığını ya da diğer işleri durdurmasın
        log.exception("canlı korner sonuç koşusu hata verdi")


def _oyuncu_json_mat() -> None:
    from app.services import stat_yenile as s
    try:
        log.info("oyuncu_json_mat yenileme: %s", s.calistir(False))
    except Exception:  # noqa: BLE001 — yenileme hatası zamanlayıcıyı ve diğer işleri durdurmasın
        log.exception("oyuncu_json_mat yenileme hata verdi")


def _st_arsiv() -> None:
    from app.routers import spor_toto_arsiv as r
    try:
        log.info("spor toto arşiv senkronu: %s", r.senkron_isi())
    except Exception:  # noqa: BLE001 — resmî API erişilemezse ya da hata verirse zamanlayıcı ve diğer işler durmasın
        log.exception("spor toto arşiv senkronu hata verdi")


def _st_oynanma() -> None:
    from app.routers import spor_toto_arsiv as r
    try:
        log.info("spor toto oynanma kaydı: %s satır", r.oynanma_isi())
    except Exception:  # noqa: BLE001 — iddaa erişilemezse ya da hata verirse zamanlayıcı ve diğer işler durmasın
        log.exception("spor toto oynanma kaydı hata verdi")


def isler() -> List[Tuple[str, dict, Callable[[], None]]]:
    """(kimlik, cron alanları, iş). GitHub iş akışlarının eski planıyla aynı saatler."""
    return [
        ("prematch-gunluk", dict(hour="3", minute="30"), _prematch("gunluk")),
        ("prematch-oran", dict(hour="5-22", minute="0,30"), _prematch("oran")),
        ("iddaa-eslestir", dict(hour="*/4", minute="20"), _eslestir),
        ("tercih-kaydet", dict(hour="5-22", minute="10,40"), _tercih("kaydet")),
        ("tercih-sonuc", dict(hour="*/2", minute="50"), _tercih("sonuc")),
        ("oyuncu-json-mat", dict(hour="2", minute="30"), _oyuncu_json_mat),
        ("canli-korner-sonuc", dict(hour="*/2", minute="55"), _canli_korner_sonuc),
        ("spor-toto-arsiv", dict(hour="6", minute="15"), _st_arsiv),
        ("spor-toto-oynanma", dict(hour="*/6", minute="5"), _st_oynanma),
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
