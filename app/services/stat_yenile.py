"""skorjin_stat.oyuncu_json_mat materialized view'ının yenilenmesi (Skorjin oyuncu sıralaması kilit pas/çalım/top kazanmayı buradan okur).

Görünüm FotMob maç JSON'undan türeyen `oyuncu_json`'un indeksli kopyasıdır; yeni biten maçları ancak yenilenince görür. Günde bir kez (gece, maçlar bittikten sonra)
`REFRESH MATERIALIZED VIEW CONCURRENTLY`: okuyanları kilitlemez (benzersiz indeks `oyuncu_json_mat_uq` gerekir). Yenileme sahibi rolü ister; yetki yoksa denenmez, nedeni raporlanır.
"""
import logging
import time
from typing import Any, Callable, Dict, Optional

log = logging.getLogger("stat-yenile")

YENILE_SQL = "REFRESH MATERIALIZED VIEW CONCURRENTLY skorjin_stat.oyuncu_json_mat"
YETKI_SQL = ("SELECT matviewowner AS sahip, current_user AS kullanici, pg_has_role(current_user, matviewowner, 'member') AS yetkili "
             "FROM pg_matviews WHERE schemaname = 'skorjin_stat' AND matviewname = 'oyuncu_json_mat'")


def _durum() -> Optional[Dict[str, Any]]:
    from app.services.db import execute_query
    r = execute_query(YETKI_SQL)
    return dict(r[0]) if r else None


def _yenile() -> None:
    """CONCURRENTLY bir işlem bloğunda çalışmaz: otomatik commit; yenileme uzun sürebileceği için zaman aşımı kapalı."""
    from sqlalchemy import text
    from app.services.db import engine
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        c.execute(text("SET statement_timeout = 0"))
        c.execute(text(YENILE_SQL))


def calistir(dry_run: bool = True, durum: Callable[[], Optional[Dict[str, Any]]] = _durum, yenile: Callable[[], None] = _yenile) -> Dict[str, Any]:
    rapor: Dict[str, Any] = dict(dry_run=dry_run, yenilendi=False)
    d = durum()
    if not d:
        return {**rapor, "yetkili": False, "neden": "görünüm yok: skorjin_stat.oyuncu_json_mat (migration 019 uygulanmamış)"}
    rapor.update(sahip=d["sahip"], kullanici=d["kullanici"], yetkili=bool(d["yetkili"]))
    if not d["yetkili"]:
        return {**rapor, "neden": f"yetki yok: görünümün sahibi {d['sahip']}, bağlanan rol {d['kullanici']}"}
    if dry_run:
        return rapor
    t0 = time.monotonic()
    yenile()
    return {**rapor, "yenilendi": True, "sure_sn": round(time.monotonic() - t0, 1)}
