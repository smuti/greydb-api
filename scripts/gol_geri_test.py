"""
Gol modeli geri testi: geçmiş maçları günlük as_of ile matchforge ms_model'e yeniden tahmin ettirir, karne biçiminde puanlar.

Okuma salt okunurdur. --yukle verilirse satırlar greydb.gol_karne_tahmin'e kaynak=geri_test olarak yazılır (tablo önceden var olmalı;
geçmiş maç kaydı güncellenmez, yalnız eksikler eklenir). Son N hafta "sınav" olarak ayrı raporlanır.

Kullanım:
    DATABASE_URL=... MATCHFORGE_DIR=~/Code/personal/matchforge  (main'de olmalı)  \\
    <matchforge venv python> scripts/gol_geri_test.py --bas 2024-08-01 --son 2026-10-27 --sinav-hafta 6 --cikti geri_test.jsonl [--yukle]
"""
import argparse
import json
import os
import sys
import time
import warnings
from datetime import date, datetime, timedelta

warnings.filterwarnings("ignore")
KOK = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, KOK)
sys.path.insert(0, os.path.expanduser(os.environ.get("MATCHFORGE_DIR", "~/Code/personal/matchforge")))

import pandas as pd  # noqa: E402
import psycopg2  # noqa: E402
from ms_model import config as MC  # noqa: E402
from ms_model.engine import MSModel  # noqa: E402

from app.services.gol_geri_test import bolumlu_rapor, satir_json, uret  # noqa: E402
from app.services.gol_karne import yazma_sorgusu  # noqa: E402

MAC_SQL = """select m.fotmob_match_id, m.home_team_id, m.away_team_id, ev.name as home_name, dep.name as away_name, m.league_id,
 m.match_date, m.home_score, m.away_score
 from public.matches m join public.teams ev on ev.id=m.home_team_id join public.teams dep on dep.id=m.away_team_id
 where m.finished and not coalesce(m.cancelled,false) and m.home_score is not null and m.away_score is not null
   and m.fotmob_match_id is not null and m.league_id = any(%s) and m.match_date >= %s and m.match_date < %s
 order by m.match_date"""


def _sa_url(url: str) -> str:
    for eski in ("postgres://", "postgresql://"):
        if url.startswith(eski):
            return url.replace(eski, "postgresql+psycopg2://", 1)
    return url


def egitim_verisi(engine):
    """Eğitim tablosu matchforge'un kendi yükleyicisinden: kurulu model sürümünün beklediği sütunlar (ör. m4 xh_dc/xa_dc) birlikte gelir."""
    from ms_model.data import load_matches
    return load_matches(engine)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bas", default="2024-08-01")
    ap.add_argument("--son", default=str(date.today() - timedelta(days=1)))
    ap.add_argument("--sinav-hafta", type=int, default=6)
    ap.add_argument("--cikti", default="gol_geri_test.jsonl")
    ap.add_argument("--yukle", action="store_true")
    a = ap.parse_args()
    bas, son = date.fromisoformat(a.bas), date.fromisoformat(a.son)
    sinav_bas = datetime.combine(son - timedelta(weeks=a.sinav_hafta) + timedelta(days=1), datetime.min.time())
    print(f"model {MC.MODEL_VERSION} · dönem {bas}..{son} · sınav {sinav_bas.date()}..{son}", flush=True)

    from sqlalchemy import create_engine, text
    salt_okunur = create_engine(_sa_url(os.environ["DATABASE_URL"]), connect_args={"options": "-c default_transaction_read_only=on"})
    D = egitim_verisi(salt_okunur)
    con = psycopg2.connect(os.environ["DATABASE_URL"])
    con.set_session(readonly=True, autocommit=True)
    cur = con.cursor()
    cur.execute(MAC_SQL, (MC.POOL_LEAGUES, bas, son + timedelta(days=1)))
    kol = [c[0] for c in cur.description]
    maclar = [dict(zip(kol, r)) for r in cur.fetchall()]
    con.close()
    print(f"eğitim verisi {len(D)} maç · tahmin edilecek {len(maclar)} maç", flush=True)

    t0 = time.time()

    def kur(as_of):
        model = MSModel.fit(D, pd.Timestamp(as_of))
        return lambda ev, dep, lig: model.predict(ev, dep, lig)
    satirlar, ozet = uret(maclar, kur)
    print(f"üretildi: {ozet} · {time.time() - t0:.0f} sn", flush=True)

    with open(a.cikti, "w", encoding="utf-8") as f:
        for s in satirlar:
            f.write(satir_json(s) + "\n")
    r = bolumlu_rapor(satirlar, sinav_bas)
    with open(os.path.splitext(a.cikti)[0] + "_rapor.json", "w", encoding="utf-8") as f:
        json.dump({"ozet": ozet, **r}, f, ensure_ascii=False, indent=1, default=str)
    for bolum in ("gelistirme", "sinav"):
        b = r[bolum]
        print(f"\n{bolum.upper()}: {b['mac']} maç · uyarı {len(b['uyarilar'])}")
        for t in b["tablo"]:
            print(f"  {t['pazar']:<16} n={t['n']:>5} bizim={t['ort_p']*100:5.1f} gerçek={t['gercek']*100:5.1f} brier={t['brier']:.4f} z={t['z']:+.2f}")

    if a.yukle:
        with create_engine(_sa_url(os.environ["DATABASE_URL"])).begin() as conn:
            for s in satirlar:
                sql, params = yazma_sorgusu(s)
                conn.execute(text(sql), params)
        print(f"\nyüklendi: {len(satirlar)} satır (kaynak=geri_test)")


if __name__ == "__main__":
    main()
