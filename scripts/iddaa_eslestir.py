"""
iddaa maç kimliğini (iddaa_event_id) kendi maçlarımıza (fotmob_match_id) bağlar. Tekrar çalıştırılabilir; cron'a uygundur.

iddaa listesi yalnızca ~3 günlük pencere verir, bu yüzden düzenli (ör. günde birkaç kez) çalıştırılmalı.
Varsayılan KURU ÇALIŞMADIR (veritabanına yazmaz, yalnızca raporlar); yazmak için --yaz.
Sütunu yoksa ekler (ADD COLUMN IF NOT EXISTS) — yalnızca --yaz ile.

Kullanım:
    DATABASE_URL=... python scripts/iddaa_eslestir.py          # rapor
    DATABASE_URL=... python scripts/iddaa_eslestir.py --yaz    # yaz
"""
import os
import sys
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services.iddaa_eslestir import eslestir  # noqa: E402

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("HATA: DATABASE_URL ortam değişkeni gerekli.")
    sys.exit(1)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+psycopg2" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

BASE = "https://sportsbookv2.iddaa.com/sportsbook/events"
BASLIK = {"User-Agent": "Mozilla/5.0", "Origin": "https://www.iddaa.com", "Referer": "https://www.iddaa.com/"}

ALTER = [
    "ALTER TABLE public.matches ADD COLUMN IF NOT EXISTS iddaa_event_id BIGINT",
    "ALTER TABLE public.upcoming_matches ADD COLUMN IF NOT EXISTS iddaa_event_id BIGINT",
    "CREATE INDEX IF NOT EXISTS idx_matches_iddaa_event_id ON public.matches (iddaa_event_id)",
    "CREATE INDEX IF NOT EXISTS idx_upcoming_matches_iddaa_event_id ON public.upcoming_matches (iddaa_event_id)",
]
# Pencere: biten son 6 saat ile gelecek 4 gün (iddaa listesi ~3 gün; canlı liste oynanmakta olanları da verir)
MACLAR = """
SELECT m.fotmob_match_id, ev.name AS home, dep.name AS away, m.match_date
FROM public.matches m JOIN public.teams ev ON ev.id = m.home_team_id JOIN public.teams dep ON dep.id = m.away_team_id
WHERE m.fotmob_match_id IS NOT NULL AND m.match_date BETWEEN :bas AND :son
UNION
SELECT u.fotmob_match_id, u.home_team_name, u.away_team_name, u.match_date
FROM public.upcoming_matches u
WHERE u.fotmob_match_id IS NOT NULL AND u.match_date BETWEEN :bas AND :son
"""


def iddaa_olaylari():
    olaylar = []
    with httpx.Client(headers=BASLIK, timeout=30) as istemci:
        for tip in (0, 1):   # 0: maç önü program, 1: canlı
            r = istemci.get(BASE, params={"st": 1, "type": tip})
            r.raise_for_status()
            olaylar += r.json()["data"]["events"]
    return olaylar


if __name__ == "__main__":
    yaz = "--yaz" in sys.argv
    simdi = datetime.now(timezone.utc).replace(tzinfo=None)
    engine = create_engine(DATABASE_URL)
    olaylar = iddaa_olaylari()
    with engine.connect() as conn:
        satirlar = conn.execute(text(MACLAR), {"bas": simdi - timedelta(hours=6), "son": simdi + timedelta(days=4)}).mappings().all()
    maclar = [
        {"fotmob_match_id": s["fotmob_match_id"], "home": s["home"], "away": s["away"],
         "kickoff_ts": s["match_date"].replace(tzinfo=timezone.utc).timestamp()}
        for s in satirlar
    ]
    sonuc = eslestir(olaylar, maclar)
    print(f"iddaa olayı: {len(olaylar)} | bizim maç (pencere): {len(maclar)} | eşleşen: {len(sonuc['eslesen'])} | "
          f"belirsiz: {len(sonuc['belirsiz'])} | eşleşmeyen: {len(sonuc['eslesmeyen'])}")
    for m in sonuc["belirsiz"]:
        print(f"  BELİRSİZ: {m['home']} - {m['away']} ({m['fotmob_match_id']})")
    if not yaz:
        print("Kuru çalışma: yazılmadı (yazmak için --yaz).")
        sys.exit(0)
    with engine.begin() as conn:
        for q in ALTER:
            conn.execute(text(q))
        for fid, i in sonuc["eslesen"].items():
            for tablo in ("public.matches", "public.upcoming_matches"):
                conn.execute(text(f"UPDATE {tablo} SET iddaa_event_id = :i WHERE fotmob_match_id = :fid"), {"i": i, "fid": fid})
    print(f"Yazıldı: {len(sonuc['eslesen'])} maç.")
