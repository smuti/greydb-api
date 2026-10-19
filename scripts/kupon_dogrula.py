"""
Bacak sonucu motorunu, admin'in ELLE sonuçlandırdığı geçmiş tahminlere karşı doğrular. YALNIZCA OKUR (veritabanına yazmaz).

Her sonuçlu tahmin için maçı (takım adı + tarih) bulur, motorla sonucu hesaplar ve admin'in sonucuyla karşılaştırır:
  uyumlu | UYUMSUZ (motor ile admin farklı: inceleyin) | veri_yok | desteklenmiyor | mac_yok (maç veritabanında bulunamadı)

Kullanım:  DATABASE_URL=... python scripts/kupon_dogrula.py [--uyumsuz]
"""
import os
import sys
from collections import Counter, defaultdict

from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services.kupon_hesap import bacak_sonucu  # noqa: E402
from app.services.kupon_mac_verisi import mac_yukle, maci_bul  # noqa: E402

URL = os.environ.get("DATABASE_URL")
if not URL:
    print("HATA: DATABASE_URL gerekli.")
    sys.exit(1)
if URL.startswith("postgres://"):
    URL = URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif URL.startswith("postgresql://") and "+psycopg2" not in URL:
    URL = URL.replace("postgresql://", "postgresql+psycopg2://", 1)
engine = create_engine(URL, pool_pre_ping=True, connect_args={"connect_timeout": 40, "keepalives": 1, "keepalives_idle": 30})
_baglanti = None


def execute_query(sql, params=None, deneme=4):
    """Tek kalıcı bağlantı (her sorguda yeni bağlantı uzak veritabanında zaman aşımı veriyordu); kopunca yeniden bağlanır. Yalnızca SELECT."""
    global _baglanti
    for i in range(deneme):
        try:
            if _baglanti is None:
                _baglanti = engine.connect()
            r = _baglanti.execute(text(sql), params or {})
            k = list(r.keys())
            return [dict(zip(k, row)) for row in r.fetchall()]
        except Exception:
            try:
                _baglanti and _baglanti.close()
            except Exception:
                pass
            _baglanti = None
            if i == deneme - 1:
                raise


if __name__ == "__main__":
    tahminler = execute_query("""
        SELECT id, home_team, away_team, match_date, market_name, pick, result
        FROM greydb.predictions WHERE result IN ('won','lost','void') ORDER BY match_date
    """)
    sayac, kotu, pazar = Counter(), [], defaultdict(Counter)
    for n, t in enumerate(tahminler, 1):
        if n % 50 == 0:
            print(f"  ...{n}/{len(tahminler)}", file=sys.stderr, flush=True)
        mac = maci_bul(execute_query, t["home_team"], t["away_team"], t["match_date"].replace(tzinfo=None))
        if mac is None:
            d = "mac_yok"
        else:
            sonuc = bacak_sonucu(t["market_name"], t["pick"], mac_yukle(execute_query, mac))
            d = ("uyumlu" if sonuc == t["result"] else "UYUMSUZ") if sonuc in ("won", "lost", "void") else sonuc
            if d == "UYUMSUZ":
                kotu.append((t, sonuc))
        sayac[d] += 1
        pazar[t["market_name"]][d] += 1
    print(f"sonuçlu tahmin: {len(tahminler)}")
    for k, v in sayac.most_common():
        print(f"  {k:16s} {v:4d}  (%{100*v/len(tahminler):.0f})")
    karar = sayac["uyumlu"] + sayac["UYUMSUZ"]
    if karar:
        print(f"motorun karar verdiği {karar} tahminde uyum: %{100*sayac['uyumlu']/karar:.1f}")
    print("\nUYUMSUZLAR (motor ≠ admin):")
    for t, s in kotu:
        print(f"  #{t['id']} {t['home_team']} - {t['away_team']} {t['match_date']:%Y-%m-%d} | {t['market_name']} / {t['pick']} | admin={t['result']} motor={s}")
    if "--pazar" in sys.argv:
        print("\nMarket bazında:")
        for m, c in sorted(pazar.items(), key=lambda x: -sum(x[1].values())):
            print(f"  {sum(c.values()):3d} {m}: {dict(c)}")
