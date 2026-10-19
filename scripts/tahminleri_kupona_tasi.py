"""
Eski tahminleri (greydb.predictions) yeni kupon modeline (greydb.kuponlar + kupon_bacaklari) taşır: her tahmin tek bacaklı kupon olur.
Tekrar çalıştırılabilir: kaynak = 'tahmin:<id>' olanlar atlanır. Eski tabloya DOKUNMAZ.

Varsayılan KURU ÇALIŞMA (yazmaz). Seçenekler:
  --yaz              kuponları yazar (önce create_kuponlar_tables.py çalışmış olmalı)
  --motorla-duzelt   admin'in girdiği sonuç, maç skorundan hesaplanan sonuçla ÇELİŞİYORSA motorun sonucunu yazar (kaynak 'otomatik') ve raporlar.
                     Verilmezse admin sonucu korunur ('elle'), çelişkiler yine raporlanır. (Maç veritabanında yoksa ya da veri eksikse admin sonucu kalır.)

Kullanım:  DATABASE_URL=... python scripts/tahminleri_kupona_tasi.py [--yaz] [--motorla-duzelt]
"""
import json
import os
import sys
from collections import Counter

from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services.kupon_hesap import bacak_sonucu  # noqa: E402
from app.services.kupon_mac_verisi import mac_yukle, maci_bul  # noqa: E402
from app.services.kupon_model import kupon_ozeti  # noqa: E402
from app.services.tahmin_tasima import tahmin_kuponu  # noqa: E402

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


def sorgu(sql, params=None, deneme=4):
    global _baglanti
    for i in range(deneme):
        try:
            if _baglanti is None:
                _baglanti = engine.connect()
            r = _baglanti.execute(text(sql), params or {})
            sonuc = None
            if r.returns_rows:
                k = list(r.keys())
                sonuc = [dict(zip(k, row)) for row in r.fetchall()]
            if sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                _baglanti.commit()      # kalıcı bağlantıda yazma kendiliğinden kesinleşmez
            return sonuc
        except Exception:
            try:
                _baglanti and _baglanti.close()
            except Exception:
                pass
            _baglanti = None
            if i == deneme - 1:
                raise


def yaz(k, bacak_kaynagi, bacak_zamani):
    sonuc_k = kupon_ozeti(k["bacaklar"], None)
    belli = sonuc_k["sonuc"] in ("won", "lost", "void")
    kid = sorgu(
        """INSERT INTO greydb.kuponlar (seviye, durum, zamanlama, one_cikan, sistem_k, nedenler, ana_risk, yorum, sinyal, risk_seviyesi, toplam_oran, en_yuksek_odeme,
               kombinasyon_sayisi, sonuc, odeme, sonuc_zamani, yayin_zamani, olusturan_email, kaynak, olusturma, guncelleme)
           VALUES (:seviye, :durum, :zamanlama, FALSE, NULL, CAST(:nedenler AS jsonb), :ana_risk, :yorum, :sinyal, :risk, :toplam, :odeme, 1, :sonuc, :od, :sz,
               CASE WHEN :durum = 'yayinda' THEN :olusturma ELSE NULL END, :email, :kaynak, :olusturma, :guncelleme) RETURNING id""",
        {"seviye": k["seviye"], "durum": k["durum"], "zamanlama": k["zamanlama"], "nedenler": json.dumps(k["nedenler"]), "ana_risk": k["ana_risk"], "yorum": k["yorum"],
         "sinyal": k["sinyal"], "risk": k["risk_seviyesi"], "toplam": k["toplam_oran"], "odeme": k["en_yuksek_odeme"],
         "sonuc": sonuc_k["sonuc"] if belli else None, "od": sonuc_k["odeme"] if belli else None, "sz": k["guncelleme"] if belli else None,
         "email": k["olusturan_email"], "kaynak": k["kaynak"], "olusturma": k["olusturma"], "guncelleme": k["guncelleme"]},
    )[0]["id"]
    for i, b in enumerate(k["bacaklar"]):
        sorgu(
            """INSERT INTO greydb.kupon_bacaklari (kupon_id, sira, iddaa_event_id, fotmob_match_id, ev, dep, lig, mac_zamani, bahis_turu, secim, oran, sonuc, sonuc_kaynagi, sonuc_zamani)
               VALUES (:kid, :sira, :iddaa, NULL, :ev, :dep, :lig, CAST(:zaman AS timestamptz) AT TIME ZONE 'utc', :tur, :secim, :oran, :sonuc, :kaynak, :sz)""",
            {"kid": kid, "sira": i, "iddaa": b["iddaa_event_id"], "ev": b["ev"], "dep": b["dep"], "lig": b["lig"], "zaman": b["mac_zamani"], "tur": b["bahis_turu"], "secim": b["secim"],
             "oran": b["oran"], "sonuc": b["sonuc"], "kaynak": None if b["sonuc"] == "pending" else bacak_kaynagi, "sz": bacak_zamani if b["sonuc"] != "pending" else None},
        )


if __name__ == "__main__":
    YAZ, DUZELT = "--yaz" in sys.argv, "--motorla-duzelt" in sys.argv
    var = {r["kaynak"] for r in (sorgu("SELECT kaynak FROM greydb.kuponlar WHERE kaynak LIKE 'tahmin:%'") if YAZ else [])}
    satirlar = sorgu("SELECT * FROM greydb.predictions ORDER BY id")
    sayac, celiski, atlanan = Counter(), [], []
    for n, t in enumerate(satirlar, 1):
        if n % 50 == 0:
            print(f"  ...{n}/{len(satirlar)}", file=sys.stderr, flush=True)
        try:
            k = tahmin_kuponu(t)
        except Exception as e:
            atlanan.append((t["id"], str(e)))
            continue
        if k["kaynak"] in var:
            sayac["zaten_var"] += 1
            continue
        b = k["bacaklar"][0]
        kaynak = "elle"
        if b["sonuc"] != "pending" or DUZELT:
            mac = maci_bul(sorgu, t["home_team"], t["away_team"], t["match_date"])
            motor = bacak_sonucu(b["bahis_turu"], b["secim"], mac_yukle(sorgu, mac)) if mac else None
            if motor in ("won", "lost", "void") and motor != b["sonuc"] and b["sonuc"] != "pending":
                celiski.append((t["id"], t["home_team"], t["away_team"], t["market_name"], t["pick"], b["sonuc"], motor))
                if DUZELT:
                    b["sonuc"], kaynak = motor, "otomatik"
            elif motor in ("won", "lost", "void") and b["sonuc"] == "pending" and DUZELT:
                b["sonuc"], kaynak = motor, "otomatik"
        sayac[f"{k['durum']}/{b['sonuc']}"] += 1
        if YAZ:
            yaz(k, kaynak, k["guncelleme"])
    print(f"tahmin: {len(satirlar)} | " + ("YAZILDI" if YAZ else "KURU ÇALIŞMA (yazılmadı)") + (" | motorla düzeltme açık" if DUZELT else ""))
    for a, v in sorted(sayac.items()):
        print(f"  {a:22s} {v}")
    if atlanan:
        print("atlanan:", atlanan)
    print(f"\nadmin sonucu ile maç skoru çelişen: {len(celiski)}")
    for c in celiski:
        print(f"  #{c[0]} {c[1]} - {c[2]} | {c[3]} / {c[4]} | admin={c[5]} motor={c[6]}" + ("  → motorun sonucu yazıldı" if DUZELT else "  → admin sonucu korundu"))
