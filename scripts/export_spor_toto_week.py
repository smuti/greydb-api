"""Bir Spor Toto haftasının tam anlık görüntüsünü (15 maç + her birinin 6 taslağı +
varsa Fuat'ın onayladığı final_metin/secilen_senaryo) yerel bir JSON dosyasına yazar.

Kullanım: DATABASE_URL=... python scripts/export_spor_toto_week.py <hafta_no>

Amaç: Fuat'ın admin panelinde taslaklardan birini seçip düzenleyip onaylaması AI'nin
ürettiği orijinal metinleri EZMEZ (DB'de entries ayrı, final_metin ayrı tutulur) ama
yine de haftalık bir yerel kopya tutmak, "Fuat ne değiştirmiş" analizini DB'ye
bağımlı kalmadan, kaybetme riski olmadan yapabilmeyi sağlar.
"""
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.db import query_to_df  # noqa: E402

SNAPSHOT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "spor_toto_snapshots")


def export_week(hafta_no: int) -> str:
    week_df = query_to_df("SELECT * FROM greydb.spor_toto_weeks WHERE hafta_no = %s", (hafta_no,))
    if week_df.empty:
        raise SystemExit(f"Hafta bulunamadı: {hafta_no}")
    week_id = int(week_df.iloc[0]["id"])
    week = week_df.iloc[0].where(week_df.iloc[0].notna(), None).to_dict()
    for k, v in week.items():
        if hasattr(v, "isoformat"):
            week[k] = v.isoformat()

    matches_df = query_to_df(
        "SELECT * FROM greydb.spor_toto_matches WHERE week_id = %s ORDER BY mac_no", (week_id,)
    )
    matches = []
    for _, mrow in matches_df.iterrows():
        match_id = int(mrow["id"])
        m = mrow.where(mrow.notna(), None).to_dict()
        for k, v in m.items():
            if hasattr(v, "isoformat"):
                m[k] = v.isoformat()
        entries_df = query_to_df(
            "SELECT senaryo, metin, onerilen FROM greydb.spor_toto_entries WHERE match_id = %s ORDER BY senaryo",
            (match_id,),
        )
        m["entries"] = entries_df.to_dict(orient="records")
        matches.append(m)

    week["matches"] = matches

    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(SNAPSHOT_DIR, f"hafta_{hafta_no}_{stamp}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(week, f, ensure_ascii=False, indent=2)
    return path


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("kullanım: python scripts/export_spor_toto_week.py <hafta_no>")
    out = export_week(int(sys.argv[1]))
    print(f"Yazıldı: {out}")
