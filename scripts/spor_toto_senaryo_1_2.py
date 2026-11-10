"""
Tek seferlik, tekrar çalıştırılabilir migration: Spor Toto senaryo kodlarına '1-2' (beraberlik dışarıda) eklenir.
Eski CHECK kısıtları yalnızca 1, 0, 2, 1-0, 0-2, 1-0-2 kabul ediyordu; 1-2 taslağı eklenince kayıt orada kopuyordu.
Veriye dokunmaz, yalnız iki kısıtı genişletir:  DATABASE_URL=... python scripts/spor_toto_senaryo_1_2.py
"""
import os
import sys

KODLAR = "ARRAY['1'::text, '0'::text, '2'::text, '1-0'::text, '1-2'::text, '0-2'::text, '1-0-2'::text]"

DDL = [
    "ALTER TABLE greydb.spor_toto_entries DROP CONSTRAINT IF EXISTS spor_toto_entries_senaryo_check",
    f"ALTER TABLE greydb.spor_toto_entries ADD CONSTRAINT spor_toto_entries_senaryo_check CHECK (senaryo = ANY ({KODLAR}))",
    "ALTER TABLE greydb.spor_toto_matches DROP CONSTRAINT IF EXISTS spor_toto_matches_secilen_senaryo_check",
    f"ALTER TABLE greydb.spor_toto_matches ADD CONSTRAINT spor_toto_matches_secilen_senaryo_check CHECK (secilen_senaryo = ANY ({KODLAR}))",
]


def main() -> None:
    from sqlalchemy import create_engine, text

    url = os.environ.get("DATABASE_URL")
    if not url:
        print("HATA: DATABASE_URL ortam değişkeni gerekli.")
        sys.exit(1)
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    with create_engine(url).begin() as conn:            # tek işlem: ikisi birden uygulanır ya da hiçbiri
        for q in DDL:
            conn.execute(text(q))
        for tablo, kisit in (("spor_toto_entries", "spor_toto_entries_senaryo_check"), ("spor_toto_matches", "spor_toto_matches_secilen_senaryo_check")):
            tanim = conn.execute(text("select pg_get_constraintdef(oid) from pg_constraint where conname = :k and conrelid = CAST(:t AS regclass)"), {"k": kisit, "t": f"greydb.{tablo}"}).scalar()
            assert tanim and "'1-2'" in tanim, f"{kisit} güncellenemedi"
    print("Tamam: spor_toto_entries.senaryo ve spor_toto_matches.secilen_senaryo kısıtları 1-2'yi kabul ediyor.")


if __name__ == "__main__":
    main()
