-- Spor Toto maçlarına istatistiksel model alanları (matchforge /api/ms/predict çıktısı).
-- Sadece mevcut tabloya NULLABLE kolon EKLER; veri silmez/değiştirmez; IF NOT EXISTS ile tekrar çalıştırılabilir.
ALTER TABLE greydb.spor_toto_matches
    ADD COLUMN IF NOT EXISTS model_p1       double precision,   -- ev galibiyeti ihtimali
    ADD COLUMN IF NOT EXISTS model_p0       double precision,   -- beraberlik ihtimali
    ADD COLUMN IF NOT EXISTS model_p2       double precision,   -- deplasman galibiyeti ihtimali
    ADD COLUMN IF NOT EXISTS model_kademe   text,               -- tek | cift | uclu
    ADD COLUMN IF NOT EXISTS model_onerilen text,               -- 1 | 2 | 1-0 | 0-2 | 1-0-2  (kart kodu)
    ADD COLUMN IF NOT EXISTS model_version  text,
    ADD COLUMN IF NOT EXISTS model_facts    jsonb;              -- güç farkı, gol beklentisi, senaryo bazlı ihtimaller, uyarılar
