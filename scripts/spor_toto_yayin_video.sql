-- Spor Toto haftasının YouTube yayın bağlantısı (sitedeki /yayinlar sayfaları için).
-- Sadece mevcut tabloya NULLABLE kolon EKLER; veri silmez/değiştirmez; IF NOT EXISTS ile tekrar çalıştırılabilir.
ALTER TABLE greydb.spor_toto_weeks
    ADD COLUMN IF NOT EXISTS youtube_linki text;   -- haftanın tek yayın videosu (maç bölümleri açıklamasındaki zaman damgalarından okunur)
