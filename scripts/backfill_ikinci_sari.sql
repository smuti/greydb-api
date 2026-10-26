-- İkinci sarıdan kırmızı (FotMob card='YellowRed') kayıtlarını düzeltir: eskiden YELLOW_CARD (88'i RED_CARD) yazılıyordu, artık YELLOW_RED_CARD.
-- Ham veri (event_data) bozulmaz; yalnızca event_type güncellenir. Tekrar çalıştırmak zararsızdır.
-- ÖNCE skorjin migration 006 uygulanmış olmalı (görünümler üç biçimi de kırmızı sayar), yoksa bu güncelleme kırmızıları görünümden düşürür.
-- Geri alma: UPDATE public.match_events SET event_type = 'YELLOW_CARD' WHERE event_type = 'YELLOW_RED_CARD';
UPDATE public.match_events
SET event_type = 'YELLOW_RED_CARD'
WHERE event_type IN ('YELLOW_CARD', 'RED_CARD') AND event_data->>'card' = 'YellowRed';
