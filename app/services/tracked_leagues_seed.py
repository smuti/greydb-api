"""Maçlar sayfasında takip ettiğimiz liglerin başlangıç listesi (greydb.tracked_leagues tablosuna yüklenir).

Eşleştirme: FotMob lig kimliği (fotmob_ids) YA DA ülke kodu + lig adı kalıbı (ulke + ad_kalibi, büyük/küçük harf duyarsız).
Avrupa kupalarında FotMob kimliği sezona göre değiştiği için onlarda ad kalıbı kullanılır.
logo_id: kalıcı FotMob lig kimliği (site logoyu kendi sunucusundan sunar; maç listesindeki sezonluk kimlik değildir).
Rusya Premier Ligi ve Bolivya Primera bilerek yok (sayfada gizli).
"""
SEED = [
    {"anahtar": "super-lig", "logo_id": 71, "ad": "Süper Lig", "oncelik": 1, "fotmob_ids": [71]},
    {"anahtar": "sampiyonlar-ligi", "logo_id": 42, "ad": "Şampiyonlar Ligi", "oncelik": 2, "ulke": "INT", "ad_kalibi": r"^(uefa )?champions league$"},
    {"anahtar": "avrupa-ligi", "logo_id": 73, "ad": "Avrupa Ligi", "oncelik": 3, "ulke": "INT", "ad_kalibi": r"^(uefa )?europa league$"},
    {"anahtar": "konferans-ligi", "logo_id": 10216, "ad": "Konferans Ligi", "oncelik": 4, "ulke": "INT", "ad_kalibi": r"^(uefa )?(europa )?conference league$"},
    {"anahtar": "premier-lig", "logo_id": 47, "ad": "Premier Lig", "oncelik": 5, "fotmob_ids": [47]},
    {"anahtar": "la-liga", "logo_id": 87, "ad": "La Liga", "oncelik": 6, "fotmob_ids": [87]},
    {"anahtar": "serie-a", "logo_id": 55, "ad": "Serie A", "oncelik": 7, "fotmob_ids": [55]},
    {"anahtar": "bundesliga", "logo_id": 54, "ad": "Bundesliga", "oncelik": 8, "fotmob_ids": [54]},
    {"anahtar": "ligue-1", "logo_id": 53, "ad": "Ligue 1", "oncelik": 9, "fotmob_ids": [53]},
    {"anahtar": "turkiye-kupasi", "logo_id": 151, "ad": "Türkiye Kupası", "oncelik": 10, "ulke": "TUR", "ad_kalibi": r"^(turkish )?cup$"},
    {"anahtar": "super-kupa", "logo_id": 166, "ad": "Süper Kupa", "oncelik": 11, "ulke": "TUR", "ad_kalibi": r"super cup|süper kupa"},
    {"anahtar": "tff-1-lig", "logo_id": 165, "ad": "TFF 1. Lig", "oncelik": 12, "ulke": "TUR", "ad_kalibi": r"^1\.? ?lig$"},
    {"anahtar": "uluslar-ligi", "logo_id": 9806, "ad": "UEFA Uluslar Ligi", "oncelik": 13, "ulke": "INT", "ad_kalibi": r"nations league"},
    # Milli maç adları FotMob'da henüz doğrulanmadı; gerçek adlar görülünce daraltılır.
    {"anahtar": "milli-maclar", "logo_id": 114, "ad": "Milli maçlar", "oncelik": 14, "ulke": "INT",
     "ad_kalibi": r"world cup qualification|euro qualification|^euro$|^(fifa )?world cup$|^friendlies$"},
]
