"""Maçlar sayfasında takip ettiğimiz liglerin başlangıç listesi (greydb.tracked_leagues tablosuna yüklenir).

Eşleştirme: FotMob lig kimliği (fotmob_ids) YA DA ülke kodu + lig adı kalıbı (ulke + ad_kalibi, büyük/küçük harf duyarsız).
Avrupa kupalarında FotMob kimliği sezona göre değiştiği için onlarda ad kalıbı kullanılır.
Rusya Premier Ligi ve Bolivya Primera bilerek yok (sayfada gizli).
"""
SEED = [
    {"anahtar": "super-lig", "ad": "Süper Lig", "oncelik": 1, "fotmob_ids": [71]},
    {"anahtar": "sampiyonlar-ligi", "ad": "Şampiyonlar Ligi", "oncelik": 2, "ulke": "INT", "ad_kalibi": r"^(uefa )?champions league$"},
    {"anahtar": "avrupa-ligi", "ad": "Avrupa Ligi", "oncelik": 3, "ulke": "INT", "ad_kalibi": r"^(uefa )?europa league$"},
    {"anahtar": "konferans-ligi", "ad": "Konferans Ligi", "oncelik": 4, "ulke": "INT", "ad_kalibi": r"^(uefa )?(europa )?conference league$"},
    {"anahtar": "premier-lig", "ad": "Premier Lig", "oncelik": 5, "fotmob_ids": [47]},
    {"anahtar": "la-liga", "ad": "La Liga", "oncelik": 6, "fotmob_ids": [87]},
    {"anahtar": "serie-a", "ad": "Serie A", "oncelik": 7, "fotmob_ids": [55]},
    {"anahtar": "bundesliga", "ad": "Bundesliga", "oncelik": 8, "fotmob_ids": [54]},
    {"anahtar": "ligue-1", "ad": "Ligue 1", "oncelik": 9, "fotmob_ids": [53]},
    {"anahtar": "turkiye-kupasi", "ad": "Türkiye Kupası", "oncelik": 10, "ulke": "TUR", "ad_kalibi": r"^(turkish )?cup$"},
    {"anahtar": "super-kupa", "ad": "Süper Kupa", "oncelik": 11, "ulke": "TUR", "ad_kalibi": r"super cup|süper kupa"},
    {"anahtar": "tff-1-lig", "ad": "TFF 1. Lig", "oncelik": 12, "ulke": "TUR", "ad_kalibi": r"^1\.? ?lig$"},
    {"anahtar": "uluslar-ligi", "ad": "UEFA Uluslar Ligi", "oncelik": 13, "ulke": "INT", "ad_kalibi": r"nations league"},
    # Milli maç adları FotMob'da henüz doğrulanmadı; gerçek adlar görülünce daraltılır.
    {"anahtar": "milli-maclar", "ad": "Milli maçlar", "oncelik": 14, "ulke": "INT",
     "ad_kalibi": r"world cup qualification|euro qualification|^euro$|^(fifa )?world cup$|^friendlies$"},
]
