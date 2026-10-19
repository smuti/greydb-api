"""Bacağın iki takımının FotMob takım kimlikleri (logo için): önce maç kimliği, sonra iddaa kimliği, en son takım adı + tarih."""
from datetime import datetime

from app.services.kupon_takim import takim_kimlikleri

BACAK = {"fotmob_match_id": None, "iddaa_event_id": None, "ev": "Trabzonspor", "dep": "Beşiktaş", "mac_zamani": datetime(2026, 10, 19, 17, 0)}


def sahte(cevaplar):
    """cevaplar: [(sorguda geçen parça, dönecek satırlar)] — ilk uyan; çağrıları da kaydeder."""
    cagrilar = []

    def yurut(sql, params=None):
        cagrilar.append((sql, params))
        for parca, satirlar in cevaplar:
            if parca in sql:
                if isinstance(satirlar, Exception):
                    raise satirlar
                return satirlar
        return []
    yurut.cagrilar = cagrilar
    return yurut


def test_maç_kimliğiyle_bulur():
    q = sahte([("fotmob_match_id = :f", [{"ev_id": 9752, "dep_id": 10188, "ev_ad": "Trabzonspor", "dep_ad": "Beşiktaş"}])])
    assert takim_kimlikleri(q, {**BACAK, "fotmob_match_id": 3219202}) == (9752, 10188)


def test_iddaa_kimliğiyle_bulur_sütun_yoksa_atlar():
    q = sahte([("iddaa_event_id = :i", [{"ev_id": 1, "dep_id": 2, "ev_ad": "A", "dep_ad": "B"}])])
    assert takim_kimlikleri(q, {**BACAK, "iddaa_event_id": 55}) == (1, 2)
    hatali = sahte([("iddaa_event_id = :i", Exception("column does not exist"))])
    assert takim_kimlikleri(hatali, {**BACAK, "iddaa_event_id": 55}) == (None, None)


def test_ad_ve_tarihle_bulur_tek_aday_olmalı():
    aday = {"ev_id": 9752, "dep_id": 10188, "ev_ad": "Trabzonspor", "dep_ad": "Beşiktaş JK"}
    assert takim_kimlikleri(sahte([("BETWEEN", [aday])]), BACAK) == (9752, 10188)
    # iki farklı maç ad benzerliği tutarsa tahmin yürütmez
    assert takim_kimlikleri(sahte([("BETWEEN", [aday, {**aday, "ev_id": 1}])]), BACAK) == (None, None)
    # başka takımlar: eşleşmez
    assert takim_kimlikleri(sahte([("BETWEEN", [{"ev_id": 7, "dep_id": 8, "ev_ad": "Rize", "dep_ad": "Konya"}])]), BACAK) == (None, None)


def test_kimlik_eksikse_none_döner_yarım_döndürmez():
    q = sahte([("BETWEEN", [{"ev_id": 9752, "dep_id": None, "ev_ad": "Trabzonspor", "dep_ad": "Beşiktaş"}])])
    assert takim_kimlikleri(q, BACAK) == (None, None)


def test_hiçbir_yerde_yoksa_none():
    assert takim_kimlikleri(sahte([]), BACAK) == (None, None)


def test_yaklaşan_maçlar_tablosuna_da_bakar():
    q = sahte([("upcoming_matches", [{"ev_id": 3, "dep_id": 4, "ev_ad": "Trabzonspor", "dep_ad": "Beşiktaş"}])])
    assert takim_kimlikleri(q, {**BACAK, "fotmob_match_id": 9}) == (3, 4)


def test_doldur_bulunanı_yazar_kuruda_yazmaz_hatayı_sayar():
    from app.services.kupon_takim import doldur
    bacaklar = [{"id": 1, "ev": "A", "dep": "B", "mac_zamani": datetime(2026, 10, 1)}, {"id": 2, "ev": "C", "dep": "D", "mac_zamani": datetime(2026, 10, 2)},
                {"id": 3, "ev": "E", "dep": "F", "mac_zamani": datetime(2026, 10, 3)}]
    sonuc = {1: (10, 11), 2: (None, None)}

    def bul(b):
        if b["id"] == 3:
            raise RuntimeError("veritabanı")
        return sonuc[b["id"]]
    yazilan = []
    kuru = doldur(bacaklar, bul, lambda i, e, d: yazilan.append((i, e, d)), kuru=True)
    assert yazilan == [] and kuru["bulunan"] == 1 and kuru["bulunamayan"] == 1 and kuru["hata"] == 1 and kuru["kuru_calisma"] is True
    assert kuru["bulunamayan_ornekler"] == ["C - D (02.10.2026)"]
    gercek = doldur(bacaklar, bul, lambda i, e, d: yazilan.append((i, e, d)), kuru=False)
    assert yazilan == [(1, 10, 11)] and gercek["bulunan"] == 1
