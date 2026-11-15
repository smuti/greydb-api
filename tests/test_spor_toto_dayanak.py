"""Senaryo metnine giren paket dayanakları: kartlar atılır, yalnız maç no + iki takım adı tutarsa eşleşir, hata üretimi düşürmez."""
from app.services import spor_toto_dayanak as D

PAKET = {"maclar": [
    {"no": 1, "ev": "Gençlerbirliği", "dep": "Gaziantep FK", "dayanaklar": [
        {"id": "seri", "baslik": "Kazanamama serisi", "yon": "bilgi", "ozet": "Gaziantep FK son 5 maçtır kazanamıyor.", "notlar": ["a", "b", "c"], "kartlar": [{"type": "table"}]},
        {"id": "piyasa", "baslik": "Piyasa oranları", "yon": "destek", "ozet": "Piyasa ev sahibine %48 veriyor.", "notlar": []},
        {"id": "bos", "baslik": "Boş", "yon": "risk", "ozet": " "},
    ]},
]}


def test_damit_kartlari_atar_yon_siralar_bos_ozeti_eler():
    d = D.damit(PAKET["maclar"][0]["dayanaklar"])
    assert [x["konu"] for x in d] == ["Piyasa oranları", "Kazanamama serisi"]
    assert d[1]["notlar"] == ["a", "b"] and "kartlar" not in d[1]


def test_eslesme_no_ve_iki_takim_adiyla():
    assert D.mac_dayanaklari(PAKET, 1, "Gençlerbirliği", "Gaziantep FK")
    assert D.mac_dayanaklari(PAKET, 1, "Genclerbirligi", "Gaziantep") is not None       # Türkçe karakter ve kısa ad toleransı
    assert D.mac_dayanaklari(PAKET, 2, "Gençlerbirliği", "Gaziantep FK") is None
    assert D.mac_dayanaklari(PAKET, 1, "Galatasaray", "Gaziantep FK") is None          # başka haftanın maçı aynı sırada olabilir
    assert D.mac_dayanaklari(None, 1, "a", "b") is None


def test_getir_hatada_none_doner():
    satir = [{"mac_no": 1, "ev_takim": "Gençlerbirliği", "deplasman_takim": "Gaziantep FK"}]
    assert D.getir(5, lambda q, p: satir, lambda: {"paket": PAKET})
    assert D.getir(5, lambda q, p: satir, lambda: None) is None
    assert D.getir(5, lambda q, p: [], lambda: {"paket": PAKET}) is None

    def patlar():
        raise RuntimeError("db")
    assert D.getir(5, lambda q, p: satir, patlar) is None


def test_yasak_terimli_cumleler_atilir_ondalik_yuvarlanir():
    d = D.damit([{"id": "x", "baslik": "Hücum", "yon": "bilgi", "ozet": "Maç başına 2,64 gol atıyor. xG üretimi yüksek.", "notlar": ["xG farkı +1,2.", "Savunmada 1,18 gol yiyor."]}])
    assert d[0]["ozet"] == "Maç başına yaklaşık 3 gol atıyor."
    assert d[0]["notlar"] == ["Savunmada yaklaşık 1 gol yiyor."]
    assert D.damit([{"id": "y", "baslik": "Model", "yon": "bilgi", "ozet": "Model olasılığı %47."}]) == []
