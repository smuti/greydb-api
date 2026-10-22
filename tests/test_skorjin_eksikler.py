"""FotMob matchDetails (yaklaşan maç) -> Skorjin eksik oyuncu verisi. Gerçek yanıt biçimiyle (ağ yok)."""
import pytest

from app.services.skorjin_prematch import eksik_oyuncular


def oyuncu(ad, tur="injury", donus="Early November 2026", deger=1_000_000, **kw):
    p = {"id": 1, "name": ad, "unavailability": {"injuryId": 42, "type": tur, "expectedReturn": donus}}
    if deger is not None:
        p["marketValue"] = deger
    return {**p, **kw}


def detay(ev=(), dep=(), lineup=True):
    if not lineup:
        return {"general": {}, "content": {}}
    return {"general": {}, "content": {"lineup": {"homeTeam": {"name": "FM Ev", "unavailable": list(ev)},
                                                   "awayTeam": {"name": "FM Dep", "unavailable": list(dep)}}}}


def takimlar(sonuc):
    return {t["name"]: t for t in sonuc["teams"]}


def test_sakat_ve_cezali_dogru_eslenir_adlar_bizim_veritabanindan():
    s = eksik_oyuncular(detay(ev=[oyuncu("Paul Onuachu"), oyuncu("Ali Veli", tur="suspension", donus="Unknown")], dep=[oyuncu("Ageu", donus="About 1-2 weeks")]),
                        "Trabzonspor", "Hearts")
    t = takimlar(s)
    assert set(t) == {"Trabzonspor", "Hearts"} and s["source"] == "FotMob"
    assert [(p["name"], p["status"]) for p in t["Trabzonspor"]["players"]] == [("Paul Onuachu", "sakat"), ("Ali Veli", "cezali")]
    assert t["Hearts"]["players"][0]["status"] == "sakat" and t["Trabzonspor"]["info_available"] is True


@pytest.mark.parametrize("donus,durum", [("Doubtful", "supheli"), ("Day to day", "supheli"), ("Out for season", "sakat"), ("Unknown", "sakat"),
                                         ("Late January 2027", "sakat")])
def test_sakat_tur_donus_bilgisine_gore_supheli_olabilir(donus, durum):
    p = takimlar(eksik_oyuncular(detay(ev=[oyuncu("A", donus=donus)], dep=[oyuncu("B")]), "E", "D"))["E"]["players"][0]
    assert p["status"] == durum


@pytest.mark.parametrize("donus,beklenen", [
    ("Early November 2026", "Kasım 2026 başı"), ("Mid October 2026", "Ekim 2026 ortası"), ("Late January 2027", "Ocak 2027 sonu"),
    ("About 1-2 weeks", "yaklaşık 1-2 hafta"), ("Day to day", "günü gününe"), ("A few days", "birkaç gün"),
    ("Out for season", "sezon sonuna kadar"), ("Back in training", "antrenmanlara döndü"),
    ("Unknown", None), ("Doubtful", None), ("Bilinmeyen Yeni Biçim", None), (None, None)])
def test_donus_metni_turkceye_cevrilir_taninmayan_yazilmaz(donus, beklenen):
    p = takimlar(eksik_oyuncular(detay(ev=[oyuncu("A", donus=donus)], dep=[oyuncu("B")]), "E", "D"))["E"]["players"][0]
    assert p["expected_return"] == beklenen and p["reason"] is None


def test_piyasa_degerine_gore_azalan_degeri_olmayan_sonda():
    ev = [oyuncu("Ucuz", deger=100), oyuncu("Degersiz", deger=None), oyuncu("Pahali", deger=9_000_000)]
    p = takimlar(eksik_oyuncular(detay(ev=ev, dep=[oyuncu("X")]), "E", "D"))["E"]["players"]
    assert [x["name"] for x in p] == ["Pahali", "Ucuz", "Degersiz"]


def test_bilinmeyen_tur_adsiz_ve_bos_kayit_atlanir():
    ev = [oyuncu("Iyi"), oyuncu("Ulusal", tur="international"), {"id": 2, "unavailability": {"type": "injury"}}, None]
    p = takimlar(eksik_oyuncular(detay(ev=ev, dep=[oyuncu("X")]), "E", "D"))["E"]["players"]
    assert [x["name"] for x in p] == ["Iyi"]


def test_bir_takimda_kayit_varsa_digerinin_bos_listesi_kayitli_eksik_yok_demektir():
    t = takimlar(eksik_oyuncular(detay(ev=[oyuncu("A")], dep=[]), "E", "D"))
    assert t["D"]["info_available"] is True and t["D"]["players"] == []


def test_iki_takimda_da_liste_bos_ise_kapsam_yok_sayilir_bilgi_yok():
    assert eksik_oyuncular(detay(ev=[], dep=[]), "E", "D") is None


def test_lineup_yok_ya_da_detay_yok_none():
    assert eksik_oyuncular(detay(lineup=False), "E", "D") is None
    assert eksik_oyuncular({}, "E", "D") is None
    assert eksik_oyuncular(None, "E", "D") is None


def test_oyuncu_sayisi_takim_basina_sinirlanir():
    ev = [oyuncu(f"P{i}", deger=1000 - i) for i in range(30)]
    assert len(takimlar(eksik_oyuncular(detay(ev=ev, dep=[]), "E", "D"))["E"]["players"]) == 15
