"""iddaa maç kimliğini (iddaa_event_id) kendi maç kaydımıza (fotmob_match_id) bağlar: takım adı benzerliği + başlama saati.
iddaa listesi yaklaşık 3 günlük pencere verir ve adları farklı yazar ("Bayern Münih", "Sporting Lizbon"): bu yüzden düzenli ve benzerlikle çalışır."""
from app.services.iddaa_eslestir import normalize_takim, takim_benzer, eslestir

T0 = 1792522800   # iddaa 'd' (saniye, UTC)


def ev(i, hn, an, d=T0, il=False):
    return {"i": i, "hn": hn, "an": an, "d": d, "il": il, "bri": i + 1000}


def mac(fid, h, a, d=T0):
    return {"fotmob_match_id": fid, "home": h, "away": a, "kickoff_ts": d}


# ---- Ad normalleştirme ve benzerlik ----
def test_normalize_aksan_noktalama_ve_dolgu_kelimeleri():
    assert normalize_takim("Paris Saint-Germain") == normalize_takim("Paris Saint Germain")
    assert normalize_takim("Beşiktaş") == "besiktas"
    assert normalize_takim("FC Porto") == normalize_takim("Porto")
    assert normalize_takim("Atlético Madrid") == normalize_takim("Atletico Madrid")
    assert normalize_takim("1. FC Köln") == normalize_takim("Köln")


def test_benzer_takim_adlari_ayni_takim_sayilir():
    for a, b in [("Bayern München", "Bayern Münih"), ("Tottenham Hotspur", "Tottenham"), ("Sporting CP", "Sporting Lizbon"),
                 ("Club Brugge", "Club Brugge"), ("Como", "Como 1907"), ("Napoli", "Napoli"), ("Real Madrid", "Real Madrid"),
                 ("Rodina", "Rodina Moscow"), ("Getafe", "Getafe"), ("Fenerbahçe", "Fenerbahce")]:
        assert takim_benzer(a, b), (a, b)


def test_farkli_takimlar_benzer_sayilmaz():
    for a, b in [("Real Madrid", "Real Sociedad"), ("Inter", "Inter Miami"), ("Manchester City", "Manchester United"),
                 ("Milan", "Inter"), ("Atletico Madrid", "Real Madrid"), ("Lyon", "Nice"), ("Arsenal", "Arsenal Tula")]:
        assert not takim_benzer(a, b), (a, b)


def test_kadin_ve_genclik_takimi_erkek_takimiyla_karismaz():
    assert not takim_benzer("Juventus", "Juventus U23")
    assert not takim_benzer("Bayern München", "Bayern Münih (K)")
    assert takim_benzer("Juventus U23", "Juventus U23")


# ---- Eşleştirme ----
def test_ad_benzerligi_ve_saat_ile_eslesir():
    r = eslestir([ev(111, "Bayern Münih", "Arsenal")], [mac(5, "Bayern München", "Arsenal")])
    assert r["eslesen"] == {5: 111} and r["belirsiz"] == [] and r["eslesmeyen"] == []


def test_saat_toleransi_disindaysa_eslesmez():
    r = eslestir([ev(111, "Napoli", "Roma", d=T0)], [mac(5, "Napoli", "Roma", d=T0 + 3 * 3600)])
    assert r["eslesen"] == {} and [m["fotmob_match_id"] for m in r["eslesmeyen"]] == [5]


def test_ayni_takimlar_iki_kez_oynuyorsa_saate_gore_dogru_olay_secilir():
    r = eslestir([ev(1, "Napoli", "Roma", d=T0), ev(2, "Napoli", "Roma", d=T0 + 7 * 86400)], [mac(5, "Napoli", "Roma", d=T0 + 7 * 86400)])
    assert r["eslesen"] == {5: 2}


def test_belirsizse_yazilmaz():
    """Aynı saat ve benzer adlı iki iddaa olayı: yanlış bağlamak yerine atla."""
    r = eslestir([ev(1, "Napoli", "Roma"), ev(2, "Napoli", "Roma")], [mac(5, "Napoli", "Roma")])
    assert r["eslesen"] == {} and [b["fotmob_match_id"] for b in r["belirsiz"]] == [5]


def test_bir_iddaa_olayi_en_fazla_bir_maca_baglanir():
    r = eslestir([ev(1, "Napoli", "Roma")], [mac(5, "Napoli", "Roma"), mac(6, "Napoli", "Roma")])
    assert len(r["eslesen"]) <= 1


def test_ev_deplasman_ters_ise_eslesmez():
    r = eslestir([ev(1, "Roma", "Napoli")], [mac(5, "Napoli", "Roma")])
    assert r["eslesen"] == {}


def test_bos_girdiler_cokmez():
    assert eslestir([], []) == {"eslesen": {}, "belirsiz": [], "eslesmeyen": []}
    assert eslestir([ev(1, "A", "B")], [])["eslesen"] == {}
    assert len(eslestir([], [mac(5, "A", "B")])["eslesmeyen"]) == 1


# ---- Gerçek veriden çıkan yazım farkları ----
def test_gercek_veriden_yazim_farklari_eslesir():
    for a, b in [("PSV Eindhoven", "PSV"), ("Lens", "RC Lens"), ("Borussia Dortmund", "B. Dortmund"),
                 ("Baltika", "FC Baltika Kaliningrad"), ("Tottenham Hotspur", "Tottenham ")]:
        assert takim_benzer(a, b), (a, b)


def test_sondaki_b_hala_ikinci_takim_isaretidir():
    assert not takim_benzer("Real Madrid", "Real Madrid B")


def test_ayni_sportradar_kimligi_iki_listede_tek_olay_sayilir():
    # iddaa aynı maçı program listesinde (3222...) ve canlı listesinde (3320...) iki ayrı 'i' ile verir; ikisi de aynı 'bri'
    olaylar = [dict(ev(3222006, "Tottenham", "Coventry"), bri=7), dict(ev(3320626, "Tottenham ", "Coventry"), bri=7)]
    s = eslestir(olaylar, [mac(1, "Tottenham Hotspur", "Coventry City")])
    assert s["eslesen"] == {1: 3320626} and not s["belirsiz"]   # canlı listedeki kimlik (iddaa.com/program/canli?d=...)


def test_sehir_eki_olan_kulup_adi_eslesir():
    assert takim_benzer("Guabirá", "Guabira Montero")
    assert not takim_benzer("Guabirá", "Guabira Cochabamba")   # farklı ek = farklı takım olabilir
