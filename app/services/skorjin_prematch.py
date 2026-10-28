"""Skorjin maç öncesi anlık görüntüsü (greydb.match_prematch): saf dönüşümler.

Skorjin (maç öncesi cevap servisi) veritabanını salt-okunur okur ve bu tablodan cevap kurar; sayıları kendisi uydurmaz.
Burası ham girdileri (maç satırları, iddaa olayı, matchforge sonucu) tablonun JSON biçimine çevirir. I/O yoktur.
Biçim sözleşmesi Skorjin deposunda: docs/prematch-snapshot.md.
"""
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from app.services.iddaa_dagilim import _ad as market_adi
from app.services.skorjin_hakem import hakem_alani, hakem_bilgisi

MS_MARKET = "Maç Sonucu"
ALT_UST_25 = "Alt/Üst 2.5"
_MS_SECIM = {"1": "home", "0": "draw", "2": "away"}


def _gun(tarih) -> str:
    return tarih.date().isoformat()


def _skorlu(s: Dict[str, Any]) -> bool:
    return s.get("home_score") is not None and s.get("away_score") is not None


_METRIKLER = (("shots", "sut"), ("sot", "isabetli"), ("corners", "korner"), ("yellow", "sari"))


def _istatistik(s: Dict[str, Any], ev_mi: bool) -> Optional[Dict[str, List[int]]]:
    """Maçın takım penceresinden istatistiği: {metrik: [takımın kendisi, rakip]}; kaynakta yoksa None.

    FotMob verisi alınamadığında kayıt her şeyi 0 yazar; şut ve korner toplamı 0 ise "veri yok" sayılır (0 ortalama uydurulmaz).
    Sarı kartın 0 olması meşrudur, bu yüzden geçerlilik kapısı şut ve kornerdir.
    """
    if any(s.get(f"{taraf}_{kolon}") is None for _, kolon in _METRIKLER for taraf in ("ev", "dep")):
        return None
    if sum(s[f"{t}_{k}"] for k in ("sut", "korner") for t in ("ev", "dep")) <= 0:
        return None
    ben, rakip = ("ev", "dep") if ev_mi else ("dep", "ev")
    return {anahtar: [int(s[f"{ben}_{kolon}"]), int(s[f"{rakip}_{kolon}"])] for anahtar, kolon in _METRIKLER}


def takim_maclari(satirlar: List[Dict[str, Any]], takim_ic_id: Any) -> List[Dict[str, Any]]:
    """Takımın bitmiş maçları, takımın penceresinden (v: H ev / A deplasman). Sıra girdideki gibi (en yeniden eskiye)."""
    cikti = []
    for s in satirlar:
        if not _skorlu(s):
            continue
        ev_mi = s["home_team_id"] == takim_ic_id
        mac = {
            "d": _gun(s["tarih"]), "v": "H" if ev_mi else "A",
            "gf": s["home_score"] if ev_mi else s["away_score"], "ga": s["away_score"] if ev_mi else s["home_score"],
            "opp": s["dep_ad"] if ev_mi else s["ev_ad"],
        }
        st = _istatistik(s, ev_mi)
        if st:
            mac["st"] = st
        cikti.append(mac)
    return cikti


def h2h_listesi(satirlar: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """İki takım arasındaki bitmiş maçlar, oynandığı gibi (ev/deplasman adları o maçtakiyle)."""
    return [{"d": _gun(s["tarih"]), "home": s["ev_ad"], "away": s["dep_ad"], "hg": s["home_score"], "ag": s["away_score"]}
            for s in satirlar if _skorlu(s)]


def _market(olay: Optional[Dict[str, Any]], cfg: Dict[str, Any], ad: str) -> Optional[Dict[str, Any]]:
    for m in (olay or {}).get("m", []):
        if market_adi(m, cfg) == ad:
            return m
    return None


def iddaa_oranlari(olay: Optional[Dict[str, Any]], cfg: Dict[str, Any]) -> Optional[Dict[str, Optional[float]]]:
    """iddaa olayından Maç Sonucu (1-0-2) ve 2.5 Alt/Üst oranları. Maç Sonucu yoksa ya da oran geçersizse None (uydurma yok)."""
    ms = _market(olay, cfg, MS_MARKET)
    if ms is None:
        return None
    oran = {_MS_SECIM[o["n"]]: o["odd"] for o in ms.get("o", []) if o.get("n") in _MS_SECIM}
    if set(oran) != {"home", "draw", "away"} or any(v is None or v < 1.0 for v in oran.values()):
        return None
    au = _market(olay, cfg, ALT_UST_25)
    secim = {o["n"]: o["odd"] for o in (au or {}).get("o", [])}
    ust, alt = secim.get("Üst"), secim.get("Alt")
    gecerli = ust is not None and alt is not None and ust >= 1.0 and alt >= 1.0
    return {**oran, "over25": ust if gecerli else None, "under25": alt if gecerli else None}


def iddaa_kalabalik(olay: Optional[Dict[str, Any]], yuzdeler: Dict[str, Dict[str, float]], cfg: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """Maç Sonucu seçimlerinin oynanma yüzdesi, market içinde normalize (toplam 1). Veri yoksa ya da toplam 0 ise None."""
    ms = _market(olay, cfg, MS_MARKET)
    y = (yuzdeler or {}).get(str(ms.get("i"))) if ms else None
    if not y:
        return None
    ham = {}
    for o in ms.get("o", []):
        taraf = _MS_SECIM.get(o.get("n"))
        if taraf is not None and y.get(str(o.get("no"))) is not None:
            ham[taraf] = float(y[str(o["no"])])
    toplam = sum(ham.values())
    if set(ham) != {"home", "draw", "away"} or toplam <= 0:
        return None
    return {k: round(v / toplam, 4) for k, v in ham.items()}


def model_verisi(sonuc: Optional[Dict[str, Any]], model_surumu: str) -> Optional[Dict[str, Any]]:
    """matchforge /api/ms/predict sonucu (include_matrix) -> skor matrisi + harman 1-X-2. Matris yoksa None."""
    if not sonuc or not sonuc.get("score_matrix"):
        return None
    return {"matrix": sonuc["score_matrix"], "blend": {"home": sonuc["p1"], "draw": sonuc["p0"], "away": sonuc["p2"]},
            "model_version": sonuc.get("model_version") or model_surumu}


# ---------------------------------------------------------------- eksik oyuncular (FotMob matchDetails)
KAYNAK_FOTMOB = "FotMob"
EKSIK_ORAN_SINIRI = 15            # takım başına en fazla (piyasa değeri en yüksekler)
_AY = {"January": "Ocak", "February": "Şubat", "March": "Mart", "April": "Nisan", "May": "Mayıs", "June": "Haziran", "July": "Temmuz",
       "August": "Ağustos", "September": "Eylül", "October": "Ekim", "November": "Kasım", "December": "Aralık"}
_DONUS_SABIT = {"Day to day": "günü gününe", "A few days": "birkaç gün", "Out for season": "sezon sonuna kadar",
                "Back in training": "antrenmanlara döndü"}
_DONUS_SUPHELI = {"Doubtful", "Day to day"}
_DONUS_AY = re.compile(r"^(Early|Mid|Late) (\w+) (\d{4})$")
_DONUS_HAFTA = re.compile(r"^About (\d+(?:-\d+)?) weeks?$")
_DONUS_ZAMAN = {"Early": "başı", "Mid": "ortası", "Late": "sonu"}


def donus_metni(ham: Optional[str]) -> Optional[str]:
    """FotMob'un İngilizce dönüş tarihi -> Türkçe. Tanınmayan/bilgisiz ("Unknown", "Doubtful") metin yazılmaz (None)."""
    if not ham:
        return None
    if ham in _DONUS_SABIT:
        return _DONUS_SABIT[ham]
    m = _DONUS_AY.match(ham)
    if m and m.group(2) in _AY:
        return f"{_AY[m.group(2)]} {m.group(3)} {_DONUS_ZAMAN[m.group(1)]}"
    m = _DONUS_HAFTA.match(ham)
    return f"yaklaşık {m.group(1)} hafta" if m else None


def _oyuncular(takim: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cikti = []
    for k in (takim or {}).get("unavailable") or []:
        if not k or not k.get("name"):
            continue
        u = k.get("unavailability") or {}
        if u.get("type") not in ("injury", "suspension"):
            continue                                            # milli takım vb.: eksik sayılmaz
        donus = u.get("expectedReturn")
        durum = "cezali" if u["type"] == "suspension" else "supheli" if donus in _DONUS_SUPHELI else "sakat"
        cikti.append(({"name": k["name"], "status": durum, "reason": None, "expected_return": donus_metni(donus)}, k.get("marketValue")))
    cikti.sort(key=lambda x: -(x[1] if x[1] is not None else -1))       # sorted kararlı: eşit değerde kaynak sırası
    return [p for p, _ in cikti[:EKSIK_ORAN_SINIRI]]


def eksik_oyuncular(detay: Optional[Dict[str, Any]], ev_ad: str, dep_ad: str) -> Optional[Dict[str, Any]]:
    """FotMob matchDetails (yaklaşan maç) -> Skorjin `absences` JSON'u. Takım adları bizim veritabanındaki yazımla.
    Kapsam yoksa None ("bilgi yok"): lineup hiç yok ya da iki takımın listesi de boş (boş liste "eksik yok" demek olamaz;
    FotMob küçük liglerde eksik takip etmiyor). Bir takımda kayıt varsa diğerinin boş listesi "kayıtlı eksik yok" sayılır."""
    lineup = ((detay or {}).get("content") or {}).get("lineup")
    if not isinstance(lineup, dict):
        return None
    ev, dep = _oyuncular(lineup.get("homeTeam")), _oyuncular(lineup.get("awayTeam"))
    if not ev and not dep:
        return None
    return {"source": KAYNAK_FOTMOB, "teams": [{"name": ev_ad, "info_available": True, "players": ev},
                                                {"name": dep_ad, "info_available": True, "players": dep}]}


# ---------------------------------------------------------------- orkestrasyon (kaynaklar enjekte edilir)
KAPSAMLAR = ("gunluk", "oran")
MODEL_LIGLERI = {4, 5, 6, 7, 8, 9, 12, 20, 21}     # matchforge ms_model kapsamı (SCOPE + EXTRA ligler)


@dataclass(frozen=True)
class Kaynaklar:
    maclar: Callable[[datetime, datetime], List[Dict[str, Any]]]                  # yaklaşan maçlar
    form: Callable[[Any, datetime], List[Dict[str, Any]]]                          # takımın maç öncesi bitmiş maçları (yeniden eskiye)
    h2h: Callable[[Any, Any, datetime], List[Dict[str, Any]]]
    model: Callable[[List[Dict[str, Any]]], Dict[int, Dict[str, Any]]]             # fotmob_match_id -> matchforge sonucu
    iddaa: Callable[[int], Tuple[Optional[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]]   # (olay, yüzdeler, market yapılandırması)
    mevcut: Callable[[List[int]], Set[int]]                                        # tabloda satırı olan maç kimlikleri
    yaz: Callable[[str, Dict[str, Any]], None]
    fotmob: Callable[[int], Optional[Dict[str, Any]]]                              # fotmob_match_id -> matchDetails (hata: istisna)
    sezon: Optional[Callable[[Any, Any, Any, datetime], Dict[str, Any]]] = None    # (lig, ev, dep, maç günü) -> season/league_avg/season_matches
    alias: Optional[Callable[[bool], Dict[str, Any]]] = None      # (dry_run) -> hakem alias senkron raporu
    program: Optional[Callable[[Any, datetime], Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]]] = None   # (takım, maç günü) -> (schedule, rotation)
    hakem: Optional[Callable[[str, Optional[str], Any, datetime], Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]]] = None   # (ad, ülke, lig, maç günü) -> (hakem, lig istatistiği)
    karne: Optional[Callable[[Dict[str, Any]], None]] = None      # gol karnesi kaydı (greydb.gol_karne_tahmin); yoksa karne tutulmaz
    son_oran_kaydi: Optional[Callable[[int], Optional[datetime]]] = None   # iddaa kimliği -> son otomatik oran kaydının zamanı (UTC, naive)
    oran_kaydet: Optional[Callable[[Dict[str, Any], Dict[str, Any]], None]] = None   # (maç, ham iddaa olayı) -> greydb.odds_snapshots'a yeni satır


def _ad(m: Dict[str, Any]) -> str:
    return f"{m['home_name']} - {m['away_name']}"


def _model_yok_nedeni(m: Dict[str, Any], sonuc: Optional[Dict[str, Any]]) -> str:
    """Matris neden yok: lig model kapsamı dışı / takım modelde yeni / matchforge matris vermedi."""
    if m["league_id"] not in MODEL_LIGLERI:
        return "kapsam_disi"
    if sonuc and (sonuc.get("new_team_home") or sonuc.get("new_team_away")):
        return "yeni_takim"
    return "matris_yok"


def _karne_kaydet(m: Dict[str, Any], sonuc: Optional[Dict[str, Any]], k: Kaynaklar, dry_run: bool, simdi: datetime, rapor: Dict[str, Any]) -> None:
    """Gol karnesi: matris varsa greydb.gol_karne_tahmin'e kaydı düşer (kuru çalışmada yalnız sayar). Hata ana satırı bozmaz."""
    from app.services.gol_karne import tahmin_satiri
    kayit = tahmin_satiri(m, sonuc, simdi)
    if kayit is None:
        return
    if dry_run:
        rapor["karne_kaydi"] += 1
        return
    try:
        k.karne(kayit)
        rapor["karne_kaydi"] += 1
    except Exception as e:  # noqa: BLE001 — karne hatası anlık görüntüyü durdurmaz
        rapor["hatalar"].append({"fotmob_match_id": m["fotmob_match_id"], "hata": f"karne: {type(e).__name__}: {e}"})


def _model_kullanilabilir(m: Dict[str, Any], sonuc: Optional[Dict[str, Any]]) -> bool:
    return bool(sonuc) and not sonuc.get("new_team_home") and not sonuc.get("new_team_away")


# Oran geçmişi (otomatik): ham iddaa olayı greydb.odds_snapshots'a, maça kalan süreye göre aralıkla (saat): >24 → 12, 6–24 → 3, ≤6 → 1.
ORAN_KAYDI_KAYNAK = "otomatik:prematch"
ORAN_KAYDI_TOLERANS_SAAT = 5 / 60          # 30 dakikalık zamanlamanın oynaması
ORAN_KAYDI_SQL = """INSERT INTO greydb.odds_snapshots (iddaa_match_id, home_team, away_team, total_markets, raw_data, interpretation, created_by_email)
VALUES (:eid, :ev, :dep, :n, CAST(:ham AS jsonb), NULL, :kaynak)"""
SON_ORAN_KAYDI_SQL = "SELECT max(created_at) AS son FROM greydb.odds_snapshots WHERE iddaa_match_id = :eid AND created_by_email = :kaynak"


def oran_kaydi_gerekli(kickoff: datetime, simdi: datetime, son: Optional[datetime]) -> bool:
    """Bu çekimde oran kaydı yazılmalı mı? Başlamış maç için hayır; ilk kayıt her zaman."""
    if kickoff <= simdi:
        return False
    if son is None:
        return True
    kalan = (kickoff - simdi).total_seconds() / 3600
    aralik = 12 if kalan > 24 else 3 if kalan > 6 else 1
    return (simdi - son).total_seconds() / 3600 >= aralik - ORAN_KAYDI_TOLERANS_SAAT


def _oran_kaydi(m: Dict[str, Any], olay: Optional[Dict[str, Any]], k: Kaynaklar, simdi: datetime, rapor: Dict[str, Any]) -> None:
    if not olay or k.oran_kaydet is None:
        return
    try:
        son = k.son_oran_kaydi(m["iddaa_event_id"]) if k.son_oran_kaydi else None
        if oran_kaydi_gerekli(m["match_date"], simdi, son):
            k.oran_kaydet(m, olay)
            rapor["oran_kaydi"] += 1
    except Exception as e:  # noqa: BLE001 — oran geçmişi yazılamazsa ön maç verisi yine hazırlanır
        rapor["hatalar"].append({"fotmob_match_id": m["fotmob_match_id"], "hata": f"oran_kaydi: {type(e).__name__}"})


def _oran_alanlari(m: Dict[str, Any], k: Kaynaklar, simdi: datetime, rapor: Dict[str, Any], dry_run: bool = True) -> Dict[str, Any]:
    """Başarılı çekimde piyasa yoksa None (artık yok); iddaa hatasında ANAHTARLAR OLMAZ (eski değer korunur). Yazan koşuda oran geçmişi de tutulur."""
    try:
        olay, yuzdeler, cfg = k.iddaa(m["iddaa_event_id"])
    except Exception:  # noqa: BLE001 — iddaa erişilemezse mevcut veri bozulmaz
        rapor["iddaa_hatasi"] += 1
        return {}
    if not dry_run:
        _oran_kaydi(m, olay, k, simdi, rapor)
    oran, kalabalik = iddaa_oranlari(olay, cfg), iddaa_kalabalik(olay, yuzdeler, cfg)
    return {"odds": oran, "odds_at": simdi if oran else None, "crowd": kalabalik, "crowd_at": simdi if kalabalik else None}


def hazirla(kapsam: str, dry_run: bool, k: Kaynaklar, simdi: datetime, model_surumu: str,
            ufuk_gun: int = 7, oran_ufku_saat: int = 48) -> Dict[str, Any]:
    """Yaklaşan maçlar için greydb.match_prematch satırlarını hazırlar. `gunluk`: tüm alanlar, `oran`: yalnız oran/oynanma yüzdesi."""
    from datetime import timedelta
    if kapsam not in KAPSAMLAR:
        raise ValueError(f"kapsam gunluk ya da oran olmalı: {kapsam}")
    ufuk = simdi + (timedelta(days=ufuk_gun) if kapsam == "gunluk" else timedelta(hours=oran_ufku_saat))
    maclar = k.maclar(simdi, ufuk)
    rapor: Dict[str, Any] = {"kapsam": kapsam, "dry_run": dry_run, "aday": len(maclar), "hazirlanan": 0, "model_yok": 0,
                             "iddaa_hatasi": 0, "fotmob_hatasi": 0, "sezon_hatasi": 0, "hakem_hatasi": 0, "program_hatasi": 0, "satiri_yok": 0, "iddaa_kimligi_yok": 0, "karne_kaydi": 0, "oran_kaydi": 0,
                             "model_yok_ayrinti": {"kapsam_disi": 0, "yeni_takim": 0, "matris_yok": 0}, "hatalar": [], "maclar": []}

    modeller: Dict[int, Dict[str, Any]] = {}
    if kapsam == "gunluk":
        kapsamdakiler = [m for m in maclar if m["league_id"] in MODEL_LIGLERI]
        if kapsamdakiler:
            try:
                modeller = k.model(kapsamdakiler)
            except Exception as e:  # noqa: BLE001 — model olmadan da form/h2h/oran hazırlanır
                rapor["hatalar"].append({"fotmob_match_id": None, "hata": f"model: {type(e).__name__}"})
    if kapsam == "gunluk" and k.alias is not None:
        try:
            rapor["hakem_alias"] = k.alias(dry_run)
        except Exception as e:  # noqa: BLE001 — alias senkronu başarısızsa kural devrede, iş sürer
            rapor["hatalar"].append({"fotmob_match_id": None, "hata": f"alias: {type(e).__name__}"})
    var = k.mevcut([m["fotmob_match_id"] for m in maclar]) if kapsam == "oran" else set()

    for m in maclar:
        fid = m["fotmob_match_id"]
        try:
            if kapsam == "oran":
                if fid not in var:
                    rapor["satiri_yok"] += 1
                    continue
                if not m.get("iddaa_event_id"):
                    rapor["iddaa_kimligi_yok"] += 1
                alanlar = {**(_oran_alanlari(m, k, simdi, rapor, dry_run) if m.get("iddaa_event_id") else {}), **_eksik_alanlari(m, k, simdi, rapor)}
                if not alanlar:
                    continue
                satir = {"fotmob_match_id": fid, **alanlar}
            else:
                satir = _gunluk_satir(m, k, modeller.get(fid), simdi, model_surumu, rapor, dry_run)
            if not dry_run:
                k.yaz(kapsam, satir)
            if kapsam == "gunluk" and k.karne is not None:
                _karne_kaydet(m, modeller.get(fid), k, dry_run, simdi, rapor)
            rapor["hazirlanan"] += 1
            rapor["maclar"].append({"fotmob_match_id": fid, "mac": _ad(m), "alanlar": sorted(
                a for a in ("form", "h2h", "scoreline", "odds", "crowd", "absences", "referee", "season", "league_avg", "season_matches", "standings", "style", "timing", "schedule", "rotation") if satir.get(a))})
        except Exception as e:  # noqa: BLE001 — tek maçın hatası işi durdurmaz
            rapor["hatalar"].append({"fotmob_match_id": fid, "hata": f"{type(e).__name__}: {e}"})
    return rapor


def _eksik_alanlari(m: Dict[str, Any], k: Kaynaklar, simdi: datetime, rapor: Dict[str, Any]) -> Dict[str, Any]:
    """FotMob matchDetails TEK istekle: eksik oyuncular (`absences`) ve hakem (`referee`). Başarılı çekimde veri yoksa None (bilgi yok);
    FotMob hatasında ANAHTARLAR OLMAZ (eski veri korunur). Hakem istatistiği sorgusu başarısızsa yalnız hakem adı yazılır."""
    try:
        detay = k.fotmob(m["fotmob_match_id"])
        eksik = eksik_oyuncular(detay, m["home_name"], m["away_name"])
    except Exception:  # noqa: BLE001 — FotMob erişilemezse mevcut veri bozulmaz
        rapor["fotmob_hatasi"] += 1
        return {}
    bilgi = hakem_bilgisi(detay)
    ist = lig = None
    if bilgi and k.hakem is not None:
        try:
            ist, lig = k.hakem(bilgi["name"], bilgi.get("country"), m["league_id"], m["match_date"])
        except Exception:  # noqa: BLE001 — istatistik sorgusu başarısızsa hakem adı yine de yazılır
            rapor["hakem_hatasi"] += 1
    hakem = hakem_alani(bilgi, ist, lig)
    return {"absences": eksik, "absences_at": simdi if eksik else None, "absences_source": KAYNAK_FOTMOB if eksik else None,
            "referee": hakem, "referee_at": simdi if hakem else None}


_SEZON_GRUPLARI = ("season", "league_avg", "season_matches", "standings", "style", "timing")


def _sezon_alanlari(m: Dict[str, Any], k: Kaynaklar, simdi: datetime, rapor: Dict[str, Any]) -> Dict[str, Any]:
    """Veri yoksa grup None; kaynak hatasında ANAHTARLAR OLMAZ (eski veri korunur). Kaynak verilmediyse hiçbir şey yazılmaz."""
    if k.sezon is None:
        return {}
    try:
        g = k.sezon(m["league_id"], m["home_team_id"], m["away_team_id"], m["match_date"])
    except Exception:  # noqa: BLE001 — sezon sorgusu başarısızsa mevcut veri bozulmaz
        rapor["sezon_hatasi"] += 1
        return {}
    alanlar: Dict[str, Any] = {}
    for ad in _SEZON_GRUPLARI:
        alanlar[ad] = g.get(ad)
        alanlar[f"{ad}_at"] = simdi if g.get(ad) else None
    return alanlar


def _program_alanlari(m: Dict[str, Any], k: Kaynaklar, simdi: datetime, rapor: Dict[str, Any]) -> Dict[str, Any]:
    """Dinlenme/yoğunluk (`schedule`) ve ilk 11 değişimi (`rotation`), iki takım için. Veri yoksa grup None; kaynak hatasında ANAHTARLAR OLMAZ."""
    if k.program is None:
        return {}
    try:
        (h_s, h_r), (a_s, a_r) = k.program(m["home_team_id"], m["match_date"]), k.program(m["away_team_id"], m["match_date"])
    except Exception:  # noqa: BLE001 — program sorgusu başarısızsa mevcut veri bozulmaz
        rapor["program_hatasi"] += 1
        return {}
    schedule = {"home": h_s, "away": a_s} if (h_s or a_s) else None
    rotation = {"home": h_r, "away": a_r} if (h_r or a_r) else None
    return {"schedule": schedule, "schedule_at": simdi if schedule else None, "rotation": rotation, "rotation_at": simdi if rotation else None}


def _gunluk_satir(m, k, sonuc, simdi, model_surumu, rapor, dry_run=True) -> Dict[str, Any]:
    tarih = m["match_date"]
    ev, dep = m["home_team_id"], m["away_team_id"]
    form = {"home": takim_maclari(k.form(ev, tarih), ev), "away": takim_maclari(k.form(dep, tarih), dep)}
    form = form if (form["home"] or form["away"]) else None
    h2h = h2h_listesi(k.h2h(ev, dep, tarih)) or None
    scoreline = model_verisi(sonuc, model_surumu) if _model_kullanilabilir(m, sonuc) else None
    if scoreline is None:
        rapor["model_yok"] += 1
        rapor["model_yok_ayrinti"][_model_yok_nedeni(m, sonuc)] += 1
    oran = _oran_alanlari(m, k, simdi, rapor, dry_run) if m.get("iddaa_event_id") else {"odds": None, "odds_at": None, "crowd": None, "crowd_at": None}
    eksik = _eksik_alanlari(m, k, simdi, rapor)
    return {
        "fotmob_match_id": m["fotmob_match_id"], "iddaa_event_id": m.get("iddaa_event_id"),
        "home_team_id": ev, "away_team_id": dep, "home_name": m["home_name"], "away_name": m["away_name"],
        "league_id": m["league_id"], "league_name": m["league_name"], "kickoff": tarih,
        "form": form, "form_at": simdi if form else None,
        "h2h": h2h, "h2h_at": simdi if h2h else None,
        "scoreline": scoreline, "scoreline_at": simdi if scoreline else None,
        **oran, **eksik, **_sezon_alanlari(m, k, simdi, rapor), **_program_alanlari(m, k, simdi, rapor),
    }


# ---------------------------------------------------------------- tablo, sorgular, gerçek kaynaklar
TABLO_DDL = """
CREATE TABLE IF NOT EXISTS greydb.match_prematch (
    fotmob_match_id BIGINT PRIMARY KEY,
    iddaa_event_id BIGINT,
    home_team_id INTEGER, away_team_id INTEGER,
    home_name TEXT NOT NULL, away_name TEXT NOT NULL,
    league_id INTEGER, league_name TEXT,
    kickoff TIMESTAMPTZ NOT NULL,
    form JSONB, form_at TIMESTAMPTZ,
    h2h JSONB, h2h_at TIMESTAMPTZ,
    scoreline JSONB, scoreline_at TIMESTAMPTZ,
    odds JSONB, odds_at TIMESTAMPTZ,
    crowd JSONB, crowd_at TIMESTAMPTZ,
    absences JSONB, absences_at TIMESTAMPTZ, absences_source TEXT,
    referee JSONB, referee_at TIMESTAMPTZ,
    season JSONB, season_at TIMESTAMPTZ,
    league_avg JSONB, league_avg_at TIMESTAMPTZ,
    season_matches JSONB, season_matches_at TIMESTAMPTZ,
    standings JSONB, standings_at TIMESTAMPTZ,
    style JSONB, style_at TIMESTAMPTZ,
    timing JSONB, timing_at TIMESTAMPTZ,
    schedule JSONB, schedule_at TIMESTAMPTZ,
    rotation JSONB, rotation_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_match_prematch_kickoff ON greydb.match_prematch (kickoff);
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS referee JSONB;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS referee_at TIMESTAMPTZ;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS season JSONB;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS season_at TIMESTAMPTZ;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS league_avg JSONB;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS league_avg_at TIMESTAMPTZ;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS style JSONB;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS style_at TIMESTAMPTZ;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS timing JSONB;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS timing_at TIMESTAMPTZ;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS schedule JSONB;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS schedule_at TIMESTAMPTZ;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS rotation JSONB;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS rotation_at TIMESTAMPTZ;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS standings JSONB;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS standings_at TIMESTAMPTZ;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS season_matches JSONB;
ALTER TABLE greydb.match_prematch ADD COLUMN IF NOT EXISTS season_matches_at TIMESTAMPTZ;
"""

_KIMLIK = ("iddaa_event_id", "home_team_id", "away_team_id", "home_name", "away_name", "league_id", "league_name", "kickoff")
_ZAMANLI = ("form", "h2h", "scoreline", "odds", "crowd", "absences")
_SEZON = ("season", "league_avg", "season_matches", "standings", "style", "timing", "schedule", "rotation")
_JSON = set(_ZAMANLI) | set(_SEZON) | {"referee"}
_EKSIK = ("absences", "absences_at", "absences_source", "referee", "referee_at")
_KOLONLAR = {
    "gunluk": _KIMLIK + tuple(c for g in _ZAMANLI[:-1] + _SEZON for c in (g, f"{g}_at")) + _EKSIK,
    "oran": ("odds", "odds_at", "crowd", "crowd_at") + _EKSIK,
}


def _js(x):
    return None if x is None else json.dumps(x, ensure_ascii=False)


def yazma_sorgusu(kapsam: str, satir: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """gunluk: satırı oluşturur/günceller (eksik oyuncu alanlarına dokunmaz); oran: yalnız oran alanlarını günceller, satır oluşturmaz."""
    kolonlar = tuple(c for c in _KOLONLAR[kapsam] if c in satir)        # satırda olmayan grup (kaynak hatası) sorguya girmez: eski veri korunur
    params = {"fotmob_match_id": satir["fotmob_match_id"], **{c: (_js(satir.get(c)) if c in _JSON else satir.get(c)) for c in kolonlar}}
    deger = lambda c: f"CAST(:{c} AS jsonb)" if c in _JSON else f":{c}"   # noqa: E731
    if kapsam == "oran":
        atama = ", ".join(f"{c} = {deger(c)}" for c in kolonlar)
        return f"UPDATE greydb.match_prematch SET {atama}, updated_at = now() WHERE fotmob_match_id = :fotmob_match_id", params
    sutunlar = ", ".join(("fotmob_match_id",) + kolonlar)
    degerler = ", ".join(["CAST(:fotmob_match_id AS BIGINT)"] + [deger(c) for c in kolonlar])
    guncelle = ", ".join(f"{c} = EXCLUDED.{c}" for c in kolonlar)
    return (f"INSERT INTO greydb.match_prematch ({sutunlar}, updated_at) VALUES ({degerler}, now()) "
            f"ON CONFLICT (fotmob_match_id) DO UPDATE SET {guncelle}, updated_at = now()"), params


# Maça kadar, bitmiş ve skorlu maçlar (match_context ile aynı süzgeç: gelecek sonuç sızmaz)
_ORTAK = """
SELECT m.match_date AS tarih, m.home_team_id, m.away_team_id, m.home_score, m.away_score, ev.name AS ev_ad, dep.name AS dep_ad
FROM public.matches m
JOIN public.teams ev ON ev.id = m.home_team_id
JOIN public.teams dep ON dep.id = m.away_team_id
"""
_FILTRE = "AND m.finished AND NOT COALESCE(m.cancelled, false) AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL AND m.match_date < :tarih"
_FORM_ORTAK = _ORTAK.replace(
    "dep.name AS dep_ad", "dep.name AS dep_ad,\n"
    "       st.home_shots AS ev_sut, st.away_shots AS dep_sut, st.home_shots_on_target AS ev_isabetli, st.away_shots_on_target AS dep_isabetli,\n"
    "       st.home_corners AS ev_korner, st.away_corners AS dep_korner, st.home_yellow_cards AS ev_sari, st.away_yellow_cards AS dep_sari"
) + "LEFT JOIN public.match_stats st ON st.match_id = m.id\n"
FORM_SQL = f"{_FORM_ORTAK} WHERE (m.home_team_id = :t OR m.away_team_id = :t) {_FILTRE} ORDER BY m.match_date DESC LIMIT 20"
H2H_SQL = (f"{_ORTAK} WHERE ((m.home_team_id = :a AND m.away_team_id = :b) OR (m.home_team_id = :b AND m.away_team_id = :a)) "
           f"{_FILTRE} ORDER BY m.match_date DESC LIMIT 10")
MACLAR_SQL = """
SELECT u.fotmob_match_id, u.iddaa_event_id, u.home_team_id, u.away_team_id, ev.name AS home_name, dep.name AS away_name,
       u.league_id, l.name AS league_name, u.match_date
FROM public.upcoming_matches u
JOIN public.teams ev ON ev.id = u.home_team_id
JOIN public.teams dep ON dep.id = u.away_team_id
LEFT JOIN public.leagues l ON l.id = u.league_id
WHERE u.fotmob_match_id IS NOT NULL AND u.match_date > :bas AND u.match_date <= :son
ORDER BY u.match_date
"""


def tablo_hazirla() -> None:
    from app.services.db import execute_insert
    from app.services.gol_karne import TABLO_DDL as KARNE_DDL
    from app.services.skorjin_hakem import ALIAS_DDL
    for parca in (TABLO_DDL + ";" + ALIAS_DDL + ";" + KARNE_DDL).split(";"):
        if parca.strip():
            execute_insert(parca)


def _yaklasan(bas: datetime, son: datetime) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    tekil: Dict[Any, Dict[str, Any]] = {}
    for m in execute_query(MACLAR_SQL, {"bas": bas, "son": son}):      # aynı maç iki kez gelirse en güncel saatli olan
        if m["fotmob_match_id"] not in tekil or m["match_date"] > tekil[m["fotmob_match_id"]]["match_date"]:
            tekil[m["fotmob_match_id"]] = m
    return list(tekil.values())


def _model(maclar: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """matchforge /api/ms/predict (tek toplu çağrı, skor matrisiyle). as_of = en erken maç günü: gelecek sonuç sızmaz."""
    import httpx
    from app.services.skorjin_match_analysis import MATCHFORGE_BASE
    yuk = {"include_matrix": True, "matches": [
        {"home_team_id": int(m["home_team_id"]), "away_team_id": int(m["away_team_id"]), "league_id": m["league_id"],
         "match_date": m["match_date"].isoformat(), "home_name": m["home_name"], "away_name": m["away_name"]} for m in maclar]}
    yanit = httpx.post(f"{MATCHFORGE_BASE}/api/ms/predict", json=yuk, timeout=120.0)
    yanit.raise_for_status()
    govde = yanit.json()
    return {m["fotmob_match_id"]: {**r, "model_version": govde.get("model_version")} for m, r in zip(maclar, govde.get("results", []))}


def _iddaa(event_id: int):
    from app.routers.iddaa import iddaa_dagilim_verisi
    olay, yuzdeler, _pay, cfg = iddaa_dagilim_verisi(event_id)
    return olay, yuzdeler, cfg


def _mevcut(ids: List[int]) -> Set[int]:
    from app.services.db import execute_query
    if not ids:
        return set()
    try:
        return {r["fotmob_match_id"] for r in execute_query(
            "SELECT fotmob_match_id FROM greydb.match_prematch WHERE fotmob_match_id = ANY(:ids)", {"ids": list(ids)})}
    except Exception:  # noqa: BLE001 — tablo henüz yoksa (kuru çalıştırma) hiçbir satır yok
        return set()


def _fotmob(fotmob_match_id: int) -> Dict[str, Any]:
    """FotMob matchDetails; erişilemezse (istek hatası/200 dışı) istisna: çağıran eski veriyi korur."""
    from app.services.yayin_toplayici import fotmob_detay
    detay = fotmob_detay(fotmob_match_id)
    if detay is None:
        raise RuntimeError(f"FotMob yanıt vermedi: {fotmob_match_id}")
    return detay


def _karne_yaz(satir: Dict[str, Any]) -> None:
    from app.services.db import execute_insert
    from app.services.gol_karne import yazma_sorgusu as karne_sorgusu
    sql, params = karne_sorgusu(satir)
    execute_insert(sql, params)


def _yaz(kapsam: str, satir: Dict[str, Any]) -> None:
    from app.services.db import execute_insert
    sql, params = yazma_sorgusu(kapsam, satir)
    execute_insert(sql, params)


def _sezon_kaynagi():
    from app.services.skorjin_sezon import sezon_kaynagi
    return sezon_kaynagi()


def _alias_senkron(dry_run: bool) -> Dict[str, Any]:
    from app.services.skorjin_hakem import alias_senkronla
    return alias_senkronla(dry_run)


def _program_kaynagi():
    from app.services.skorjin_program import program_kaynagi
    return program_kaynagi()


def _hakem_kaynagi():
    from app.services.skorjin_hakem import hakem_kaynagi
    return hakem_kaynagi()


def gercek_kaynaklar() -> Kaynaklar:
    from app.services.db import execute_query
    return Kaynaklar(
        maclar=_yaklasan,
        form=lambda takim, tarih: execute_query(FORM_SQL, {"t": takim, "tarih": tarih}),
        h2h=lambda a, b, tarih: execute_query(H2H_SQL, {"a": a, "b": b, "tarih": tarih}),
        model=_model, iddaa=_iddaa, mevcut=_mevcut, yaz=_yaz, fotmob=_fotmob, sezon=_sezon_kaynagi(), hakem=_hakem_kaynagi(), program=_program_kaynagi(), alias=_alias_senkron, karne=_karne_yaz,
        son_oran_kaydi=_son_oran_kaydi, oran_kaydet=_oran_kaydet,
    )


def _son_oran_kaydi(eid: int) -> Optional[datetime]:
    from app.services import db
    r = db.execute_query(SON_ORAN_KAYDI_SQL, {"eid": eid, "kaynak": ORAN_KAYDI_KAYNAK})
    son = r[0]["son"] if r else None
    return son.astimezone(timezone.utc).replace(tzinfo=None) if son is not None and son.tzinfo else son


def _oran_kaydet(m: Dict[str, Any], olay: Dict[str, Any]) -> None:
    import json
    from app.services import db
    db.execute_insert(ORAN_KAYDI_SQL, {"eid": m["iddaa_event_id"], "ev": m["home_name"], "dep": m["away_name"], "n": len(olay.get("m") or []),
                                       "ham": json.dumps(olay, ensure_ascii=False), "kaynak": ORAN_KAYDI_KAYNAK})


from contextlib import contextmanager  # noqa: E402

LOCK_ANAHTARI = 7_340_001     # pg_advisory_lock kimliği (Skorjin anlık görüntü işi)


@contextmanager
def tek_kosu(adi: str = "skorjin_prematch"):
    """Aynı anda tek koşu: oturum düzeyinde advisory lock. Kilit alınamazsa False verir (çağıran 409 döner)."""
    from sqlalchemy import text
    from app.services.db import engine
    conn = engine.connect()
    try:
        alindi = bool(conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_ANAHTARI}).scalar())
        try:
            yield alindi
        finally:
            if alindi:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_ANAHTARI})
    finally:
        conn.close()
