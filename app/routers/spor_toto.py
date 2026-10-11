"""
Spor Toto Router - haftalık 15 maç için Fuat-tarzı 6 senaryo üretimi ve onayı
"""
from datetime import datetime
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.db import query_to_df, execute_insert
from app.services import spor_toto_fetch as stf

router = APIRouter(tags=["spor-toto"])


# ==================== Şemalar ====================

class FetchWeekRequest(BaseModel):
    game_round_id: int
    hafta_etiketi: Optional[str] = None  # ör. "10. Hafta" - available-rounds'tan secilen round'un kendi adi


class ManualTeamLinkRequest(BaseModel):
    side: str  # "home" | "away"
    matchforge_team_id: int


class SelectionUpdate(BaseModel):
    secilen_senaryo: str
    final_metin: str
    updated_by_email: Optional[str] = None


class VideoLinkUpdate(BaseModel):
    video_linki: str
    updated_by_email: Optional[str] = None


# ==================== Yardımcılar ====================

def _get_or_create_week(game_round_id: int, hafta_etiketi: Optional[str]) -> int:
    existing = query_to_df("SELECT id FROM greydb.spor_toto_weeks WHERE hafta_no = %s", (game_round_id,))
    if not existing.empty:
        return int(existing.iloc[0]["id"])
    row = execute_insert(
        "INSERT INTO greydb.spor_toto_weeks (hafta_no, hafta_etiketi) VALUES (:hafta_no, :etiket) RETURNING id",
        {"hafta_no": game_round_id, "etiket": hafta_etiketi},
    )
    return int(row["id"])


def _get_or_create_match(week_id: int, m: dict) -> tuple[int, bool]:
    """Döner: (match_id, zaten_onaylanmis_mi). "lig" (gerçek lig adı, ör. "Süper Lig")
    burada DEĞİL, _generate_for_match başarılı üretimden sonra yazar - çünkü ancak
    takım eşleşip gerçek lig çözüldükten sonra bilinir. Burada sadece Sportoto'nun
    kendi per-maç round etiketi olan "lig_haftasi" (ör. "8. Hafta") yazılır."""
    existing = query_to_df(
        "SELECT id, final_metin FROM greydb.spor_toto_matches WHERE week_id = %s AND mac_no = %s",
        (week_id, m["mac_no"]),
    )
    if not existing.empty:
        match_id = int(existing.iloc[0]["id"])
        approved = pd.notna(existing.iloc[0]["final_metin"])
        if not approved:
            execute_insert(
                """
                UPDATE greydb.spor_toto_matches
                SET ev_takim = :ev_takim, deplasman_takim = :deplasman_takim, lig_haftasi = :lig_haftasi
                WHERE id = :id
                """,
                {
                    "ev_takim": m["ev_takim"], "deplasman_takim": m["deplasman_takim"],
                    "lig_haftasi": m["lig_haftasi"], "id": match_id,
                },
            )
        return match_id, approved

    row = execute_insert(
        """
        INSERT INTO greydb.spor_toto_matches (week_id, mac_no, ev_takim, deplasman_takim, lig_haftasi)
        VALUES (:week_id, :mac_no, :ev_takim, :deplasman_takim, :lig_haftasi)
        RETURNING id
        """,
        {
            "week_id": week_id, "mac_no": m["mac_no"], "ev_takim": m["ev_takim"],
            "deplasman_takim": m["deplasman_takim"], "lig_haftasi": m["lig_haftasi"],
        },
    )
    return int(row["id"]), False


def _set_match_team_ids(match_id: int, home_team_id: Optional[int], away_team_id: Optional[int], durum: str) -> None:
    execute_insert(
        "UPDATE greydb.spor_toto_matches SET home_team_id = :h, away_team_id = :a, esleme_durumu = :d WHERE id = :id",
        {"h": home_team_id, "a": away_team_id, "d": durum, "id": match_id},
    )


def _save_entries(match_id: int, scenarios: list[dict]) -> None:
    execute_insert("DELETE FROM greydb.spor_toto_entries WHERE match_id = :match_id", {"match_id": match_id})
    for s in scenarios:
        execute_insert(
            "INSERT INTO greydb.spor_toto_entries (match_id, senaryo, metin, onerilen) VALUES (:match_id, :senaryo, :metin, :onerilen)",
            {"match_id": match_id, "senaryo": s["senaryo"], "metin": s["metin"], "onerilen": bool(s["onerilen"])},
        )


def _generate_for_match(match_id: int, home: dict, away: dict, lig_haftasi: Optional[str]) -> None:
    match_data = stf.build_match_data(home, away, lig_haftasi)
    scenarios = stf.generate_scenarios(match_data)
    _save_entries(match_id, scenarios)
    # Gercek lig adi (ör. "Süper Lig") sadece takim-lig eslesmesi basarili olduysa
    # yazilir - build_match_data'nin "lig" alani basarisiz eslesmede lig_haftasi'na
    # (Sportoto'nun per-mac round etiketine, ör. "8. Hafta") duser, onu DB'ye yazmiyoruz,
    # cunku lig_haftasi zaten ayri bir kolonda duruyor.
    home_league = stf.get_team_league(home["team_id"]) if home.get("team_id") else {}
    away_league = stf.get_team_league(away["team_id"]) if away.get("team_id") else {}
    real_lig = home_league.get("league_name") or away_league.get("league_name")
    if real_lig:
        execute_insert(
            "UPDATE greydb.spor_toto_matches SET lig = :lig WHERE id = :id",
            {"lig": real_lig, "id": match_id},
        )


# ==================== Endpoint'ler ====================

@router.get("/spor-toto/available-rounds")
async def available_rounds(year: str = "2026/2027"):
    try:
        rounds = stf.list_available_rounds(year)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Sportoto round listesi alınamadı: {e}")
    return {"rounds": rounds}


@router.post("/spor-toto/weeks/fetch")
async def fetch_week(request: FetchWeekRequest):
    """Sadece HIZLI kısmı yapar: Sportoto'dan 15 maçı çeker, hafta/maç satırlarını
    oluşturur, takımları eşleştirir (cache'den okuduğu için hızlı, LLM çağrısı yok).
    Senaryo üretimi YOK — admin ekranı bu cevapla maç listesini hemen gösterip,
    ardından her maç için ayrı ayrı POST /matches/{id}/generate çağırır, böylece
    sonuçlar tek tek geldikçe ekrana yansır."""
    try:
        sportoto_matches = stf.fetch_round_matches(request.game_round_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Sportoto maç listesi alınamadı: {e}")

    if not sportoto_matches:
        raise HTTPException(status_code=404, detail="Bu round için maç bulunamadı")

    # hafta_etiketi Spor Toto KUPONUNUN kendi haftasi (ör. "10. Hafta", available-rounds'tan
    # secilen round'un adi) - mac'lerin KENDI ligindeki hafta numarasiyla (lig_haftasi,
    # ör. "8. Hafta" Süper Lig icin) KARISTIRILMAMALI, ikisi farkli kavram. Frontend
    # dropdown'dan secileni gondermezse (eski istemciler icin) ilk macin lig_haftasi'na duseriz.
    hafta_etiketi = request.hafta_etiketi or sportoto_matches[0].get("lig_haftasi")
    week_id = _get_or_create_week(request.game_round_id, hafta_etiketi)

    results = []
    for m in sportoto_matches:
        match_id, already_approved = _get_or_create_match(week_id, m)

        if already_approved:
            results.append({"mac_no": m["mac_no"], "match_id": match_id, "durum": "atlandi_onaylanmis"})
            continue

        home = stf.resolve_team(m["sportoto_home_id"], m["ev_takim"]) if m["sportoto_home_id"] else None
        away = stf.resolve_team(m["sportoto_away_id"], m["deplasman_takim"]) if m["sportoto_away_id"] else None

        if not home or not away:
            _set_match_team_ids(
                match_id,
                home.get("team_id") if home else None,
                away.get("team_id") if away else None,
                "eslesmedi",
            )
            results.append({"mac_no": m["mac_no"], "durum": "takim_eslesmedi", "match_id": match_id})
            continue

        _set_match_team_ids(match_id, home["team_id"], away["team_id"], "eslesti")
        results.append({"mac_no": m["mac_no"], "durum": "eslesti_uretim_bekliyor", "match_id": match_id})

    return {"week_id": week_id, "hafta_no": request.game_round_id, "sonuclar": results}


@router.post("/spor-toto/matches/{match_id}/generate")
async def generate_match(match_id: int):
    """Tek bir maç için 6 senaryoyu üretir — /weeks/fetch'in eşleştirdiği her maç
    için admin ekranı bunu sırayla çağırır, her cevap geldikçe o maçın kartı güncellenir."""
    match_df = query_to_df("SELECT * FROM greydb.spor_toto_matches WHERE id = %s", (match_id,))
    if match_df.empty:
        raise HTTPException(status_code=404, detail="Maç bulunamadı")
    m = match_df.iloc[0]

    if pd.notna(m["final_metin"]):
        raise HTTPException(status_code=409, detail="Bu maç zaten onaylanmış, üzerine yazılmaz")
    if pd.isna(m["home_team_id"]) or pd.isna(m["away_team_id"]):
        raise HTTPException(status_code=400, detail="Takımlar henüz eşleşmedi")

    try:
        _generate_for_match(
            match_id,
            {"team_id": int(m["home_team_id"]), "name": m["ev_takim"]},
            {"team_id": int(m["away_team_id"]), "name": m["deplasman_takim"]},
            m["lig_haftasi"],
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Üretim başarısız: {e}")

    return {"message": "Üretildi", "match_id": match_id}


@router.get("/spor-toto/weeks")
async def list_weeks():
    df = query_to_df("SELECT * FROM greydb.spor_toto_weeks ORDER BY hafta_no DESC")
    return {"weeks": df.to_dict(orient="records")}


@router.get("/spor-toto/weeks/{hafta_no}")
async def get_week(hafta_no: int):
    week_df = query_to_df("SELECT * FROM greydb.spor_toto_weeks WHERE hafta_no = %s", (hafta_no,))
    if week_df.empty:
        raise HTTPException(status_code=404, detail="Hafta bulunamadı")
    week_id = int(week_df.iloc[0]["id"])

    matches_df = query_to_df(
        "SELECT * FROM greydb.spor_toto_matches WHERE week_id = %s ORDER BY mac_no", (week_id,)
    )

    matches = []
    for _, mrow in matches_df.iterrows():
        match_id = int(mrow["id"])
        entries_df = query_to_df(
            "SELECT senaryo, metin, onerilen FROM greydb.spor_toto_entries WHERE match_id = %s ORDER BY senaryo",
            (match_id,),
        )
        match_dict = mrow.where(pd.notna(mrow), None).to_dict()
        match_dict["entries"] = entries_df.to_dict(orient="records")
        matches.append(match_dict)

    week_dict = week_df.iloc[0].where(pd.notna(week_df.iloc[0]), None).to_dict()
    week_dict["matches"] = matches
    return week_dict


@router.get("/spor-toto/team-search")
async def team_search(q: str):
    if not q or len(q) < 2:
        raise HTTPException(status_code=400, detail="En az 2 karakter girin")
    return {"teams": stf.search_teams(q)}


@router.post("/spor-toto/matches/{match_id}/manual-team-link")
async def manual_team_link(match_id: int, request: ManualTeamLinkRequest):
    if request.side not in ("home", "away"):
        raise HTTPException(status_code=400, detail="side 'home' veya 'away' olmalı")

    match_df = query_to_df("SELECT * FROM greydb.spor_toto_matches WHERE id = %s", (match_id,))
    if match_df.empty:
        raise HTTPException(status_code=404, detail="Maç bulunamadı")
    match_row = match_df.iloc[0]

    sportoto_name = match_row["ev_takim"] if request.side == "home" else match_row["deplasman_takim"]
    # sportoto_team_id'yi haritada bu maça özel saklamıyoruz; isimle elle eşlerken
    # haritaya isim üzerinden yeni bir anahtar yazmak yerine doğrudan maçın
    # home_team_id/away_team_id alanını güncelliyoruz — gelecek haftalarda aynı
    # isim yine eşleşmezse admin yine bu arama kutusunu kullanır (v1 kapsamı).
    if request.side == "home":
        execute_insert(
            "UPDATE greydb.spor_toto_matches SET home_team_id = :tid WHERE id = :id",
            {"tid": request.matchforge_team_id, "id": match_id},
        )
    else:
        execute_insert(
            "UPDATE greydb.spor_toto_matches SET away_team_id = :tid WHERE id = :id",
            {"tid": request.matchforge_team_id, "id": match_id},
        )

    match_df = query_to_df("SELECT * FROM greydb.spor_toto_matches WHERE id = %s", (match_id,))
    match_row = match_df.iloc[0]
    if pd.notna(match_row["home_team_id"]) and pd.notna(match_row["away_team_id"]):
        execute_insert(
            "UPDATE greydb.spor_toto_matches SET esleme_durumu = 'eslesti' WHERE id = :id", {"id": match_id}
        )
        try:
            _generate_for_match(
                match_id,
                {"team_id": int(match_row["home_team_id"]), "name": match_row["ev_takim"]},
                {"team_id": int(match_row["away_team_id"]), "name": match_row["deplasman_takim"]},
                match_row["lig_haftasi"],
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Eşleştirme kaydedildi ama üretim başarısız: {e}")

    return {"message": "Eşleştirme kaydedildi", "match_id": match_id}


@router.put("/spor-toto/matches/{match_id}/selection")
async def update_selection(match_id: int, update: SelectionUpdate):
    row = execute_insert(
        """
        UPDATE greydb.spor_toto_matches
        SET secilen_senaryo = :senaryo, final_metin = :metin,
            final_metin_guncellenme_at = :ts, final_metin_guncelleyen_email = :email
        WHERE id = :id
        RETURNING id
        """,
        {
            "senaryo": update.secilen_senaryo, "metin": update.final_metin,
            "ts": datetime.now(), "email": update.updated_by_email, "id": match_id,
        },
    )
    if not row:
        raise HTTPException(status_code=404, detail="Maç bulunamadı")
    return {"message": "Kaydedildi", "match_id": match_id}


@router.put("/spor-toto/matches/{match_id}/video")
async def update_video_link(match_id: int, update: VideoLinkUpdate):
    row = execute_insert(
        """
        UPDATE greydb.spor_toto_matches
        SET video_linki = :link, video_eklenme_at = :ts
        WHERE id = :id
        RETURNING id
        """,
        {"link": update.video_linki, "ts": datetime.now(), "id": match_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="Maç bulunamadı")
    return {"message": "Video linki kaydedildi", "match_id": match_id}
