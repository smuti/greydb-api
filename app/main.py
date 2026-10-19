"""
GreyDB API - Futbol Maç Verileri ve İstatistikleri
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers import form, h2h, predictions, coupons, match_comments, feedback, skorjin, leagues, tracked_leagues, analysis_matches, standings, match_detail, match_context, match_data, spor_toto, odds_snapshots, player_stats, iddaa, kuponlar, kupon_sonuclari, veri_ozeti

settings = get_settings()

# FastAPI app
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=settings.api_description,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS - Tüm origin'lere izin ver
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  # "*" ile credentials kullanılamaz
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(form.router, prefix="/api")
app.include_router(h2h.router, prefix="/api")
app.include_router(predictions.router, prefix="/api")
app.include_router(coupons.router, prefix="/api")
app.include_router(match_comments.router, prefix="/api")
app.include_router(feedback.router, prefix="/api")
app.include_router(skorjin.router, prefix="/api")
app.include_router(leagues.router, prefix="/api")
app.include_router(tracked_leagues.router, prefix="/api")
app.include_router(analysis_matches.router, prefix="/api")
app.include_router(standings.router, prefix="/api")
app.include_router(match_detail.router, prefix="/api")
app.include_router(match_context.router, prefix="/api")
app.include_router(match_data.router, prefix="/api")
app.include_router(veri_ozeti.router, prefix="/api")
app.include_router(spor_toto.router, prefix="/api")
app.include_router(odds_snapshots.router, prefix="/api")
app.include_router(iddaa.router, prefix="/api")
app.include_router(kupon_sonuclari.router, prefix="/api")   # /kuponlar/{id}'den önce
app.include_router(kuponlar.router, prefix="/api")
app.include_router(player_stats.router, prefix="/api")


@app.get("/")
async def root():
    """API bilgisi"""
    return {
        "name": settings.api_title,
        "version": settings.api_version,
        "docs": "/docs",
        "endpoints": {
            "form": "/api/form/{team_fotmob_id}",
            "h2h": "/api/h2h/{team1}/{team2}",
            "predictions": "/api/predictions",
            "feedback": "/api/feedback"
        }
    }


@app.get("/health")
async def health():
    """Health check"""
    return {"status": "ok"}


# Takım ID'leri referans
TEAM_IDS = {
    "Galatasaray": 8637,
    "Fenerbahçe": 8695,
    "Beşiktaş": 6437,
    "Trabzonspor": 9752,
    "Başakşehir": 7410,
}

LEAGUE_IDS = {
    "Süper Lig": 71,
    "Premier League": 47,
    "La Liga": 87,
    "Serie A": 55,
    "Bundesliga": 54,
    "Ligue 1": 53,
    "Champions League": 42,
    "Europa League": 73,
}


@app.get("/api/reference")
async def reference():
    """Takım ve Lig ID referansları"""
    return {
        "teams": TEAM_IDS,
        "leagues": LEAGUE_IDS
    }



@app.on_event("startup")
def veri_ozetini_isit():
    """Sunucu açılırken veri özetini arka planda hazırlar: ilk ziyaretçi 36 milyon değerin sayılmasını beklemesin."""
    import threading

    from app.services.db import execute_query
    from app.services.veri_ozeti import isin, ozet_olustur
    threading.Thread(target=lambda: isin(lambda: ozet_olustur(execute_query)), daemon=True).start()
