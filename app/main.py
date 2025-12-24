"""
GreyDB API - Futbol Maç Verileri ve İstatistikleri
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers import form, h2h, predictions, coupons, match_comments, feedback, skorjin, leagues, match_data

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
app.include_router(match_data.router, prefix="/api")


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

