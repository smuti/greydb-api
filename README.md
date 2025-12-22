# GreyDB API

Football statistics API built with FastAPI. Provides team form analysis and head-to-head statistics.

## Features

- **Team Form**: Last N matches (overall, home, away, league-specific)
- **H2H Stats**: Head-to-head history between teams
- **Statistics**: Win/draw/loss, goals, BTTS%, form strings

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/form/{team_fotmob_id}` | Overall form (last 5 matches) |
| `GET /api/form/{team_fotmob_id}/home` | Home form |
| `GET /api/form/{team_fotmob_id}/away` | Away form |
| `GET /api/form/{team_fotmob_id}/league/{league_fotmob_id}` | League-specific form |
| `GET /api/h2h/{team1}/{team2}` | H2H statistics |
| `GET /api/h2h/{team1}/{team2}/home-advantage` | H2H where team1 is home |

## Local Development

```bash
# Create venv
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create .env
cp .env.example .env
# Edit .env with your DATABASE_URL

# Run
uvicorn app.main:app --reload --port 8000
```

## Docker

```bash
docker build -t greydb-api .
docker run -p 8000:8000 -e DATABASE_URL=your_db_url greydb-api
```

## Deploy to Railway

1. Push to GitHub
2. Create new Railway service from GitHub repo
3. Set `DATABASE_URL` environment variable
4. Railway will auto-detect Dockerfile and deploy

## Tech Stack

- FastAPI
- PostgreSQL
- Pandas/NumPy
- psycopg2

