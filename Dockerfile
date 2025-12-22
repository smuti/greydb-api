FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app/ ./app/

# Expose port (Railway uses $PORT)
EXPOSE 8000

# Run with Railway's PORT env var
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}

