FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app/ ./app/

# Expose port
EXPOSE 8080

# Run with Railway's PORT env var (defaults to 8080)
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}

