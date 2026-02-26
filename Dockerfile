# SesomNod Engine — FastAPI Backend
# Railway / GCP Cloud Run compatible Docker image
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application modules
COPY main.py .
COPY dagens_kamp.py .
COPY auto_result.py .
COPY bankroll.py .

# Railway injects PORT env variable automatically, default to 8000
ENV PORT=8000

# EXPOSE the port for Railway healthcheck
EXPOSE 8000

# Run the application
CMD uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
