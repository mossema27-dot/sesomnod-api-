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

# Railway injects PORT env variable automatically
ENV PORT=8080

# Run the application
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 1
