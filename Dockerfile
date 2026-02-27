# SESOMNOD ENGINE — FastAPI Backend v3.1 (ULTIMATE STABILIZATION)
# 10/10 Industrial Grade: Docker Build
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

WORKDIR /app

# Install system dependencies for build stability
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy all application modules
COPY . .

# Force port 8000 for Railway stability as requested
EXPOSE 8000

# Guaranteed startup command with no variable expansion risks
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--log-level", "info"]
