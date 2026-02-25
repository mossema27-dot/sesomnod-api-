# SesomNod Engine — GCP Cloud Run Deploy Guide

## Forutsetninger
- Google Cloud-konto (gratis tier: 2M requests/mnd)
- Google Cloud CLI installert (`gcloud`)
- Docker installert

---

## Steg 1: Opprett GCP-konto og prosjekt

1. Gå til [console.cloud.google.com](https://console.cloud.google.com)
2. Klikk "Create Project" → gi det et navn (f.eks. `sesomnod-engine`)
3. Aktiver fakturering (kreves for Cloud Run, men gratis tier er svært raust)
4. Aktiver Cloud Run API:
   ```
   gcloud services enable run.googleapis.com
   gcloud services enable containerregistry.googleapis.com
   ```

---

## Steg 2: Konfigurer miljøvariabler

Kopier `.env.example` til `.env` og fyll inn dine verdier:
```bash
cp .env.example .env
nano .env
```

---

## Steg 3: Bygg og push Docker-image

```bash
# Sett prosjekt-ID
export PROJECT_ID=sesomnod-engine  # Bytt til ditt prosjekt-ID

# Bygg image
docker build -t gcr.io/$PROJECT_ID/sesomnod-api:latest .

# Autentiser Docker mot GCR
gcloud auth configure-docker

# Push image
docker push gcr.io/$PROJECT_ID/sesomnod-api:latest
```

---

## Steg 4: Deploy til Cloud Run

```bash
gcloud run deploy sesomnod-api \
  --image gcr.io/$PROJECT_ID/sesomnod-api:latest \
  --platform managed \
  --region europe-west1 \
  --allow-unauthenticated \
  --set-env-vars TELEGRAM_TOKEN=xxx,TELEGRAM_CHAT_ID=xxx,ODDS_API_KEY=xxx,SUPABASE_PAT=xxx,SUPABASE_PROJECT=xxx \
  --memory 512Mi \
  --cpu 1 \
  --min-instances 0 \
  --max-instances 3
```

**Merk:** Erstatt `xxx` med dine faktiske verdier, eller bruk Secret Manager (anbefalt for produksjon).

---

## Steg 5: Oppdater frontend API_BASE

Etter deploy får du en URL som:
`https://sesomnod-api-xxxx-ew.a.run.app`

Oppdater `client/src/lib/api.ts`:
```typescript
const API_BASE = import.meta.env.DEV
  ? "http://localhost:8000"
  : "https://sesomnod-api-xxxx-ew.a.run.app";  // ← Din Cloud Run URL
```

---

## Steg 6: Sett opp Cloud Scheduler (06:00 automatisk)

```bash
# Aktiver Cloud Scheduler
gcloud services enable cloudscheduler.googleapis.com

# Opprett jobb for Dagens Kamp kl 06:00
gcloud scheduler jobs create http sesomnod-dagens-kamp \
  --schedule="0 5 * * *" \
  --uri="https://sesomnod-api-xxxx-ew.a.run.app/dagens-kamp/analyze/sync" \
  --http-method=POST \
  --time-zone="Europe/Oslo" \
  --location=europe-west1

# Opprett jobb for daglig oppsummering kl 23:00
gcloud scheduler jobs create http sesomnod-daily-summary \
  --schedule="0 22 * * *" \
  --uri="https://sesomnod-api-xxxx-ew.a.run.app/telegram/summary" \
  --http-method=POST \
  --time-zone="Europe/Oslo" \
  --location=europe-west1
```

---

## Kostnadsestimat

| Tjeneste | Gratis tier | Estimert kostnad |
|----------|-------------|------------------|
| Cloud Run | 2M requests/mnd | $0/mnd |
| Cloud Scheduler | 3 jobber gratis | $0/mnd |
| Container Registry | 0.5 GB gratis | $0/mnd |
| **Total** | | **$0/mnd** |

---

## Feilsøking

```bash
# Se logs
gcloud run logs read sesomnod-api --region europe-west1

# Test endepunkt
curl https://sesomnod-api-xxxx-ew.a.run.app/health
```

---

## Sikkerhet (produksjon)

For produksjon, bruk Secret Manager i stedet for env-variabler:
```bash
# Opprett secrets
gcloud secrets create TELEGRAM_TOKEN --data-file=- <<< "din_token"
gcloud secrets create ODDS_API_KEY --data-file=- <<< "din_nøkkel"

# Deploy med Secret Manager
gcloud run deploy sesomnod-api \
  --set-secrets TELEGRAM_TOKEN=TELEGRAM_TOKEN:latest,ODDS_API_KEY=ODDS_API_KEY:latest
```
