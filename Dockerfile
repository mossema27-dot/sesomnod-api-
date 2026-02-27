FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=25s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--log-level", "info", \
     "--no-access-log"]
```
→ **Commit** ✅

---

### 📄 FIL 4 — `main.py`
Last ned filen direkte fra lenken over — den er 595 linjer og for lang til å kopiere manuelt. Åpne filen → kopier alt innhold → GitHub → main.py → ✏️ → slett alt → lim inn → **Commit** ✅

---

## 🚀 Etter siste commit:
Railway deployer automatisk. Du skal se i loggene:
```
✅ SesomNod Engine KLAR! (FULL DATABASE MODE)
