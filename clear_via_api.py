# Admin script for å tømme snapshots via HTTP API
# Kjøres lokalt mot Railway

import httpx
import asyncio

async def clear_snapshots():
    # Dette vil ikke fungere uten auth, men prøver
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                "https://sesomnod-api-production.up.railway.app/admin/clear-snapshots",
                timeout=30
            )
            print(f"Status: {resp.status_code}")
            print(f"Response: {resp.text}")
        except Exception as e:
            print(f"Feil: {e}")

if __name__ == "__main__":
    asyncio.run(clear_snapshots())