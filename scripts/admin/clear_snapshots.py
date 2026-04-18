#!/usr/bin/env python3
"""
Admin script: Tøm gamle odds_snapshots og tving ny fetch
Kjøres mot Railway database
"""

import asyncio
import asyncpg
import os

async def clear_old_snapshots():
    # Hent DATABASE_URL fra Railway miljø
    db_url = os.getenv('DATABASE_URL')
    
    if not db_url:
        print("❌ DATABASE_URL ikke satt")
        return False
    
    try:
        conn = await asyncpg.connect(db_url)
        
        # Slett alle odds_snapshots
        result = await conn.execute("DELETE FROM odds_snapshots")
        print(f"✅ Slettet gamle snapshots: {result}")
        
        # Sjekk at tabellen er tom
        count = await conn.fetchval("SELECT COUNT(*) FROM odds_snapshots")
        print(f"📊 Gjenstående rader: {count}")
        
        await conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Feil: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(clear_old_snapshots())
    exit(0 if success else 1)