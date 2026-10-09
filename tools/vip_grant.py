"""Grant/revoke VIP Inner Circle access. Requires DATABASE_URL and --confirm.

  python tools/vip_grant.py grant  her@example.com --note "..." --confirm
  python tools/vip_grant.py revoke her@example.com --confirm
  python tools/vip_grant.py list
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.vip_access import SOURCE, TIER, normalize  # noqa: E402


async def main(a):
    import asyncpg
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    try:
        if a.cmd == "list":
            for r in await conn.fetch("SELECT email, tier, source, granted_at, revoked_at FROM vip_access ORDER BY granted_at"):
                print(dict(r))
            return
        email = normalize(a.email)
        if not email:
            sys.exit("invalid email")
        if not a.confirm:
            sys.exit(f"dry run: would {a.cmd} {email} (tier={TIER}, source={SOURCE!r}); add --confirm")
        if a.cmd == "grant":
            await conn.execute(
                "INSERT INTO vip_access (email, tier, source, note) VALUES ($1, $2, $3, $4) "
                "ON CONFLICT (email) DO UPDATE SET revoked_at = NULL, note = EXCLUDED.note",
                email, TIER, SOURCE, a.note)
        else:
            await conn.execute("UPDATE vip_access SET revoked_at = NOW() WHERE email = $1", email)
        print(f"{a.cmd}: {email}")
    finally:
        await conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["grant", "revoke", "list"])
    p.add_argument("email", nargs="?")
    p.add_argument("--note")
    p.add_argument("--confirm", action="store_true")
    asyncio.run(main(p.parse_args()))
