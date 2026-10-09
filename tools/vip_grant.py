"""Grant/revoke VIP Inner Circle access. Requires DATABASE_URL and --confirm.

  python tools/vip_grant.py grant  her@example.com --note "..." --confirm
  python tools/vip_grant.py revoke her@example.com --confirm
  python tools/vip_grant.py set-code her@example.com --out ~/path/CODE.txt --confirm   (plaintext only to --out, chmod 600)
  python tools/vip_grant.py revoke-code her@example.com --confirm
  python tools/vip_grant.py list
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.vip_access import SOURCE, TIER, normalize, generate_code, hash_code  # noqa: E402


async def main(a):
    import asyncpg
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    try:
        if a.cmd == "list":
            for r in await conn.fetch("SELECT email, tier, source, granted_at, revoked_at, code_set_at, code_revoked_at, (code_hash IS NOT NULL) AS has_code FROM vip_access ORDER BY granted_at"):
                print(dict(r))
            return
        email = normalize(a.email)
        if not email:
            sys.exit("invalid email")
        if not a.confirm:
            sys.exit(f"dry run: would {a.cmd} {email} (tier={TIER}, source={SOURCE!r}); add --confirm")
        if a.cmd == "set-code":
            if not a.out:
                sys.exit("--out required (code is never printed)")
            code = generate_code()
            n = await conn.execute("UPDATE vip_access SET code_hash = $2, code_set_at = NOW(), code_revoked_at = NULL "
                                   "WHERE email = $1 AND revoked_at IS NULL", email, hash_code(code))
            if not n.endswith("1"):
                sys.exit("no active VIP row for that email")
            out = Path(os.path.expanduser(a.out))
            fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(f"VIP-kode for {email}\n{code}\n")
            os.chmod(out, 0o600)
            print(f"set-code: {email} (code written to {out}, mode 600)")
            return
        if a.cmd == "revoke-code":
            await conn.execute("UPDATE vip_access SET code_revoked_at = NOW() WHERE email = $1", email)
            print(f"revoke-code: {email}")
            return
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
    p.add_argument("cmd", choices=["grant", "revoke", "list", "set-code", "revoke-code"])
    p.add_argument("email", nargs="?")
    p.add_argument("--note")
    p.add_argument("--out")
    p.add_argument("--confirm", action="store_true")
    asyncio.run(main(p.parse_args()))
