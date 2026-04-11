import hashlib
import json
import secrets
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_receipt_slug(
    pick_id: int,
    kickoff: datetime | None = None
) -> str:
    date_str = (kickoff or datetime.utcnow()).strftime('%Y%m%d')
    rand = secrets.token_hex(2)
    return f"SES-{date_str}-{pick_id}-{rand}"


def generate_receipt_hash(data: dict) -> str:
    clean = {
        k: str(v) for k, v in sorted(data.items())
        if v is not None
    }
    payload = json.dumps(clean, separators=(',', ':'))
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


def calculate_synergy(
    dc_prob: float | None,
    xgb_prob: float | None,
    market_prob: float | None,
    threshold: float = 0.08
) -> tuple[str, float | None]:
    probs = [p for p in [dc_prob, xgb_prob] if p is not None]
    if len(probs) < 2:
        return 'UNRESOLVED', None
    diff = abs(probs[0] - probs[1])
    avg = sum(probs) / len(probs)
    if diff > threshold:
        return 'DIVERGENT', round(avg, 3)
    if market_prob and abs(avg - market_prob) < threshold:
        return 'CONSENSUS', round(avg, 3)
    return 'UNRESOLVED', round(avg, 3)


def calculate_edge_status(
    edge_pct: float | None,
    synergy: str,
    kelly_verified: bool,
    minutes_to_kickoff: int | None
) -> tuple[str, str]:
    edge = edge_pct or 0
    if minutes_to_kickoff is not None and minutes_to_kickoff < 30:
        return (
            'DEAD_EDGE',
            'Edge-vindu lukket — under 30 min til spark'
        )
    if edge < 8.0:
        return ('PASS', f'Edge {edge:.1f}% under 8% terskel')
    if synergy == 'DIVERGENT':
        return (
            'WATCH',
            'Modell-divergens — Dixon-Coles og XGBoost uenige'
        )
    if edge >= 8.0 and synergy == 'CONSENSUS' and kelly_verified:
        return (
            'BET_NOW',
            f'Edge {edge:.1f}% bekreftet av begge modeller'
        )
    return (
        'WATCH',
        f'Edge {edge:.1f}% til stede, ikke fullt bekreftet'
    )


async def create_or_update_receipt(
    db,
    pick_id: int,
    pick_data: dict
) -> dict | None:
    """Create or update a receipt for a pick. Never raises."""
    try:
        existing = await db.fetchrow(
            "SELECT id, receipt_slug FROM pick_receipts "
            "WHERE pick_id = $1", pick_id
        )

        kickoff = (pick_data.get('kickoff_time')
                   or pick_data.get('kickoff')
                   or pick_data.get('match_time'))
        if isinstance(kickoff, str):
            try:
                kickoff = datetime.fromisoformat(
                    kickoff.replace('Z', '+00:00')
                )
            except Exception:
                kickoff = None

        slug = (existing['receipt_slug'] if existing
                else generate_receipt_slug(pick_id, kickoff))

        edge_pct = (pick_data.get('edge')
                    or pick_data.get('edge_pct')
                    or pick_data.get('soft_edge'))
        if edge_pct is not None:
            edge_pct = float(edge_pct)
            if edge_pct <= 1:
                edge_pct = edge_pct * 100

        synergy_status, synergy_score = calculate_synergy(
            pick_data.get('dc_home_win'),
            pick_data.get('xgb_home_win'),
            None
        )

        now = datetime.utcnow()
        mins = None
        if kickoff and hasattr(kickoff, 'replace'):
            try:
                delta = kickoff.replace(tzinfo=None) - now
                mins = int(delta.total_seconds() / 60)
            except Exception:
                pass

        edge_status, edge_reason = calculate_edge_status(
            edge_pct,
            synergy_status,
            bool(pick_data.get('kelly_verified', False)),
            mins
        )

        hash_data = {
            'pick_id': pick_id,
            'slug': slug,
            'edge_pct': str(edge_pct),
            'synergy': synergy_status,
            'created': now.isoformat()[:19]
        }
        receipt_hash = generate_receipt_hash(hash_data)

        match_name = (
            f"{pick_data.get('home_team', '')} "
            f"vs {pick_data.get('away_team', '')}"
        ).strip()
        if not match_name or match_name == 'vs':
            match_name = pick_data.get('match_name', '') or pick_data.get('match', '')

        if existing:
            await db.execute("""
                UPDATE pick_receipts SET
                    edge_pct = $1,
                    synergy_status = $2,
                    synergy_score = $3,
                    edge_status = $4,
                    edge_status_reason = $5,
                    receipt_hash = $6,
                    match_name = $7
                WHERE pick_id = $8
            """,
                edge_pct, synergy_status, synergy_score,
                edge_status, edge_reason, receipt_hash,
                match_name, pick_id
            )
        else:
            omega_score = pick_data.get('omega_score') or pick_data.get('atomic_score') or pick_data.get('score')
            posted_odds = pick_data.get('odds') or pick_data.get('posted_odds')
            ev_pct = pick_data.get('ev') or pick_data.get('ev_pct') or pick_data.get('soft_ev')
            btts_val = pick_data.get('btts_yes')
            xg_home = pick_data.get('xg_home') or pick_data.get('signal_xg_home')
            xg_away = pick_data.get('xg_away') or pick_data.get('signal_xg_away')
            kelly_frac = pick_data.get('kelly_fraction') or pick_data.get('kelly_stake')

            # Safe numeric conversion
            def _num(v):
                if v is None:
                    return None
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return None

            await db.execute("""
                INSERT INTO pick_receipts (
                    pick_id, receipt_slug, posted_at,
                    match_name, league, kickoff,
                    pick_description, posted_odds,
                    edge_pct, ev_pct, omega_score,
                    btts_yes, xg_home, xg_away,
                    kelly_fraction, kelly_verified,
                    synergy_status, synergy_score,
                    edge_status, edge_status_reason,
                    receipt_hash, phase
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                    $11,$12,$13,$14,$15,$16,$17,$18,
                    $19,$20,$21,$22
                )
            """,
                pick_id, slug, now,
                match_name,
                str(pick_data.get('league', '') or ''),
                kickoff,
                str(pick_data.get('pick_description', '') or pick_data.get('pick', '') or ''),
                _num(posted_odds),
                _num(edge_pct),
                _num(ev_pct),
                _num(omega_score),
                _num(btts_val),
                _num(xg_home),
                _num(xg_away),
                _num(kelly_frac),
                bool(pick_data.get('kelly_verified', False)),
                synergy_status, synergy_score,
                edge_status, edge_reason,
                receipt_hash, 'Phase 0'
            )

        return {
            'slug': slug,
            'edge_status': edge_status,
            'synergy_status': synergy_status,
            'receipt_hash': receipt_hash,
            'match_name': match_name
        }

    except Exception as e:
        logger.error(f"Receipt engine error: {e}")
        return None
