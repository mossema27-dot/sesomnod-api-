"""
SmartPick LIGHT v1 — narrative generators + payload builder.

Input: candidate-dict (runtime from run_analysis) OR picks_v2-row (historic).
Output: complete SmartPick payload dict, ready for JSONB cache + Telegram format.

All generators are pure (no side effects). Safe fallbacks for missing fields.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger("sesomnod.smartpick")

_OSLO = ZoneInfo("Europe/Oslo")
_INACTIVE = {None, "", "NEUTRAL", "UNKNOWN", "UNAVAILABLE", "NO_DATA"}

MARKET_DISPLAY_MAP = {
    "over_25": "Over 2.5 mål",
    "under_25": "Under 2.5 mål",
    "over_15": "Over 1.5 mål",
    "under_15": "Under 1.5 mål",
    "over_35": "Over 3.5 mål",
    "under_35": "Under 3.5 mål",
    "btts_yes": "BTTS Ja",
    "btts_no": "BTTS Nei",
    "home_win": "Hjemmeseier",
    "away_win": "Borteseier",
    "draw": "Uavgjort",
    "h2h": "Kampvinner",
    # predicted_outcome enum fallbacks (uppercase):
    "OVER_25": "Over 2.5 mål",
    "UNDER_25": "Under 2.5 mål",
    "BTTS_YES": "BTTS Ja",
    "BTTS_NO": "BTTS Nei",
    "HOME_WIN": "Hjemmeseier",
    "AWAY_WIN": "Borteseier",
    "DRAW": "Uavgjort",
}


def _resolve_market_display(candidate: dict) -> str:
    """Pick the first non-empty source and map to readable Norwegian."""
    raw = (
        candidate.get("market_type")
        or candidate.get("pick")
        or candidate.get("selection")
        or candidate.get("predicted_outcome")
        or ""
    )
    raw_s = str(raw).strip()
    if not raw_s:
        return "Ukjent marked"
    return MARKET_DISPLAY_MAP.get(raw_s, MARKET_DISPLAY_MAP.get(raw_s.lower(), raw_s))


def _is_active(sig: Any) -> bool:
    if sig is None:
        return False
    s = str(sig).strip().upper()
    if s in _INACTIVE:
        return False
    for marker in ("_UNAVAILABLE", "_UNKNOWN", "_ERROR", "_TIMEOUT", "_NOT_FOUND", "_NOT_ASSIGNED"):
        if marker in s:
            return False
    return True


def _f(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def count_active_signals(c: dict) -> int:
    keys = (
        "signal_velocity",
        "signal_xg",
        "signal_weather",
        "signal_referee",
        "signal_streak_home",
        "signal_streak_away",
    )
    return sum(1 for k in keys if _is_active(c.get(k)))


def generate_convergence_summary(c: dict) -> str:
    n = count_active_signals(c)
    if n >= 5:
        return f"Full konvergens — {n} av 6 atomic signaler aktive."
    if n >= 4:
        return f"Sterk konvergens — {n} av 6 signaler aktive."
    if n >= 3:
        return f"{n} av 6 signaler peker mot pick."
    return f"Begrenset konvergens — kun {n} av 6 aktive."


def generate_xg_summary(c: dict) -> str:
    h = _f(c.get("xg_divergence_home"))
    a = _f(c.get("xg_divergence_away"))
    if h is None and a is None:
        return "xG-data ikke tilgjengelig for dette oppgjøret."
    if h is not None and a is not None:
        if h > 0.3 and a > 0.3:
            return f"Begge lag underprestrer xG (hjemme +{h:.2f}, borte +{a:.2f}) — regresjon sannsynlig."
        if h < -0.3 and a < -0.3:
            return f"Begge lag overprestrer xG (hjemme {h:.2f}, borte {a:.2f}) — risiko for regresjon ned."
        if h > 0.3:
            return f"Hjemmelaget underprestrer xG (+{h:.2f}) — oppside-potensiale."
        if a > 0.3:
            return f"Bortelaget underprestrer xG (+{a:.2f}) — oppside-potensiale."
        return "xG-divergens nøytral for begge lag."
    side_h = f"hjemme {h:+.2f}" if h is not None else "hjemme n/a"
    side_a = f"borte {a:+.2f}" if a is not None else "borte n/a"
    return f"xG-divergens delvis tilgjengelig ({side_h}, {side_a})."


def generate_signal_summary(c: dict) -> str:
    parts = []
    if _is_active(c.get("signal_velocity")):
        parts.append("Sharp money (velocity)")
    if _is_active(c.get("signal_xg")):
        parts.append("xG-divergens")
    if _is_active(c.get("signal_weather")):
        parts.append("vær-påvirkning")
    if _is_active(c.get("signal_referee")):
        parts.append("dommer-profil")
    if _is_active(c.get("signal_streak_home")) or _is_active(c.get("signal_streak_away")):
        parts.append("scoring streak")
    if not parts:
        return "Ingen aktive atomic signaler."
    return "Aktive signaler: " + " + ".join(parts) + "."


def generate_attack_angle(c: dict) -> str:
    atomic = int(c.get("atomic_score") or 0)
    if atomic >= 7:
        return f"Pick med {atomic}/9 atomic score — institusjonell konvergens. Sjelden kombinasjon av signaler."

    velocity = str(c.get("signal_velocity") or "").upper()
    if "SHARP" in velocity:
        return "Sharp money beveger linjen i pick-retning. Markedet justerer seg mot vår posisjon."

    h = _f(c.get("xg_divergence_home")) or 0.0
    a = _f(c.get("xg_divergence_away")) or 0.0
    if abs(h) > 0.5 or abs(a) > 0.5:
        return "Stort xG-gap — markedet priser ikke inn underliggende kvalitet."

    weather = str(c.get("signal_weather") or "").upper()
    market = str(c.get("market_type") or "").lower()
    if weather in ("HIGH_WIND", "HEAVY_RAIN") and "under" in market:
        wind = _f(c.get("wind_speed"))
        wind_str = f"Vind {wind:.0f} m/s" if wind else "Vær"
        return f"{wind_str} favoriserer Under — modellen og været konvergerer."

    edge_pct = _f(c.get("soft_edge")) or 0.0
    market_display = _resolve_market_display(c)
    odds = _f(c.get("odds")) or 0.0
    return f"Modellen ser +{edge_pct:.1f}% edge mot markedet på {market_display} @ {odds:.2f}."


def generate_risks(c: dict) -> list[dict]:
    risks: list[dict] = []
    soft_edge = _f(c.get("soft_edge")) or 0.0
    if soft_edge > 15.0:
        risks.append({
            "icon": "🟡",
            "text": f"Stort edge-gap ({soft_edge:.1f}%) — markedsfeil eller modell-overprestasjon?",
            "severity": "medium",
        })

    atomic = int(c.get("atomic_score") or 0)
    if atomic < 4:
        risks.append({
            "icon": "🟡",
            "text": f"Lav signal-konvergens ({atomic}/9)",
            "severity": "medium",
        })

    wind = _f(c.get("wind_speed"))
    if wind is not None and wind > 10:
        risks.append({
            "icon": "🟡",
            "text": f"Vind {wind:.1f} m/s — påvirker tempo og presisjon",
            "severity": "medium",
        })

    temp = _f(c.get("temperature"))
    if temp is not None and temp < 0:
        risks.append({
            "icon": "🟡",
            "text": f"Kulde under frysepunktet ({temp:.0f}°C)",
            "severity": "low",
        })

    ref_count = c.get("referee_matches_count")
    ref_name = c.get("referee_name")
    if ref_count is not None and int(ref_count) < 5 and ref_count != 0:
        risks.append({
            "icon": "🟡",
            "text": f"Begrenset historikk på dommer ({ref_name or 'ukjent'}) — kun {int(ref_count)} kamper",
            "severity": "medium",
        })

    if str(c.get("signal_referee") or "").upper() == "INSUFFICIENT_DATA":
        risks.append({
            "icon": "🟡",
            "text": "Dommer-data utilstrekkelig",
            "severity": "low",
        })

    if c.get("pinnacle_clv") is None:
        risks.append({
            "icon": "🟡",
            "text": "Ingen Pinnacle-referanse — usikker markedspris",
            "severity": "low",
        })

    if not risks:
        return [{"icon": "🟢", "text": "Ingen røde flagg identifisert", "severity": "low"}]
    return risks


def calculate_confidence(c: dict, risks: list[dict], active_signals: int | None = None) -> str:
    high = sum(1 for r in risks if r.get("severity") == "high")
    medium = sum(1 for r in risks if r.get("severity") == "medium")
    atomic = int(c.get("atomic_score") or 0)
    if active_signals is None:
        active_signals = count_active_signals(c)
    # Hard gate: need ≥3 active signals to qualify for HIGH/MEDIUM
    if active_signals < 3:
        return "LOW"
    if atomic >= 6 and active_signals >= 4 and high == 0:
        return "HIGH"
    if atomic >= 4 and active_signals >= 3 and high == 0 and medium <= 2:
        return "MEDIUM"
    return "LOW"


def calculate_bankroll(kelly_stake: Any) -> dict:
    k = _f(kelly_stake)
    if k is None or k <= 0:
        return {"conservative_pct": 0.0, "standard_pct": 0.0, "aggressive_pct": 0.0}
    return {
        "conservative_pct": round(k / 4, 2),
        "standard_pct": round(k / 2, 2),
        "aggressive_pct": round(k, 2),
    }


async def get_tier_track_record(tier: str, db) -> dict:
    """
    LATERAL JOIN picks_v2 → dagens_kamp to read result (WIN/LOSS uppercase).
    Returns {note: ...} if n < 10.
    Never raises.
    """
    try:
        rows = await db.fetch("""
            SELECT dk.result
            FROM picks_v2 p
            LEFT JOIN LATERAL (
                SELECT result FROM dagens_kamp
                WHERE home_team = p.home_team
                  AND away_team = p.away_team
                  AND kickoff::date = p.kickoff_time::date
                ORDER BY id DESC
                LIMIT 1
            ) dk ON TRUE
            WHERE p.tier = $1
              AND p.created_at > NOW() - INTERVAL '30 days'
              AND dk.result IN ('WIN', 'LOSS')
        """, tier)
    except Exception as e:
        logger.warning(f"[SmartPick] track record query failed for {tier}: {e}")
        return {"tier": tier, "n_picks": 0, "note": "Track record utilgjengelig"}

    n = len(rows)
    if n < 10:
        return {"tier": tier, "n_picks": n, "note": f"Verifiseres etter 10 picks ({n})"}

    wins = sum(1 for r in rows if r["result"] == "WIN")
    losses = n - wins
    return {
        "tier": tier,
        "n_picks": n,
        "wins": wins,
        "losses": losses,
        "hit_rate": round(wins / n * 100, 1),
        "note": None,
    }


def _format_oslo(kickoff: Any) -> str:
    if kickoff is None:
        return ""
    try:
        if isinstance(kickoff, str):
            dt = datetime.fromisoformat(kickoff.replace("Z", "+00:00"))
        else:
            dt = kickoff
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_OSLO).strftime("%d.%m %H:%M")
    except Exception:
        return str(kickoff)[:16]


async def build_smartpick_payload(candidate: dict, db) -> dict:
    """
    Build complete SmartPick payload from candidate-dict (runtime) or picks_v2 row.
    pick_id is filled in by caller after INSERT.
    """
    risks = generate_risks(candidate)
    active_signals = count_active_signals(candidate)
    confidence = calculate_confidence(candidate, risks, active_signals)
    bankroll = calculate_bankroll(candidate.get("kelly_stake"))
    tier = candidate.get("tier") or "MONITORED"
    track_record = await get_tier_track_record(tier, db)
    market_display = _resolve_market_display(candidate)

    kickoff = candidate.get("kickoff_time") or candidate.get("kickoff") or candidate.get("commence_time")

    return {
        "pick_id": None,
        "match": {
            "home_team": candidate.get("home_team", ""),
            "away_team": candidate.get("away_team", ""),
            "league": candidate.get("league", ""),
            "kickoff_oslo": _format_oslo(kickoff),
        },
        "selection": {
            "market": market_display,
            "side": "",
            "odds": _f(candidate.get("odds")),
        },
        "math": {
            "model_prob": _f(candidate.get("model_prob")),
            "market_prob": _f(candidate.get("market_prob")),
            "edge_pct": round(_f(candidate.get("soft_edge") or candidate.get("edge")) or 0.0, 1),
            "ev_pct": round(_f(candidate.get("soft_ev") or candidate.get("ev")) or 0.0, 1),
            "kelly_pct": _f(candidate.get("kelly_stake")),
            "tier": tier,
            "atomic_score": int(candidate.get("atomic_score") or 0),
        },
        "atomic_signals": {
            "velocity": candidate.get("signal_velocity"),
            "xg": {
                "signal": candidate.get("signal_xg"),
                "divergence_home": _f(candidate.get("xg_divergence_home")),
                "divergence_away": _f(candidate.get("xg_divergence_away")),
            },
            "weather": {
                "signal": candidate.get("signal_weather"),
                "wind_speed": _f(candidate.get("wind_speed")),
                "temperature": _f(candidate.get("temperature")),
            },
            "referee": {
                "signal": candidate.get("signal_referee"),
                "name": candidate.get("referee_name"),
                "matches_count": candidate.get("referee_matches_count"),
            },
            "streak": {
                "home": candidate.get("signal_streak_home"),
                "away": candidate.get("signal_streak_away"),
            },
        },
        "why": {
            "convergence_summary": generate_convergence_summary(candidate),
            "xg_summary": generate_xg_summary(candidate),
            "signal_summary": generate_signal_summary(candidate),
        },
        "attack_angle": generate_attack_angle(candidate),
        "risks": risks,
        "confidence": confidence,
        "atomic_signals_used": active_signals,
        "bankroll": bankroll,
        "tier_track_record": track_record,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


_MDV2_SPECIAL = r"_*[]()~`>#+-=|{}.!"


def _escape_mdv2(text: str) -> str:
    """Minimal MarkdownV2 escape (duplicate of main.py:_mdv2_escape for standalone use)."""
    if text is None:
        return ""
    s = str(text)
    return "".join("\\" + c if c in _MDV2_SPECIAL else c for c in s)


def format_smartpick_telegram(payload: dict, escape_fn=None) -> str:
    """
    Render SmartPick payload as MarkdownV2 Telegram message.
    escape_fn: defaults to local _escape_mdv2; main.py should pass its _mdv2_escape.
    """
    e = escape_fn or _escape_mdv2
    p = payload

    risks = p.get("risks") or []
    risks_lines = "\n".join(f"{r['icon']} {e(r['text'])}" for r in risks)

    track = p.get("tier_track_record") or {}
    if track.get("note"):
        track_line = e(track["note"])
    else:
        wins = track.get("wins", 0)
        losses = track.get("losses", 0)
        hit_rate = track.get("hit_rate", 0)
        track_line = f"{wins}W/{losses}L \\({e(f'{hit_rate}')}%\\)"

    math = p["math"]
    match_ = p["match"]
    sel = p["selection"]
    why = p["why"]
    br = p["bankroll"]

    odds_s = f"{sel['odds']:.2f}" if sel.get("odds") else "—"
    edge_s = str(math["edge_pct"])
    ev_s = str(math["ev_pct"])
    kelly_v = math.get("kelly_pct")
    kelly_s = f"{kelly_v:.2f}" if kelly_v is not None else "0.00"

    pick_id = p.get("pick_id") or "pending"
    tier = math.get("tier") or "MONITORED"

    home = e(match_["home_team"])
    away = e(match_["away_team"])
    league = e(match_["league"])
    kickoff = e(match_["kickoff_oslo"])
    market = e(sel.get("market") or "")
    side_raw = sel.get("side") or ""
    side = (" " + e(side_raw)) if side_raw else ""
    odds_e = e(odds_s)
    edge_e = e(edge_s)
    ev_e = e(ev_s)
    kelly_e = e(kelly_s)
    conv = e(why["convergence_summary"])
    xg_s = e(why["xg_summary"])
    sig_s = e(why["signal_summary"])
    attack = e(p["attack_angle"])
    conf = e(p["confidence"])
    pid_e = e(str(pick_id))
    tier_e = e(tier)
    cons_e = e(str(br["conservative_pct"]))
    std_e = e(str(br["standard_pct"]))
    agg_e = e(str(br["aggressive_pct"]))
    atomic_used = p["atomic_signals_used"]
    atomic_score = math["atomic_score"]

    sep = "—————————————————————————"

    msg = (
        f"🎯 *SMARTPICK \\#{pid_e} — {tier_e}*\n"
        f"{home} vs {away}\n"
        f"{league} · {kickoff}\n"
        f"\n"
        f"📊 *PICK:* {market}{side} @ {odds_e}\n"
        f"💰 *Edge:* \\+{edge_e}% · *EV:* \\+{ev_e}% · *Kelly:* {kelly_e}%\n"
        f"⚛️ *Atomic Score:* {atomic_score}/9\n"
        f"\n"
        f"{sep}\n"
        f"🧠 *HVORFOR DENNE PICKEN*\n"
        f"\n"
        f"{conv}\n"
        f"\n"
        f"{xg_s}\n"
        f"\n"
        f"{sig_s}\n"
        f"\n"
        f"{sep}\n"
        f"🔥 *ANGREPSVINKEL*\n"
        f"\n"
        f"{attack}\n"
        f"\n"
        f"{sep}\n"
        f"⚠️ *RISIKO\\-FAKTORER*\n"
        f"\n"
        f"{risks_lines}\n"
        f"\n"
        f"KONFIDENS: {conf}\n"
        f"{atomic_used}/6 atomic signaler aktive\n"
        f"\n"
        f"{sep}\n"
        f"🎲 *BANKROLL\\-FORSLAG*\n"
        f"\n"
        f"Konservativ \\(Kelly/4\\): {cons_e}%\n"
        f"Standard \\(Kelly/2\\): {std_e}%\n"
        f"Aggressiv \\(Full Kelly\\): {agg_e}%\n"
        f"\n"
        f"{sep}\n"
        f"📈 *{tier_e} TRACK RECORD*\n"
        f"\n"
        f"{track_line}\n"
        f"\n"
        f"{sep}\n"
        f"SesomNod · ID \\#{pid_e}"
    )
    return msg
