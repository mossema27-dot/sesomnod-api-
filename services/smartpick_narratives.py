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


# ─────────────────────────────────────────────────────────────────────────
# DAILY SIGNALDESK — Probability-first MVP (Phase 2 pivot)
# Universal Telegram-format for ENHVER ATOMIC/EDGE pick i picks_v2.
# Pure functions; ingen DB-kall. Caller henter picks og passer som list[dict].
# ─────────────────────────────────────────────────────────────────────────

# Hit rate-analyse 2026-04-27 (n=28 settled): atomic_score=0+EV+0.0% er
# dummy-picks som forurenser. Filtrer dem fra Daily Signaldesk.
DUMMY_EV_THRESHOLD = 0.05  # picks med EV under 0.05% regnes som "ingen ekte edge"


def is_dummy_pick(pick: dict) -> bool:
    """Filter mot atomic_score=0 + EV~0% picks (per hit-rate analyse 2026-04-27)."""
    score = int(pick.get("atomic_score") or 0)
    ev = abs(_f(pick.get("soft_ev") or pick.get("ev")) or 0.0)
    return score == 0 and ev < DUMMY_EV_THRESHOLD


def calculate_data_tag(pick: dict) -> tuple[str, str]:
    """
    Data-completeness tag — kunde-vendt transparency.
    Returnerer (label, emoji_dots) basert på atomic_score + populated signals.
    """
    populated = sum([
        _f(pick.get("signal_xg_home")) is not None,
        _f(pick.get("signal_xg_away")) is not None,
        _is_active(pick.get("signal_velocity")),
        _is_active(pick.get("signal_weather")),
        bool(pick.get("signals_triggered")),
    ])
    score = int(pick.get("atomic_score") or 0)

    if score >= 7 and populated >= 4:
        return ("FULL DATA", "🟢🟢🟢")
    if 4 <= score <= 9 and 2 <= populated <= 3:
        return ("SOLID", "🟢🟢⚪")
    if score <= 3 or populated == 1:
        return ("LIMITED", "🟢⚪⚪")
    return ("MODEL ONLY", "⚪⚪⚪")


def generate_why_points(pick: dict) -> list[dict]:
    """
    Dynamic why-points per pick. Aldri 0 — alltid minimum 2 (model fallback).
    Hver point: {emoji, text, source}. Returnerer max 5.
    """
    points: list[dict] = []

    velocity = pick.get("signal_velocity")
    if _is_active(velocity):
        points.append({
            "emoji": "💸",
            "text": f"Sharp money beveger linjen ({velocity})",
            "source": "Pinnacle velocity tracker",
        })

    xg_h = _f(pick.get("signal_xg_home"))
    xg_a = _f(pick.get("signal_xg_away"))
    if xg_h is not None and xg_a is not None:
        if xg_h > 1.5 or xg_a > 1.5:
            stronger_team = pick["home_team"] if xg_h >= xg_a else pick["away_team"]
            stronger_xg = max(xg_h, xg_a)
            points.append({
                "emoji": "📈",
                "text": f"{stronger_team} skaper {stronger_xg:.1f} xG i snitt",
                "source": "xG-modell",
            })
        if abs(xg_h - xg_a) > 1.0:
            points.append({
                "emoji": "🎯",
                "text": (
                    f"xG-mismatch: {pick['home_team']} {xg_h:.1f} vs "
                    f"{pick['away_team']} {xg_a:.1f}"
                ),
                "source": "xG-divergens",
            })

    if _is_active(pick.get("signal_weather")) and pick.get("weather_market_impact"):
        points.append({
            "emoji": "🌬️",
            "text": f"Vær påvirker spillestil: {pick['weather_market_impact']}",
            "source": "OpenWeather",
        })

    sigs = pick.get("signals_triggered")
    if isinstance(sigs, list) and sigs:
        named = [s for s in sigs if isinstance(s, str)]
        if named:
            label_map = {
                "STRONG_EDGE_35PCT": "Sterk edge ≥3.5%",
                "STRONG_EV_5PCT": "Sterk EV ≥5%",
                "BRUTAL_OMEGA": "Brutal omega-konvergens",
            }
            for s in named[:2]:
                label = label_map.get(s, s.replace("_", " ").title())
                points.append({
                    "emoji": "🧬",
                    "text": label,
                    "source": "Signal triggers",
                })

    score = int(pick.get("atomic_score") or 0)
    if score >= 7:
        points.append({
            "emoji": "⚛️",
            "text": f"{score}/9 atomic-signaler konvergerer",
            "source": "Atomic Signal Architecture",
        })

    if len(points) < 2:
        edge = _f(pick.get("soft_edge") or pick.get("edge")) or 0.0
        if edge > 0:
            points.append({
                "emoji": "💰",
                "text": f"Modell ser +{edge:.1f}% edge mot markedet",
                "source": "Dixon-Coles + Pinnacle",
            })
        odds = _f(pick.get("odds")) or 0.0
        if odds > 0:
            implied = (1.0 / odds) * 100.0
            points.append({
                "emoji": "📊",
                "text": f"Odds {odds:.2f} = {implied:.0f}% implisitt sannsynlighet",
                "source": "Bookmaker odds",
            })

    return points[:5]


def select_highest_prob_event(pick: dict) -> dict:
    """
    Identifiser dette pick'ets høyeste sannsynlighet event.
    For nå: bruker dk_pick / market_type som primær (1X2). Over/Under og BTTS
    krever motor-fix (BTTS hardkodet 50/50 per CLAUDE.md MEMORY).

    Probability-fallback rekkefølge:
    1. model_prob fra DB (hvis populert > 0)
    2. market_prob + (edge / 100) — implisitt fra odds + edge
    3. 1/odds + (edge / 100) — fallback når market_prob mangler
    """
    market_display = _resolve_market_display(pick)
    model_prob = _f(pick.get("model_prob"))
    market_prob = _f(pick.get("market_prob"))
    odds = _f(pick.get("odds")) or 0.0
    edge = _f(pick.get("soft_edge") or pick.get("edge")) or 0.0

    if (market_prob is None or market_prob == 0) and odds > 0:
        market_prob = 1.0 / odds

    if (model_prob is None or model_prob == 0) and market_prob is not None:
        model_prob = market_prob + (edge / 100.0)

    return {
        "label": market_display,
        "model_prob_pct": round((model_prob or 0.0) * 100, 1),
        "market_prob_pct": round((market_prob or 0.0) * 100, 1),
        "edge_pct": round(edge, 1),
        "odds": odds,
    }


def build_event_card(pick: dict, escape_fn=None) -> dict:
    """Bundle pick til ett event-card payload (Daily Signaldesk thread item)."""
    event = select_highest_prob_event(pick)
    why_points = generate_why_points(pick)
    tag_label, tag_emoji = calculate_data_tag(pick)
    score = int(pick.get("atomic_score") or 0)
    tier = pick.get("tier") or "MONITORED"

    if score >= 7:
        confidence_dots = "●●●●●"
        confidence_level = "HØY"
    elif score >= 4:
        confidence_dots = "●●●○○"
        confidence_level = "MEDIUM"
    else:
        confidence_dots = "●●○○○"
        confidence_level = "LAV"

    kickoff = pick.get("kickoff_time") or pick.get("kickoff") or pick.get("commence_time")

    return {
        "pick_id": pick.get("id") or pick.get("pick_id"),
        "match": {
            "home_team": pick.get("home_team", ""),
            "away_team": pick.get("away_team", ""),
            "league": pick.get("league") or "",
            "kickoff_oslo": _format_oslo(kickoff),
        },
        "event": event,
        "why_points": why_points,
        "data_tag": {"label": tag_label, "emoji": tag_emoji},
        "system": {
            "atomic_score": score,
            "tier": tier,
            "confidence_dots": confidence_dots,
            "confidence_level": confidence_level,
        },
    }


MIN_PROB_PCT = 30.0  # under 30% = ikke "høysannsynlighet" — ekskluderes


def build_daily_signaldesk(picks: list[dict], date_iso: str | None = None,
                           max_events: int = 5,
                           min_prob_pct: float = MIN_PROB_PCT) -> dict:
    """
    Bygg dagens Signaldesk fra liste med picks_v2-rader (caller henter fra DB).

    Filterkjede (i rekkefølge):
    1. Tier ∈ {ATOMIC, EDGE}
    2. Ikke dummy-pick (atomic=0 + EV~0%)
    3. Event model_prob_pct >= min_prob_pct (default 30%)
    4. Sorter event.model_prob_pct descending, velg topp max_events
    """
    if date_iso is None:
        date_iso = datetime.now(timezone.utc).date().isoformat()

    eligible = [
        p for p in picks
        if (p.get("tier") in ("ATOMIC", "EDGE")) and not is_dummy_pick(p)
    ]
    cards = [build_event_card(p) for p in eligible]
    cards = [c for c in cards if c["event"]["model_prob_pct"] >= min_prob_pct]
    cards.sort(key=lambda c: c["event"]["model_prob_pct"], reverse=True)
    selected = cards[:max_events]

    if selected:
        probs = [c["event"]["model_prob_pct"] for c in selected]
        edges = [c["event"]["edge_pct"] for c in selected]
        stats = {
            "avg_probability_pct": round(sum(probs) / len(probs), 1),
            "expected_hit_count": round(sum(probs) / 100.0, 1),
            "total_edge_pct": round(sum(edges), 1),
            "event_count": len(selected),
        }
    else:
        stats = {
            "avg_probability_pct": 0.0,
            "expected_hit_count": 0.0,
            "total_edge_pct": 0.0,
            "event_count": 0,
        }

    return {
        "date": date_iso,
        "phase": "Phase 2 — Probability System",
        "events": selected,
        "stats": stats,
        "filtered_count": {
            "input_total": len(picks),
            "dummy_filtered": sum(1 for p in picks if is_dummy_pick(p)),
            "tier_filtered": sum(
                1 for p in picks
                if p.get("tier") not in ("ATOMIC", "EDGE")
            ),
            "eligible": len(eligible),
            "selected": len(selected),
        },
    }


def _format_norsk_date(iso_date: str) -> str:
    months = ["jan","feb","mar","apr","mai","jun","jul","aug","sep","okt","nov","des"]
    try:
        d = datetime.fromisoformat(iso_date)
        return f"{d.day}\\. {months[d.month - 1]} {d.year}"
    except Exception:
        return iso_date


def format_signaldesk_telegram(signaldesk: dict, escape_fn=None) -> str:
    """Render Daily Signaldesk stack as MarkdownV2 (top-of-thread post)."""
    e = escape_fn or _escape_mdv2
    events = signaldesk.get("events") or []
    stats = signaldesk.get("stats") or {}
    date_norsk = _format_norsk_date(signaldesk.get("date") or "")
    phase = signaldesk.get("phase") or "Phase 2"

    if not events:
        return (
            "🔥 *SESOMNOD SIGNALDESK*\n"
            f"{date_norsk} · {e(phase)}\n\n"
            "_Ingen høysannsynlighets\\-events i dag\\._\n\n"
            "Vi venter heller enn å presse picks som ikke konvergerer\\. "
            "Disiplin \\> volum\\.\n\n"
            "🌐 sesomnod\\.com · 18\\+ Spill ansvarlig"
        )

    lines = [
        "🔥 *SESOMNOD SIGNALDESK*",
        f"{date_norsk} · {e(phase)}",
        "",
        f"_Dagens {len(events)} høyeste sannsynligheter:_",
        "",
    ]

    for i, c in enumerate(events, 1):
        tier_emoji = "⚛️" if c["system"]["tier"] == "ATOMIC" else "🟡"
        match_short = f"{c['match']['home_team']} vs {c['match']['away_team']}"
        if len(match_short) > 38:
            match_short = match_short[:35] + "..."
        lines.append(
            f"{i}\\. {tier_emoji} {e(c['event']['label'])} — "
            f"*{c['event']['model_prob_pct']}%*"
        )
        lines.append(
            f"   {e(match_short)} @ *{c['event']['odds']:.2f}* "
            f"\\| {c['data_tag']['emoji']}"
        )
        lines.append("")

    lines.extend([
        "═══════════════════════════════",
        f"📊 *Avg sannsynlighet:* {stats.get('avg_probability_pct', 0)}%",
        f"📈 *Forventet hit:* {stats.get('expected_hit_count', 0)}/{len(events)}",
        f"💰 *Total edge:* \\+{stats.get('total_edge_pct', 0)}%",
        "",
        "_Full analyse av hver event følger nedenfor →_",
        "",
        "🌐 sesomnod\\.com · 18\\+ Spill ansvarlig",
    ])

    return "\n".join(lines)


def format_event_card_telegram(card: dict, position: int, total: int,
                               ladder: dict | None = None,
                               escape_fn=None) -> str:
    """Render single event-card as MarkdownV2 (thread reply under stack)."""
    e = escape_fn or _escape_mdv2
    m = card["match"]
    ev = card["event"]
    why = card.get("why_points") or []
    tag = card["data_tag"]
    sys_ = card["system"]

    league_str = e(m['league']) if m.get('league') else "\\—"
    kickoff_str = e(m['kickoff_oslo']) if m.get('kickoff_oslo') else "\\—"
    lines = [
        f"🔥 *\\#{position}/{total}: {e(ev['label'])}*",
        f"*{e(m['home_team'])} vs {e(m['away_team'])}* · {league_str}",
        f"🕐 {kickoff_str}",
        "",
        "═══════════════════════════════",
        f"🎯 *Sannsynlighet: {ev['model_prob_pct']}%*",
        f"📊 Modell: *{ev['model_prob_pct']}%* \\| Marked: *{ev['market_prob_pct']}%*",
        f"💰 Edge: *\\+{ev['edge_pct']}%* \\| Odds: *{ev['odds']:.2f}*",
        "",
        "═══════════════════════════════",
        "🧠 *HVORFOR DETTE ER MEST SANNSYNLIG:*",
        "",
    ]

    for p in why:
        lines.append(f"{p['emoji']} {e(p['text'])}")
    lines.append("")

    if ladder:
        cur = ladder.get("current_nok", 1000)
        growth = ladder.get("growth_pct", 0)
        next_pick = ladder.get("next_pick") or ""
        next_odds = ladder.get("next_odds") or 0.0
        potential = ladder.get("potential_nok", 0)
        lines.extend([
            "═══════════════════════════════",
            "🤖 *ORAKLION LIVE LADDER*",
            f"Start: 1 000 → Nå: *{cur} kr* \\({growth}%\\)",
            f"Neste: {e(next_pick)} @ {next_odds:.2f}",
            f"Potensial: *{potential} kr*",
            "",
        ])

    lines.extend([
        "═══════════════════════════════",
        "🧬 *SYSTEM SIGNAL*",
        f"Omega: `{sys_['atomic_score']}/9` · *{e(sys_['tier'])}*",
        f"Konfidens: {sys_['confidence_dots']} {e(sys_['confidence_level'])}",
        f"Data: {e(tag['label'])} {tag['emoji']}",
        "",
        "🌐 sesomnod\\.com",
    ])

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────
# DECISION ENGINE — Phase 3: Dominance, ikke valg-liste
# Vi viser HVA som er beste valg + HVORFOR alt annet er dårligere.
# Pure functions; ingen DB-kall. Caller henter picks og passer som list[dict].
# ─────────────────────────────────────────────────────────────────────────

# Filter-grenser (ikke-forhandlingsbare per Don)
DECISION_MIN_PROB_PCT = 65.0
DECISION_MIN_EDGE_PCT = 5.0
DECISION_MIN_CONFIDENCE = 0.6  # SILVER threshold
DECISION_MAX_PLAYS_PER_DAY = 3

# V2 (Phase 3, 2026-04-27): strengere — confidence GOLD (0.70), dominance >= 0.055
DECISION_V2_MIN_CONFIDENCE = 0.70
DECISION_V2_MIN_DOMINANCE = 0.055

# Ekskluderte markeder (motor-status per CLAUDE.md MEMORY)
EXCLUDED_MARKETS = {"btts", "btts_yes", "btts_no", "corners"}


def _count_populated_signals(pick: dict) -> int:
    """Tell hvor mange signal-felter pick'en har faktisk data for."""
    return sum([
        _f(pick.get("signal_xg_home")) is not None,
        _f(pick.get("signal_xg_away")) is not None,
        _is_active(pick.get("signal_velocity")),
        _is_active(pick.get("signal_weather")),
        bool(pick.get("signals_triggered")),
    ])


def calculate_dominance_score(market: dict) -> float:
    """
    DOMINANCE_SCORE = (prob*0.5 + edge*0.3 + confidence*0.2) / risk_factor

    risk_factor = 1.0 + (1 - prob)  → høy prob gir lav risk → høyere score.
    Resultat clamped til [0, 1].
    """
    prob = max(0.0, min(1.0, (market.get("probability") or 0.0) / 100.0))
    edge = max(0.0, (market.get("edge") or 0.0) / 100.0)
    confidence = max(0.0, min(1.0, market.get("confidence") or 0.0))
    risk_factor = 1.0 + (1.0 - prob)
    raw = (prob * 0.5 + edge * 0.3 + confidence * 0.2) / risk_factor
    return round(min(1.0, raw), 4)


def calculate_market_confidence(pick: dict, market: dict) -> float:
    """
    Konfidens 0-1 basert på data-completeness + signal-konvergens.
    Brukes som "konfidens"-vekt i dominance-score.
    """
    populated = _count_populated_signals(pick)
    score = int(pick.get("atomic_score") or 0)
    edge = market.get("edge") or 0.0

    if score >= 9 and populated >= 4 and edge >= 12:
        return 0.95
    if score >= 7 and populated >= 3 and edge >= 8:
        return 0.85
    if score >= 7 and populated >= 2 and edge >= 5:
        return 0.70
    if score >= 4 and populated >= 1:
        return 0.55
    return 0.40


def generate_all_markets_for_pick(pick: dict) -> list[dict]:
    """
    Generer alle markeder vi har modell for, per pick.

    Per 2026-04-27 har vi pålitelig modell for:
    - 1X2 main pick (fra dk_pick + model_prob/market_prob)

    DEFERED (motor-fix kreves):
    - BTTS (hardkodet 50/50 i dixon_coles_engine.py per CLAUDE.md MEMORY)
    - Over/Under 1.5/2.5/3.5 (krever signal_xg_home + signal_xg_away populert
      for hver pick — i dag 0/12 — pipeline-fix venter)
    - Corners (ikke i picks_v2 schema)

    Når xG-pipeline fikses: legg til Poisson OU-markeder her.
    """
    markets: list[dict] = []
    main_event = select_highest_prob_event(pick)
    label_lower = (main_event.get("label") or "").lower()

    if not any(x in label_lower for x in EXCLUDED_MARKETS):
        confidence = calculate_market_confidence(pick, {
            "edge": main_event.get("edge_pct") or 0.0,
        })
        markets.append({
            "label": main_event["label"],
            "market_key": "main_pick",
            "probability": main_event["model_prob_pct"],
            "market_implied": main_event["market_prob_pct"],
            "edge": main_event["edge_pct"],
            "odds": main_event["odds"],
            "confidence": confidence,
            "data_source": "Dixon-Coles + Pinnacle",
            "excluded_reason": None,
        })

    # Stub for Over/Under når xG er populert
    xg_h = _f(pick.get("signal_xg_home"))
    xg_a = _f(pick.get("signal_xg_away"))
    if xg_h is not None and xg_a is not None:
        total_xg = xg_h + xg_a
        # Enkel Poisson approximation for OU 2.5: P(total > 2.5) når λ = total_xg
        # P(0..2 mål) = e^-λ * (1 + λ + λ²/2). Vi gjør konservativ approks:
        try:
            import math
            lam = total_xg
            p0 = math.exp(-lam)
            p1 = p0 * lam
            p2 = p1 * lam / 2.0
            p_over_25 = max(0.0, min(1.0, 1.0 - (p0 + p1 + p2)))
            edge_25 = (p_over_25 * 100) - 55.0  # rough market-implied baseline
            confidence_25 = calculate_market_confidence(pick, {"edge": edge_25})
            markets.append({
                "label": "Over 2.5 mål",
                "market_key": "over_25",
                "probability": round(p_over_25 * 100, 1),
                "market_implied": 55.0,
                "edge": round(edge_25, 1),
                "odds": round(1.0 / max(0.01, p_over_25 - (edge_25 / 100)), 2),
                "confidence": confidence_25,
                "data_source": "Poisson(λ=total_xG)",
                "excluded_reason": None,
            })
        except Exception:
            pass

    return markets


def select_dominant_play(markets: list[dict]) -> dict | None:
    """
    Filtrer + velg DEN ENE høyeste dominance_score.
    Filter-grenser ikke-forhandlingsbare:
      probability >= 65, edge >= +5%, confidence >= 0.6, ikke EXCLUDED-marked.
    """
    eligible = []
    for m in markets:
        label_lower = (m.get("label") or "").lower()
        if any(x in label_lower for x in EXCLUDED_MARKETS):
            continue
        if (m.get("probability") or 0.0) < DECISION_MIN_PROB_PCT:
            continue
        if (m.get("edge") or 0.0) < DECISION_MIN_EDGE_PCT:
            continue
        if (m.get("confidence") or 0.0) < DECISION_MIN_CONFIDENCE:
            continue
        m_with_score = dict(m)
        m_with_score["dominance_score"] = calculate_dominance_score(m)
        eligible.append(m_with_score)

    if not eligible:
        return None
    eligible.sort(key=lambda x: x["dominance_score"], reverse=True)
    return eligible[0]


def generate_why_not_others(markets: list[dict], chosen: dict) -> list[str]:
    """Eksklusjons-grunn for hvert ikke-valgt marked. Returner max 4 linjer."""
    reasons: list[str] = []
    chosen_label = (chosen or {}).get("label", "")
    chosen_prob = (chosen or {}).get("probability", 0.0)

    for m in markets:
        if m.get("label") == chosen_label:
            continue
        label_lower = (m.get("label") or "").lower()
        prob = m.get("probability") or 0.0
        edge = m.get("edge") or 0.0
        conf = m.get("confidence") or 0.0
        label = m.get("label") or "?"

        if any(x in label_lower for x in EXCLUDED_MARKETS):
            reasons.append(f"{label}: deferes (motor under kalibrering)")
        elif prob < 55.0:
            reasons.append(f"{label}: {prob:.0f}% (coinflip — ingen edge)")
        elif edge < 3.0:
            reasons.append(f"{label}: edge +{edge:.1f}% (markedet er pris-effektivt)")
        elif conf < DECISION_MIN_CONFIDENCE:
            reasons.append(f"{label}: utilstrekkelig data-konfidens")
        elif prob < chosen_prob:
            diff = chosen_prob - prob
            reasons.append(
                f"{label}: {prob:.0f}% ({diff:.0f}pp lavere safety enn dominant play)"
            )
        else:
            reasons.append(f"{label}: dominance_score under valgt play")

    return reasons[:4]


def calculate_tier(market: dict, pick: dict) -> tuple[str, str, str]:
    """
    Returner (tier_name, tier_emoji, star_string).

    PLATINUM: atomic >= 9 + signals >= 4 + edge > 12% + Big5
    GOLD:     atomic >= 7 + signals >= 3 + edge > 8%
    SILVER:   atomic >= 7 + signals >= 2 + edge > 5%
    EXCLUDED: alt under
    """
    score = int(pick.get("atomic_score") or 0)
    edge = market.get("edge") or 0.0
    populated = _count_populated_signals(pick)
    league = (pick.get("league") or "").lower()
    is_big5 = any(
        l in league for l in ("premier", "la liga", "bundesliga", "serie a", "ligue 1")
    )

    if score >= 9 and populated >= 4 and edge > 12 and is_big5:
        return ("PLATINUM", "🏆", "⭐⭐⭐⭐⭐")
    if score >= 7 and populated >= 3 and edge > 8:
        return ("GOLD", "💎", "⭐⭐⭐⭐")
    if score >= 7 and populated >= 2 and edge > 5:
        return ("SILVER", "🥈", "⭐⭐⭐")
    return ("EXCLUDED", "❌", "")


def build_decision_play(pick: dict) -> dict | None:
    """
    Bygg ett dominant play fra én pick. Returner None hvis ingen kvalifiserer.

    Inkluderer pick-meta + valgt market + why_not_others + tier.
    """
    if is_dummy_pick(pick):
        return None
    markets = generate_all_markets_for_pick(pick)
    chosen = select_dominant_play(markets)
    if chosen is None:
        return None

    tier_name, tier_emoji, star_string = calculate_tier(chosen, pick)
    if tier_name == "EXCLUDED":
        return None

    why = generate_why_points(pick)
    why_not = generate_why_not_others(markets, chosen)
    tag_label, tag_emoji = calculate_data_tag(pick)
    kickoff = pick.get("kickoff_time") or pick.get("kickoff") or pick.get("commence_time")

    return {
        "pick_id": pick.get("id") or pick.get("pick_id"),
        "match": {
            "home_team": pick.get("home_team", ""),
            "away_team": pick.get("away_team", ""),
            "league": pick.get("league") or "",
            "kickoff_oslo": _format_oslo(kickoff),
        },
        "chosen": chosen,
        "all_markets": markets,
        "why_points": why,
        "why_not_others": why_not,
        "tier": {"name": tier_name, "emoji": tier_emoji, "stars": star_string},
        "data_tag": {"label": tag_label, "emoji": tag_emoji},
    }


def build_decision_desk(picks: list[dict], date_iso: str | None = None,
                        scanned_matches: int | None = None) -> dict:
    """
    Generer dagens Decision Desk fra liste pending picks_v2-rader.

    Returnerer max DECISION_MAX_PLAYS_PER_DAY plays sortert etter
    dominance_score descending. Empty-state hvis 0 kvalifiserer.
    """
    if date_iso is None:
        date_iso = datetime.now(timezone.utc).date().isoformat()

    plays_raw = []
    for p in picks:
        play = build_decision_play(p)
        if play:
            plays_raw.append(play)

    plays_raw.sort(
        key=lambda x: x["chosen"].get("dominance_score", 0.0), reverse=True
    )
    plays = plays_raw[:DECISION_MAX_PLAYS_PER_DAY]

    if scanned_matches is None:
        scanned_matches = len(picks)
    qualified = len(plays)
    filtered_out = max(0, scanned_matches - qualified)

    if plays:
        scores = [p["chosen"].get("dominance_score", 0.0) for p in plays]
        edges = [p["chosen"].get("edge", 0.0) for p in plays]
        probs = [p["chosen"].get("probability", 0.0) for p in plays]
        stats = {
            "avg_dominance_score": round(sum(scores) / len(scores), 4),
            "expected_hit_count": round(sum(probs) / 100.0, 1),
            "total_edge_pct": round(sum(edges), 1),
        }
    else:
        stats = {
            "avg_dominance_score": 0.0,
            "expected_hit_count": 0.0,
            "total_edge_pct": 0.0,
        }

    return {
        "date": date_iso,
        "phase": "Phase 3 — Decision Engine",
        "scanned_matches": scanned_matches,
        "qualified_matches": qualified,
        "filtered_out": filtered_out,
        "dominant_plays": plays,
        "stats": stats,
        "no_qualified_today": qualified == 0,
    }


def format_decision_desk_telegram(desk: dict, escape_fn=None) -> str:
    """Render Decision Desk som MarkdownV2-streng for Telegram."""
    e = escape_fn or _escape_mdv2
    plays = desk.get("dominant_plays") or []
    stats = desk.get("stats") or {}
    date_norsk = _format_norsk_date(desk.get("date") or "")
    phase = desk.get("phase") or "Phase 3"
    scanned = desk.get("scanned_matches") or 0
    filtered_out = desk.get("filtered_out") or 0

    if not plays:
        return (
            "🔒 *SESOMNOD DECISION DESK*\n"
            f"{date_norsk}\n\n"
            "_Ingen kvalifiserte beslutninger i dag\\._\n\n"
            f"Av *{scanned}* kamper analysert ble ingen klassifisert "
            "som DOMINANT etter våre filtre:\n"
            "\\- Sannsynlighet ≥ 65%\n"
            "\\- Edge ≥ \\+5%\n"
            "\\- Konfidens ≥ SILVER\n"
            "\\- Bevist modell\\-marked\n\n"
            "_Vi sender ikke svake spill\\. Vi venter til markedet "
            "gir oss klar edge\\._\n\n"
            "Tilbake i morgen kl 09:00\\.\n\n"
            "🌐 sesomnod\\.com"
        )

    count = len(plays)
    lines = [
        "🔥 *SESOMNOD DECISION DESK*",
        f"{date_norsk} · {e(phase)}",
        "",
        f"Vi analyserte *{scanned}* kamper\\.",
        f"Vi sender *{count}* beslutninger\\.",
        "",
    ]

    for i, play in enumerate(plays, 1):
        m = play["match"]
        c = play["chosen"]
        tier = play["tier"]
        tag = play["data_tag"]
        why = play.get("why_points") or []
        why_not = play.get("why_not_others") or []

        league_str = e(m["league"]) if m.get("league") else "\\—"
        kickoff_str = e(m["kickoff_oslo"]) if m.get("kickoff_oslo") else "\\—"

        lines.append("═══════════════════════════════")
        lines.append(f"🏆 *DOMINANT \\#{i} — {tier['emoji']} {e(tier['name'])}*")
        lines.append(f"*{e(c['label'])}* — *{c['probability']}%*")
        lines.append(f"{e(m['home_team'])} vs {e(m['away_team'])} · {kickoff_str}")
        lines.append(f"_{league_str}_")
        lines.append("")
        lines.append("🧠 *HVORFOR DET DOMINERER:*")
        for p in why[:4]:
            lines.append(f"{p['emoji']} {e(p['text'])}")
        lines.append("")

        if why_not:
            lines.append("❌ *HVORFOR IKKE ANDRE SPILL:*")
            for r in why_not:
                lines.append(f"\\- {e(r)}")
            lines.append("")

        lines.append(
            f"📊 Modell: *{c['probability']}%* \\| "
            f"Marked: *{c['market_implied']}%* \\| "
            f"Edge: *\\+{c['edge']}%*"
        )
        lines.append(
            f"{tier['stars']} *{e(tier['name'])}* · {tag['emoji']} {e(tag['label'])}"
        )
        lines.append(f"⚖️ Dominance score: `{c.get('dominance_score', 0):.3f}`")
        lines.append("")

    lines.extend([
        "═══════════════════════════════",
        f"📊 *Avg DOMINANCE\\-score:* {stats.get('avg_dominance_score', 0):.3f}",
        f"🎯 *Forventet hit:* {stats.get('expected_hit_count', 0)}/{count}",
        f"💰 *Total edge:* \\+{stats.get('total_edge_pct', 0)}%",
        f"🔍 *Filtrert bort:* {filtered_out} kamper",
        "",
        "🌐 sesomnod\\.com · Phase 3 · 18\\+ Spill ansvarlig",
    ])

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────
# DECISION ENGINE V2 — Probability Event-baseert, 3-lag filter, Dominance V2
# Bruker services.probability_event_generator for å produsere 9 events per
# pick (med dc_*-data), filtrere via 3 lag, score via V2-formel, og levere
# max 3 dominant plays per dag.
# ─────────────────────────────────────────────────────────────────────────

def calculate_dominance_score_v2(market: dict) -> float:
    """
    DOMINANCE_V2 = (prob*0.4 + edge*0.4 + confidence*0.2) × log10(odds)

    Justeringer fra V1:
    - Edge vekt: 0.3 → 0.4 (straffer null-edge tyngre)
    - Prob vekt: 0.5 → 0.4
    - Multiplier: log10(odds) — straffer ultra-favoritter (~0% bidrag fra
      Over 0.5 ved odds 1.05) men belønner reell payoff
    """
    import math as _math
    prob = max(0.0, min(1.0, (market.get("probability") or 0.0) / 100.0))
    edge = max(0.0, (market.get("edge") or 0.0) / 100.0)
    confidence = max(0.0, min(1.0, market.get("confidence") or 0.0))
    odds = max(1.01, market.get("odds") or 1.0)
    base = prob * 0.4 + edge * 0.4 + confidence * 0.2
    multiplier = _math.log10(odds)
    return round(base * multiplier, 4)


def passes_all_filters_v2(event: dict, pick: dict) -> tuple[bool, list[str]]:
    """
    3-lag filter for Decision Engine V2. Returnerer (passed, failed_reasons).

    LAYER 1 — DATA QUALITY: confidence ≥ GOLD, completeness ∈ {FULL, SOLID},
    ingen fallback-kilde
    LAYER 2 — STATISTICAL: prob ≥ 65%, edge ≥ +5%
    LAYER 3 — BUSINESS: ekskluderte markeder, Brasil/Argentina ATOMIC blocked,
    dominance V2 ≥ 0.055
    """
    failed: list[str] = []

    # LAYER 1
    conf = event.get("confidence", 0.0)
    if conf < DECISION_V2_MIN_CONFIDENCE:
        failed.append(f"L1: confidence {conf:.2f} < GOLD ({DECISION_V2_MIN_CONFIDENCE})")
    completeness = event.get("data_completeness") or ""
    if completeness not in ("FULL", "SOLID"):
        failed.append(f"L1: data_completeness {completeness or 'none'}")
    calc_src = (event.get("calculation_source") or "").lower()
    if "fallback" in calc_src or "1/odds" in calc_src:
        failed.append(f"L1: fallback calculation_source")

    # LAYER 2
    prob = event.get("probability") or 0.0
    edge = event.get("edge") or 0.0
    if prob < DECISION_MIN_PROB_PCT:
        failed.append(f"L2: prob {prob:.1f}% < {DECISION_MIN_PROB_PCT}%")
    if edge < DECISION_MIN_EDGE_PCT:
        failed.append(f"L2: edge +{edge:.1f}% < +{DECISION_MIN_EDGE_PCT}%")

    # LAYER 3
    label_lower = (event.get("label") or "").lower()
    if any(x in label_lower for x in EXCLUDED_MARKETS):
        failed.append(f"L3: market i ekskluderingsliste")
    league = (pick.get("league") or "").lower()
    tier = pick.get("tier") or ""
    if tier == "ATOMIC" and any(x in league for x in ("brasil", "argentin")):
        failed.append("L3: Brasil/Argentina ATOMIC blocked (0/2 historisk)")
    dom = event.get("dominance_score_v2", 0.0)
    if dom < DECISION_V2_MIN_DOMINANCE:
        failed.append(f"L3: dominance V2 {dom:.4f} < {DECISION_V2_MIN_DOMINANCE}")

    return (len(failed) == 0, failed)


def _enrich_event_with_filter_meta(event: dict, pick: dict) -> dict:
    """
    Ta et raw event fra probability_event_generator og berik med:
    - confidence (mappet fra atomic_score + data_completeness)
    - dominance_score_v2
    Disse trengs av passes_all_filters_v2.
    """
    score = int(pick.get("atomic_score") or 0)
    completeness = event.get("data_completeness") or "LIMITED"
    if score >= 9 and completeness == "FULL":
        confidence = 0.95
    elif score >= 7 and completeness in ("FULL", "SOLID"):
        confidence = 0.85
    elif score >= 4 and completeness in ("FULL", "SOLID"):
        confidence = 0.70
    else:
        confidence = 0.55
    enriched = dict(event)
    # Felter passes_all_filters_v2 forventer i flat form:
    enriched["probability"] = event.get("probability_pct", 0.0)
    enriched["edge"] = event.get("edge_pct", 0.0) or 0.0
    enriched["confidence"] = confidence
    enriched["dominance_score_v2"] = calculate_dominance_score_v2(enriched)
    return enriched


def build_decision_play_v2(pick: dict) -> dict | None:
    """
    Bygg ett dominant play V2 fra én pick med dc_*-data.

    Steg:
    1. Generer alle events via probability_event_generator
    2. Selv-validér events (mat. konsistens)
    3. Berik hvert event med confidence + dominance_score_v2
    4. Filtrer via 3-lag (passes_all_filters_v2)
    5. Velg event med høyeste dominance_score_v2
    6. Returner None hvis ingen passerer
    """
    if is_dummy_pick(pick):
        return None

    try:
        from services.probability_event_generator import (
            generate_events_for_match, validate_event_coherence,
        )
    except ImportError:
        return None

    raw_events = generate_events_for_match(pick)
    if not raw_events:
        return None

    validation = validate_event_coherence(raw_events)
    events = validation.get("events") or []
    if not events:
        return None

    eligible = []
    rejected = []
    for ev in events:
        enriched = _enrich_event_with_filter_meta(ev, pick)
        passed, reasons = passes_all_filters_v2(enriched, pick)
        if passed:
            eligible.append(enriched)
        else:
            rejected.append({"label": ev["label"], "reasons": reasons,
                              "probability_pct": ev["probability_pct"]})

    if not eligible:
        return None

    eligible.sort(key=lambda x: x["dominance_score_v2"], reverse=True)
    chosen = eligible[0]

    why_not = []
    for r in rejected[:6]:
        primary = r["reasons"][0] if r["reasons"] else "ekskludert"
        why_not.append(f"{r['label']}: {primary} (prob {r['probability_pct']}%)")

    tag_label, tag_emoji = calculate_data_tag(pick)
    tier = pick.get("tier") or "MONITORED"
    score = int(pick.get("atomic_score") or 0)

    if chosen["confidence"] >= 0.95:
        tier_name, tier_emoji, stars = "PLATINUM", "🏆", "⭐⭐⭐⭐⭐"
    elif chosen["confidence"] >= 0.85:
        tier_name, tier_emoji, stars = "GOLD", "💎", "⭐⭐⭐⭐"
    else:
        tier_name, tier_emoji, stars = "SILVER", "🥈", "⭐⭐⭐"

    kickoff = pick.get("kickoff_time") or pick.get("kickoff") or pick.get("commence_time")

    return {
        "pick_id": pick.get("id") or pick.get("pick_id"),
        "match": {
            "home_team": pick.get("home_team", ""),
            "away_team": pick.get("away_team", ""),
            "league": pick.get("league") or "",
            "kickoff_oslo": _format_oslo(kickoff),
        },
        "chosen": {
            "label": chosen["label"],
            "category": chosen.get("category"),
            "probability_pct": chosen["probability_pct"],
            "confidence_interval": chosen.get("confidence_interval"),
            "edge_pct": chosen.get("edge_pct"),
            "odds": chosen.get("odds"),
            "calculation_source": chosen.get("calculation_source"),
            "dominance_score_v2": chosen["dominance_score_v2"],
            "confidence": chosen["confidence"],
        },
        "why_points": chosen.get("why_points") or [],
        "why_not_others": why_not,
        "all_events_count": len(events),
        "passed_filter_count": len(eligible),
        "tier": {"name": tier_name, "emoji": tier_emoji, "stars": stars},
        "data_tag": {"label": tag_label, "emoji": tag_emoji},
        "atomic_score": score,
        "asset_tier": tier,
    }


def build_decision_desk_v2(picks: list[dict], date_iso: str | None = None,
                           scanned_matches: int | None = None) -> dict:
    """
    Phase 3 V2 — generer dagens Decision Desk fra liste pending picks
    med dc_*-data populert. Maks DECISION_MAX_PLAYS_PER_DAY plays.
    """
    if date_iso is None:
        date_iso = datetime.now(timezone.utc).date().isoformat()

    plays_raw = []
    for p in picks:
        play = build_decision_play_v2(p)
        if play:
            plays_raw.append(play)

    plays_raw.sort(key=lambda x: x["chosen"].get("dominance_score_v2", 0.0),
                   reverse=True)
    plays = plays_raw[:DECISION_MAX_PLAYS_PER_DAY]

    if scanned_matches is None:
        scanned_matches = len(picks)
    qualified = len(plays)
    filtered_out = max(0, scanned_matches - qualified)

    if plays:
        scores = [p["chosen"]["dominance_score_v2"] for p in plays]
        edges = [p["chosen"].get("edge_pct") or 0.0 for p in plays]
        probs = [p["chosen"]["probability_pct"] for p in plays]
        stats = {
            "avg_dominance_v2": round(sum(scores) / len(scores), 4),
            "expected_hit_count": round(sum(probs) / 100.0, 1),
            "total_edge_pct": round(sum(edges), 1),
            "avg_probability_pct": round(sum(probs) / len(probs), 1),
        }
    else:
        stats = {
            "avg_dominance_v2": 0.0, "expected_hit_count": 0.0,
            "total_edge_pct": 0.0, "avg_probability_pct": 0.0,
        }

    return {
        "date": date_iso,
        "phase": "Phase 3 V2 — Probability Event Engine",
        "scanned_matches": scanned_matches,
        "qualified_matches": qualified,
        "filtered_out": filtered_out,
        "dominant_plays": plays,
        "stats": stats,
        "no_qualified_today": qualified == 0,
    }


def format_decision_desk_v2_telegram(desk: dict, escape_fn=None) -> str:
    """V2-format med konfidens-intervall, why_not_others, tier-emoji."""
    e = escape_fn or _escape_mdv2
    plays = desk.get("dominant_plays") or []
    stats = desk.get("stats") or {}
    date_norsk = _format_norsk_date(desk.get("date") or "")
    scanned = desk.get("scanned_matches") or 0
    filtered_out = desk.get("filtered_out") or 0

    if not plays:
        return (
            "🔒 *SESOMNOD DECISION DESK V2*\n"
            f"{date_norsk}\n\n"
            "_Ingen kvalifiserte beslutninger i dag\\._\n\n"
            f"Av *{scanned}* picks analysert ble ingen klassifisert "
            "som DOMINANT etter 3\\-lags filter:\n"
            "\\- L1: Konfidens ≥ GOLD \\+ data SOLID/FULL\n"
            "\\- L2: Sannsynlighet ≥ 65% \\+ edge ≥ \\+5%\n"
            "\\- L3: Dominance V2 ≥ 0\\.055\n\n"
            "_Vi sender ikke svake spill\\. Vi venter til markedet "
            "gir oss klar edge\\._\n\n"
            "🌐 sesomnod\\.com"
        )

    count = len(plays)
    lines = [
        "🔥 *SESOMNOD DECISION DESK V2*",
        f"{date_norsk} · Phase 3 · Probability Event Engine",
        "",
        f"Vi analyserte *{scanned}* picks\\.",
        f"Vi sender *{count}* dominant beslutninger\\.",
        "",
    ]

    for i, play in enumerate(plays, 1):
        m = play["match"]
        c = play["chosen"]
        tier = play["tier"]
        tag = play["data_tag"]
        why = play.get("why_points") or []
        why_not = play.get("why_not_others") or []
        ci = c.get("confidence_interval") or [c["probability_pct"], c["probability_pct"]]
        league_str = e(m["league"]) if m.get("league") else "\\—"
        kickoff_str = e(m["kickoff_oslo"]) if m.get("kickoff_oslo") else "\\—"
        edge_str = f"\\+{c.get('edge_pct') or 0:.1f}%" if c.get("edge_pct") is not None else "n/a"
        odds_str = f"{c.get('odds') or 0:.2f}" if c.get("odds") else "n/a"

        lines.append("═══════════════════════════════")
        lines.append(f"🏆 *DOMINANT \\#{i} — {tier['emoji']} {e(tier['name'])}*")
        lines.append(f"*{e(c['label'])}* — *{c['probability_pct']}%* "
                     f"\\[{ci[0]}\\-{ci[1]}\\]")
        lines.append(f"{e(m['home_team'])} vs {e(m['away_team'])} · {kickoff_str}")
        lines.append(f"_{league_str}_")
        lines.append("")
        lines.append("🧠 *HVORFOR DET DOMINERER:*")
        for p in why[:4]:
            lines.append(f"{p['emoji']} {e(p['text'])}")
        lines.append("")

        if why_not:
            lines.append("❌ *HVORFOR IKKE ANDRE EVENTS:*")
            for r in why_not[:4]:
                lines.append(f"\\- {e(r)}")
            lines.append("")

        lines.append(f"📊 Edge: *{edge_str}* \\| Odds: *{odds_str}*")
        lines.append(
            f"{tier['stars']} *{e(tier['name'])}* · {tag['emoji']} {e(tag['label'])}"
        )
        lines.append(
            f"⚖️ Dominance V2: `{c.get('dominance_score_v2', 0):.4f}` "
            f"\\| Events filtrert: {play.get('passed_filter_count', 0)}/"
            f"{play.get('all_events_count', 0)}"
        )
        lines.append("")

    lines.extend([
        "═══════════════════════════════",
        f"📊 *Avg DOMINANCE V2:* {stats.get('avg_dominance_v2', 0):.4f}",
        f"🎯 *Forventet hit:* {stats.get('expected_hit_count', 0)}/{count}",
        f"💰 *Total edge:* \\+{stats.get('total_edge_pct', 0)}%",
        f"🔍 *Filtrert bort:* {filtered_out} picks",
        "",
        "🌐 sesomnod\\.com · Phase 3 · 18\\+ Spill ansvarlig",
    ])

    return "\n".join(lines)


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
