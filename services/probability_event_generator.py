"""
Probability Event Generator (Phase 3).

Genererer 4-9 markedsspesifikke events per kamp fra picks_v2-rader som
har dc_*-felter populert (etter VEI A 2026-04-27).

Eksporter:
- generate_events_for_match(pick) -> list[event-dict]
- validate_event_coherence(events) -> {valid, violations, events}

Pure functions; ingen DB-kall, ingen network. Caller henter pick-rad
fra picks_v2 og passer som dict.

Hver event:
  {
    "label": "Over 1.5 mål",
    "category": "totals" | "1x2" | "btts",
    "probability_pct": float,
    "confidence_interval": [low, high],
    "market_implied_pct": float | None,
    "edge_pct": float | None,
    "odds": float | None,
    "calculation_source": str,
    "data_completeness": "FULL" | "SOLID" | "LIMITED",
    "why_points": list[dict],
  }
"""
from __future__ import annotations

import logging
import math
from typing import Any

logger = logging.getLogger("sesomnod.probability_events")

# Eksempler vi genererer (i prioritert rekkefølge for visning)
EVENT_DEFS = [
    # (label_norsk, category, dc_field, threshold_for_valid)
    ("Over 0.5 mål", "totals", None, None),    # P(over 0.5) = 1 - P(0 mål) — beregnet fra lambda
    ("Over 1.5 mål", "totals", "dc_over_15", None),
    ("Over 2.5 mål", "totals", "dc_over_25", None),
    ("Over 3.5 mål", "totals", "dc_over_35", None),
    ("Under 2.5 mål", "totals", "dc_under_25", None),
    ("Under 3.5 mål", "totals", "dc_under_35", None),
    ("Hjemmeseier", "1x2", "dc_home_win_prob", None),
    ("Borteseier", "1x2", "dc_away_win_prob", None),
    ("Uavgjort", "1x2", "dc_draw_prob", None),
    # BTTS deferes inntil vi har odds-feed for BTTS (motor virker per VEI A
    # surfacing — btts_yes 51% Bayern vs Augsburg er ekte penaltyblog-output).
]


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _data_completeness(pick: dict) -> str:
    """FULL hvis dc_lambda_home + dc_over_25 + atomic_score >= 7. Ellers grade."""
    score = int(pick.get("atomic_score") or 0)
    has_lambda = _f(pick.get("dc_lambda_home")) is not None
    has_ou = _f(pick.get("dc_over_25")) is not None
    if has_lambda and has_ou and score >= 7:
        return "FULL"
    if has_lambda and has_ou:
        return "SOLID"
    return "LIMITED"


def _confidence_interval(prob: float, n_eq: int = 50) -> tuple[float, float]:
    """
    Wilson-score 95% CI for ekvivalent binomial sample (n_eq).
    Default n_eq=50 = konservativ (bredere CI for små samples).
    Returnerer (low_pct, high_pct) i prosent.
    """
    if not 0 < prob < 1:
        return (round(prob * 100, 1), round(prob * 100, 1))
    z = 1.96
    denom = 1 + z**2 / n_eq
    centre = (prob + z**2 / (2 * n_eq)) / denom
    margin = z * math.sqrt(prob * (1 - prob) / n_eq + z**2 / (4 * n_eq**2)) / denom
    low = max(0.0, centre - margin)
    high = min(1.0, centre + margin)
    return (round(low * 100, 1), round(high * 100, 1))


def _market_implied(odds_field: float | None) -> float | None:
    if odds_field is None or odds_field <= 1.0:
        return None
    return 1.0 / odds_field


def _odds_from_prob(prob: float) -> float:
    if prob <= 0:
        return 0.0
    return round(1.0 / prob, 2)


def _why_points_for_event(pick: dict, label: str, category: str,
                          probability: float, edge: float | None) -> list[dict]:
    """Inline why-points basert på dc_*-data + signal-felt."""
    points: list[dict] = []

    lambda_h = _f(pick.get("dc_lambda_home"))
    lambda_a = _f(pick.get("dc_lambda_away"))
    if category == "totals" and lambda_h is not None and lambda_a is not None:
        total = lambda_h + lambda_a
        points.append({
            "emoji": "📈",
            "text": f"Forventet mål total: {total:.2f} ({lambda_h:.2f}+{lambda_a:.2f})",
            "source": "Dixon-Coles lambda",
        })

    if category == "1x2" and lambda_h is not None and lambda_a is not None:
        if "Hjemme" in label:
            points.append({
                "emoji": "🎯",
                "text": f"Hjemmelaget skaper {lambda_h:.2f} forventede mål",
                "source": "Dixon-Coles lambda",
            })
        elif "Borte" in label:
            points.append({
                "emoji": "🎯",
                "text": f"Bortelaget skaper {lambda_a:.2f} forventede mål",
                "source": "Dixon-Coles lambda",
            })

    if edge is not None and abs(edge) >= 5.0:
        sign = "+" if edge > 0 else ""
        points.append({
            "emoji": "💰",
            "text": f"Modell vs marked: {sign}{edge:.1f}% edge",
            "source": "Dixon-Coles + Pinnacle",
        })

    sigs = pick.get("signals_triggered") or []
    if isinstance(sigs, list) and sigs:
        label_map = {
            "STRONG_EDGE_35PCT": "Sterk edge ≥3.5%",
            "STRONG_EV_5PCT": "Sterk EV ≥5%",
            "BRUTAL_OMEGA": "Brutal omega-konvergens",
        }
        for s in sigs[:1]:
            if isinstance(s, str):
                points.append({
                    "emoji": "🧬",
                    "text": label_map.get(s, s.replace("_", " ").title()),
                    "source": "Atomic Signal Architecture",
                })

    score = int(pick.get("atomic_score") or 0)
    if score >= 7:
        points.append({
            "emoji": "⚛️",
            "text": f"{score}/9 atomic-signaler konvergerer",
            "source": "Atomic Signal Architecture",
        })

    if not points:
        points.append({
            "emoji": "📊",
            "text": f"Modell-sannsynlighet: {probability * 100:.1f}%",
            "source": "Dixon-Coles",
        })

    return points[:5]


def generate_events_for_match(pick: dict) -> list[dict]:
    """
    Returner alle markeder modellen kan beregne probability for.

    Prerequisites: pick må ha dc_*-felter populert (kjør backfill først).
    Returnerer tom liste hvis ingen dc-data.
    """
    completeness = _data_completeness(pick)
    if completeness == "LIMITED":
        return []

    lambda_h = _f(pick.get("dc_lambda_home"))
    lambda_a = _f(pick.get("dc_lambda_away"))
    events: list[dict] = []

    # Over 0.5 mål via Poisson(lambda_total)
    if lambda_h is not None and lambda_a is not None:
        lam = lambda_h + lambda_a
        prob_zero = math.exp(-lam) if lam > 0 else 1.0
        prob_over_05 = max(0.0, min(1.0, 1.0 - prob_zero))
        events.append(_build_event(
            label="Over 0.5 mål", category="totals",
            probability=prob_over_05,
            calc_source="Poisson(λ_total)",
            pick=pick, completeness=completeness,
            implied_odds_field=None,
        ))

    # Resten fra dc_*-felt
    for label, category, dc_field, _ in EVENT_DEFS[1:]:
        if not dc_field:
            continue
        prob = _f(pick.get(dc_field))
        if prob is None or prob <= 0:
            continue
        events.append(_build_event(
            label=label, category=category,
            probability=prob,
            calc_source="Dixon-Coles prob_grid",
            pick=pick, completeness=completeness,
            implied_odds_field=None,
        ))

    return events


def _build_event(*, label: str, category: str, probability: float,
                 calc_source: str, pick: dict, completeness: str,
                 implied_odds_field: float | None) -> dict:
    ci_low, ci_high = _confidence_interval(probability, n_eq=50)
    market_implied = _market_implied(implied_odds_field) if implied_odds_field else None
    edge = (probability * 100 - market_implied * 100) if market_implied is not None else None
    odds = _odds_from_prob(probability)
    why = _why_points_for_event(pick, label, category, probability, edge)

    return {
        "label": label,
        "category": category,
        "probability_pct": round(probability * 100, 1),
        "confidence_interval": [ci_low, ci_high],
        "market_implied_pct": round(market_implied * 100, 1) if market_implied else None,
        "edge_pct": round(edge, 1) if edge is not None else None,
        "odds": odds,
        "calculation_source": calc_source,
        "data_completeness": completeness,
        "why_points": why,
    }


def validate_event_coherence(events: list[dict]) -> dict:
    """
    Verifiser matematisk konsistens FØR events publiseres.

    Regler:
    R1: over_05 ≥ over_15 ≥ over_25 ≥ over_35
    R2: under_X ≈ 100 - over_X (±0.5%)
    R3: home_win + draw + away_win ≈ 100 (±2%)
    R4: alle prob ∈ [0, 100]
    R5: ingen NaN eller None i probability_pct

    Returnerer {valid, violations, events}. Hvis valid=False:
    events filtreres til kun de som passerer regler.
    """
    violations: list[str] = []

    by_label = {e["label"]: e for e in events}

    def _p(label: str) -> float | None:
        e = by_label.get(label)
        return e["probability_pct"] if e else None

    o05, o15, o25, o35 = _p("Over 0.5 mål"), _p("Over 1.5 mål"), _p("Over 2.5 mål"), _p("Over 3.5 mål")
    chain = [(0.5, o05), (1.5, o15), (2.5, o25), (3.5, o35)]
    chain_present = [(t, v) for t, v in chain if v is not None]
    for i in range(len(chain_present) - 1):
        t1, v1 = chain_present[i]
        t2, v2 = chain_present[i + 1]
        if v2 > v1 + 0.1:
            violations.append(f"R1 BROKEN: over_{t2} ({v2}) > over_{t1} ({v1})")

    pairs = [("Over 2.5 mål", "Under 2.5 mål"), ("Over 3.5 mål", "Under 3.5 mål")]
    for over_lbl, under_lbl in pairs:
        ov = _p(over_lbl); un = _p(under_lbl)
        if ov is not None and un is not None:
            total = ov + un
            if abs(total - 100.0) > 0.5:
                violations.append(f"R2 BROKEN: {over_lbl}+{under_lbl}={total} (expected 100±0.5)")

    h, d, a = _p("Hjemmeseier"), _p("Uavgjort"), _p("Borteseier")
    if all(x is not None for x in (h, d, a)):
        total = h + d + a
        if abs(total - 100.0) > 2.0:
            violations.append(f"R3 BROKEN: 1X2 sum={total} (expected 100±2)")

    for e in events:
        p = e.get("probability_pct")
        if p is None or not isinstance(p, (int, float)) or p < 0 or p > 100:
            violations.append(f"R4/R5 BROKEN: {e.get('label')} prob_pct={p}")

    valid = len(violations) == 0
    if not valid:
        valid_events = [e for e in events if isinstance(e.get("probability_pct"), (int, float))
                        and 0 <= e["probability_pct"] <= 100]
        logger.error(
            "[ProbEvents] coherence violations: %d → events filtered %d→%d",
            len(violations), len(events), len(valid_events),
        )
    else:
        valid_events = events

    return {
        "valid": valid,
        "violations": violations,
        "events": valid_events,
    }
