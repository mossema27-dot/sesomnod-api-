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

# Event-definisjoner (VEI A 2026-04-29): list-of-dicts.
# Info-events (totals/BTTS) listet for kuratering/visning, men droppet fra
# dominant-flyt fordi picks_v2 ikke har odds for disse markedene.
EVENT_DEFS = [
    # ── Info-events (ingen market-odds → INFO_EVENTS, droppet fra dominant) ──
    {"label": "Over 0.5 mål", "category": "totals",
     "model_prob_field_computed": "poisson_over_05",
     "expected_edge_range": (-8, 8)},
    {"label": "Over 1.5 mål", "category": "totals",
     "model_prob_field": "dc_over_15",
     "expected_edge_range": (-8, 8)},
    {"label": "Over 2.5 mål", "category": "totals",
     "model_prob_field": "dc_over_25",
     "expected_edge_range": (-12, 12)},
    {"label": "Over 3.5 mål", "category": "totals",
     "model_prob_field": "dc_over_35",
     "expected_edge_range": (-15, 15)},
    {"label": "Over 4.5 mål", "category": "totals",
     "model_prob_field_computed": "poisson_over_45",
     "expected_edge_range": (-15, 15)},
    {"label": "Under 1.5 mål", "category": "totals",
     "model_prob_field_computed": "complement_over_15",
     "expected_edge_range": (-12, 12)},
    {"label": "Under 2.5 mål", "category": "totals",
     "model_prob_field": "dc_under_25",
     "expected_edge_range": (-12, 12)},
    {"label": "Under 3.5 mål", "category": "totals",
     "model_prob_field": "dc_under_35",
     "expected_edge_range": (-12, 12)},

    # ── Edge-events (1X2): full odds-coverage via home/draw/away_odds_raw ──
    {"label": "Hjemmeseier", "category": "1x2",
     "model_prob_field": "dc_home_win_prob",
     "odds_field": "home_odds_raw",
     "expected_edge_range": (-15, 15)},
    {"label": "Uavgjort", "category": "1x2",
     "model_prob_field": "dc_draw_prob",
     "odds_field": "draw_odds_raw",
     "expected_edge_range": (-12, 12)},
    {"label": "Borteseier", "category": "1x2",
     "model_prob_field": "dc_away_win_prob",
     "odds_field": "away_odds_raw",
     "expected_edge_range": (-15, 15)},

    # ── Edge-events (Double Chance): avledet fra implied 1X2 ──
    {"label": "Double Chance 1X", "category": "double_chance",
     "model_prob_field_computed": "dc_home_plus_draw",
     "expected_edge_range": (-10, 10)},
    {"label": "Double Chance X2", "category": "double_chance",
     "model_prob_field_computed": "dc_draw_plus_away",
     "expected_edge_range": (-10, 10)},
    {"label": "Double Chance 12", "category": "double_chance",
     "model_prob_field_computed": "dc_home_plus_away",
     "expected_edge_range": (-10, 10)},

    # ── Edge-events (Draw No Bet): normalisert 1X2 uten draw ──
    {"label": "Draw No Bet hjemme", "category": "draw_no_bet",
     "model_prob_field_computed": "dnb_home",
     "expected_edge_range": (-15, 15)},
    {"label": "Draw No Bet borte", "category": "draw_no_bet",
     "model_prob_field_computed": "dnb_away",
     "expected_edge_range": (-15, 15)},
    # BTTS Ja/Nei: deferred — INGEN market-odds i picks_v2 schema.
]


# Info-events: drops i dominant-flyt fordi picks_v2 ikke har totals/BTTS-odds.
# Behold for visning/kuratering, men returneres ikke fra _build_event.
INFO_EVENTS = {
    "Over 0.5 mål", "Over 1.5 mål", "Over 2.5 mål", "Over 3.5 mål", "Over 4.5 mål",
    "Under 1.5 mål", "Under 2.5 mål", "Under 3.5 mål",
    "BTTS Ja", "BTTS Nei",
}


# Market-implied resolvers per edge-event. Returnerer (pct, source) eller (None, None).
# Krever pick er enriched via enrich_implied_inline (eller har implied_*_prob fra før).
def _resolver_implied_h(p):
    v = p.get("implied_home_prob")
    return (v * 100, "implied_field_h") if v else (None, None)

def _resolver_implied_d(p):
    v = p.get("implied_draw_prob")
    return (v * 100, "implied_field_d") if v else (None, None)

def _resolver_implied_a(p):
    v = p.get("implied_away_prob")
    return (v * 100, "implied_field_a") if v else (None, None)

def _resolver_dc_1x(p):
    h, d = p.get("implied_home_prob"), p.get("implied_draw_prob")
    return ((h + d) * 100, "computed_dc_1x") if (h and d) else (None, None)

def _resolver_dc_x2(p):
    d, a = p.get("implied_draw_prob"), p.get("implied_away_prob")
    return ((d + a) * 100, "computed_dc_x2") if (d and a) else (None, None)

def _resolver_dc_12(p):
    h, a = p.get("implied_home_prob"), p.get("implied_away_prob")
    return ((h + a) * 100, "computed_dc_12") if (h and a) else (None, None)

def _resolver_dnb_h(p):
    h, a = p.get("implied_home_prob"), p.get("implied_away_prob")
    if h and a and (h + a) > 0:
        return ((h / (h + a)) * 100, "computed_dnb_h")
    return (None, None)

def _resolver_dnb_a(p):
    h, a = p.get("implied_home_prob"), p.get("implied_away_prob")
    if h and a and (h + a) > 0:
        return ((a / (h + a)) * 100, "computed_dnb_a")
    return (None, None)


EVENT_RESOLVERS = {
    "Hjemmeseier": _resolver_implied_h,
    "Uavgjort": _resolver_implied_d,
    "Borteseier": _resolver_implied_a,
    "Double Chance 1X": _resolver_dc_1x,
    "Double Chance X2": _resolver_dc_x2,
    "Double Chance 12": _resolver_dc_12,
    "Draw No Bet hjemme": _resolver_dnb_h,
    "Draw No Bet borte": _resolver_dnb_a,
}


def enrich_implied_inline(pick: dict) -> dict:
    """
    Idempotent vig-removal helper. Returnerer NY dict med implied_*_prob (0-1)
    + vig_total_pct, ELLER pick uendret hvis allerede enriched / mangler odds.
    """
    if (pick.get("implied_home_prob")
            and pick.get("implied_draw_prob")
            and pick.get("implied_away_prob")):
        return pick

    home_odds = _f(pick.get("home_odds_raw"))
    draw_odds = _f(pick.get("draw_odds_raw"))
    away_odds = _f(pick.get("away_odds_raw"))

    if not (home_odds and draw_odds and away_odds):
        return pick
    if home_odds <= 1.0 or draw_odds <= 1.0 or away_odds <= 1.0:
        return pick

    raw_h = 1.0 / home_odds
    raw_d = 1.0 / draw_odds
    raw_a = 1.0 / away_odds
    raw_total = raw_h + raw_d + raw_a

    enriched = dict(pick)
    enriched["implied_home_prob"] = raw_h / raw_total
    enriched["implied_draw_prob"] = raw_d / raw_total
    enriched["implied_away_prob"] = raw_a / raw_total
    enriched["vig_total_pct"] = (raw_total - 1.0) * 100
    return enriched


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


def _poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def _compute_model_prob(pick: dict, event_def: dict) -> float | None:
    """Returner model probability (0-1) eller None hvis utilgjengelig."""
    field_name = event_def.get("model_prob_field")
    if field_name:
        return _f(pick.get(field_name))

    computed = event_def.get("model_prob_field_computed")
    if not computed:
        return None

    lambda_h = _f(pick.get("dc_lambda_home"))
    lambda_a = _f(pick.get("dc_lambda_away"))
    home_p = _f(pick.get("dc_home_win_prob"))
    draw_p = _f(pick.get("dc_draw_prob"))
    away_p = _f(pick.get("dc_away_win_prob"))

    if computed == "poisson_over_05":
        if lambda_h is None or lambda_a is None:
            return None
        lam = lambda_h + lambda_a
        return max(0.0, min(1.0, 1.0 - (math.exp(-lam) if lam > 0 else 1.0)))
    if computed == "poisson_over_45":
        if lambda_h is None or lambda_a is None:
            return None
        lam = lambda_h + lambda_a
        prob_le_4 = sum(_poisson_pmf(k, lam) for k in range(5))
        return max(0.0, min(1.0, 1.0 - prob_le_4))
    if computed == "complement_over_15":
        over_15 = _f(pick.get("dc_over_15"))
        if over_15 is None or not 0 < over_15 < 1:
            return None
        return max(0.0, min(1.0, 1.0 - over_15))
    if computed == "dc_home_plus_draw":
        if home_p is None or draw_p is None:
            return None
        return max(0.0, min(1.0, home_p + draw_p))
    if computed == "dc_draw_plus_away":
        if draw_p is None or away_p is None:
            return None
        return max(0.0, min(1.0, draw_p + away_p))
    if computed == "dc_home_plus_away":
        if home_p is None or away_p is None:
            return None
        return max(0.0, min(1.0, home_p + away_p))
    if computed == "dnb_home":
        if home_p is None or away_p is None or (home_p + away_p) <= 0:
            return None
        return max(0.0, min(1.0, home_p / (home_p + away_p)))
    if computed == "dnb_away":
        if home_p is None or away_p is None or (home_p + away_p) <= 0:
            return None
        return max(0.0, min(1.0, away_p / (home_p + away_p)))
    return None


def _build_event(pick_enriched: dict, event_def: dict,
                 completeness: str) -> dict | None:
    """
    VEI A: bygg ett event med edge ELLER returner None.

    Anti-fake-fix: alle coherence-feilskjebner returnerer None i stedet for
    å passe edge=None nedstrøms. Info-events (uten market-odds) droppes også.
    """
    label = event_def["label"]

    if label in INFO_EVENTS:
        return None

    model_prob_raw = _compute_model_prob(pick_enriched, event_def)
    if model_prob_raw is None or model_prob_raw <= 0:
        return None
    model_prob_pct = model_prob_raw * 100 if model_prob_raw <= 1 else model_prob_raw

    resolver = EVENT_RESOLVERS.get(label)
    if resolver is None:
        return None
    market_pct, market_source = resolver(pick_enriched)
    if market_pct is None:
        return None
    if not (5 <= market_pct <= 95):
        logger.error(
            "[COHERENCE] %s implied %.1f%% out of range (pick=%s)",
            label, market_pct, pick_enriched.get("id"),
        )
        return None

    edge_pct = model_prob_pct - market_pct
    if not (-50 <= edge_pct <= 50):
        logger.error(
            "[COHERENCE] %s edge %.1f%% wildly unrealistic (pick=%s)",
            label, edge_pct, pick_enriched.get("id"),
        )
        return None

    expected_low, expected_high = event_def.get("expected_edge_range", (-15, 15))
    if not (expected_low <= edge_pct <= expected_high):
        logger.warning(
            "[REALISM] %s edge %.1f%% outside expected (%d–%d%%)",
            label, edge_pct, expected_low, expected_high,
        )

    extreme_flag = edge_pct > 20.0
    if extreme_flag:
        logger.warning(
            "[QUARANTINE] %s edge %.1f%% extreme — flagged but kept",
            label, edge_pct,
        )

    odds = _f(pick_enriched.get(event_def.get("odds_field"))) if event_def.get("odds_field") else None
    if not odds or odds <= 1.0:
        if market_pct > 0:
            odds = round(100.0 / market_pct, 2)
        else:
            return None

    ci_low, ci_high = _confidence_interval(model_prob_raw, n_eq=50)
    why = _why_points_for_event(
        pick_enriched, label, event_def.get("category", "main"),
        model_prob_raw, edge_pct,
    )

    return {
        "label": label,
        "category": event_def.get("category", "main"),
        "probability_pct": round(model_prob_pct, 1),
        "confidence_interval": [ci_low, ci_high],
        "market_implied_pct": round(market_pct, 1),
        "market_source": market_source,
        "edge_pct": round(edge_pct, 1),
        "odds": odds,
        "calculation_source": "dixon_coles",
        "data_completeness": completeness,
        "extreme_edge_flag": extreme_flag,
        "why_points": why,
    }


def generate_events_for_match(pick: dict) -> list[dict]:
    """
    VEI A (2026-04-29): edge-events kun (1X2 + DC + DNB = 8 events).

    Pipeline:
    1. enrich_implied_inline (idempotent vig-fjerning fra home/draw/away_odds_raw)
    2. Iterer EVENT_DEFS, bygg event via _build_event
    3. Anti-fake-fix: drop event ved manglende model/market/coherence
    4. Validate via validate_event_coherence; returner [] hvis violations

    Info-events (totals/BTTS) droppes deterministisk via INFO_EVENTS-set.
    """
    completeness = _data_completeness(pick)
    if completeness == "LIMITED":
        return []

    enriched = enrich_implied_inline(pick)

    events: list[dict] = []
    no_market_count = 0
    for event_def in EVENT_DEFS:
        event = _build_event(enriched, event_def, completeness)
        if event:
            events.append(event)
        elif event_def["label"] in INFO_EVENTS:
            no_market_count += 1

    if no_market_count > 0:
        logger.info(
            "[NO_MARKET] Pick %s dropped %d info-events (totals/BTTS)",
            pick.get("id"), no_market_count,
        )

    validation = validate_event_coherence(events)
    if not validation.get("valid"):
        logger.error(
            "[VALIDATION] Pick %s violations: %s",
            pick.get("id"), validation.get("violations"),
        )
    return validation.get("events") or []


def validate_event_coherence(events: list[dict]) -> dict:
    """
    Verifiser matematisk konsistens FØR events publiseres.

    Regler:
    R1: over_05 ≥ over_15 ≥ over_25 ≥ over_35 ≥ over_45
    R2: under_X ≈ 100 - over_X (±0.5%)
    R3: home_win + draw + away_win ≈ 100 (±2%)
    R4: alle prob ∈ [0, 100]
    R5: ingen NaN eller None i probability_pct
    R6 (B1.1): Double Chance — 1X + X2 - X ≈ 100 (±2%)
    R7 (B1.1): Draw No Bet — DNB_home + DNB_away ≈ 100 (±0.5%)

    Returnerer {valid, violations, events}. Hvis valid=False:
    events filtreres til kun de som passerer regler.
    """
    violations: list[str] = []

    by_label = {e["label"]: e for e in events}

    def _p(label: str) -> float | None:
        e = by_label.get(label)
        return e["probability_pct"] if e else None

    o05, o15, o25, o35, o45 = (_p("Over 0.5 mål"), _p("Over 1.5 mål"),
                                _p("Over 2.5 mål"), _p("Over 3.5 mål"),
                                _p("Over 4.5 mål"))
    chain = [(0.5, o05), (1.5, o15), (2.5, o25), (3.5, o35), (4.5, o45)]
    chain_present = [(t, v) for t, v in chain if v is not None]
    for i in range(len(chain_present) - 1):
        t1, v1 = chain_present[i]
        t2, v2 = chain_present[i + 1]
        if v2 > v1 + 0.1:
            violations.append(f"R1 BROKEN: over_{t2} ({v2}) > over_{t1} ({v1})")

    pairs = [("Over 1.5 mål", "Under 1.5 mål"),
             ("Over 2.5 mål", "Under 2.5 mål"),
             ("Over 3.5 mål", "Under 3.5 mål")]
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

    # R6: Double Chance — 1X + X2 - X = (H+D) + (D+A) - D = H+D+A ≈ 100
    dc_1x = _p("Double Chance 1X")
    dc_x2 = _p("Double Chance X2")
    if dc_1x is not None and dc_x2 is not None and d is not None:
        total = dc_1x + dc_x2 - d
        if abs(total - 100.0) > 2.0:
            violations.append(f"R6 BROKEN: 1X+X2-X={total:.2f} (expected 100±2)")

    # R7: Draw No Bet — DNB_home + DNB_away = 100
    dnb_h = _p("Draw No Bet hjemme")
    dnb_a = _p("Draw No Bet borte")
    if dnb_h is not None and dnb_a is not None:
        total = dnb_h + dnb_a
        if abs(total - 100.0) > 0.5:
            violations.append(f"R7 BROKEN: DNB sum={total:.2f} (expected 100±0.5)")

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
