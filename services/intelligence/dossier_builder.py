"""
dossier_builder.py — SesomNod MOAT Foundation
Bygger og validerer match-dossier iht. DOSSIER_QUALITY_DOCTRINE.md

Regler:
- Aldri kall sync DB-funksjon fra async context
- Aldri importer fra main.py
- Ingen side-effects — kun ren databygging
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Completeness weights (iht. DOSSIER_QUALITY_DOCTRINE.md)
# ---------------------------------------------------------------------------
_CRITICAL_FIELDS: list[tuple[str, str]] = [
    ("team_priors.home.xg_avg_home_last10", "home_xg_avg_last10"),
    ("team_priors.away.xg_avg_away_last10", "away_xg_avg_last10"),
    ("market.current_odds", "current_odds"),
    ("market.line_movement_pct", "line_movement_pct"),
    ("sesomnod_core.edge_pct", "core_edge"),
    ("sesomnod_core.confidence", "core_confidence"),
    ("squad.lineup_confirmed", "lineup_confirmed"),
]

_IMPORTANT_FIELDS: list[tuple[str, str]] = [
    ("team_priors.home.form_last8", "home_form_last5"),
    ("team_priors.away.form_last8", "away_form_last5"),
    ("market.sharp_money_indicator", "sharp_money_indicator"),
    ("squad.home_injuries", "key_absences_home"),
    ("generated_at", "dossier_generated_at"),
]

_OPTIONAL_FIELDS: list[tuple[str, str]] = [
    ("referee_data", "referee_data"),
    ("weather", "weather"),
    ("head_to_head", "head_to_head"),
    ("narrative_state", "narrative_state"),
]

_CRITICAL_PENALTY = 20
_IMPORTANT_PENALTY = 10
_OPTIONAL_PENALTY = 5


def _get_nested(obj: dict, dotpath: str) -> Any:
    """Hent nested verdi via dot-notasjon. Returnerer None hvis ikke funnet."""
    parts = dotpath.split(".")
    cur: Any = obj
    for part in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def calculate_completeness(dossier: dict) -> tuple[float, list[str]]:
    """
    Beregn dossier completeness prosent og returner liste av manglende felt.

    Returns:
        (completeness_pct: float, missing_flags: list[str])
    """
    penalty = 0.0
    missing: list[str] = []

    for dotpath, label in _CRITICAL_FIELDS:
        val = _get_nested(dossier, dotpath)
        if val is None:
            penalty += _CRITICAL_PENALTY
            missing.append(f"CRITICAL:{label}")

    for dotpath, label in _IMPORTANT_FIELDS:
        val = _get_nested(dossier, dotpath)
        if val is None:
            penalty += _IMPORTANT_PENALTY
            missing.append(f"IMPORTANT:{label}")

    for dotpath, label in _OPTIONAL_FIELDS:
        val = _get_nested(dossier, dotpath)
        if val is None:
            penalty += _OPTIONAL_PENALTY
            missing.append(f"OPTIONAL:{label}")

    completeness = max(0.0, 100.0 - penalty)
    return round(completeness, 1), missing


def _build_dossier_id(home_team: str, away_team: str, kickoff_utc: str, market_pick: str) -> str:
    """
    Generer deterministisk dossier_id basert pa kamp og marked.
    Format: {home_slug}-{away_slug}-{YYYYMMDD}-{market_slug}
    """
    def slug(s: str) -> str:
        return s.lower().replace(" ", "-").replace("_", "-")

    date_str = kickoff_utc[:10].replace("-", "") if kickoff_utc else "00000000"
    return f"{slug(home_team)}-{slug(away_team)}-{date_str}-{slug(market_pick)}"


def build_dossier(
    home_team: str,
    away_team: str,
    league: str,
    kickoff_utc: str,
    market: dict,
    sesomnod_core: dict,
    team_priors: dict,
    squad: dict,
    simulation_question: str,
    simulation_constraints: list[str] | None = None,
    operator_question: str | None = None,
    venue: str | None = None,
    is_neutral: bool = False,
    intelligence_state: dict | None = None,
) -> dict:
    """
    Bygg et komplett match-dossier iht. match_dossier.schema.json.

    Parametere speiler schema-strukturen 1:1.
    Returnerer ferdig dossier-dict med completeness beregnet.

    Brukes av: fusion_engine.py (via FusionResult)
    Aldri kalt fra main.py direkte.
    """
    market_pick = market.get("market_pick", sesomnod_core.get("selection", "unknown"))
    dossier_id = _build_dossier_id(home_team, away_team, kickoff_utc, market_pick)

    dossier: dict = {
        "dossier_id": dossier_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "match": {
            "home_team": home_team,
            "away_team": away_team,
            "league": league,
            "kickoff_utc": kickoff_utc,
            "venue": venue,
            "is_neutral": is_neutral,
        },
        "market": market,
        "sesomnod_core": sesomnod_core,
        "team_priors": team_priors,
        "squad": squad,
        "intelligence_state": intelligence_state or {
            "false_confidence_triggers": [],
            "contradiction_density": 0.0,
            "narrative_distortion_index": 0.0,
            "dossier_completeness_flags": [],
        },
        "simulation_question": simulation_question,
        "simulation_constraints": simulation_constraints or [],
        "operator_question": operator_question or "",
    }

    completeness_pct, missing_flags = calculate_completeness(dossier)
    dossier["dossier_completeness_pct"] = completeness_pct
    dossier["intelligence_state"]["dossier_completeness_flags"] = missing_flags

    if completeness_pct < 50:
        logger.warning(
            "Dossier %s har completeness %.1f%% — under 50%%, NO-BET trigger aktivert",
            dossier_id,
            completeness_pct,
        )
    elif completeness_pct < 65:
        logger.warning(
            "Dossier %s completeness %.1f%% — false confidence trigger aktiv",
            dossier_id,
            completeness_pct,
        )

    return dossier


def dossier_sha256(dossier: dict) -> str:
    """Beregn deterministisk SHA-256 fingerprint av dossier (ekskl. generated_at)."""
    snapshot = {k: v for k, v in dossier.items() if k != "generated_at"}
    raw = json.dumps(snapshot, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()
