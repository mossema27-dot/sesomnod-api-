"""
operator_output_formatter.py — SesomNod MOAT Foundation
Formaterer FusionResult til operator-brief og Telegram-melding.

Output-struktur:
  full_brief   — komplett dict klar for pick_intelligence_log
  telegram_short — kort Telegram-melding (maks ~280 tegn)
  receipt      — sha256 + logged_at + pick_number + phase

Regler:
- Ingen DB-kall
- Ingen imports fra main.py
- Ingen blocking I/O
- Telegram-format: ingen hardkodet emoji — kun tekst-symboler
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from services.intelligence.fusion_engine import FusionResult

logger = logging.getLogger(__name__)

_PHASE = "Phase-1"


def _receipt_sha(pick_id: str, verdict: str, logged_at: str) -> str:
    raw = f"{pick_id}:{verdict}:{logged_at}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _build_warn_lines(fusion: FusionResult) -> list[str]:
    """Bygg liste av WARN-linjer for operator-brief."""
    warns: list[str] = []

    for trigger in fusion.false_confidence_triggers:
        warns.append(f"FC-trigger: {trigger}")

    for c in fusion.contradictions.get("medium", []):
        warns.append(f"Medium contradiction: {c}")

    for c in fusion.contradictions.get("critical", []):
        warns.append(f"KRITISK contradiction: {c}")

    if fusion.confidence_downgraded:
        warns.append(
            f"Confidence nedgradert: {fusion.original_confidence} -> {fusion.effective_confidence}"
        )

    return warns


def _build_why(fusion: FusionResult, dossier: dict) -> str:
    """Bygg 'why this pick' forklaringstekst."""
    core = dossier.get("sesomnod_core", {})
    match = dossier.get("match", {})
    edge = core.get("edge_pct", 0.0)
    omega = core.get("omega_score", "N/A")
    selection = core.get("selection", "?")
    home = match.get("home_team", "?")
    away = match.get("away_team", "?")
    eis = fusion.scores.edge_integrity_score

    return (
        f"{home} vs {away} | Valg: {selection} | "
        f"Edge: {edge:.1f}% | Omega: {omega} | EIS: {eis:.0f}/100 | "
        f"Completeness: {fusion.dossier_completeness_pct:.0f}%"
    )


def _build_stress(fusion: FusionResult) -> str:
    """Oppsummer stress-faktorer i en linje."""
    sc = fusion.scores
    parts = [
        f"NBPI={sc.no_bet_pressure_index:.0f}",
        f"CFS={sc.confidence_fragility_score:.0f}",
        f"CD={sc.contradiction_density:.0f}",
        f"SSR={sc.scenario_survival_rate:.0f}%",
    ]
    return " | ".join(parts)


def format_operator_brief(
    fusion: FusionResult,
    dossier: dict,
    pick_number: int = 0,
) -> dict[str, Any]:
    """
    Formater FusionResult + dossier til komplett operator-brief.

    Returns dict med:
      full_brief       — lagres i pick_intelligence_log
      telegram_short   — klar for Telegram-posting
      receipt          — sha256 fingerprint
    """
    logged_at = datetime.now(timezone.utc).isoformat()
    sha = _receipt_sha(fusion.pick_id, fusion.verdict, logged_at)

    warns = _build_warn_lines(fusion)
    why = _build_why(fusion, dossier)
    stress = _build_stress(fusion)

    receipt = {
        "sha256": sha,
        "logged_at": logged_at,
        "pick_number": pick_number,
        "phase": _PHASE,
    }

    operator_output = {
        "why": why,
        "warn": warns,
        "stress": stress,
        "verdict": fusion.verdict,
        "receipt": receipt,
    }

    full_brief: dict[str, Any] = {
        "pick_id": fusion.pick_id,
        "generated_at": logged_at,
        "dossier_completeness_pct": fusion.dossier_completeness_pct,
        "scores": {
            "edge_integrity_score": fusion.scores.edge_integrity_score,
            "confidence_fragility_score": fusion.scores.confidence_fragility_score,
            "narrative_distortion_index": fusion.scores.narrative_distortion_index,
            "sharp_alignment_score": fusion.scores.sharp_alignment_score,
            "scenario_survival_rate": fusion.scores.scenario_survival_rate,
            "no_bet_pressure_index": fusion.scores.no_bet_pressure_index,
            "contradiction_density": fusion.scores.contradiction_density,
        },
        "false_confidence_triggers": fusion.false_confidence_triggers,
        "contradictions": fusion.contradictions,
        "verdict": fusion.verdict,
        "verdict_reason": fusion.verdict_reason,
        "operator_output": operator_output,
        "telegram_output": _build_telegram_short(fusion, dossier),
        "post_match": {
            "actual_outcome": None,
            "closing_clv": None,
            "flags_correct": [],
            "flags_noise": [],
            "mirofish_improved_decision": None,
        },
    }

    logger.info(
        "Operator brief formattert | pick=%s verdict=%s warns=%d sha=%s",
        fusion.pick_id,
        fusion.verdict,
        len(warns),
        sha,
    )

    return {
        "full_brief": full_brief,
        "telegram_short": full_brief["telegram_output"],
        "receipt": receipt,
    }


def _build_telegram_short(fusion: FusionResult, dossier: dict) -> str:
    """
    Bygg kompakt Telegram-melding.
    Maks ~300 tegn. Ingen emojis.
    """
    core = dossier.get("sesomnod_core", {})
    match = dossier.get("match", {})

    home = match.get("home_team", "?")
    away = match.get("away_team", "?")
    selection = core.get("selection", "?")
    edge = core.get("edge_pct", 0.0)
    omega = core.get("omega_score", "N/A")
    odds = dossier.get("market", {}).get("current_odds", "?")
    verdict = fusion.verdict
    nbpi = fusion.scores.no_bet_pressure_index

    warn_count = len(fusion.false_confidence_triggers) + len(fusion.contradictions.get("critical", []))
    warn_suffix = f" [{warn_count} WARN]" if warn_count > 0 else ""

    return (
        f"[{verdict}] {home} vs {away}\n"
        f"Pick: {selection} @ {odds}\n"
        f"Edge: {edge:.1f}% | Omega: {omega} | NBPI: {nbpi:.0f}"
        f"{warn_suffix}"
    )
