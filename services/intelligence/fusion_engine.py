"""
fusion_engine.py — SesomNod MOAT Foundation
Fusjonerer dossier + intelligence scores til endelig verdict.

Gates (prioritert rekkefolge):
  1. Completeness <50%            → NO-BET
  2. NBPI >80                     → NO-BET
  3. FC-triggers >= 4             → NO-BET
  4. 2+ kritiske contradictions   → NO-BET
  5. FC-triggers >= 3             → REVIEW
  6. 1 kritisk contradiction      → REVIEW
  7. Completeness 50-59%          → REVIEW
  8. NBPI 60-79                   → REVIEW
  9. Bestått alle gates           → PUBLISH

Regler:
- Ingen DB-kall
- Ingen imports fra main.py
- Fire-and-forget-trygg — ingen blocking I/O
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from services.intelligence.intelligence_scorer import IntelligenceScores

logger = logging.getLogger(__name__)


@dataclass
class FusionResult:
    """Resultat fra fusion_engine — klar for operator_output_formatter."""
    pick_id: str
    verdict: str                          # PUBLISH | REVIEW | NO-BET | HOLD
    verdict_reason: str
    scores: IntelligenceScores
    dossier_completeness_pct: float
    false_confidence_triggers: list[str] = field(default_factory=list)
    contradictions: dict = field(default_factory=lambda: {"weak": [], "medium": [], "critical": []})
    gates_evaluated: list[str] = field(default_factory=list)
    confidence_downgraded: bool = False
    original_confidence: str = ""
    effective_confidence: str = ""


def apply_fusion_doctrine(
    dossier: dict,
    scores: IntelligenceScores,
) -> FusionResult:
    """
    Anvend alle doctrine-gates pa dossier + scores og returner FusionResult.

    Denne funksjonen er den eneste autoritative kilden for pick-verdict.
    Kaller aldri ekstern kode — ren logikk.
    """
    pick_id: str = dossier.get("dossier_id", "unknown")
    completeness_pct: float = dossier.get("dossier_completeness_pct", 0.0)
    core = dossier.get("sesomnod_core", {})
    original_confidence: str = core.get("confidence", "LOW")
    effective_confidence: str = original_confidence
    confidence_downgraded = False
    gates_evaluated: list[str] = []
    verdict = "PUBLISH"
    verdict_reason = "Alle gates bestatt"

    # --- Gate 1: Completeness <50% → NO-BET ---
    gates_evaluated.append("G1_COMPLETENESS_HARD_GATE")
    if completeness_pct < 50:
        verdict = "NO-BET"
        verdict_reason = f"Dossier completeness {completeness_pct:.1f}% er under 50%% — aldri simuler"
        return _build_result(
            pick_id, verdict, verdict_reason, scores, completeness_pct,
            effective_confidence, original_confidence, confidence_downgraded,
            gates_evaluated,
        )

    # --- Gate 2: NBPI >80 → NO-BET ---
    gates_evaluated.append("G2_NBPI_HARD_GATE")
    if scores.no_bet_pressure_index > 80:
        verdict = "NO-BET"
        verdict_reason = f"NBPI {scores.no_bet_pressure_index:.1f} overstiger 80 — dodeligt contradiction pressure"
        return _build_result(
            pick_id, verdict, verdict_reason, scores, completeness_pct,
            effective_confidence, original_confidence, confidence_downgraded,
            gates_evaluated,
        )

    # --- Gate 3: FC-triggers >= 4 → NO-BET ---
    gates_evaluated.append("G3_FALSE_CONFIDENCE_HARD_GATE")
    n_fc = len(scores.false_confidence_triggers)
    if n_fc >= 4:
        verdict = "NO-BET"
        verdict_reason = f"{n_fc} false confidence-triggers aktive — automatisk NO-BET"
        return _build_result(
            pick_id, verdict, verdict_reason, scores, completeness_pct,
            effective_confidence, original_confidence, confidence_downgraded,
            gates_evaluated,
        )

    # --- Gate 4: 2+ kritiske contradictions → NO-BET ---
    gates_evaluated.append("G4_CRITICAL_CONTRADICTION_GATE")
    n_critical = len(scores.contradictions_critical)
    if n_critical >= 2:
        verdict = "NO-BET"
        verdict_reason = f"{n_critical} kritiske contradictions — automatisk NO-BET"
        return _build_result(
            pick_id, verdict, verdict_reason, scores, completeness_pct,
            effective_confidence, original_confidence, confidence_downgraded,
            gates_evaluated,
        )

    # --- Confidence downgrade: 2 FC-triggers → HIGH → MEDIUM ---
    gates_evaluated.append("G5_CONFIDENCE_DOWNGRADE")
    if n_fc >= 2 and effective_confidence == "HIGH":
        effective_confidence = "MEDIUM"
        confidence_downgraded = True
        logger.info("Pick %s: confidence nedgradert HIGH → MEDIUM (%d FC-triggers)", pick_id, n_fc)

    # --- Gate 6: FC-triggers >= 3 → REVIEW ---
    gates_evaluated.append("G6_FC_REVIEW_GATE")
    if n_fc >= 3:
        verdict = "REVIEW"
        verdict_reason = f"{n_fc} false confidence-triggers — manuell gjennomgang kreves"

    # --- Gate 7: 1 kritisk contradiction → REVIEW ---
    gates_evaluated.append("G7_CRITICAL_CONTRADICTION_REVIEW")
    if n_critical >= 1 and verdict == "PUBLISH":
        verdict = "REVIEW"
        verdict_reason = f"1 kritisk contradiction: {scores.contradictions_critical[0]}"

    # --- Gate 8: Completeness 50-59% → REVIEW ---
    gates_evaluated.append("G8_COMPLETENESS_REVIEW_GATE")
    if 50 <= completeness_pct < 60 and verdict == "PUBLISH":
        verdict = "REVIEW"
        verdict_reason = f"Dossier completeness {completeness_pct:.1f}% i review-sone (50-59%)"

    # --- Gate 9: NBPI 60-79 → REVIEW ---
    gates_evaluated.append("G9_NBPI_REVIEW_GATE")
    if 60 <= scores.no_bet_pressure_index <= 80 and verdict == "PUBLISH":
        verdict = "REVIEW"
        verdict_reason = f"NBPI {scores.no_bet_pressure_index:.1f} i review-sone (60-80)"

    # --- Gate 10: EIS <60 er FC-trigger, men ikke hard gate — allerede i FC-listen ---

    logger.info(
        "FusionResult pick=%s verdict=%s completeness=%.1f NBPI=%.1f",
        pick_id, verdict, completeness_pct, scores.no_bet_pressure_index,
    )

    return _build_result(
        pick_id, verdict, verdict_reason, scores, completeness_pct,
        effective_confidence, original_confidence, confidence_downgraded,
        gates_evaluated,
    )


def _build_result(
    pick_id: str,
    verdict: str,
    verdict_reason: str,
    scores: IntelligenceScores,
    completeness_pct: float,
    effective_confidence: str,
    original_confidence: str,
    confidence_downgraded: bool,
    gates_evaluated: list[str],
) -> FusionResult:
    return FusionResult(
        pick_id=pick_id,
        verdict=verdict,
        verdict_reason=verdict_reason,
        scores=scores,
        dossier_completeness_pct=completeness_pct,
        false_confidence_triggers=scores.false_confidence_triggers,
        contradictions={
            "weak": scores.contradictions_weak,
            "medium": scores.contradictions_medium,
            "critical": scores.contradictions_critical,
        },
        gates_evaluated=gates_evaluated,
        confidence_downgraded=confidence_downgraded,
        original_confidence=original_confidence,
        effective_confidence=effective_confidence,
    )
