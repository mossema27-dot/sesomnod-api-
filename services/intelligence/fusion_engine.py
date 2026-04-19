"""
fusion_engine.py — SesomNod MOAT Foundation
Fusjonerer dossier + intelligence scores til endelig verdict.

Gates (prioritert rekkefolge):
  P1. edge < 8%                   → NO-BET  (hard gate, sjekkes forst)
  P2. confidence != HIGH          → NO-BET  (hard gate)
  P3. lineup ukonfirmert >2t      → HOLD
  1.  Completeness <50%           → NO-BET
  2.  NBPI >80                    → NO-BET
  3.  FC-triggers >= 4            → NO-BET
  4.  2+ kritiske contradictions  → NO-BET
  5.  FC-triggers >= 3            → REVIEW
  6.  1 kritisk contradiction     → REVIEW
  7.  Completeness 50-59%         → REVIEW
  8.  NBPI > 65                   → REVIEW  (M1: terskel hevet fra 60)
  P4. SSR < 45                    → REVIEW
  P5. SAS < 40 AND edge < 12      → REVIEW
  9.  Bestatt alle gates          → PUBLISH (adjusted_edge = core_edge * EIS/100)

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
    verdict: str                          # PUBLISH | REVIEW | NO-BET | HOLD
    reason: str                           # Primær forklaring (P1-P9)
    rules_triggered: list[str] = field(default_factory=list)
    adjusted_edge: float = 0.0           # Kun satt i PUBLISH-path (P6)

    # Ekstra kontekst — populert av dossier-kall
    pick_id: str = ""
    verdict_reason: str = ""             # Alias for bakoverkompatibilitet
    scores: IntelligenceScores = field(default_factory=IntelligenceScores)
    dossier_completeness_pct: float = 0.0
    false_confidence_triggers: list[str] = field(default_factory=list)
    contradictions: dict = field(default_factory=lambda: {"weak": [], "medium": [], "critical": []})
    gates_evaluated: list[str] = field(default_factory=list)
    confidence_downgraded: bool = False
    original_confidence: str = ""
    effective_confidence: str = ""


def apply_fusion_doctrine(
    core_edge: float,
    core_confidence: str,
    scores: IntelligenceScores,
    dossier_completeness: float = 100.0,
    lineup_confirmed: bool = True,
    hours_to_kickoff: float = 0.0,
) -> FusionResult:
    """
    Anvend alle doctrine-gates og returner FusionResult.

    Parametre:
      core_edge           — raa edge i prosent (f.eks. 15.3)
      core_confidence     — "HIGH" | "MEDIUM" | "LOW"
      scores              — IntelligenceScores fra intelligence_scorer
      dossier_completeness — 0-100
      lineup_confirmed    — True hvis lineup er bekreftet
      hours_to_kickoff    — timer til kampstart

    Denne funksjonen er den eneste autoritative kilden for pick-verdict.
    Kaller aldri ekstern kode — ren logikk.

    Signatur utvidet (fra opprinnelig dossier-basert) for doctrine-compliance:
    P1/P2/P3 krever core_edge, core_confidence, lineup_confirmed, hours_to_kickoff
    som eksplisitte parametre.
    """
    review_triggers: list[str] = []

    # -----------------------------------------------------------------------
    # P1: edge < 8% → NO-BET (sjekkes absolutt forst)
    # -----------------------------------------------------------------------
    if core_edge < 8:
        return FusionResult(
            verdict="NO-BET",
            reason="edge < 8% — hard gate",
            rules_triggered=["edge_gate"],
            adjusted_edge=core_edge,
        )

    # -----------------------------------------------------------------------
    # P2: confidence != HIGH → NO-BET
    # -----------------------------------------------------------------------
    if core_confidence != "HIGH":
        return FusionResult(
            verdict="NO-BET",
            reason=f"confidence {core_confidence} != HIGH — hard gate",
            rules_triggered=["confidence_gate"],
            adjusted_edge=core_edge,
        )

    # -----------------------------------------------------------------------
    # P3: HOLD — lineup ikke bekreftet og >2t til kickoff
    # -----------------------------------------------------------------------
    if not lineup_confirmed and hours_to_kickoff > 2:
        return FusionResult(
            verdict="HOLD",
            reason="Lineup ikke bekreftet — sjekk igjen om 2t",
            rules_triggered=["lineup_hold"],
            adjusted_edge=core_edge,
        )

    # -----------------------------------------------------------------------
    # Gate 1: Completeness <50% → NO-BET
    # -----------------------------------------------------------------------
    if dossier_completeness < 50:
        return FusionResult(
            verdict="NO-BET",
            reason=f"Dossier completeness {dossier_completeness:.1f}% er under 50% — aldri simuler",
            rules_triggered=["completeness_hard_gate"],
            adjusted_edge=core_edge,
        )

    # -----------------------------------------------------------------------
    # Gate 2: NBPI >80 → NO-BET
    # -----------------------------------------------------------------------
    if scores.no_bet_pressure_index > 80:
        return FusionResult(
            verdict="NO-BET",
            reason=f"NBPI {scores.no_bet_pressure_index:.1f} overstiger 80 — dodeligt contradiction pressure",
            rules_triggered=["nbpi_hard_gate"],
            adjusted_edge=core_edge,
        )

    # -----------------------------------------------------------------------
    # Gate 3: FC-triggers >= 4 → NO-BET
    # -----------------------------------------------------------------------
    n_fc = len(scores.false_confidence_triggers)
    if n_fc >= 4:
        return FusionResult(
            verdict="NO-BET",
            reason=f"{n_fc} false confidence-triggers aktive — automatisk NO-BET",
            rules_triggered=["fc_hard_gate"],
            adjusted_edge=core_edge,
        )

    # -----------------------------------------------------------------------
    # Gate 4: 2+ kritiske contradictions → NO-BET
    # -----------------------------------------------------------------------
    n_critical = len(scores.contradictions_critical)
    if n_critical >= 2:
        return FusionResult(
            verdict="NO-BET",
            reason=f"{n_critical} kritiske contradictions — automatisk NO-BET",
            rules_triggered=["critical_contradiction_hard_gate"],
            adjusted_edge=core_edge,
        )

    # -----------------------------------------------------------------------
    # Review-akkumulering (gates 5-9 + P4/P5)
    # Alle vurderes — forste REVIEW-trigger vinner
    # -----------------------------------------------------------------------

    # Gate 5: FC-triggers >= 3 → REVIEW
    if n_fc >= 3:
        review_triggers.append(f"{n_fc} false confidence-triggers — manuell gjennomgang kreves")

    # Gate 6: 1 kritisk contradiction → REVIEW
    if n_critical >= 1:
        review_triggers.append(f"1 kritisk contradiction: {scores.contradictions_critical[0]}")

    # Gate 7: Completeness 50-59% → REVIEW
    if 50 <= dossier_completeness < 60:
        review_triggers.append(f"Dossier completeness {dossier_completeness:.1f}% i review-sone (50-59%)")

    # Gate 8 (M1): NBPI > 65 → REVIEW (terskel hevet fra 60)
    if scores.no_bet_pressure_index > 65:
        review_triggers.append(f"NBPI {scores.no_bet_pressure_index:.1f} i review-sone (>65)")

    # P4: SSR < 45 → REVIEW
    if scores.scenario_survival_rate < 45:
        review_triggers.append(f"SSR {scores.scenario_survival_rate:.0f}% < 45")

    # P5: SAS < 40 AND edge < 12 → REVIEW
    if scores.sharp_alignment_score < 40 and core_edge < 12:
        review_triggers.append("sharp_misalignment + low_edge")

    if review_triggers:
        return FusionResult(
            verdict="REVIEW",
            reason=review_triggers[0],
            rules_triggered=review_triggers,
            adjusted_edge=core_edge,
        )

    # -----------------------------------------------------------------------
    # P6: PUBLISH — adjusted_edge kun i publish-path
    # -----------------------------------------------------------------------
    adjusted_edge = core_edge * (scores.edge_integrity_score / 100.0)

    logger.info(
        "FusionResult PUBLISH core_edge=%.1f adjusted=%.1f EIS=%.1f",
        core_edge, adjusted_edge, scores.edge_integrity_score,
    )

    return FusionResult(
        verdict="PUBLISH",
        reason=f"Alle gates passert. Adjusted edge: {adjusted_edge:.1f}%",
        rules_triggered=[],
        adjusted_edge=adjusted_edge,
    )


def apply_fusion_doctrine_from_dossier(
    dossier: dict,
    scores: IntelligenceScores,
) -> FusionResult:
    """
    Bakoverkompatibel wrapper som henter parametre fra dossier-dict
    og delegerer til apply_fusion_doctrine.

    Brukes av eldre scaffold-kode som sender fullt dossier.
    """
    core = dossier.get("sesomnod_core", {})
    squad = dossier.get("squad", {})
    intel = dossier.get("intelligence_state", {})

    pick_id: str = dossier.get("dossier_id", "unknown")
    completeness_pct: float = dossier.get("dossier_completeness_pct", 0.0)
    core_confidence: str = core.get("confidence", "LOW")
    core_edge: float = core.get("edge_pct", 0.0)
    lineup_confirmed: bool = bool(squad.get("lineup_confirmed", True))
    hours_to_kickoff: float = float(intel.get("hours_to_kickoff", 0.0) or 0.0)

    result = apply_fusion_doctrine(
        core_edge=core_edge,
        core_confidence=core_confidence,
        scores=scores,
        dossier_completeness=completeness_pct,
        lineup_confirmed=lineup_confirmed,
        hours_to_kickoff=hours_to_kickoff,
    )

    # Berik med dossier-kontekst for bakoverkompatibilitet
    result.pick_id = pick_id
    result.verdict_reason = result.reason
    result.scores = scores
    result.dossier_completeness_pct = completeness_pct
    result.false_confidence_triggers = scores.false_confidence_triggers
    result.contradictions = {
        "weak": scores.contradictions_weak,
        "medium": scores.contradictions_medium,
        "critical": scores.contradictions_critical,
    }
    result.original_confidence = core_confidence
    result.effective_confidence = core_confidence

    return result
