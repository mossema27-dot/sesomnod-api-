"""
intelligence_scorer.py — SesomNod MOAT Foundation
Beregner alle intelligence-scores iht. FALSE_CONFIDENCE_DOCTRINE
og CONTRADICTION_DOCTRINE.

Score-definisjoner:
  EIS  — Edge Integrity Score       (0-100, hoy = sterk edge)
  CFS  — Confidence Fragility Score (0-100, hoy = skjor/farlig)
  NDI  — Narrative Distortion Index (0-100, hoy = media-stoey)
  SAS  — Sharp Alignment Score      (0-100, hoy = sharp peker med oss)
  SSR  — Scenario Survival Rate     (0-100, hoy = overlever stress)
  NBPI — No-Bet Pressure Index      (0-100, hoy = sterkt press mot bet)
  CD   — Contradiction Density      (0-100, jf. CONTRADICTION_DOCTRINE)

Regler:
- Ingen DB-kall
- Ingen imports fra main.py
- Alle scores er deterministiske gitt input
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class IntelligenceScores:
    """Container for alle beregnet intelligence-scores."""
    edge_integrity_score: float = 0.0          # EIS
    confidence_fragility_score: float = 0.0    # CFS
    narrative_distortion_index: float = 0.0    # NDI
    sharp_alignment_score: float = 100.0       # SAS  (starter hoy — trekkes ned)
    scenario_survival_rate: float = 100.0      # SSR  (starter hoy — trekkes ned)
    no_bet_pressure_index: float = 0.0         # NBPI
    contradiction_density: float = 0.0         # CD
    false_confidence_triggers: list[str] = field(default_factory=list)
    contradictions_weak: list[str] = field(default_factory=list)
    contradictions_medium: list[str] = field(default_factory=list)
    contradictions_critical: list[str] = field(default_factory=list)


def calculate_eis(
    edge_pct: float,
    line_movement_pct: float | None,
    completeness_pct: float,
    omega_score: int | None,
) -> float:
    """
    Edge Integrity Score — maler hvor solid kanten faktisk er.

    Faktorer:
    - Grunnlinje = edge_pct skalert til 0-100
    - Fratrekk: line movement mot oss, lav completeness, lav omega
    """
    # Grunnlinje: edge 0% = EIS 0, edge 25%+ = EIS 100
    base = min(100.0, (edge_pct / 25.0) * 100.0)

    # Line movement mot oss trekker ned
    lm_penalty = 0.0
    if line_movement_pct is not None and line_movement_pct < 0:
        # -10% lm = -20 EIS, -25% lm = -50 EIS
        lm_penalty = min(50.0, abs(line_movement_pct) * 2.0)

    # Completeness under 75% trekker ned
    comp_penalty = 0.0
    if completeness_pct < 75:
        comp_penalty = (75.0 - completeness_pct) * 0.5

    # Omega under 55 trekker ned
    omega_penalty = 0.0
    if omega_score is not None and omega_score < 55:
        omega_penalty = (55 - omega_score) * 0.4

    eis = max(0.0, base - lm_penalty - comp_penalty - omega_penalty)
    return round(eis, 1)


def calculate_cfs(
    confidence: str,
    false_confidence_triggers: list[str],
    lineup_confirmed: bool | None,
    completeness_pct: float,
    hours_to_kickoff: float | None,
    edge_pct: float,
) -> float:
    """
    Confidence Fragility Score — maler risikoen for at HIGH confidence er feil.

    Hoy CFS = fragil = farlig.
    """
    base = 0.0

    # FALSE_CONFIDENCE_DOCTRINE: antall aktive triggers
    n_triggers = len(false_confidence_triggers)
    base += n_triggers * 20.0  # 1 trigger = +20 CFS

    # Lineup ukonfirmert <4t + edge <15%
    if (
        lineup_confirmed is False
        and hours_to_kickoff is not None
        and hours_to_kickoff < 4
        and edge_pct < 15
    ):
        base += 15.0

    # Completeness under 65% er FC-trigger
    if completeness_pct < 65:
        base += 15.0

    # HIGH confidence med svak underbygging
    if confidence == "HIGH" and completeness_pct < 75:
        base += 10.0

    return round(min(100.0, base), 1)


def calculate_contradiction_density(
    weak: list[str],
    medium: list[str],
    critical: list[str],
) -> float:
    """
    Contradiction Density score iht. CONTRADICTION_DOCTRINE.md.
    """
    n_weak = len(weak)
    n_medium = len(medium)
    n_critical = len(critical)

    if n_critical >= 2:
        return 80.0 + min(20.0, (n_critical - 2) * 5.0)
    if n_critical == 1:
        return 65.0
    if n_medium >= 2:
        return 50.0
    if n_medium == 1:
        return 30.0
    if n_weak >= 2:
        return 20.0
    if n_weak == 1:
        return 10.0
    return 0.0


def calculate_nbpi(
    eis: float,
    cfs: float,
    contradiction_density: float,
    completeness_pct: float,
    sharp_alignment_score: float,
) -> float:
    """
    No-Bet Pressure Index — aggregert press mot a legge bet.

    NBPI >80 = automatisk NO-BET (iht. CONTRADICTION_DOCTRINE).
    """
    # Vektet gjennomsnitt: lav EIS + hoy CFS + hoy CD = hoy NBPI
    eis_contribution = (100.0 - eis) * 0.30
    cfs_contribution = cfs * 0.25
    cd_contribution = contradiction_density * 0.25
    comp_contribution = (100.0 - completeness_pct) * 0.10
    sas_contribution = (100.0 - sharp_alignment_score) * 0.10

    nbpi = eis_contribution + cfs_contribution + cd_contribution + comp_contribution + sas_contribution
    return round(min(100.0, max(0.0, nbpi)), 1)


def score_pick(dossier: dict) -> IntelligenceScores:
    """
    Hovedinngang — beregner alle scores fra et ferdig dossier-dict.

    Returnerer IntelligenceScores med alle scores og trigger-lister populert.
    """
    core = dossier.get("sesomnod_core", {})
    market = dossier.get("market", {})
    squad = dossier.get("squad", {})
    intel = dossier.get("intelligence_state", {})
    team_priors = dossier.get("team_priors", {})

    edge_pct: float = core.get("edge_pct", 0.0)
    confidence: str = core.get("confidence", "LOW")
    omega_score: int | None = core.get("omega_score")
    completeness_pct: float = dossier.get("dossier_completeness_pct", 0.0)
    lineup_confirmed: bool | None = squad.get("lineup_confirmed")
    line_movement_pct: float | None = market.get("line_movement_pct")
    sharp_money: str = market.get("sharp_money_indicator", "neutral")

    # Timer til kickoff (kan injiseres via intelligence_state)
    hours_to_kickoff: float | None = intel.get("hours_to_kickoff")

    # ---------------------------------------------------------------------------
    # False Confidence Triggers (iht. FALSE_CONFIDENCE_DOCTRINE.md)
    # ---------------------------------------------------------------------------
    fc_triggers: list[str] = []

    if (
        lineup_confirmed is False
        and hours_to_kickoff is not None
        and hours_to_kickoff < 4
        and edge_pct < 15
    ):
        fc_triggers.append("LINEUP_UNCONFIRMED_CLOSE_TO_KICKOFF")

    if line_movement_pct is not None and line_movement_pct < -10:
        fc_triggers.append("LINE_MOVEMENT_AGAINST_10PCT")

    eis_prelim = calculate_eis(edge_pct, line_movement_pct, completeness_pct, omega_score)
    if eis_prelim < 60:
        fc_triggers.append("EIS_BELOW_60")

    # xG form divergence — hentes fra intelligence_state hvis tilgjengelig
    xg_divergence: float | None = intel.get("xg_form_divergence_pct")
    if xg_divergence is not None and xg_divergence > 30:
        fc_triggers.append("XG_FORM_DIVERGENCE_30PCT")

    if completeness_pct < 65:
        fc_triggers.append("COMPLETENESS_BELOW_65")

    # HIGH confidence med 3+ motsigende signaler
    n_contradictions = len(intel.get("false_confidence_triggers", []))
    if confidence == "HIGH" and n_contradictions >= 3:
        fc_triggers.append("HIGH_CONFIDENCE_MULTIPLE_CONTRADICTIONS")

    # ---------------------------------------------------------------------------
    # Contradictions
    # ---------------------------------------------------------------------------
    weak: list[str] = []
    medium: list[str] = []
    critical: list[str] = []

    # Medium: line movement >8% mot oss
    if line_movement_pct is not None and line_movement_pct < -8:
        medium.append("LINE_MOVEMENT_8PCT_AGAINST")

    # Medium: sharp money mot oss
    selection = core.get("selection", "")
    _selection_side = _infer_side(selection)
    if sharp_money != "neutral" and sharp_money != _selection_side:
        medium.append("SHARP_MONEY_AGAINST")

    # Kritisk: line movement >15% mot oss
    if line_movement_pct is not None and line_movement_pct < -15:
        critical.append("LINE_MOVEMENT_15PCT_AGAINST_CRITICAL")

    # Kritisk: sharp alignment <35 (injisert)
    sas_input: float | None = intel.get("sharp_alignment_score_input")
    if sas_input is not None and sas_input < 35:
        critical.append("SHARP_ALIGNMENT_BELOW_35")

    # Kritisk: MiroFish SSR <40%
    ssr_input: float | None = intel.get("mirofish_ssr")
    if ssr_input is not None and ssr_input < 40:
        critical.append("MIROFISH_SSR_BELOW_40")

    # Dedupliser medium — line movement kan trigge bade medium og kritisk
    if "LINE_MOVEMENT_15PCT_AGAINST_CRITICAL" in critical and "LINE_MOVEMENT_8PCT_AGAINST" in medium:
        medium = [x for x in medium if x != "LINE_MOVEMENT_8PCT_AGAINST"]

    # ---------------------------------------------------------------------------
    # Beregn alle scores
    # ---------------------------------------------------------------------------
    cd = calculate_contradiction_density(weak, medium, critical)

    # Sharp Alignment Score
    sas: float = sas_input if sas_input is not None else 100.0
    if sharp_money != "neutral" and sharp_money != _selection_side:
        sas = max(0.0, sas - 30.0)

    # Scenario Survival Rate — reduseres av CD og FC-triggers
    ssr: float = ssr_input if ssr_input is not None else 100.0
    ssr = max(0.0, ssr - cd * 0.3 - len(fc_triggers) * 5.0)

    eis = calculate_eis(edge_pct, line_movement_pct, completeness_pct, omega_score)
    cfs = calculate_cfs(confidence, fc_triggers, lineup_confirmed, completeness_pct, hours_to_kickoff, edge_pct)
    ndi: float = intel.get("narrative_distortion_index", 0.0)
    nbpi = calculate_nbpi(eis, cfs, cd, completeness_pct, sas)

    scores = IntelligenceScores(
        edge_integrity_score=eis,
        confidence_fragility_score=cfs,
        narrative_distortion_index=round(ndi, 1),
        sharp_alignment_score=round(sas, 1),
        scenario_survival_rate=round(ssr, 1),
        no_bet_pressure_index=nbpi,
        contradiction_density=cd,
        false_confidence_triggers=fc_triggers,
        contradictions_weak=weak,
        contradictions_medium=medium,
        contradictions_critical=critical,
    )

    logger.info(
        "Scored pick %s | EIS=%.1f CFS=%.1f NBPI=%.1f CD=%.1f FC-triggers=%d",
        dossier.get("dossier_id", "?"),
        eis, cfs, nbpi, cd, len(fc_triggers),
    )

    return scores


def _infer_side(selection: str) -> str:
    """Forsok a mappe selection-tekst til home/draw/away for sharp-sammenligning."""
    s = selection.lower()
    if "home" in s or "1" == s.strip():
        return "home"
    if "away" in s or "2" == s.strip():
        return "away"
    if "draw" in s or "x" == s.strip():
        return "draw"
    return "neutral"
