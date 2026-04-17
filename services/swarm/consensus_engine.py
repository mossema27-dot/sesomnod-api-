"""
consensus_engine.py — Kjerne konsensus-motor for MiroFish Surgical Swarm V2
"""
from __future__ import annotations

import math
import random
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

import numpy as np

from .oraklion_schema import (
    AgentPrediction,
    ConflictReport,
    ConfidenceBreakdown,
    RiskFlagDetail,
    Signal,
    SignalType,
)

# ---------------------------------------------------------------------------
# Konstanter
# ---------------------------------------------------------------------------

RISK_LAYER_WEIGHT: float = 0.4
VALID_CONSENSUS_THRESHOLD: float = 0.70
WATCH_CONSENSUS_THRESHOLD: float = 0.50
VALID_EDGE_THRESHOLD: float = 0.08
WATCH_EDGE_MIN: float = 0.05
MIN_EDGE: float = 0.05
NEGATIVE_EDGE_VETO: float = -0.05
MIN_KELLY_BET: float = 0.005
MAX_KELLY_BET: float = 0.05
KELLY_FRACTION: float = 0.25
SOFTMAX_TEMPERATURE: float = 2.0
CALIBRATION_WINDOW: int = 90  # dager
CLV_LOOKBACK: int = 30  # dager


class ConsensusEngine:
    """Nash-inspirert konsensusmotor med risk-layer veto."""

    def __init__(self, risk_layer_weight: float = 0.4) -> None:
        self.risk_layer_weight = risk_layer_weight
        self._team_history: dict[str, list[bool]] = defaultdict(list)
        self._call_count: int = 0

    # ------------------------------------------------------------------
    # Offentlig API
    # ------------------------------------------------------------------

    def compute_consensus(self, predictions: list[AgentPrediction]) -> Signal:
        """Beregn vektet konsensus med Nash-inspirert vekting."""
        self._call_count += 1
        if not predictions:
            return Signal(
                signal_type=SignalType.NO_BET,
                prediction=None,
                consensus_ratio=0.0,
                edge_percent=0.0,
                confidence=0.0,
                supporting_teams=[],
                conflicts=[],
                risk_flags=[self._make_no_data_flag()],
                kelly_fraction=0.0,
                timestamp=datetime.utcnow(),
            )

        # 1. Beregn Nash-vekter per lag
        nash_weights = self._compute_nash_weights(predictions)

        # 2. Vektet konsensus per utfall (H / D / A)
        outcome_probs: dict[str, float] = {"H": 0.0, "D": 0.0, "A": 0.0}
        outcome_confidence: dict[str, list[float]] = defaultdict(list)

        for pred in predictions:
            team = pred.team
            weight = nash_weights.get(team, 0.1)
            w = weight * pred.omega_weight * pred.confidence
            outcome_probs[pred.prediction] = outcome_probs.get(pred.prediction, 0.0) + w
            outcome_confidence[pred.prediction].append(pred.confidence)

        # Normaliser til sannsynligheter
        total = sum(outcome_probs.values())
        if total > 0:
            for k in outcome_probs:
                outcome_probs[k] /= total

        best_outcome = max(outcome_probs, key=outcome_probs.get)  # type: ignore[arg-type]
        consensus_ratio = outcome_probs[best_outcome]

        # Gjennomsnittlig confidence for beste utfall
        confidences = outcome_confidence.get(best_outcome, [0.5])
        avg_confidence = float(np.mean(confidences))

        # 3. Beregn edge og Kelly
        avg_odds = np.mean([p.odds for p in predictions if p.prediction == best_outcome] or [2.0])
        edge_percent = self._calculate_edge(consensus_ratio, avg_odds)
        kelly = self._calculate_kelly(consensus_ratio, avg_odds)

        # 4. Konflikt-deteksjon
        conflicts = self._detect_conflicts(predictions)

        # 5. Risk flags
        risk_flags: list[RiskFlagDetail] = []
        if edge_percent < 0:
            risk_flags.append(
                RiskFlagDetail(
                    flag_type="negative_edge",
                    severity="high",
                    description=f"Negativ edge ({edge_percent:.2f}%) — forventet tap",
                    recommended_action="Ikke spill",
                )
            )
        if len(conflicts) > 0:
            risk_flags.append(
                RiskFlagDetail(
                    flag_type="team_conflict",
                    severity="medium" if len(conflicts) <= 2 else "high",
                    description=f"{len(conflicts)} konflikt(er) mellom lag",
                    recommended_action="Vurder motstridende signaler nøye",
                )
            )
        if avg_confidence < 0.6:
            risk_flags.append(
                RiskFlagDetail(
                    flag_type="low_confidence",
                    severity="low",
                    description=f"Lav gjennomsnittlig confidence ({avg_confidence:.2f})",
                    recommended_action="Reduser innsats eller vent",
                )
            )

        # 6. Signal-klassifisering
        signal_type = self._classify_signal(consensus_ratio, edge_percent)

        # 7. Bygg Signal-objekt
        supporting_teams = list(
            {p.team for p in predictions if p.prediction == best_outcome}
        )

        signal = Signal(
            signal_type=signal_type,
            prediction=best_outcome,
            consensus_ratio=consensus_ratio,
            edge_percent=edge_percent,
            confidence=avg_confidence,
            supporting_teams=supporting_teams,
            conflicts=conflicts,
            risk_flags=risk_flags,
            kelly_fraction=kelly,
            timestamp=datetime.utcnow(),
        )

        # 8. Risk veto — hvis edge < -5% → NO_BET
        return self._apply_risk_veto(signal, predictions)

    def get_confidence_breakdown(
        self, predictions: list[AgentPrediction]
    ) -> list[ConfidenceBreakdown]:
        """Returner confidence-breakdown per lag."""
        by_team: dict[str, list[AgentPrediction]] = defaultdict(list)
        for p in predictions:
            by_team[p.team].append(p)

        breakdowns: list[ConfidenceBreakdown] = []
        for team, preds in by_team.items():
            avg_conf = float(np.mean([p.confidence for p in preds]))
            predictions_list = [p.prediction for p in preds]
            most_common = max(set(predictions_list), key=predictions_list.count)
            agreement = predictions_list.count(most_common) / len(predictions_list)
            breakdowns.append(
                ConfidenceBreakdown(
                    team=team,
                    avg_confidence=avg_conf,
                    agreement_ratio=agreement,
                    agent_count=len(preds),
                )
            )
        return breakdowns

    # ------------------------------------------------------------------
    # Interne metoder — Nash-vekter
    # ------------------------------------------------------------------

    def _compute_nash_weights(
        self, predictions: list[AgentPrediction]
    ) -> dict[str, float]:
        """Beregn Nash-lignende likevektvekt for hvert lag."""
        by_team: dict[str, list[AgentPrediction]] = defaultdict(list)
        for p in predictions:
            by_team[p.team].append(p)

        teams = list(by_team.keys())
        if not teams:
            return {}

        # Beregn diversitets-score: hvor unik er lagets prediksjon vs andre?
        all_preds = [p.prediction for p in predictions]
        global_dist = {k: all_preds.count(k) / len(all_preds) for k in set(all_preds)}

        scores: dict[str, float] = {}
        for team in teams:
            preds = by_team[team]
            pred_list = [p.prediction for p in preds]
            confs = [p.confidence for p in preds]

            # Intern enighet (inverse std dev)
            std_conf = float(np.std(confs))
            internal_consistency = max(0.1, 1.0 - std_conf)

            # Diversitets-bonus: er laget unikt?
            team_dist = {k: pred_list.count(k) / len(pred_list) for k in set(pred_list)}
            diversitet = 0.0
            for outcome, prob in team_dist.items():
                global_p = global_dist.get(outcome, 0.01)
                diversitet += abs(prob - global_p)
            diversitet = min(1.0, diversitet)

            # Historisk accuracy (simulert)
            hist = self._team_history.get(team, [])
            accuracy = sum(hist[-20:]) / len(hist[-20:]) if hist else 0.5

            # Nash-score = accuracy * (1 + diversitet_bonus) * internal_consistency
            nash_score = accuracy * (1.0 + diversitet * 0.5) * internal_consistency
            scores[team] = max(0.01, nash_score)

        # Normaliser til vekter som summerer til 1.0
        total_score = sum(scores.values())
        return {t: scores[t] / total_score for t in teams}

    # ------------------------------------------------------------------
    # Interne metoder — Konflikt-deteksjon
    # ------------------------------------------------------------------

    def _detect_conflicts(
        self, predictions: list[AgentPrediction]
    ) -> list[ConflictReport]:
        """Finn team-par som predikerer motsatte utfall."""
        by_team: dict[str, list[AgentPrediction]] = defaultdict(list)
        for p in predictions:
            by_team[p.team].append(p)

        # Hvert lags dominerende prediksjon
        team_majority: dict[str, str] = {}
        for team, preds in by_team.items():
            pred_list = [p.prediction for p in preds]
            majority = max(set(pred_list), key=pred_list.count)
            team_majority[team] = majority

        conflicts: list[ConflictReport] = []
        teams = list(team_majority.keys())
        for i in range(len(teams)):
            for j in range(i + 1, len(teams)):
                ta, tb = teams[i], teams[j]
                pa, pb = team_majority[ta], team_majority[tb]
                if pa != pb:
                    confs_a = [p.confidence for p in by_team[ta]]
                    confs_b = [p.confidence for p in by_team[tb]]
                    gap = abs(float(np.mean(confs_a)) - float(np.mean(confs_b)))
                    severity = (
                        "high"
                        if gap > 0.2
                        else "medium"
                        if gap > 0.1
                        else "low"
                    )
                    conflicts.append(
                        ConflictReport(
                            team_a=ta,
                            team_b=tb,
                            prediction_a=pa,
                            prediction_b=pb,
                            severity=severity,
                            resolution=None,
                        )
                    )
        return conflicts

    # ------------------------------------------------------------------
    # Interne metoder — Risk Layer
    # ------------------------------------------------------------------

    def _apply_risk_veto(
        self, signal: Signal, predictions: list[AgentPrediction]
    ) -> Signal:
        """Risk layer har 40% vekt — veto ved negativ edge."""
        # Veto hvis edge < -5%
        if signal.edge_percent < NEGATIVE_EDGE_VETO * 100:
            return Signal(
                signal_type=SignalType.NO_BET,
                prediction=signal.prediction,
                consensus_ratio=signal.consensus_ratio,
                edge_percent=signal.edge_percent,
                confidence=signal.confidence,
                supporting_teams=signal.supporting_teams,
                conflicts=signal.conflicts,
                risk_flags=signal.risk_flags
                + [
                    RiskFlagDetail(
                        flag_type="risk_veto",
                        severity="critical",
                        description=f"RISK VETO: Edge {signal.edge_percent:.2f}% < {NEGATIVE_EDGE_VETO * 100:.1f}%",
                        recommended_action="NO_BET — forventet negativ avkastning",
                    )
                ],
                kelly_fraction=0.0,
                timestamp=datetime.utcnow(),
            )
        return signal

    # ------------------------------------------------------------------
    # Interne metoder — Hjelpefunksjoner
    # ------------------------------------------------------------------

    def _classify_signal(self, consensus_ratio: float, edge_percent: float) -> SignalType:
        if consensus_ratio >= VALID_CONSENSUS_THRESHOLD and edge_percent > VALID_EDGE_THRESHOLD * 100:
            return SignalType.VALID
        if consensus_ratio >= WATCH_CONSENSUS_THRESHOLD and edge_percent >= WATCH_EDGE_MIN * 100:
            return SignalType.WATCH
        return SignalType.NO_BET

    def _calculate_edge(self, consensus_prob: float, odds: float) -> float:
        """edge = (consensus_prob * odds) - 1, konvertert til prosent."""
        return (consensus_prob * odds - 1.0) * 100.0

    def _calculate_kelly(self, prob: float, odds: float, fraction: float = KELLY_FRACTION) -> float:
        """Standard Kelly med grenser."""
        if odds <= 1.0:
            return 0.0
        b = odds - 1.0
        raw_kelly = (prob * b - (1.0 - prob)) / b * fraction
        kelly = max(0.0, min(MAX_KELLY_BET, raw_kelly))
        if kelly < MIN_KELLY_BET:
            return 0.0
        return kelly

    def _make_no_data_flag(self) -> RiskFlagDetail:
        return RiskFlagDetail(
            flag_type="no_data",
            severity="critical",
            description="Ingen prediksjoner mottatt",
            recommended_action="Ingen handling mulig uten data",
        )

    def record_result(self, team: str, was_correct: bool) -> None:
        """Registrer resultat for et lag for fremtidig Nash-beregning."""
        self._team_history[team].append(was_correct)
