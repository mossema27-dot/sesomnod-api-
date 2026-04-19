"""
moat_engine.py — MOAT Engine: CLV-læring, accuracy, recalibrering
"""
from __future__ import annotations

import math
import random
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional

import numpy as np

from .oraklion_schema import (
    AgentMetrics,
    AgentPrediction,
    CLVRecord,
    MarketImpactModel,
    RecalibrationResult,
)

# ---------------------------------------------------------------------------
# Konstanter
# ---------------------------------------------------------------------------

SOFTMAX_TEMPERATURE: float = 2.0
CALIBRATION_WINDOW: int = 90  # dager
CLV_LOOKBACK: int = 30  # dager
MIN_KELLY_BET: float = 0.005
MAX_KELLY_BET: float = 0.05


def _softmax(values: list[float], temperature: float = 1.0) -> list[float]:
    """Numerisk stabil softmax."""
    if not values:
        return []
    v = np.array(values, dtype=float)
    v = v / temperature
    v_max = np.max(v)
    exp_v = np.exp(v - v_max)
    s = np.sum(exp_v)
    return [float(x) for x in exp_v / s]


class TransferLearningModel:
    """Overføringslæring mellom ligaer."""

    def __init__(self) -> None:
        self._league_profiles: dict[str, dict[str, float]] = {
            "Premier League": {"avg_goals": 2.75, "variance": 1.45, "home_adv": 0.35, "strength": 0.92},
            "La Liga": {"avg_goals": 2.55, "variance": 1.30, "home_adv": 0.38, "strength": 0.88},
            "Bundesliga": {"avg_goals": 3.05, "variance": 1.60, "home_adv": 0.32, "strength": 0.85},
            "Serie A": {"avg_goals": 2.65, "variance": 1.35, "home_adv": 0.40, "strength": 0.87},
            "Ligue 1": {"avg_goals": 2.35, "variance": 1.25, "home_adv": 0.33, "strength": 0.80},
            "Eliteserien": {"avg_goals": 2.95, "variance": 1.70, "home_adv": 0.30, "strength": 0.60},
            "Champions League": {"avg_goals": 2.85, "variance": 1.50, "home_adv": 0.25, "strength": 0.95},
        }

    def compute_league_similarity(self, league_a: str, league_b: str) -> float:
        """Beregn likhet mellom to ligaer basert på profiler."""
        pa = self._league_profiles.get(league_a)
        pb = self._league_profiles.get(league_b)
        if not pa or not pb:
            return 0.5
        keys = ["avg_goals", "variance", "home_adv", "strength"]
        diffs = [abs(pa[k] - pb[k]) for k in keys]
        avg_diff = sum(diffs) / len(diffs)
        return max(0.0, 1.0 - avg_diff / max(max(pa.values()) - min(pa.values()), 0.01))

    def transfer_weights(
        self, source_league: str, target_league: str, agent_metrics: dict[str, Any]
    ) -> dict[str, Any]:
        """Overfør tilpassede vekter mellom lignende ligaer."""
        similarity = self.compute_league_similarity(source_league, target_league)
        result: dict[str, Any] = {"similarity": similarity}
        for key, val in agent_metrics.items():
            if isinstance(val, (int, float)):
                result[key] = val * (0.5 + 0.5 * similarity)
            else:
                result[key] = val
        return result


class MOATEngine:
    """MOAT Engine — kontinuerlig læring og recalibrering."""

    def __init__(self, db_path: str = "moat.db") -> None:
        self.db_path = db_path
        self.MIN_KELLY_BET = MIN_KELLY_BET
        self.MAX_KELLY_BET = MAX_KELLY_BET
        self._agent_history: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "predictions": 0,
                "correct": 0,
                "brier_scores": [],
                "log_losses": [],
                "clv_values": [],
                "last_updated": datetime.utcnow(),
            }
        )
        self._clv_records: list[CLVRecord] = []
        self._weights: dict[str, float] = {}
        self._transfer_model = TransferLearningModel()
        self._last_clv_update: Optional[datetime] = None
        self._last_recalibration: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Offentlig API
    # ------------------------------------------------------------------

    def update_accuracy(
        self, match_id: str, actual_result: str, predictions: list[AgentPrediction]
    ) -> dict[str, AgentMetrics]:
        """Oppdater Brier score, hit-rate, log-loss per agent."""
        results: dict[str, AgentMetrics] = {}
        for pred in predictions:
            agent_id = pred.agent_id
            hist = self._agent_history[agent_id]
            hist["predictions"] += 1
            was_correct = pred.prediction == actual_result
            if was_correct:
                hist["correct"] += 1

            # Brier = (pred_prob - actual)^2
            pred_prob = pred.confidence if pred.prediction == actual_result else 1.0 - pred.confidence
            actual_val = 1.0 if was_correct else 0.0
            brier = (pred_prob - actual_val) ** 2
            hist["brier_scores"].append(brier)

            # Log-loss
            eps = 1e-15
            prob = max(eps, min(1.0 - eps, pred_prob))
            ll = -math.log(prob) if actual_val == 1.0 else -math.log(1.0 - prob)
            hist["log_losses"].append(ll)
            hist["last_updated"] = datetime.utcnow()

            hit_rate = hist["correct"] / max(1, hist["predictions"])
            avg_brier = float(np.mean(hist["brier_scores"][-50:])) if hist["brier_scores"] else 0.25
            avg_ll = float(np.mean(hist["log_losses"][-50:])) if hist["log_losses"] else 0.69

            results[agent_id] = AgentMetrics(
                agent_id=agent_id,
                team=pred.team,
                brier_score=avg_brier,
                log_loss=avg_ll,
                hit_rate=hit_rate,
                clv_generated=sum(hist["clv_values"]) if hist["clv_values"] else 0.0,
                total_predictions=hist["predictions"],
                correct_predictions=hist["correct"],
                last_updated=hist["last_updated"],
            )
        return results

    def compute_clv(
        self, match_id: str, agent_predictions: list[AgentPrediction], closing_odds: dict[str, float]
    ) -> list[CLVRecord]:
        """CLV = vår odds - closing odds (hvis positiv, har vi slått markedet)."""
        records: list[CLVRecord] = []
        for pred in agent_predictions:
            outcome = pred.prediction
            opening = pred.odds
            closing = closing_odds.get(outcome, opening * 0.95)
            clv_val = opening - closing  # Positiv = vi fikk bedre odds enn markedet
            record = CLVRecord(
                match_id=match_id,
                agent_id=pred.agent_id,
                opening_odds=opening,
                closing_odds=closing,
                predicted_outcome=outcome,
                actual_outcome="",  # Fylles inn senere
                clv_value=clv_val,
                edge_captured=max(0.0, clv_val / opening * 100) if opening > 0 else 0.0,
                timestamp=datetime.utcnow(),
            )
            records.append(record)
            self._agent_history[pred.agent_id]["clv_values"].append(clv_val)

        self._clv_records.extend(records)
        self._last_clv_update = datetime.utcnow()
        return records

    def generate_weights(self) -> dict[str, float]:
        """Generer omega_weights per agent: softmax(-brier * TEMPERATURE) * accuracy_bonus."""
        weights: dict[str, float] = {}
        agent_ids = list(self._agent_history.keys())
        if not agent_ids:
            return {}

        briers = []
        for aid in agent_ids:
            hist = self._agent_history[aid]
            recent_brier = (
                float(np.mean(hist["brier_scores"][-30:]))
                if hist["brier_scores"]
                else 0.25
            )
            briers.append(-recent_brier)  # Negativ for softmax (lavere brier = høyere vekt)

        softmax_vals = _softmax(briers, SOFTMAX_TEMPERATURE)
        for aid, sm_val in zip(agent_ids, softmax_vals):
            hist = self._agent_history[aid]
            hit_rate = hist["correct"] / max(1, hist["predictions"])
            accuracy_bonus = 0.5 + hit_rate  # 0.5 til 1.5
            weights[aid] = round(sm_val * accuracy_bonus, 4)

        self._weights = weights
        return weights

    def detect_edge_erosion(self, lookback: int = CLV_LOOKBACK) -> dict[str, float]:
        """Sjekk om edges avtar over tid."""
        erosion: dict[str, float] = {}
        cutoff = datetime.utcnow() - timedelta(days=lookback)
        by_agent: dict[str, list[float]] = defaultdict(list)

        for rec in self._clv_records:
            if rec.timestamp > cutoff:
                by_agent[rec.agent_id].append(rec.edge_captured)

        for aid, edges in by_agent.items():
            if len(edges) >= 5:
                # Simpel lineær trend
                x = np.arange(len(edges))
                if len(set(x)) > 1:
                    slope = float(np.polyfit(x, edges, 1)[0])
                    erosion[aid] = round(slope, 4)
                else:
                    erosion[aid] = 0.0
            else:
                erosion[aid] = 0.0

        # Erosjon per team (gjennomsnitt)
        team_erosion: dict[str, list[float]] = defaultdict(list)
        for aid, val in erosion.items():
            team = aid.rsplit("_", 1)[0] if "_" in aid else "unknown"
            team_erosion[team].append(val)

        return {t: round(float(np.mean(v)), 4) for t, v in team_erosion.items()}

    def apply_recalibration(self) -> RecalibrationResult:
        """Månedlig recalibrering — justerer alle weights basert på siste 90 dager."""
        self._last_recalibration = datetime.utcnow()
        weights_before = dict(self._weights)
        new_weights = self.generate_weights()

        # Straff agenter med dårlig performance
        for aid, weight in new_weights.items():
            hist = self._agent_history[aid]
            if hist["predictions"] >= 10:
                hit_rate = hist["correct"] / hist["predictions"]
                if hit_rate < 0.4:
                    new_weights[aid] = weight * 0.5  # Halver vekten
                elif hit_rate > 0.6:
                    new_weights[aid] = weight * 1.2  # Bonus

        self._weights = new_weights
        changed = {
            aid: round(new_weights.get(aid, 0.0) - weights_before.get(aid, 0.0), 4)
            for aid in set(new_weights) | set(weights_before)
        }
        changed = {k: v for k, v in changed.items() if abs(v) > 0.001}

        return RecalibrationResult(
            agents_updated=len(new_weights),
            weights_changed=changed,
            new_distributions=new_weights,
            timestamp=datetime.utcnow(),
        )

    @staticmethod
    def brier_score(predicted_prob: float, actual_outcome: float) -> float:
        """Brier score: (predicted_prob - actual_outcome)^2."""
        return (predicted_prob - actual_outcome) ** 2

    def kelly_criterion(self, prob: float, odds: float, fraction: float = 0.25) -> float:
        """Standard Kelly med grenser."""
        if odds <= 1.0:
            return 0.0
        b = odds - 1.0
        raw = (prob * b - (1.0 - prob)) / b * fraction
        kelly = max(0.0, min(MAX_KELLY_BET, raw))
        return kelly if kelly >= MIN_KELLY_BET else 0.0

    def load_historical_clv(self, records: list) -> int:
        """
        Seed MOATEngine from historical mirofish_clv rows.
        Each record must have: pick_id, outcome, clv_pct, closing_odds.
        Returns count of records loaded.
        SELECT-only — never writes to mirofish_clv.
        """
        loaded = 0
        for row in records:
            try:
                pick_id     = str(row.get("pick_id") or row["pick_id"])
                outcome     = row.get("outcome")       # WIN / LOSS / VOID / None
                clv_pct     = row.get("clv_pct")
                closing_odds = row.get("closing_odds")

                if outcome is None or clv_pct is None:
                    continue

                agent_id = f"historical_{pick_id}"
                hist     = self._agent_history[agent_id]
                hist["predictions"] += 1

                was_correct = str(outcome).upper() == "WIN"
                if was_correct:
                    hist["correct"] += 1

                # Synthetic Brier: CLV >0 = good prediction → lower brier
                clv_float = float(clv_pct) if clv_pct else 0.0
                brier = max(0.0, min(1.0, 0.25 - clv_float / 100.0))
                hist["brier_scores"].append(brier)

                # CLV record
                if clv_pct is not None:
                    hist["clv_values"].append(clv_float)

                hist["last_updated"] = datetime.utcnow()
                loaded += 1
            except Exception:
                continue
        return loaded

    def calibrate_confidence(self, agent_id: str, raw_conf: float = 0.7) -> float:
        """Brier-score basert kalibrering."""
        hist = self._agent_history.get(agent_id)
        if not hist or not hist["brier_scores"]:
            calibration_factor = 0.75
        else:
            recent_brier = float(np.mean(hist["brier_scores"][-50:]))
            calibration_factor = max(0.3, 1.0 - recent_brier)
        calibrated = raw_conf * calibration_factor
        return max(0.1, min(1.0, calibrated))

    def estimate_market_impact(
        self, bet_size: float, current_odds: float, liquidity: float
    ) -> float:
        """impact = bet_size / liquidity * odds_drift_factor * exp(-time_decay)."""
        odds_drift_factor = 0.05
        time_decay = 0.1
        if liquidity <= 0:
            return 0.0
        impact = bet_size / liquidity * odds_drift_factor * math.exp(-time_decay)
        return round(impact, 6)

    def get_agent_dashboard(self) -> list[AgentMetrics]:
        """Returner komplett dashboard med alle metrics."""
        dashboard: list[AgentMetrics] = []
        for aid, hist in self._agent_history.items():
            team = aid.rsplit("_", 1)[0] if "_" in aid else "unknown"
            total = hist["predictions"]
            correct = hist["correct"]
            hit_rate = correct / max(1, total)
            avg_brier = (
                float(np.mean(hist["brier_scores"][-50:])) if hist["brier_scores"] else 0.25
            )
            avg_ll = (
                float(np.mean(hist["log_losses"][-50:])) if hist["log_losses"] else 0.69
            )
            total_clv = sum(hist["clv_values"]) if hist["clv_values"] else 0.0
            dashboard.append(
                AgentMetrics(
                    agent_id=aid,
                    team=team,
                    brier_score=avg_brier,
                    log_loss=avg_ll,
                    hit_rate=hit_rate,
                    clv_generated=total_clv,
                    total_predictions=total,
                    correct_predictions=correct,
                    last_updated=hist["last_updated"],
                )
            )
        return dashboard

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def last_clv_update(self) -> Optional[datetime]:
        return self._last_clv_update

    @property
    def last_recalibration(self) -> Optional[datetime]:
        return self._last_recalibration
