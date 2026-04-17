"""
oraklion_schema.py — Pydantic v2 modeller for MiroFish Surgical Swarm V2
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enummer
# ---------------------------------------------------------------------------

class SignalType(str, Enum):
    VALID = "VALID"
    WATCH = "WATCH"
    NO_BET = "NO_BET"


# ---------------------------------------------------------------------------
# Grunnleggende modeller
# ---------------------------------------------------------------------------

class AgentPrediction(BaseModel):
    agent_id: str
    team: str
    prediction: str  # "H", "D", eller "A"
    confidence: float = Field(..., ge=0.0, le=1.0)
    omega_weight: float = 1.0
    odds: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    features: dict[str, Any] = Field(default_factory=dict)


class MarketDepth(BaseModel):
    liquidity_score: float = Field(..., ge=0.0, le=1.0)
    spread: float
    volume_estimate: float


class TimingRecommendationDetail(BaseModel):
    optimal_window: str
    urgency: str  # "spill_nå", "vent", "ikke_spill"
    line_movement_prediction: str  # "fall", "stabil", "stigning"


class AlternativeMarket(BaseModel):
    market_type: str
    selection: str
    odds: float
    edge_percent: float
    confidence: float


class RiskFlagDetail(BaseModel):
    flag_type: str
    severity: str  # "low", "medium", "high", "critical"
    description: str
    recommended_action: str


class ConfidenceBreakdown(BaseModel):
    team: str
    avg_confidence: float
    agreement_ratio: float
    agent_count: int


class ConflictReport(BaseModel):
    team_a: str
    team_b: str
    prediction_a: str
    prediction_b: str
    severity: str
    resolution: Optional[str] = None


class CLVExpected(BaseModel):
    expected_clv: float
    clv_probability: float
    edge_capture_estimate: float


# ---------------------------------------------------------------------------
# Signal
# ---------------------------------------------------------------------------

class Signal(BaseModel):
    signal_type: SignalType
    prediction: Optional[str] = None
    consensus_ratio: float
    edge_percent: float
    confidence: float
    supporting_teams: list[str] = Field(default_factory=list)
    conflicts: list[ConflictReport] = Field(default_factory=list)
    risk_flags: list[RiskFlagDetail] = Field(default_factory=list)
    kelly_fraction: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Respons-modeller
# ---------------------------------------------------------------------------

class OraklionResponse(BaseModel):
    match_id: str
    timestamp: datetime
    signal: SignalType
    prediction: Optional[str] = None
    consensus_ratio: float
    edge_percent: float
    confidence: float
    kelly_fraction: float
    supporting_teams: list[str] = Field(default_factory=list)
    opposing_teams: list[str] = Field(default_factory=list)
    conflicts: list[ConflictReport] = Field(default_factory=list)
    risk_flags: list[RiskFlagDetail] = Field(default_factory=list)
    confidence_breakdown: list[ConfidenceBreakdown] = Field(default_factory=list)
    clv_expected: CLVExpected
    market_depth: MarketDepth
    timing_recommendation: TimingRecommendationDetail
    alternative_markets: list[AlternativeMarket] = Field(default_factory=list)
    agent_count: int
    conflict_rate: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class MatchSummary(BaseModel):
    match_id: str
    home_team: str
    away_team: str
    league: str
    kickoff: str
    signal: Optional[SignalType] = None


class DailyMatchList(BaseModel):
    date: str
    matches: list[MatchSummary] = Field(default_factory=list)
    total_count: int = 0


class ConsensusRequest(BaseModel):
    predictions: list[AgentPrediction]
    match_id: str


class ConsensusResponse(BaseModel):
    signal: Signal
    processing_time_ms: float


class RecalibrationResult(BaseModel):
    agents_updated: int
    weights_changed: dict[str, float] = Field(default_factory=dict)
    new_distributions: dict[str, float] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class HealthStatus(BaseModel):
    status: str  # "healthy", "degraded", "down"
    active_agents: int
    consensus_engine: str
    moat_engine: str
    last_clv_update: Optional[str] = None
    last_recalibration: Optional[str] = None


# ---------------------------------------------------------------------------
# MOAT / Agent-modeller
# ---------------------------------------------------------------------------

class AgentMetrics(BaseModel):
    agent_id: str
    team: str
    brier_score: float
    log_loss: float
    hit_rate: float
    clv_generated: float
    total_predictions: int
    correct_predictions: int
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class CLVRecord(BaseModel):
    match_id: str
    agent_id: str
    opening_odds: float
    closing_odds: float
    predicted_outcome: str
    actual_outcome: str
    clv_value: float
    edge_captured: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class MarketImpactModel(BaseModel):
    bet_size_threshold: float
    odds_drift_factor: float
    time_decay: float
