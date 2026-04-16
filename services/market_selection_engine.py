"""
SesomNod Market Selection Engine
Selects the best betting market for a given match based on composite scoring.
"""

import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


class MarketType(Enum):
    """Enumeration of supported market types."""
    HOME_WIN = "1x2_home"
    DRAW = "1x2_draw"
    AWAY_WIN = "1x2_away"
    OVER_05 = "over_05"
    OVER_15 = "over_15"
    OVER_25 = "over_25"
    OVER_35 = "over_35"
    OVER_45 = "over_45"
    UNDER_05 = "under_05"
    UNDER_15 = "under_15"
    UNDER_25 = "under_25"
    UNDER_35 = "under_35"
    BTTS = "btts"
    HOME_SCORES = "home_scores"
    AWAY_SCORES = "away_scores"
    HOME_CS = "home_cs"
    AWAY_CS = "away_cs"
    AH_HOME_MINUS_05 = "ah_home_minus_05"
    AH_HOME_MINUS_15 = "ah_home_minus_15"
    AH_AWAY_PLUS_05 = "ah_away_plus_05"
    AH_AWAY_PLUS_15 = "ah_away_plus_15"


@dataclass
class MarketEvaluation:
    """Evaluation result for a single market."""
    market: str
    selection: str
    model_prob: float
    market_prob: float
    implied_odds: float
    edge: float
    kelly: float
    confidence: float
    composite_score: float
    is_valid: bool
    rejection_reason: Optional[str] = None


@dataclass
class MarketSelectionResult:
    """Final result from market selection."""
    market: str
    selection: str
    model_prob: float
    market_prob: float
    implied_odds: float
    edge: float
    kelly: float
    confidence: float
    composite_score: float
    rejected_markets: List[Dict] = field(default_factory=list)


class MarketSelectionEngine:
    """
    Evaluates all available markets and selects the best one based on composite scoring.
    """
    
    # Liga-spesifikke edge-terskler
    LEAGUE_THRESHOLDS = {
        'premier_league':  {'1x2': 0.045, 'over_under': 0.035, 'btts': 0.040, 'ah': 0.030},
        'la_liga':         {'1x2': 0.050, 'over_under': 0.040, 'btts': 0.045, 'ah': 0.035},
        'bundesliga':      {'1x2': 0.048, 'over_under': 0.035, 'btts': 0.038, 'ah': 0.032},
        'serie_a':         {'1x2': 0.052, 'over_under': 0.042, 'btts': 0.048, 'ah': 0.038},
        'ligue_1':         {'1x2': 0.055, 'over_under': 0.043, 'btts': 0.050, 'ah': 0.040},
        'championship':    {'1x2': 0.060, 'over_under': 0.048, 'btts': 0.055, 'ah': 0.045},
        'default':         {'1x2': 0.080, 'over_under': 0.060, 'btts': 0.065, 'ah': 0.055},
    }
    
    # Market type to category mapping
    MARKET_CATEGORIES = {
        'home_win': '1x2',
        'draw': '1x2',
        'away_win': '1x2',
        'over_05': 'over_under',
        'over_15': 'over_under',
        'over_25': 'over_under',
        'over_35': 'over_under',
        'over_45': 'over_under',
        'under_05': 'over_under',
        'under_15': 'over_under',
        'under_25': 'over_under',
        'under_35': 'over_under',
        'btts': 'btts',
        'home_scores': 'btts',
        'away_scores': 'btts',
        'home_cs': 'ah',
        'away_cs': 'ah',
        'ah_home_minus_05': 'ah',
        'ah_home_minus_15': 'ah',
        'ah_away_plus_05': 'ah',
        'ah_away_plus_15': 'ah',
    }
    
    # Market names for display
    MARKET_NAMES = {
        'home_win': 'Hjemmeseier',
        'draw': 'Uavgjort',
        'away_win': 'Borteseier',
        'over_05': 'Over 0.5 mål',
        'over_15': 'Over 1.5 mål',
        'over_25': 'Over 2.5 mål',
        'over_35': 'Over 3.5 mål',
        'over_45': 'Over 4.5 mål',
        'under_05': 'Under 0.5 mål',
        'under_15': 'Under 1.5 mål',
        'under_25': 'Under 2.5 mål',
        'under_35': 'Under 3.5 mål',
        'btts': 'Begge lag scorer',
        'home_scores': 'Hjemmelag scorer',
        'away_scores': 'Bortelag scorer',
        'home_cs': 'Hjemmelag holder nullen',
        'away_cs': 'Bortelag holder nullen',
        'ah_home_minus_05': 'AH Hjemme -0.5',
        'ah_home_minus_15': 'AH Hjemme -1.5',
        'ah_away_plus_05': 'AH Borte +0.5',
        'ah_away_plus_15': 'AH Borte +1.5',
    }
    
    def __init__(self):
        """Initialize the Market Selection Engine."""
        pass
    
    def time_adjust_threshold(self, base: float, hours: float) -> float:
        """
        Adjust edge threshold based on time to kickoff.
        
        Args:
            base: Base threshold value
            hours: Hours until kickoff
            
        Returns:
            Adjusted threshold
        """
        if hours > 72:
            return base * 1.15  # Higher requirements early
        elif hours > 24:
            return base * 1.00  # Standard
        elif hours > 6:
            return base * 0.92  # Sweet spot
        elif hours > 2:
            return base * 0.85  # Closing value
        else:
            return base * 1.20  # Too late - strict
    
    def get_threshold_for_market(self, market: str, league: str, hours_to_kickoff: float) -> float:
        """
        Get the edge threshold for a specific market in a specific league.
        
        Args:
            market: Market name
            league: League name
            hours_to_kickoff: Hours until match starts
            
        Returns:
            Edge threshold (minimum edge required)
        """
        category = self.MARKET_CATEGORIES.get(market, '1x2')
        league_thresholds = self.LEAGUE_THRESHOLDS.get(league.lower(), self.LEAGUE_THRESHOLDS['default'])
        base_threshold = league_thresholds.get(category, 0.080)
        
        return self.time_adjust_threshold(base_threshold, hours_to_kickoff)
    
    def calculate_edge(self, model_prob: float, market_prob: float) -> float:
        """
        Calculate edge as percentage difference.
        
        Edge = (Model - Market) / Market
        
        Args:
            model_prob: Model probability
            market_prob: Market implied probability (1/odds)
            
        Returns:
            Edge as decimal (e.g., 0.15 = 15% edge)
        """
        if market_prob <= 0:
            return -1.0
        return (model_prob - market_prob) / market_prob
    
    def calculate_kelly(self, edge: float, odds: float, confidence: float = 1.0) -> float:
        """
        Calculate Kelly criterion stake fraction.
        
        Standard Kelly: f = edge / (odds - 1)
        Adjusted Kelly: f * confidence
        
        Args:
            edge: Edge as decimal
            odds: Decimal odds
            confidence: Confidence factor (0-1)
            
        Returns:
            Kelly fraction (capped at 0.05 = 5%)
        """
        if edge <= 0 or odds <= 1.0:
            return 0.0
        
        b = odds - 1  # Net odds received
        kelly = edge / b
        
        # Apply confidence adjustment
        kelly = kelly * confidence
        
        # Hard cap at 5%
        return min(kelly, 0.05)
    
    def calculate_composite_score(self, edge: float, kelly: float, 
                                   liquidity_score: float, confidence: float) -> float:
        """
        Calculate composite score for market ranking.
        
        Formula:
        composite = edge_score * 0.35 + kelly_score * 0.25 + 
                    liquidity_score * 0.20 + confidence_score * 0.20
        
        Args:
            edge: Edge as decimal
            kelly: Kelly fraction
            liquidity_score: Liquidity score (0-1)
            confidence: Confidence (0-1)
            
        Returns:
            Composite score (0-100)
        """
        # Normalize edge to 0-100 scale (cap at 30% edge = 100)
        edge_score = min(edge / 0.30, 1.0) * 100
        
        # Normalize Kelly to 0-100 scale (cap at 5% = 100)
        kelly_score = min(kelly / 0.05, 1.0) * 100
        
        # Liquidity and confidence already 0-100
        liquidity = liquidity_score * 100
        confidence_score = confidence * 100
        
        composite = (
            edge_score * 0.35 +
            kelly_score * 0.25 +
            liquidity * 0.20 +
            confidence_score * 0.20
        )
        
        return composite
    
    def evaluate_market(self, market: str, model_prob: float, odds: float,
                        bookmaker_count: int, data_quality: float,
                        league: str, hours_to_kickoff: float) -> MarketEvaluation:
        """
        Evaluate a single market.
        
        Args:
            market: Market name
            model_prob: Model probability
            odds: Decimal odds
            bookmaker_count: Number of bookmakers offering this market
            data_quality: Data quality score (0-1)
            league: League name
            hours_to_kickoff: Hours until kickoff
            
        Returns:
            MarketEvaluation object
        """
        market_prob = 1.0 / odds if odds > 0 else 0.0
        edge = self.calculate_edge(model_prob, market_prob)
        
        # Get threshold for this market
        threshold = self.get_threshold_for_market(market, league, hours_to_kickoff)
        
        # Check if edge meets threshold
        is_valid = edge >= threshold
        rejection_reason = None
        if not is_valid:
            rejection_reason = f"edge_too_low ({edge:.1%} < {threshold:.1%})"
        
        # Calculate Kelly
        kelly = self.calculate_kelly(edge, odds, confidence=data_quality)
        
        # Liquidity score based on bookmaker count
        liquidity_score = min(bookmaker_count / 5.0, 1.0)  # 5+ bookmakers = max liquidity
        
        # Composite score
        composite = self.calculate_composite_score(edge, kelly, liquidity_score, data_quality)
        
        return MarketEvaluation(
            market=market,
            selection=self.MARKET_NAMES.get(market, market),
            model_prob=model_prob,
            market_prob=market_prob,
            implied_odds=odds,
            edge=edge,
            kelly=kelly,
            confidence=data_quality,
            composite_score=composite,
            is_valid=is_valid,
            rejection_reason=rejection_reason
        )
    
    def select_best_market(self, market_probs: Dict[str, float], 
                           market_odds: Dict[str, float],
                           league: str,
                           hours_to_kickoff: float,
                           bookmaker_counts: Optional[Dict[str, int]] = None,
                           data_quality: float = 0.85) -> MarketSelectionResult:
        """
        Evaluate all available markets and select the best one.
        
        Args:
            market_probs: Dict mapping market names to model probabilities
            market_odds: Dict mapping market names to decimal odds
            league: League name
            hours_to_kickoff: Hours until match starts
            bookmaker_counts: Dict mapping market names to bookmaker count
            data_quality: Overall data quality score (0-1)
            
        Returns:
            MarketSelectionResult with best market and rejected alternatives
        """
        if bookmaker_counts is None:
            bookmaker_counts = {market: 3 for market in market_probs.keys()}
        
        evaluations = []
        rejected = []
        
        # Evaluate all markets
        for market, model_prob in market_probs.items():
            if market not in market_odds:
                continue
            
            odds = market_odds[market]
            bm_count = bookmaker_counts.get(market, 3)
            
            eval_result = self.evaluate_market(
                market=market,
                model_prob=model_prob,
                odds=odds,
                bookmaker_count=bm_count,
                data_quality=data_quality,
                league=league,
                hours_to_kickoff=hours_to_kickoff
            )
            
            evaluations.append(eval_result)
            
            if not eval_result.is_valid:
                rejected.append({
                    'market': market,
                    'edge': f"{eval_result.edge:.1%}",
                    'reason': eval_result.rejection_reason
                })
        
        # Sort by composite score (descending)
        evaluations.sort(key=lambda x: x.composite_score, reverse=True)
        
        # Find best valid market
        best_market = None
        for eval_result in evaluations:
            if eval_result.is_valid:
                best_market = eval_result
                break
        
        # If no valid market, return top market with rejection info
        if best_market is None:
            if evaluations:
                best_market = evaluations[0]
            else:
                raise ValueError("No markets to evaluate")
        
        # Add valid but lower-scoring markets to rejected
        for eval_result in evaluations:
            if eval_result.is_valid and eval_result.market != best_market.market:
                rejected.append({
                    'market': eval_result.market,
                    'edge': f"{eval_result.edge:.1%}",
                    'reason': 'lower_composite'
                })
        
        return MarketSelectionResult(
            market=best_market.market,
            selection=best_market.selection,
            model_prob=best_market.model_prob,
            market_prob=best_market.market_prob,
            implied_odds=best_market.implied_odds,
            edge=best_market.edge,
            kelly=best_market.kelly,
            confidence=best_market.confidence,
            composite_score=best_market.composite_score,
            rejected_markets=rejected
        )


# === UNIT TESTS ===
def test_market_selection_engine():
    """Unit tests for MarketSelectionEngine."""
    engine = MarketSelectionEngine()
    
    # Test time adjustment
    assert engine.time_adjust_threshold(0.08, 100) > 0.08  # Early = higher threshold
    assert engine.time_adjust_threshold(0.08, 12) < 0.08   # Sweet spot = lower threshold
    assert engine.time_adjust_threshold(0.08, 1) > 0.08    # Late = higher threshold
    
    # Test edge calculation
    edge = engine.calculate_edge(0.60, 0.50)  # Model 60%, market 50%
    assert abs(edge - 0.20) < 0.001  # 20% edge
    
    # Test Kelly
    kelly = engine.calculate_kelly(0.20, 2.0)  # 20% edge at odds 2.0
    assert kelly > 0
    assert kelly <= 0.05  # Capped at 5%
    
    # Test full selection
    market_probs = {
        'home_win': 0.55,
        'over_25': 0.65,
        'btts': 0.58
    }
    market_odds = {
        'home_win': 1.90,  # Implied 52.6%
        'over_25': 2.10,   # Implied 47.6%
        'btts': 1.80       # Implied 55.6%
    }
    
    result = engine.select_best_market(
        market_probs=market_probs,
        market_odds=market_odds,
        league='premier_league',
        hours_to_kickoff=24
    )
    
    assert result.market in market_probs
    assert result.edge > 0
    assert result.composite_score > 0
    
    print("All MarketSelectionEngine tests passed!")
    return True


if __name__ == "__main__":
    test_market_selection_engine()
