"""
SesomNod Market Extractor
Extracts all betting markets from Poisson P(i,j) matrix.
Vectorized with NumPy for performance.
"""

import numpy as np
from typing import Dict, Tuple, List
from dataclasses import dataclass


@dataclass
class MarketProbabilities:
    """Container for all market probabilities extracted from Poisson matrix."""
    # Goals markets
    P_over_05: float
    P_over_15: float
    P_over_25: float
    P_over_35: float
    P_over_45: float
    P_under_05: float
    P_under_15: float
    P_under_25: float
    P_under_35: float
    
    # Team scoring markets
    P_btts: float
    P_home_scores: float
    P_away_scores: float
    P_home_cs: float
    P_away_cs: float
    
    # 1X2 markets
    P_home_win: float
    P_draw: float
    P_away_win: float
    
    # Asian Handicap markets
    P_ah_home_minus_05: float
    P_ah_home_minus_15: float
    P_ah_away_plus_05: float
    P_ah_away_plus_15: float
    
    # Exact scores (top 6)
    top_scores: Dict[str, float]


class MarketExtractor:
    """
    Extracts all betting market probabilities from a Poisson probability matrix.
    
    The Poisson matrix P[i][j] represents the probability of home team scoring i goals
    and away team scoring j goals.
    """
    
    def __init__(self, max_goals: int = 10):
        """
        Initialize the MarketExtractor.
        
        Args:
            max_goals: Maximum number of goals to consider in the matrix (default 10)
        """
        self.max_goals = max_goals
    
    def compute_poisson_matrix(self, lambda_home: float, lambda_away: float) -> np.ndarray:
        """
        Compute the Poisson probability matrix P[i][j].
        
        P(i,j) = Poisson(i, lambda_home) * Poisson(j, lambda_away)
        
        Args:
            lambda_home: Expected goals for home team
            lambda_away: Expected goals for away team
            
        Returns:
            2D numpy array where P[i][j] is probability of home=i, away=j
        """
        # Create goal grids
        home_goals = np.arange(self.max_goals + 1)
        away_goals = np.arange(self.max_goals + 1)
        
        # Compute Poisson probabilities for each team
        import math
        home_probs = np.array([np.exp(-lambda_home) * (lambda_home**i) / math.factorial(i) for i in home_goals])
        away_probs = np.array([np.exp(-lambda_away) * (lambda_away**j) / math.factorial(j) for j in away_goals])
        
        # Create joint probability matrix (outer product)
        P = np.outer(home_probs, away_probs)
        
        # Normalize to ensure sum = 1 (numerical safety)
        P = P / P.sum()
        
        return P
    
    def extract_all_markets(self, lambda_home: float, lambda_away: float) -> MarketProbabilities:
        """
        Extract all market probabilities from Poisson matrix.
        
        Args:
            lambda_home: Expected goals for home team
            lambda_away: Expected goals for away team
            
        Returns:
            MarketProbabilities object with all market probabilities
        """
        P = self.compute_poisson_matrix(lambda_home, lambda_away)
        
        # Create coordinate grids
        home_grid = np.arange(self.max_goals + 1).reshape(-1, 1)
        away_grid = np.arange(self.max_goals + 1).reshape(1, -1)
        
        total_goals = home_grid + away_grid
        goal_diff = home_grid - away_grid
        
        # === GOALS MARKETS ===
        P_over_05 = P[total_goals >= 1].sum()
        P_over_15 = P[total_goals >= 2].sum()
        P_over_25 = P[total_goals >= 3].sum()
        P_over_35 = P[total_goals >= 4].sum()
        P_over_45 = P[total_goals >= 5].sum()
        
        P_under_05 = P[total_goals == 0].sum()
        P_under_15 = P[total_goals <= 1].sum()
        P_under_25 = P[total_goals <= 2].sum()
        P_under_35 = P[total_goals <= 3].sum()
        
        # === TEAM SCORING MARKETS ===
        P_home_scores = P[1:, :].sum()         # home scores >= 1
        P_away_scores = P[:, 1:].sum()         # away scores >= 1
        P_btts = P[1:, 1:].sum()               # both score >= 1

        P_home_cs = P[:, 0].sum()              # away scores 0 = home clean sheet
        P_away_cs = P[0, :].sum()              # home scores 0 = away clean sheet
        
        # === 1X2 MARKETS ===
        P_home_win = P[goal_diff > 0].sum()
        P_draw = P[goal_diff == 0].sum()
        P_away_win = P[goal_diff < 0].sum()
        
        # === ASIAN HANDICAP MARKETS ===
        P_ah_home_minus_05 = P[goal_diff > 0].sum()  # Same as home win
        P_ah_home_minus_15 = P[goal_diff > 1].sum()  # Win by 2+
        P_ah_away_plus_05 = P[goal_diff <= 0].sum()  # Not home win
        P_ah_away_plus_15 = P[goal_diff <= 1].sum()  # Not home win by 2+
        
        # === EXACT SCORES (top 6) ===
        flat_indices = np.argsort(P.flatten())[::-1][:6]
        top_scores = {}
        for idx in flat_indices:
            i = idx // (self.max_goals + 1)
            j = idx % (self.max_goals + 1)
            score_key = f"{i}-{j}"
            top_scores[score_key] = float(P[i, j])
        
        return MarketProbabilities(
            P_over_05=float(P_over_05),
            P_over_15=float(P_over_15),
            P_over_25=float(P_over_25),
            P_over_35=float(P_over_35),
            P_over_45=float(P_over_45),
            P_under_05=float(P_under_05),
            P_under_15=float(P_under_15),
            P_under_25=float(P_under_25),
            P_under_35=float(P_under_35),
            P_btts=float(P_btts),
            P_home_scores=float(P_home_scores),
            P_away_scores=float(P_away_scores),
            P_home_cs=float(P_home_cs),
            P_away_cs=float(P_away_cs),
            P_home_win=float(P_home_win),
            P_draw=float(P_draw),
            P_away_win=float(P_away_win),
            P_ah_home_minus_05=float(P_ah_home_minus_05),
            P_ah_home_minus_15=float(P_ah_home_minus_15),
            P_ah_away_plus_05=float(P_ah_away_plus_05),
            P_ah_away_plus_15=float(P_ah_away_plus_15),
            top_scores=top_scores
        )
    
    def get_market_by_name(self, markets: MarketProbabilities, market_name: str) -> float:
        """
        Get a specific market probability by name.
        
        Args:
            markets: MarketProbabilities object
            market_name: Name of the market (e.g., 'over_25', 'btts', 'home_win')
            
        Returns:
            Probability value for the specified market
        """
        market_map = {
            'over_05': markets.P_over_05,
            'over_15': markets.P_over_15,
            'over_25': markets.P_over_25,
            'over_35': markets.P_over_35,
            'over_45': markets.P_over_45,
            'under_05': markets.P_under_05,
            'under_15': markets.P_under_15,
            'under_25': markets.P_under_25,
            'under_35': markets.P_under_35,
            'btts': markets.P_btts,
            'home_scores': markets.P_home_scores,
            'away_scores': markets.P_away_scores,
            'home_cs': markets.P_home_cs,
            'away_cs': markets.P_away_cs,
            'home_win': markets.P_home_win,
            'draw': markets.P_draw,
            'away_win': markets.P_away_win,
            'ah_home_minus_05': markets.P_ah_home_minus_05,
            'ah_home_minus_15': markets.P_ah_home_minus_15,
            'ah_away_plus_05': markets.P_ah_away_plus_05,
            'ah_away_plus_15': markets.P_ah_away_plus_15,
        }
        
        if market_name not in market_map:
            raise ValueError(f"Unknown market: {market_name}. Available: {list(market_map.keys())}")
        
        return market_map[market_name]


# === UNIT TESTS ===
def test_market_extractor():
    """Unit tests for MarketExtractor with known values."""
    extractor = MarketExtractor(max_goals=10)
    
    # Test case 1: Equal teams, lambda = 1.5 each
    # Expected total goals = 3.0, so over 2.5 should be ~50%
    markets = extractor.extract_all_markets(lambda_home=1.5, lambda_away=1.5)
    
    # Probabilities should sum appropriately
    assert 0.99 < markets.P_home_win + markets.P_draw + markets.P_away_win < 1.01, "1X2 should sum to ~1"
    assert 0.99 < markets.P_over_25 + markets.P_under_25 < 1.01, "Over/Under 2.5 should sum to ~1"
    # BTTS must be <= min(home_scores, away_scores)
    assert markets.P_btts <= min(markets.P_home_scores, markets.P_away_scores) + 0.01, "BTTS should be <= min(home_scores, away_scores)"
    
    # Test case 2: High scoring game
    markets_high = extractor.extract_all_markets(lambda_home=2.5, lambda_away=2.0)
    assert markets_high.P_over_25 > markets.P_over_25, "Higher lambda should increase over 2.5 prob"
    
    # Test case 3: Low scoring game
    markets_low = extractor.extract_all_markets(lambda_home=0.8, lambda_away=0.7)
    assert markets_low.P_under_25 > markets.P_under_25, "Lower lambda should increase under 2.5 prob"
    
    # Test case 4: Known Poisson values
    # P(0) for lambda=1.0 = e^-1 = 0.368
    markets_unit = extractor.extract_all_markets(lambda_home=1.0, lambda_away=1.0)
    # Both teams score 0 with prob 0.368 * 0.368 = 0.135
    expected_under_05 = 0.135
    assert abs(markets_unit.P_under_05 - expected_under_05) < 0.01, "P(0-0) should be ~0.135"
    
    print("All unit tests passed!")
    return True


if __name__ == "__main__":
    test_market_extractor()
    
    # Performance test
    import time
    extractor = MarketExtractor()
    start = time.time()
    for _ in range(1000):
        extractor.extract_all_markets(lambda_home=1.8, lambda_away=1.4)
    elapsed = time.time() - start
    print(f"Performance: {elapsed/1000*1000:.2f}ms per extraction")
