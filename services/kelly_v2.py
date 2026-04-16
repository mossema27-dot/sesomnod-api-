"""
SesomNod Kelly V2
Kelly criterion with confidence, volatility, and correlation adjustments.
"""

from typing import Optional, List
from dataclasses import dataclass


@dataclass
class KellyResult:
    """Result from Kelly calculation."""
    kelly_fraction: float  # 0-0.05 (capped at 5%)
    raw_kelly: float  # Uncapped Kelly
    confidence_adjusted: float  # After confidence adjustment
    volatility_adjusted: float  # After volatility adjustment
    correlation_adjusted: float  # After correlation adjustment
    drawdown_adjusted: float  # After drawdown adjustment
    final_stake_pct: float  # Final recommended stake


class KellyV2:
    """
    Kelly Criterion V2 with multiple risk adjustments.
    
    Formula progression:
    1. Standard Kelly: f = edge / (odds - 1)
    2. Confidence adjustment: kelly *= confidence
    3. Volatility adjustment: kelly *= (1 - volatility * 0.5)
    4. Correlation penalty: kelly *= (1 - correlation_penalty)
    5. Drawdown adjustment: kelly *= 0.5 if drawdown > 10%
    6. Hard cap: min(kelly, 0.05)
    """
    
    # Constants
    MAX_KELLY = 0.05  # Never more than 5% of bankroll
    FRACTIONAL_KELLY = 1.0  # Use full Kelly (adjust if needed)
    DRAWDOWN_THRESHOLD = 0.10  # 10% drawdown triggers reduction
    DRAWDOWN_REDUCTION = 0.50  # Reduce to 50% during drawdown
    
    def __init__(self, current_drawdown: float = 0.0):
        """
        Initialize Kelly V2 calculator.
        
        Args:
            current_drawdown: Current drawdown percentage (0-1)
        """
        self.current_drawdown = current_drawdown
    
    def calculate(self,
                  edge: float,
                  odds: float,
                  confidence: float = 1.0,
                  volatility: float = 0.0,
                  correlation_penalty: float = 0.0) -> KellyResult:
        """
        Calculate Kelly stake with all adjustments.
        
        Args:
            edge: Edge as decimal (e.g., 0.15 = 15% edge)
            odds: Decimal odds (e.g., 2.10)
            confidence: Confidence in edge estimate (0-1)
            volatility: Recent volatility measure (0-1)
            correlation_penalty: Portfolio correlation penalty (0-1)
            
        Returns:
            KellyResult with all intermediate values
        """
        # Validate inputs
        if edge <= 0:
            return KellyResult(
                kelly_fraction=0.0,
                raw_kelly=0.0,
                confidence_adjusted=0.0,
                volatility_adjusted=0.0,
                correlation_adjusted=0.0,
                drawdown_adjusted=0.0,
                final_stake_pct=0.0
            )
        
        if odds <= 1.0:
            return KellyResult(
                kelly_fraction=0.0,
                raw_kelly=0.0,
                confidence_adjusted=0.0,
                volatility_adjusted=0.0,
                correlation_adjusted=0.0,
                drawdown_adjusted=0.0,
                final_stake_pct=0.0
            )
        
        # Step 1: Standard Kelly
        # f = edge / (odds - 1)
        b = odds - 1  # Net odds
        raw_kelly = edge / b
        
        # Step 2: Confidence adjustment
        # Reduce Kelly if we're not confident in our edge estimate
        confidence_adjusted = raw_kelly * confidence
        
        # Step 3: Volatility adjustment
        # Reduce Kelly during high volatility periods
        volatility_factor = max(0.0, 1.0 - volatility * 0.5)
        volatility_adjusted = confidence_adjusted * volatility_factor
        
        # Step 4: Correlation penalty
        # Reduce Kelly if this pick correlates with existing positions
        correlation_factor = max(0.0, 1.0 - correlation_penalty)
        correlation_adjusted = volatility_adjusted * correlation_factor
        
        # Step 5: Drawdown adjustment
        # Reduce Kelly if we're in a drawdown
        if self.current_drawdown > self.DRAWDOWN_THRESHOLD:
            drawdown_adjusted = correlation_adjusted * self.DRAWDOWN_REDUCTION
        else:
            drawdown_adjusted = correlation_adjusted
        
        # Step 6: Hard cap at 5%
        kelly_fraction = min(drawdown_adjusted, self.MAX_KELLY)
        
        # Final stake percentage
        final_stake_pct = kelly_fraction * self.FRACTIONAL_KELLY
        
        return KellyResult(
            kelly_fraction=kelly_fraction,
            raw_kelly=raw_kelly,
            confidence_adjusted=confidence_adjusted,
            volatility_adjusted=volatility_adjusted,
            correlation_adjusted=correlation_adjusted,
            drawdown_adjusted=drawdown_adjusted,
            final_stake_pct=final_stake_pct
        )
    
    def calculate_simple(self, edge: float, odds: float) -> float:
        """
        Simple Kelly calculation without adjustments.
        
        Args:
            edge: Edge as decimal
            odds: Decimal odds
            
        Returns:
            Kelly fraction (capped at 5%)
        """
        result = self.calculate(edge=edge, odds=odds)
        return result.kelly_fraction
    
    def calculate_unit_stake(self, 
                             edge: float, 
                             odds: float,
                             bankroll: float,
                             unit_size: float = 100.0) -> float:
        """
        Calculate stake in currency units.
        
        Args:
            edge: Edge as decimal
            odds: Decimal odds
            bankroll: Total bankroll
            unit_size: Base unit size for staking
            
        Returns:
            Recommended stake in currency
        """
        kelly = self.calculate_simple(edge, odds)
        return kelly * bankroll
    
    def calculate_level_stakes(self, 
                               edge: float,
                               odds: float,
                               confidence: float = 1.0) -> int:
        """
        Calculate stake in level units (1-5 units).
        
        Args:
            edge: Edge as decimal
            odds: Decimal odds
            confidence: Confidence in the pick
            
        Returns:
            Number of units to stake (1-5)
        """
        result = self.calculate(edge=edge, odds=odds, confidence=confidence)
        
        # Map Kelly fraction to unit stakes
        kelly = result.kelly_fraction
        
        if kelly >= 0.04:
            return 5
        elif kelly >= 0.03:
            return 4
        elif kelly >= 0.02:
            return 3
        elif kelly >= 0.01:
            return 2
        elif kelly > 0:
            return 1
        else:
            return 0
    
    def update_drawdown(self, pnl_history: List[float]) -> float:
        """
        Update current drawdown based on P&L history.
        
        Args:
            pnl_history: List of daily P&L values
            
        Returns:
            Current drawdown percentage
        """
        if not pnl_history:
            self.current_drawdown = 0.0
            return 0.0
        
        # Calculate running maximum
        running_max = pnl_history[0]
        max_drawdown = 0.0
        
        for pnl in pnl_history:
            if pnl > running_max:
                running_max = pnl
            
            drawdown = (running_max - pnl) / running_max if running_max > 0 else 0
            max_drawdown = max(max_drawdown, drawdown)
        
        self.current_drawdown = max_drawdown
        return max_drawdown
    
    def get_stake_recommendation(self, 
                                  edge: float,
                                  odds: float,
                                  bankroll: float,
                                  confidence: float = 1.0,
                                  volatility: float = 0.0,
                                  correlation_penalty: float = 0.0) -> dict:
        """
        Get complete stake recommendation.
        
        Args:
            edge: Edge as decimal
            odds: Decimal odds
            bankroll: Total bankroll
            confidence: Confidence (0-1)
            volatility: Volatility measure (0-1)
            correlation_penalty: Correlation penalty (0-1)
            
        Returns:
            Dict with all stake information
        """
        result = self.calculate(
            edge=edge,
            odds=odds,
            confidence=confidence,
            volatility=volatility,
            correlation_penalty=correlation_penalty
        )
        
        stake_amount = result.final_stake_pct * bankroll
        units = self.calculate_level_stakes(edge, odds, confidence)
        
        return {
            "kelly_fraction": result.kelly_fraction,
            "raw_kelly": result.raw_kelly,
            "stake_percentage": result.final_stake_pct * 100,
            "stake_amount": stake_amount,
            "units": units,
            "confidence_adjustment": confidence,
            "volatility_adjustment": 1.0 - volatility * 0.5,
            "correlation_adjustment": 1.0 - correlation_penalty,
            "drawdown_adjustment": self.DRAWDOWN_REDUCTION if self.current_drawdown > self.DRAWDOWN_THRESHOLD else 1.0,
            "recommendation": "BET" if units >= 1 else "NO BET"
        }


# === UNIT TESTS ===
def test_kelly_v2():
    """Unit tests for KellyV2."""
    
    # Test 1: Basic Kelly
    kelly = KellyV2()
    result = kelly.calculate(edge=0.20, odds=2.0)
    # Edge 20%, odds 2.0 -> Kelly = 0.20 / 1.0 = 20% -> capped at 5%
    assert result.kelly_fraction == 0.05
    assert result.raw_kelly == 0.20
    print("Test 1 (basic Kelly) passed")
    
    # Test 2: Negative edge
    result = kelly.calculate(edge=-0.05, odds=2.0)
    assert result.kelly_fraction == 0.0
    print("Test 2 (negative edge) passed")
    
    # Test 3: Confidence adjustment
    result = kelly.calculate(edge=0.20, odds=2.0, confidence=0.5)
    # Raw Kelly 20%, confidence 50% -> 10% -> capped at 5%
    assert result.confidence_adjusted == 0.10
    print("Test 3 (confidence adjustment) passed")
    
    # Test 4: Volatility adjustment
    result = kelly.calculate(edge=0.20, odds=2.0, volatility=0.5)
    # Volatility factor = 1 - 0.5*0.5 = 0.75
    assert result.volatility_adjusted == 0.15 * 0.75  # 0.15 after confidence
    print("Test 4 (volatility adjustment) passed")
    
    # Test 5: Correlation penalty
    result = kelly.calculate(edge=0.20, odds=2.0, correlation_penalty=0.3)
    # Correlation factor = 1 - 0.3 = 0.7
    assert result.correlation_adjusted < result.volatility_adjusted
    print("Test 5 (correlation penalty) passed")
    
    # Test 6: Drawdown adjustment
    kelly_dd = KellyV2(current_drawdown=0.15)  # 15% drawdown
    result = kelly_dd.calculate(edge=0.20, odds=2.0)
    # Should be reduced by 50%
    assert result.drawdown_adjusted == result.correlation_adjusted * 0.5
    print("Test 6 (drawdown adjustment) passed")
    
    # Test 7: Level stakes
    units = kelly.calculate_level_stakes(edge=0.15, odds=2.0)
    assert units >= 1
    print("Test 7 (level stakes) passed")
    
    # Test 8: Full recommendation
    rec = kelly.get_stake_recommendation(
        edge=0.15,
        odds=2.0,
        bankroll=10000,
        confidence=0.9,
        volatility=0.2,
        correlation_penalty=0.1
    )
    assert "stake_amount" in rec
    assert "units" in rec
    assert rec["recommendation"] == "BET"
    print("Test 8 (full recommendation) passed")
    
    print("\nAll KellyV2 tests passed!")
    return True


if __name__ == "__main__":
    test_kelly_v2()
