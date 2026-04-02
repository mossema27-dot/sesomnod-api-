"""
Kelly Criterion stake calculator for value betting.
Uses Half-Kelly (50% of raw Kelly) for safety, capped at 25% of bankroll.
"""
from dataclasses import dataclass


@dataclass
class KellyResult:
    """Result from Kelly Criterion calculation."""
    raw_kelly_fraction: float
    half_kelly_fraction: float
    recommended_stake_pct: float   # as percentage e.g. 3.1
    recommended_stake_units: float  # assuming given bankroll
    edge_pct: float
    is_value_bet: bool
    kelly_tier: str  # "STRONG", "MODERATE", "WEAK", "NO_VALUE"

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "raw_kelly_fraction": self.raw_kelly_fraction,
            "half_kelly_fraction": self.half_kelly_fraction,
            "recommended_stake_pct": self.recommended_stake_pct,
            "recommended_stake_units": self.recommended_stake_units,
            "edge_pct": self.edge_pct,
            "is_value_bet": self.is_value_bet,
            "kelly_tier": self.kelly_tier,
        }


def calculate_kelly(
    model_prob: float,
    decimal_odds: float,
    bankroll: float = 100.0,
) -> KellyResult:
    """
    Calculate Kelly Criterion stake size.
    Uses Half-Kelly for safety (50% of raw Kelly).
    Caps at 25% of bankroll maximum.

    Args:
        model_prob: Our model's probability (0-1)
        decimal_odds: Bookmaker decimal odds (e.g. 2.10)
        bankroll: Total bankroll in units (default 100)

    Returns:
        KellyResult with stake recommendation
    """
    # Edge calculation
    edge_pct = (model_prob * decimal_odds - 1) * 100

    # Raw Kelly formula: f* = (bp - q) / b
    b = decimal_odds - 1  # net odds
    if b <= 0:
        return KellyResult(
            raw_kelly_fraction=0.0,
            half_kelly_fraction=0.0,
            recommended_stake_pct=0.0,
            recommended_stake_units=0.0,
            edge_pct=round(edge_pct, 2),
            is_value_bet=False,
            kelly_tier="NO_VALUE",
        )

    raw_kelly = (model_prob * b - (1 - model_prob)) / b

    # No value if Kelly is negative
    if raw_kelly <= 0:
        return KellyResult(
            raw_kelly_fraction=round(raw_kelly, 4),
            half_kelly_fraction=0.0,
            recommended_stake_pct=0.0,
            recommended_stake_units=0.0,
            edge_pct=round(edge_pct, 2),
            is_value_bet=False,
            kelly_tier="NO_VALUE",
        )

    # Cap at 25%
    capped_kelly = min(raw_kelly, 0.25)

    # Half-Kelly for safety
    half_kelly = capped_kelly * 0.5
    stake_pct = half_kelly * 100
    stake_units = half_kelly * bankroll

    # Tier classification
    if edge_pct >= 8.0 and stake_pct >= 3.0:
        tier = "STRONG"
    elif edge_pct >= 5.0:
        tier = "MODERATE"
    else:
        tier = "WEAK"

    return KellyResult(
        raw_kelly_fraction=round(raw_kelly, 4),
        half_kelly_fraction=round(half_kelly, 4),
        recommended_stake_pct=round(stake_pct, 2),
        recommended_stake_units=round(stake_units, 2),
        edge_pct=round(edge_pct, 2),
        is_value_bet=True,
        kelly_tier=tier,
    )
