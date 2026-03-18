from decimal import (
    Decimal, ROUND_HALF_DOWN,
    InvalidOperation,
)
from typing import Optional
from dataclasses import dataclass


@dataclass
class KellyResult:
    stake_units: Decimal
    fraction: Decimal
    tier: str
    calculation_trace: str
    error: Optional[str] = None


class KellyEngine:
    """
    Kelly Criterion med Decimal.
    Ingen float-drift.
    PostgreSQL DECIMAL(6,2) kompatibel.
    """
    MAX_STAKE = Decimal("5.00")
    FRACTIONAL = Decimal("0.25")
    PRECISION = Decimal("0.01")
    MIN_ODDS = Decimal("1.01")
    MIN_EDGE = Decimal("0.1")

    TIER_MULTIPLIERS = {
        "ATOMIC": Decimal("1.0"),
        "EDGE": Decimal("0.5"),
        "MONITORED": Decimal("0.0"),
    }

    def calculate(
        self,
        edge_pct,
        odds,
        tier: str,
    ) -> KellyResult:
        try:
            edge = Decimal(str(edge_pct))
            o = Decimal(str(odds))
        except InvalidOperation as e:
            return KellyResult(
                Decimal("0.00"),
                Decimal("0.0000"),
                tier,
                "Invalid input",
                str(e),
            )

        # Zero-division guard
        if o <= self.MIN_ODDS:
            return KellyResult(
                Decimal("0.00"),
                Decimal("0.0000"),
                tier,
                f"Odds too low: {o}",
                "Odds <= 1.01",
            )

        if edge <= self.MIN_EDGE:
            return KellyResult(
                Decimal("0.00"),
                Decimal("0.0000"),
                tier,
                f"Edge too low: {edge}",
                None,
            )

        # Kelly formula med Decimal
        b = o - Decimal("1")
        p = (edge / Decimal("100")) + (Decimal("1") / o)
        q = Decimal("1") - p

        if b <= Decimal("0"):
            return KellyResult(
                Decimal("0.00"),
                Decimal("0.0000"),
                tier,
                "b <= 0",
                None,
            )

        kelly = (b * p - q) / b
        fractional = (kelly * self.FRACTIONAL).quantize(
            Decimal("0.0001"),
            rounding=ROUND_HALF_DOWN,
        )

        multiplier = self.TIER_MULTIPLIERS.get(tier, Decimal("0"))
        raw_stake = fractional * multiplier * Decimal("100")

        stake = min(raw_stake, self.MAX_STAKE).quantize(
            self.PRECISION,
            rounding=ROUND_HALF_DOWN,
        )

        if stake < Decimal("0"):
            stake = Decimal("0.00")

        trace = (
            f"edge={edge}% odds={o} "
            f"b={b} p={p:.4f} q={q:.4f} "
            f"kelly={kelly:.4f} "
            f"frac={fractional} "
            f"mult={multiplier} "
            f"stake={stake}u"
        )

        return KellyResult(
            stake_units=stake,
            fraction=fractional,
            tier=tier,
            calculation_trace=trace,
        )


# Singleton
kelly_engine = KellyEngine()
