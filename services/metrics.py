"""
Prometheus metrics for SesomNod live Phase 0 and backtest tracking.
"""
import logging
from prometheus_client import Gauge, Counter

logger = logging.getLogger("sesomnod.metrics")

# Live Phase 0 metrics
LIVE_HIT_RATE = Gauge("sesomnod_hit_rate", "Live pick hit rate")
LIVE_CLV = Gauge("sesomnod_avg_clv_pct", "Live average CLV %")
LIVE_BRIER = Gauge("sesomnod_avg_brier", "Live average Brier score")
LIVE_PROFIT = Gauge("sesomnod_profit_units", "Live cumulative profit units")
LIVE_PICKS = Gauge("sesomnod_picks_settled", "Live settled picks count")
LIVE_DRAWDOWN = Gauge("sesomnod_max_drawdown_pct", "Live max drawdown %")

# Backtest metrics
BT_HIT_RATE = Gauge("sesomnod_backtest_hit_rate", "Backtest hit rate")
BT_ROI = Gauge("sesomnod_backtest_roi_pct", "Backtest ROI %")
BT_CLV = Gauge("sesomnod_backtest_avg_clv", "Backtest average CLV")
BT_PICKS = Gauge("sesomnod_backtest_qualified_picks", "Backtest qualified picks")
BT_BRIER = Gauge("sesomnod_backtest_avg_brier", "Backtest average Brier score")
BT_DRAWDOWN = Gauge("sesomnod_backtest_max_drawdown", "Backtest max drawdown %")

# Pick counters
PICKS_POSTED = Counter("sesomnod_picks_posted_total", "Total picks posted", ["tier"])


def update_live_metrics(
    hit_rate: float,
    avg_clv: float,
    avg_brier: float,
    profit_units: float,
    picks_settled: int,
    max_drawdown_pct: float,
) -> None:
    """Update live Phase 0 Prometheus gauges."""
    try:
        LIVE_HIT_RATE.set(hit_rate)
        LIVE_CLV.set(avg_clv)
        LIVE_BRIER.set(avg_brier)
        LIVE_PROFIT.set(profit_units)
        LIVE_PICKS.set(picks_settled)
        LIVE_DRAWDOWN.set(max_drawdown_pct)
    except Exception as e:
        logger.warning("Live metrics update failed: %s", e)


def update_backtest_metrics(summary) -> None:
    """Update backtest Prometheus gauges from BacktestSummary."""
    try:
        BT_HIT_RATE.set(summary.hit_rate)
        BT_ROI.set(summary.roi_pct)
        BT_CLV.set(summary.avg_clv)
        BT_PICKS.set(summary.qualified_picks)
        BT_BRIER.set(summary.avg_brier)
        BT_DRAWDOWN.set(summary.max_drawdown_pct)
    except Exception as e:
        logger.warning("Backtest metrics update failed: %s", e)
