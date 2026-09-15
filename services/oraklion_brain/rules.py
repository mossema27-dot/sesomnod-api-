"""
Utvalgsregel v1 og statusoverganger (brev §3.3, §5.2). Ren Python, ingen I/O.

Alle terskler kommer fra BrainConfig (speiler oraklion.brain_config).
Terskler senkes aldri i kode; endring = CONFIG_CHANGE-hendelse.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

SELECTION_RULE_VERSION = "v1"

# Kanoniske markedsnøkler (brev §4.1). Kun det kilden faktisk har er aktivt.
SNIPER_MARKET_TO_KEY = {
    "OVER_2_5": "OU|2.5|OVER",
}

SKIP_REASONS = (
    "P_BELOW_MIN", "ODDS_OUT_OF_BAND", "EV_BELOW_MIN", "MARKET_NOT_ALLOWED",
    "KICKOFF_WINDOW", "STALE_ODDS", "ODDS_TS_UNVERIFIED", "TIER_NOT_ALLOWED",
)

HOLD_REASONS = (
    "NO_CANDIDATES", "OPEN_DECISION_EXISTS", "SOURCE_STALE",
    "NO_MODEL_ROWS", "API_CAP", "ODDS_TS_UNVERIFIED",
)


@dataclass(frozen=True)
class BrainConfig:
    p_min: float = 0.55
    odds_min: float = 1.50
    odds_max: float = 2.60
    ev_min: float = 0.05
    min_minutes_to_kickoff: int = 60
    max_hours_to_kickoff: int = 48
    max_lock_age_min: int = 90
    flat_stake: int = 1000
    markets: tuple = ("OU|2.5|OVER",)
    candidate_tiers: tuple = ("PRIMARY",)
    source_sla_sec: int = 108000
    tick_interval_sec: int = 900
    settle_stable_min: int = 30
    methodology_version: str = "v2"
    selection_rule_version: str = SELECTION_RULE_VERSION
    settlement_rules_version: str = "v1"

    @classmethod
    def from_rows(cls, rows: dict) -> "BrainConfig":
        """rows: {key: value} fra oraklion.brain_config. Manglende nøkler → default."""
        g = rows.get
        def f(k, d):  # noqa: E306
            v = g(k)
            return float(v) if v is not None else d
        def i(k, d):  # noqa: E306
            v = g(k)
            return int(v) if v is not None else d
        def t(k, d):  # noqa: E306
            v = g(k)
            return tuple(x.strip() for x in v.split(",")) if v else d
        return cls(
            p_min=f("P_MIN", 0.55), odds_min=f("ODDS_MIN", 1.50), odds_max=f("ODDS_MAX", 2.60),
            ev_min=f("EV_MIN", 0.05), min_minutes_to_kickoff=i("MIN_MINUTES_TO_KICKOFF", 60),
            max_hours_to_kickoff=i("MAX_HOURS_TO_KICKOFF", 48), max_lock_age_min=i("MAX_LOCK_AGE_MIN", 90),
            flat_stake=i("FLAT_STAKE", 1000), markets=t("MARKETS", ("OU|2.5|OVER",)),
            candidate_tiers=t("CANDIDATE_TIERS", ("PRIMARY",)), source_sla_sec=i("SOURCE_SLA_SEC", 108000),
            tick_interval_sec=i("TICK_INTERVAL_SEC", 900), settle_stable_min=i("SETTLE_STABLE_MIN", 30),
            methodology_version=g("METHODOLOGY_VERSION") or "v2",
            selection_rule_version=g("SELECTION_RULE_VERSION") or SELECTION_RULE_VERSION,
            settlement_rules_version=g("SETTLEMENT_RULES_VERSION") or "v1",
        )


@dataclass
class Candidate:
    source_table: str
    source_id: int
    fixture_id: str
    league: str
    home: str
    away: str
    kickoff_utc: datetime
    market_key: str
    p_model: float
    odds: float
    odds_ts_utc: Optional[datetime]
    odds_ts_basis: str          # 'column' | 'row_created_verified' | 'unknown'
    tier: str = "PRIMARY"
    model_version: str = "sniper_live"
    skip_reason: Optional[str] = field(default=None)

    @property
    def ev(self) -> float:
        return self.p_model * self.odds - 1.0


def evaluate(c: Candidate, now: datetime, cfg: BrainConfig) -> Optional[str]:
    """Returnerer skip-grunn, eller None hvis kandidaten passerer porten."""
    if c.tier not in cfg.candidate_tiers:
        return "TIER_NOT_ALLOWED"
    if c.market_key not in cfg.markets:
        return "MARKET_NOT_ALLOWED"
    if c.odds_ts_basis not in ("column", "row_created_verified") or c.odds_ts_utc is None:
        return "ODDS_TS_UNVERIFIED"
    minutes_to_ko = (c.kickoff_utc - now).total_seconds() / 60.0
    if minutes_to_ko < cfg.min_minutes_to_kickoff or minutes_to_ko > cfg.max_hours_to_kickoff * 60:
        return "KICKOFF_WINDOW"
    lock_age_min = (now - c.odds_ts_utc).total_seconds() / 60.0
    if lock_age_min > cfg.max_lock_age_min:
        return "STALE_ODDS"
    if c.p_model < cfg.p_min:
        return "P_BELOW_MIN"
    if c.odds < cfg.odds_min or c.odds > cfg.odds_max:
        return "ODDS_OUT_OF_BAND"
    if c.ev < cfg.ev_min:
        return "EV_BELOW_MIN"
    return None


@dataclass
class Selection:
    chosen: Optional[Candidate]
    scanned_n: int
    passed_n: int
    skip_counts: dict


def select(cands: list[Candidate], now: datetime, cfg: BrainConfig) -> Selection:
    """Rang: EV synkende, tie-break høyest p, deretter tidligst kickoff."""
    passed: list[Candidate] = []
    skip_counts: dict = {}
    for c in cands:
        r = evaluate(c, now, cfg)
        c.skip_reason = r
        if r is None:
            passed.append(c)
        else:
            skip_counts[r] = skip_counts.get(r, 0) + 1
    passed.sort(key=lambda x: (-x.ev, -x.p_model, x.kickoff_utc))
    return Selection(chosen=passed[0] if passed else None, scanned_n=len(cands),
                     passed_n=len(passed), skip_counts=skip_counts)


def lock_age_minutes(now: datetime, odds_ts_utc: datetime) -> int:
    return int((now - odds_ts_utc).total_seconds() // 60)


# ── Statusoverganger (brev §5.2). Ingen andre er lovlige. ──
ALLOWED_TRANSITIONS = frozenset({
    ("OPEN", "SETTLED"),
    ("OPEN", "VOID"),
    ("OPEN", "SUSPENDED"),
    ("SUSPENDED", "SETTLED"),
    ("SUSPENDED", "VOID"),
})


class IllegalTransition(Exception):
    pass


def check_transition(old: str, new: str) -> None:
    if (old, new) not in ALLOWED_TRANSITIONS:
        raise IllegalTransition(f"{old} -> {new} is not allowed")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def void_deadline(original_kickoff: datetime) -> datetime:
    """PST: ikke fullført innen 48 t + 3 t fra opprinnelig avspark → VOID."""
    return original_kickoff + timedelta(hours=51)
