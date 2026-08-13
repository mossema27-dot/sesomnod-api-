"""
Oraklion Mentor — safety-gated Q&A over Sesomnod-aggregater.

Pipeline (BRIEF 2026-08-03):
  1. RateLimiter — sesjonssjekk mot oraklion.rate_limiter
  2. SafetyGate — blokkliste + PII-detektor FØR modellkall
  3. FactRetriever — henter offentlige aggregater fra /public/oraklion/*-kildene
  4. Anthropic Claude Sonnet 5 via v1/messages (httpx, ingen SDK-dep)
  5. NumericGuard — refuser umulige tall (odds > 20, ROI > 100 % osv.)
  6. PhraseGuard — refuser forbudte fraser ("guaranteed win", "sure bet", …)
  7. AuditLog — INSERT til oraklion.mentor_log med PII-maskering

Miljøkrav:
  ANTHROPIC_API_KEY  — Anthropic-nøkkel (Railway env)
  Postgres-tabellene fra services/oraklion_mentor_ddl.sql må eksistere.

Skal aldri hoste eller eksponere:
  - Personopplysninger fra bruker-input (maskeres i log)
  - Kamp-id, lagnavn eller uavgjort-utfall
  - Interne edge-terskler utover det som allerede publiseres via /public/oraklion/*
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("sesomnod.oraklion_mentor")

# ─────────────────────────────────────────────────────────────────────
# KONFIG
# ─────────────────────────────────────────────────────────────────────
ANTHROPIC_MODEL = "claude-sonnet-5"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
MAX_TOKENS = 800
REQUEST_TIMEOUT_SEC = 20.0

RATE_LIMIT_PER_MINUTE = 6
RATE_LIMIT_PER_DAY = 60

MENTOR_SYSTEM_PROMPT = """\
You are Oraklion Mentor — Sesomnod's public assistant.

You explain how the Sesomnod prediction system works, what its published
aggregates mean, and how to interpret CLV, ROI and calibration. You are
not a tipster and you never issue a signal.

RULES YOU MUST FOLLOW WITHOUT EXCEPTION:
- Never name a team, league or fixture. Never suggest a bet on a specific
  match, outcome or market.
- Never promise or imply a positive result. Never use the words
  "guaranteed", "sure", "cannot lose", or equivalents.
- Never invent numbers. If a metric is not in the FACTS block below,
  answer with "not measured yet" and stop.
- Always mention that gambling is risky and reference the helpline
  800 800 40 (Norway) when the user asks anything transactional.
- Keep answers under 180 words. Plain prose. No lists longer than 4 items.

You are permitted to explain: the four-engine convergence gate, the
calibration window, the CLV methodology, evidence chain guarantees,
and Sesomnod's discipline rules (three signals per day, eight lives).
"""

# ─────────────────────────────────────────────────────────────────────
# SafetyGate — input-side
# ─────────────────────────────────────────────────────────────────────
BLOCKED_INPUT_PATTERNS = [
    r"\b(?:guaranteed|sure)\s+win\b",
    r"\bfixed\s+match\b",
    r"\binside\s+info\b",
    r"\bjailbreak\b",
    r"ignore\s+(?:the\s+)?previous\s+(?:instructions?|rules?)",
]

PII_PATTERNS = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "phone": re.compile(r"\b(?:\+?\d[\d\s\-]{6,}\d)\b"),
    "credit_card": re.compile(r"\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b"),
    "iban": re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b"),
}


@dataclass
class GateResult:
    passed: bool
    reason: str | None
    detail: str | None


class SafetyGate:
    @staticmethod
    def check(user_input: str) -> GateResult:
        if not user_input or not user_input.strip():
            return GateResult(False, "empty_input", "Melding er tom.")
        if len(user_input) > 4000:
            return GateResult(False, "too_long", "Melding over 4000 tegn.")
        lowered = user_input.lower()
        for pat in BLOCKED_INPUT_PATTERNS:
            if re.search(pat, lowered):
                return GateResult(False, "blocked_phrase", pat)
        return GateResult(True, None, None)


def mask_pii(text: str) -> str:
    """Erstatter PII med [REDACTED-<type>] før tekst skrives til logg."""
    if not text:
        return text
    out = text
    for kind, pat in PII_PATTERNS.items():
        out = pat.sub(f"[REDACTED-{kind}]", out)
    return out


# ─────────────────────────────────────────────────────────────────────
# FactRetriever — henter offentlige aggregater fra egen DB
# ─────────────────────────────────────────────────────────────────────
class FactRetriever:
    @staticmethod
    async def fetch(pool) -> dict[str, Any]:
        facts: dict[str, Any] = {
            "engine_paused": None,
            "settled_30d": 0,
            "median_clv_pct": None,
            "positive_clv_ratio": None,
            "roi_pct": None,
            "hit_rate_pct": None,
            "calibration_day": None,
            "calibration_target": 30,
        }
        if pool is None:
            return facts
        try:
            async with pool.acquire() as conn:
                paused_row = await conn.fetchrow(
                    "SELECT value FROM system_state WHERE key = 'sniper_pick_gen_paused';"
                )
                facts["engine_paused"] = bool(paused_row) and (
                    paused_row["value"] or ""
                ).lower() == "true"

                eye = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE result IN ('WIN','LOSS'))              AS settled,
                        percentile_cont(0.5) WITHIN GROUP (ORDER BY clv_close_pct)
                            FILTER (WHERE clv_close_pct IS NOT NULL)                  AS median_clv,
                        COUNT(*) FILTER (WHERE is_positive_clv_close IS TRUE)         AS pos_clv,
                        COUNT(*) FILTER (WHERE clv_close_pct IS NOT NULL)             AS n_clv,
                        SUM(profit_units) FILTER (WHERE result IN ('WIN','LOSS'))     AS profit,
                        COUNT(*) FILTER (WHERE result = 'WIN')                        AS wins
                    FROM sniper_bets_v1
                    WHERE market_tier = 'PRIMARY'
                      AND settled_at >= NOW() - INTERVAL '30 days';
                    """
                )
                settled = int(eye["settled"] or 0)
                facts["settled_30d"] = settled
                if settled > 0:
                    profit = float(eye["profit"] or 0)
                    wins = int(eye["wins"] or 0)
                    facts["roi_pct"] = round(profit / settled * 100, 2)
                    facts["hit_rate_pct"] = round(wins / settled * 100, 2)
                n_clv = int(eye["n_clv"] or 0)
                if n_clv > 0:
                    facts["median_clv_pct"] = (
                        round(float(eye["median_clv"]), 2)
                        if eye["median_clv"] is not None else None
                    )
                    facts["positive_clv_ratio"] = round(
                        int(eye["pos_clv"] or 0) / n_clv, 4
                    )
        except Exception as e:
            logger.warning("[Mentor] FactRetriever failed: %s", e)
        return facts


# ─────────────────────────────────────────────────────────────────────
# NumericGuard + PhraseGuard — output-side
# ─────────────────────────────────────────────────────────────────────
NUMERIC_LIMITS = {
    "odds": (1.01, 20.0),
    "roi_pct": (-100.0, 100.0),
    "clv_pct": (-25.0, 25.0),
    "percent": (-100.0, 100.0),
}


class NumericGuard:
    """Refuser umulige tall i responsen. Naiv regex — skanner alle
    tall og krysssjekker mot kontekst-ord (odds/roi/clv/%)."""

    ODDS_PAT = re.compile(r"\bodds\D{0,10}(\d+(?:\.\d+)?)", re.I)
    ROI_PAT = re.compile(r"\broi\D{0,10}([\-\+]?\d+(?:\.\d+)?)", re.I)
    CLV_PAT = re.compile(r"\bclv\D{0,10}([\-\+]?\d+(?:\.\d+)?)", re.I)
    PCT_PAT = re.compile(r"([\-\+]?\d+(?:\.\d+)?)\s?%")

    @classmethod
    def check(cls, text: str) -> GateResult:
        violations: list[str] = []
        for num_str in cls.ODDS_PAT.findall(text):
            v = float(num_str)
            lo, hi = NUMERIC_LIMITS["odds"]
            if v < lo or v > hi:
                violations.append(f"odds={v}")
        for num_str in cls.ROI_PAT.findall(text):
            v = float(num_str)
            lo, hi = NUMERIC_LIMITS["roi_pct"]
            if v < lo or v > hi:
                violations.append(f"roi={v}%")
        for num_str in cls.CLV_PAT.findall(text):
            v = float(num_str)
            lo, hi = NUMERIC_LIMITS["clv_pct"]
            if v < lo or v > hi:
                violations.append(f"clv={v}%")
        for num_str in cls.PCT_PAT.findall(text):
            v = float(num_str)
            lo, hi = NUMERIC_LIMITS["percent"]
            if v < lo or v > hi:
                violations.append(f"pct={v}%")
        if violations:
            return GateResult(False, "impossible_number", ", ".join(violations))
        return GateResult(True, None, None)


BLOCKED_OUTPUT_PHRASES = [
    r"\bguaranteed\b",
    r"\bsure\s+(?:bet|thing|win)\b",
    r"\bcannot\s+lose\b",
    r"\brisk[-\s]?free\b",
    r"\b100%\s+(?:certain|sure|guaranteed)\b",
]


class PhraseGuard:
    @staticmethod
    def check(text: str) -> GateResult:
        lowered = text.lower()
        for pat in BLOCKED_OUTPUT_PHRASES:
            if re.search(pat, lowered):
                return GateResult(False, "forbidden_phrase", pat)
        return GateResult(True, None, None)


# ─────────────────────────────────────────────────────────────────────
# RateLimiter — DB-basert
# ─────────────────────────────────────────────────────────────────────
class RateLimiter:
    @staticmethod
    async def allow(pool, session_id: str) -> GateResult:
        if pool is None:
            return GateResult(True, None, None)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 minute') AS n_1min,
                        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 day')    AS n_1day
                    FROM oraklion.rate_limiter
                    WHERE session_id = $1;
                    """,
                    session_id,
                )
                n_1min = int(row["n_1min"] or 0)
                n_1day = int(row["n_1day"] or 0)
                if n_1min >= RATE_LIMIT_PER_MINUTE:
                    return GateResult(False, "rate_limit_minute",
                                      f"{n_1min}/{RATE_LIMIT_PER_MINUTE}")
                if n_1day >= RATE_LIMIT_PER_DAY:
                    return GateResult(False, "rate_limit_day",
                                      f"{n_1day}/{RATE_LIMIT_PER_DAY}")
                await conn.execute(
                    "INSERT INTO oraklion.rate_limiter (session_id) VALUES ($1);",
                    session_id,
                )
        except Exception as e:
            logger.warning("[Mentor] RateLimiter check failed: %s", e)
            # Fail open (log warning, allow) — RateLimiter defekt skal ikke
            # DoS'e brukeren. Egen alert håndteres via /admin/scheduler-health.
        return GateResult(True, None, None)


# ─────────────────────────────────────────────────────────────────────
# Anthropic-kall
# ─────────────────────────────────────────────────────────────────────
async def call_anthropic(user_input: str, facts: dict[str, Any]) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY missing")
    facts_block = "\n".join(f"  {k}: {v}" for k, v in facts.items())
    system_with_facts = (
        MENTOR_SYSTEM_PROMPT
        + "\n\nFACTS (only these numbers are permitted in your answer):\n"
        + facts_block
    )
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": MAX_TOKENS,
        "system": system_with_facts,
        "messages": [{"role": "user", "content": user_input}],
    }
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SEC) as client:
        r = await client.post(
            ANTHROPIC_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": ANTHROPIC_VERSION,
                "content-type": "application/json",
            },
            json=payload,
        )
        r.raise_for_status()
        data = r.json()
    content = data.get("content") or []
    text_parts = [c["text"] for c in content if c.get("type") == "text" and c.get("text")]
    return "".join(text_parts).strip()


# ─────────────────────────────────────────────────────────────────────
# AuditLog — INSERT til oraklion.mentor_log
# ─────────────────────────────────────────────────────────────────────
async def log_interaction(
    pool,
    *,
    session_id: str,
    user_input_raw: str,
    response: str | None,
    verdict: str,
    reason: str | None,
    detail: str | None,
    latency_ms: int | None,
) -> None:
    if pool is None:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO oraklion.mentor_log
                    (session_id, created_at, user_input_masked,
                     response_masked, verdict, reason, detail, latency_ms)
                VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7);
                """,
                session_id,
                mask_pii(user_input_raw)[:4000],
                mask_pii(response or "")[:8000] if response else None,
                verdict,
                reason,
                detail,
                latency_ms,
            )
    except Exception as e:
        logger.warning("[Mentor] mentor_log INSERT failed: %s", e)


# ─────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────
@dataclass
class MentorResponse:
    ok: bool
    text: str | None
    verdict: str
    reason: str | None
    detail: str | None
    facts: dict[str, Any]


async def run_mentor(pool, *, session_id: str, message: str) -> MentorResponse:
    started = datetime.now(timezone.utc)

    rl = await RateLimiter.allow(pool, session_id)
    if not rl.passed:
        resp = MentorResponse(False, None, "rate_limited", rl.reason, rl.detail, {})
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=None, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail, latency_ms=0,
        )
        return resp

    gate = SafetyGate.check(message)
    if not gate.passed:
        resp = MentorResponse(False, None, "blocked_input", gate.reason, gate.detail, {})
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=None, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail, latency_ms=0,
        )
        return resp

    facts = await FactRetriever.fetch(pool)

    try:
        text = await call_anthropic(message, facts)
    except Exception as e:
        logger.error("[Mentor] anthropic call failed: %s", e, exc_info=True)
        resp = MentorResponse(False, None, "upstream_error", "anthropic_failed",
                              str(e)[:200], facts)
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=None, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail, latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
        )
        return resp

    ng = NumericGuard.check(text)
    if not ng.passed:
        resp = MentorResponse(False, None, "blocked_output", ng.reason, ng.detail, facts)
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=text, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail, latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
        )
        return resp

    pg = PhraseGuard.check(text)
    if not pg.passed:
        resp = MentorResponse(False, None, "blocked_output", pg.reason, pg.detail, facts)
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=text, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail, latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
        )
        return resp

    latency_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
    resp = MentorResponse(True, text, "ok", None, None, facts)
    await log_interaction(
        pool, session_id=session_id, user_input_raw=message,
        response=text, verdict=resp.verdict, reason=None, detail=None,
        latency_ms=latency_ms,
    )
    return resp
