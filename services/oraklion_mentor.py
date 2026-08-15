"""
Oraklion Mentor — safety-gated Q&A over Sesomnod-aggregater.

Pipeline (BRIEF 2026-08-15):
  1. SafetyGate.classify — CRISIS / DEPENDENCY / PANIC / NONE
       KJØRES FØRST. En bruker i krise skal aldri møte rate-limit-avvis.
       Ved != NONE: aktiver SAFE-modus (sticky, DB-persistert) og
       returner fast, hjelpelinje-førende svar. Ingen Anthropic-kall.
  2. SAFE-sticky sjekk: hvis sesjonen tidligere har vært CRISIS/DEP/PANIC
       fortsetter vi å svare med krise-respons — kan ikke reverseres.
  3. RateLimiter — kun for NONE + ikke-SAFE input
  4. FactRetriever — offentlige aggregater fra sniper_bets_v1 + system_state
  5. Anthropic Claude Sonnet 5 via v1/messages (httpx, ingen SDK-dep)
  6. NumericGuard — refuser umulige tall (odds > 20, |CLV| > 25, ROI > 100)
       Skanner KUN med kontekst-ord (odds/roi/clv/%). Hjelpelinjenummer
       "800 800 40" har ingen slike ord → skal ALLTID passere.
  7. PhraseGuard — refuser forbudte fraser (guaranteed / sure bet / …)
  8. AuditLog — INSERT til oraklion.mentor_log med PII-maskering

Miljøkrav:
  ANTHROPIC_API_KEY — Railway env
  DDL: services/oraklion_mentor_ddl.sql
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

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

HELPLINE = "800 800 40"

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
  """ + HELPLINE + """ (Norway) when the user asks anything transactional.
- Keep answers under 180 words. Plain prose. No lists longer than 4 items.

You are permitted to explain: the four-engine convergence gate, the
calibration window, the CLV methodology, evidence chain guarantees,
and Sesomnod's discipline rules (three signals per day, eight lives).
"""

# ─────────────────────────────────────────────────────────────────────
# INPUT-NORMALISERING
#   leet-map + strip whitespace + lowercase.
#   Beholder norske tegn (æ ø å) siden mye krisespråk bruker dem.
# ─────────────────────────────────────────────────────────────────────
LEET_MAP = str.maketrans({
    "0": "o", "1": "i", "3": "e", "4": "a",
    "5": "s", "7": "t", "@": "a", "$": "s",
})
_WS_AND_PUNCT_RE = re.compile(r"[\s\-_./\\|:;,!?()\[\]{}\"']+")


def _normalize(text: str) -> str:
    """Whitespace-fri, lowercase, leetspeak-normalisert form."""
    if not text:
        return ""
    lowered = text.lower().translate(LEET_MAP)
    stripped = _WS_AND_PUNCT_RE.sub("", lowered)
    return stripped


# ─────────────────────────────────────────────────────────────────────
# SafetyGate — verdikter og klassifisering
# ─────────────────────────────────────────────────────────────────────
Verdict = Literal["NONE", "CRISIS", "DEPENDENCY", "PANIC"]

# Nøkkelord matches mot _normalize'd input (whitespace-fri, lowercase,
# leet-normalisert). Skriv keywords whitespace-frie.

# CRISIS: aktiv suicid-, selvskade- eller livstruende ideation.
#   Både norsk og engelsk. Norske stemmer inn: "vil ikke leve",
#   "orker ikke mer", "vil dø", "gjør slutt på".
CRISIS_KEYWORDS = [
    "selvmord",
    "selvskade",
    "vilikkeleve",
    "vilikkelevemer",
    "vilikkelevelenger",
    "vildø",
    "vildo",
    "vilikkefortsette",
    "orkerikkelenger",
    "orkerikkemer",
    "orkerikke",
    "gjørsluttpåmeg",
    "gjorsluttpameg",
    "gjørsluttpålivet",
    "gjorsluttpalivet",
    "takelivet",
    "takemittliv",
    "endelivet",
    "suicide",
    "killmyself",
    "endmylife",
]

# DEPENDENCY: spillavhengighet-markører. Bruker/har brukt penger de
# ikke har (husleie/matpenger/lånte penger), eller kan ikke stoppe.
#   Norske bestemte former: "husleia", "matpengene", "leia".
#   Mellomrom-normalisert: "kanikkeslutte", "klarerikkestoppe".
DEPENDENCY_KEYWORDS = [
    "låntpenger",
    "lantepenger",
    "harlånt",
    "harlant",
    "lånerpenger",
    "lanerpenger",
    "kanikkeslutte",
    "kanikkestoppe",
    "klarerikkeslutte",
    "klarerikkestoppe",
    "spillavhengig",
    "gamblingaddict",
    "brukthusleia",
    "brukthusleie",
    "brukthusleien",
    "brukthusleien",
    "brukthusleier",
    "brukhusleie",
    "brukthuslei",
    "bruktmatpenger",
    "bruktmatpengene",
    "brukttrygden",
    "brukttrygd",
    "kastetbortpenger",
    "kanikkekontrollere",
    "harmistetalt",
    "tapthusleia",
    "tapthusleie",
    "cantstop",
    "cannotstop",
    "borrowedmoney",
    "spentrent",
    "spentmyrent",
]

# PANIC: "må ha nå", "lås opp dagen", "trenger vinnere nå" —
# akutt press på systemet for en umulig-hurtig løsning.
PANIC_KEYWORDS = [
    "låseopp",
    "laseopp",
    "låsoppdagen",
    "lasoppdagen",
    "åpnedagen",
    "apnedagen",
    "lasopp",
    "låsopp",
    "trengervinnernå",
    "trengervinnerna",
    "måhanå",
    "mahana",
    "gimenå",
    "gimena",
    "jegtrengeret",
    "unlockday",
    "unlocktoday",
    "needwinnersnow",
]


@dataclass
class GateResult:
    passed: bool
    reason: str | None
    detail: str | None
    verdict: Verdict = "NONE"


class SafetyGate:
    """Klassifiserer bruker-input FØR den slippes videre.

    NONE       — trygg å sende videre til pipeline.
    CRISIS     — akutt fare. Krise-svar, aldri Anthropic. SAFE aktiveres.
    DEPENDENCY — spillavhengighet-markør. Samme respons-mønster.
    PANIC      — akutt press. Samme respons-mønster.
    """

    @staticmethod
    def classify(user_input: str) -> GateResult:
        if not user_input or not user_input.strip():
            return GateResult(False, "empty_input", "Melding er tom.", "NONE")
        if len(user_input) > 4000:
            return GateResult(False, "too_long", "Melding over 4000 tegn.", "NONE")

        normed = _normalize(user_input)

        for kw in CRISIS_KEYWORDS:
            if kw in normed:
                return GateResult(True, "crisis_detected", kw, "CRISIS")
        for kw in DEPENDENCY_KEYWORDS:
            if kw in normed:
                return GateResult(True, "dependency_detected", kw, "DEPENDENCY")
        for kw in PANIC_KEYWORDS:
            if kw in normed:
                return GateResult(True, "panic_detected", kw, "PANIC")

        return GateResult(True, None, None, "NONE")


# ─────────────────────────────────────────────────────────────────────
# Krise-respons-tekster (norsk, alltid inkl. hjelpelinjen 800 800 40)
# ─────────────────────────────────────────────────────────────────────
def crisis_response(verdict: Verdict) -> str:
    if verdict == "CRISIS":
        return (
            f"Vi er bekymret for det du sier. Ring Hjelpelinjen {HELPLINE} "
            "— åpen hele døgnet, gratis. Du trenger ikke være alene med dette. "
            "Vi svarer ikke på annet fra denne sesjonen i dag."
        )
    if verdict == "DEPENDENCY":
        return (
            "Sesomnod er ikke stedet du søker svar hvis du taper mer enn du "
            f"tåler. Ring Hjelpelinjen for spilleavhengige {HELPLINE} — "
            "gratis, anonymt, uten fordommer. Vi svarer ikke på annet fra "
            "denne sesjonen i dag."
        )
    if verdict == "PANIC":
        return (
            "Det du beskriver er panikkfølelse. Vi låser aldri opp "
            "signaler etter forespørsel — det er selve poenget med "
            f"disiplinen. Ring Hjelpelinjen {HELPLINE} hvis det haster. "
            "Vi svarer ikke på annet fra denne sesjonen i dag."
        )
    return ""


# ─────────────────────────────────────────────────────────────────────
# PII-maskering
# ─────────────────────────────────────────────────────────────────────
PII_PATTERNS = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "credit_card": re.compile(r"\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b"),
    "iban": re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b"),
}
# Merk: telefonnummer maskeres IKKE her. Hjelpelinjen 800 800 40 skal
# alltid være lesbar i audit-log slik at Don kan verifisere at krise-
# svar faktisk ga rett nummer.


def mask_pii(text: str) -> str:
    if not text:
        return text
    out = text
    for kind, pat in PII_PATTERNS.items():
        out = pat.sub(f"[REDACTED-{kind}]", out)
    return out


# ─────────────────────────────────────────────────────────────────────
# SAFE-modus persistens — sticky per session_id
# ─────────────────────────────────────────────────────────────────────
async def is_safe_mode(pool, session_id: str) -> tuple[bool, Verdict | None]:
    """True hvis sesjonen tidligere har blitt merket CRISIS/DEP/PANIC.
    Uangripelig — kan aldri reverseres innen sesjonen."""
    if pool is None:
        return False, None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT verdict
                FROM oraklion.safe_sessions
                WHERE session_id = $1
                LIMIT 1;
                """,
                session_id,
            )
            if row:
                return True, row["verdict"]
    except Exception as e:
        logger.warning("[Mentor] is_safe_mode check failed: %s", e)
    return False, None


async def activate_safe_mode(pool, session_id: str, verdict: Verdict,
                              first_message_masked: str) -> None:
    """INSERT en gang per session_id. ON CONFLICT DO NOTHING sørger for
    at et senere klassifisert-verdikt ikke overstyrer den første — SAFE
    er sticky OG første-verdikt-vinner."""
    if pool is None or verdict == "NONE":
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO oraklion.safe_sessions
                    (session_id, verdict, activated_at, first_message_masked)
                VALUES ($1, $2, NOW(), $3)
                ON CONFLICT (session_id) DO NOTHING;
                """,
                session_id, verdict, first_message_masked[:1000],
            )
    except Exception as e:
        logger.warning("[Mentor] activate_safe_mode failed: %s", e)


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
    """Refuser umulige tall i responsen.

    Skanner KUN når kontekst-ord (odds/roi/clv/%) er tilstede. Rene
    tall uten kontekst — som hjelpelinjenummer "800 800 40" — skal
    ALDRI blokkeres."""

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
# RateLimiter — DB-basert, fail open. Kjøres KUN for NONE-input.
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
    facts: dict[str, Any] = field(default_factory=dict)


async def run_mentor(pool, *, session_id: str, message: str) -> MentorResponse:
    started = datetime.now(timezone.utc)

    # STEG 1 — SafetyGate FØR alt annet. En bruker i krise skal aldri
    # møte rate-limit eller upstream_error. Her klassifiseres første.
    gate = SafetyGate.classify(message)

    if gate.verdict in ("CRISIS", "DEPENDENCY", "PANIC"):
        await activate_safe_mode(pool, session_id, gate.verdict,
                                  mask_pii(message))
        text = crisis_response(gate.verdict)
        resp = MentorResponse(True, text, gate.verdict.lower(),
                              gate.reason, gate.detail, {})
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=text, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail,
            latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
        )
        return resp

    if not gate.passed:
        # empty/too_long — behandles som vanlig avvis
        resp = MentorResponse(False, None, "blocked_input", gate.reason,
                              gate.detail, {})
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=None, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail,
            latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
        )
        return resp

    # STEG 2 — SAFE-modus sticky-sjekk. Tidligere krise-verdikt i sesjonen
    # låser den permanent i SAFE. Aldri reverseres. Ingen Anthropic-kall.
    safe, prior_verdict = await is_safe_mode(pool, session_id)
    if safe and prior_verdict:
        text = crisis_response(prior_verdict)  # type: ignore[arg-type]
        resp = MentorResponse(True, text, f"safe_locked_{prior_verdict.lower()}",
                              "sticky_safe_mode", prior_verdict, {})
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=text, verdict="safe_locked", reason=resp.reason,
            detail=resp.detail,
            latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
        )
        return resp

    # STEG 3 — RateLimiter (kun for NONE + ikke-SAFE)
    rl = await RateLimiter.allow(pool, session_id)
    if not rl.passed:
        resp = MentorResponse(False, None, "rate_limited", rl.reason,
                              rl.detail, {})
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=None, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail, latency_ms=0,
        )
        return resp

    # STEG 4 — Fakta + modellkall
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
            detail=resp.detail,
            latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
        )
        return resp

    # STEG 5 — Output-guards
    ng = NumericGuard.check(text)
    if not ng.passed:
        resp = MentorResponse(False, None, "blocked_output", ng.reason,
                              ng.detail, facts)
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=text, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail,
            latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
        )
        return resp

    pg = PhraseGuard.check(text)
    if not pg.passed:
        resp = MentorResponse(False, None, "blocked_output", pg.reason,
                              pg.detail, facts)
        await log_interaction(
            pool, session_id=session_id, user_input_raw=message,
            response=text, verdict=resp.verdict, reason=resp.reason,
            detail=resp.detail,
            latency_ms=int((datetime.now(timezone.utc) - started).total_seconds() * 1000),
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
