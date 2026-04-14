"""
services/mirofish_agent.py  — v2.0 PRODUCTION
MiroFish Intelligence Layer — 11-agent simulation via Claude API.
One API call per match. ~$0.002 per match using Haiku.
Anti-hallucination layer built in. Max 15% Omega weight.

FIXES vs v1:
- Real API key from env (works in Railway production)
- _validate_and_clamp no longer tries to int() string fields
- Retry with exponential backoff (3 attempts)
- 25s hard timeout per call
- _fallback_output matches full meta schema
- Parallel calls in batch mode
- Model string verified
"""

import asyncio
import json
import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

CLAUDE_API_URL  = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL    = "claude-haiku-4-5-20251001"
MAX_RETRIES     = 3
CALL_TIMEOUT    = 25.0
MAX_MIROFISH_OMEGA_WEIGHT = 0.15   # MiroFish never contributes more than 15% of Omega

# ── SYSTEM PROMPT ─────────────────────────────────────────────────────────────

AGENT_SYSTEM_PROMPT = """\
You are a football betting market simulation engine.
Given structured match data, simulate how 11 distinct market participants analyze this match.
Each agent has different information access, cognitive biases, and decision logic.

CRITICAL RULES:
1. Return ONLY valid JSON. No prose. No markdown fences. No explanation outside the JSON.
2. Every field shown below is required.
3. Numeric scores are integers 0-100.
4. Direction fields use exactly one of: home | away | draw | over | under | none
5. Do not hallucinate statistics. If data is absent, use "unknown" and lower confidence to ≤30.
6. MiroFish is a SECONDARY signal. It cannot override hard quantitative evidence.

Return this exact JSON (no extra keys, no missing keys):

{
  "agents": {
    "sharp_bettor": {
      "conviction": 0,
      "direction": "home",
      "reasoning": "string",
      "signal_value": 0
    },
    "public_bettor": {
      "conviction": 0,
      "direction": "home",
      "reasoning": "string",
      "signal_value": 0
    },
    "bookmaker_risk_desk": {
      "likely_action": "shade",
      "vulnerable_market": "h2h",
      "confidence": 0
    },
    "odds_mover": {
      "expected_direction": "stable",
      "trigger": "string",
      "magnitude": "small"
    },
    "injury_reactor": {
      "impact_level": "none",
      "affected_selection": "none",
      "confidence": 0
    },
    "narrative_overreactor": {
      "dominant_narrative": "string",
      "distortion_risk": 0,
      "direction": "none"
    },
    "stats_purist": {
      "model_alignment": 0,
      "key_metric": "string",
      "verdict": "monitor"
    },
    "momentum_chaser": {
      "hot_team": "none",
      "streak_impact": 0,
      "direction": "none"
    },
    "contrarian": {
      "fade_signal": "string",
      "strength": 0,
      "direction": "none"
    },
    "noisy_tipster": {
      "noise_level": 0,
      "false_consensus_risk": 0,
      "contaminated_markets": ["none"]
    },
    "line_shopper": {
      "best_value_market": "string",
      "timing_recommendation": "avoid",
      "timing_edge": 0
    }
  },
  "meta": {
    "narrative_pressure": 0,
    "public_bias_direction": "none",
    "sharp_disagreement_score": 0,
    "market_distortion_score": 0,
    "false_consensus_risk": 0,
    "actionability": "skip",
    "what_to_ignore": "string",
    "what_to_watch": "string",
    "invalidation_triggers": ["string"],
    "mirofish_confidence": 0
  }
}"""


def _build_prompt(match: dict) -> str:
    return f"""Simulate the market for this football match.

MATCH: {match.get('home_team', '?')} vs {match.get('away_team', '?')}
LEAGUE: {match.get('league', 'Unknown')}
KICKOFF: {match.get('commence_time', 'Unknown')}

QUANTITATIVE DATA:
- Model P(home win): {match.get('model_prob', 'N/A')}%   [from Dixon-Coles + XGBoost]
- Market implied P(home win): {match.get('market_prob', 'N/A')}%   [no-vig]
- Value gap on selection: {match.get('value_gap', 'N/A')}%
- Omega score: {match.get('omega', 'N/A')}/100
- Best odds available: {match.get('best_odds', 'N/A')}
- xG home season avg: {match.get('xg_home', 'N/A')}
- xG away season avg: {match.get('xg_away', 'N/A')}
- Home form last 5: {match.get('form_home', 'N/A')}
- Away form last 5: {match.get('form_away', 'N/A')}
- Sharp bookmakers present: {match.get('sharp_book', False)}
- Line moved toward selection: {match.get('line_moved', False)}

CONTEXTUAL DATA:
- Known injuries: {match.get('injuries', 'None reported')}
- Recent news: {match.get('news', 'None')}
- Head to head: {match.get('h2h', 'N/A')}
- Primary selection: {match.get('selection', 'N/A')}

Run all 11 agents and return JSON only. No prose."""


# ── INTEGER FIELDS ONLY — string fields excluded from clamping ────────────────

_INT_META_FIELDS = {
    "narrative_pressure", "sharp_disagreement_score",
    "market_distortion_score", "false_consensus_risk", "mirofish_confidence",
}
_INT_AGENT_FIELDS = {
    "conviction", "signal_value", "confidence",
    "distortion_risk", "model_alignment", "streak_impact", "strength",
    "noise_level", "false_consensus_risk", "timing_edge",
}


class MiroFishAgent:
    def __init__(self):
        self.api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not self.api_key:
            logger.warning("ANTHROPIC_API_KEY not set — MiroFish calls will fail")

    async def analyze_match(self, match: dict) -> dict:
        """
        Run 11-agent simulation. Retries 3× with backoff.
        Returns structured output with confidence and weight.
        Never raises — returns fallback on failure.
        """
        prompt = _build_prompt(match)
        headers = {
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
            "x-api-key": self.api_key,
        }
        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": 1400,
            "system": AGENT_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": prompt}],
        }

        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=CALL_TIMEOUT) as c:
                    r = await c.post(CLAUDE_API_URL, headers=headers, json=payload)

                if r.status_code == 200:
                    raw = r.json()["content"][0]["text"].strip()
                    parsed = json.loads(raw)
                    parsed = self._validate(parsed)
                    parsed["meta"]["mirofish_weight"] = self._weight(match, parsed)
                    return parsed

                if r.status_code in (429, 529):
                    wait = 2 ** attempt
                    logger.warning(f"MiroFish rate-limited (attempt {attempt}) — wait {wait}s")
                    await asyncio.sleep(wait)
                    continue

                logger.error(f"MiroFish HTTP {r.status_code}: {r.text[:120]}")
                last_error = f"HTTP {r.status_code}"
                break

            except json.JSONDecodeError as e:
                logger.error(f"MiroFish JSON parse error (attempt {attempt}): {e}")
                last_error = "json_parse"
                break
            except httpx.TimeoutException:
                wait = 2 ** attempt
                logger.warning(f"MiroFish timeout (attempt {attempt}) — wait {wait}s")
                last_error = "timeout"
                await asyncio.sleep(wait)
            except Exception as e:
                logger.error(f"MiroFish unexpected error (attempt {attempt}): {e}")
                last_error = str(e)
                break

        logger.error(f"MiroFish failed after {MAX_RETRIES} attempts: {last_error}")
        return self._fallback(last_error or "unknown")

    async def analyze_batch(self, matches: list[dict]) -> list[dict]:
        """Run analyze_match in parallel for a list of matches."""
        tasks = [self.analyze_match(m) for m in matches]
        return await asyncio.gather(*tasks)

    # ── VALIDATION ────────────────────────────────────────────────────────────

    def _validate(self, data: dict) -> dict:
        """
        Anti-hallucination layer.
        - Clamps integer fields to 0-100
        - Does NOT touch string/direction fields
        - Forces actionability = skip if confidence < 40
        - Quarantines if > 3 required agents missing
        - Ensures invalidation_triggers is a list
        """
        meta = data.get("meta", {})

        # Clamp integer meta fields only
        for key in _INT_META_FIELDS:
            val = meta.get(key)
            if isinstance(val, (int, float)):
                meta[key] = int(min(max(val, 0), 100))

        # Confidence degradation
        if meta.get("mirofish_confidence", 0) < 40:
            meta["actionability"] = "skip"

        # Ensure invalidation_triggers is list
        if not isinstance(meta.get("invalidation_triggers"), list):
            meta["invalidation_triggers"] = ["data_incomplete"]

        # Agent field clamping (integers only)
        agents = data.get("agents", {})
        for agent_name, agent_data in agents.items():
            if not isinstance(agent_data, dict):
                continue
            for field in _INT_AGENT_FIELDS:
                val = agent_data.get(field)
                if isinstance(val, (int, float)):
                    agent_data[field] = int(min(max(val, 0), 100))

        # Quarantine check
        required = [
            "sharp_bettor", "public_bettor", "bookmaker_risk_desk",
            "narrative_overreactor", "stats_purist", "noisy_tipster"
        ]
        missing = sum(1 for a in required if a not in agents)
        if missing > 3:
            logger.warning(f"MiroFish quarantine: {missing} agents missing")
            meta["actionability"] = "skip"
            meta["mirofish_confidence"] = 0

        data["meta"] = meta
        return data

    def _weight(self, match: dict, sim: dict) -> float:
        """
        Secondary signal weight for Omega integration.
        Hard cap: MAX_MIROFISH_OMEGA_WEIGHT (0.15).
        Degrades when seed data is thin.
        """
        confidence  = sim.get("meta", {}).get("mirofish_confidence", 0)
        q = sum([
            bool(match.get("model_prob")),
            bool(match.get("best_odds")),
            bool(match.get("form_home")),
        ]) / 3.0
        return round((confidence / 100.0) * q * MAX_MIROFISH_OMEGA_WEIGHT, 3)

    def _fallback(self, reason: str) -> dict:
        """
        Full-schema fallback — all scores zero, actionability skip.
        Matches complete meta schema so downstream never hits KeyError.
        """
        return {
            "agents": {},
            "meta": {
                "narrative_pressure":       0,
                "public_bias_direction":    "none",
                "sharp_disagreement_score": 0,
                "market_distortion_score":  0,
                "false_consensus_risk":     0,
                "actionability":            "skip",
                "what_to_ignore":           "simulation_failed",
                "what_to_watch":            "run_manual_check",
                "invalidation_triggers":    [f"simulation_failed:{reason}"],
                "mirofish_confidence":      0,
                "mirofish_weight":          0.0,
            },
            "simulation_status": "failed",
            "failure_reason": reason,
        }


# ── OMEGA INTEGRATION ─────────────────────────────────────────────────────────

def apply_mirofish_to_omega(base_omega: int, sim: dict) -> dict:
    """
    Integrate MiroFish into Omega score.
    Rules:
    - MiroFish can ADD up to +8 Omega points (never subtract base)
    - false_consensus_risk > 70 → apply −5 penalty
    - actionability == skip → zero bonus regardless
    - Returns full enriched dict for API response
    """
    meta              = sim.get("meta", {})
    weight            = meta.get("mirofish_weight", 0.0)
    actionability     = meta.get("actionability", "skip")
    false_consensus   = meta.get("false_consensus_risk", 0)
    market_distortion = meta.get("market_distortion_score", 0)
    sharp_disagree    = meta.get("sharp_disagreement_score", 0)

    if actionability in ("high", "medium") and weight > 0:
        distortion_bonus = int(market_distortion * weight * 0.5)
        consensus_bonus  = int((100 - sharp_disagree) * weight * 0.3)
        bonus = min(distortion_bonus + consensus_bonus, 8)
    else:
        bonus = 0

    penalty = 5 if false_consensus > 70 else 0

    adjusted = int(min(100, max(0, base_omega + bonus - penalty)))

    return {
        "original_omega":         base_omega,
        "adjusted_omega":         adjusted,
        "mirofish_bonus":         bonus,
        "mirofish_penalty":       penalty,
        "actionability":          actionability,
        "narrative_pressure":     meta.get("narrative_pressure", 0),
        "public_bias":            meta.get("public_bias_direction", "none"),
        "sharp_disagreement":     meta.get("sharp_disagreement_score", 0),
        "market_distortion":      meta.get("market_distortion_score", 0),
        "false_consensus_risk":   meta.get("false_consensus_risk", 0),
        "what_to_watch":          meta.get("what_to_watch", ""),
        "what_to_ignore":         meta.get("what_to_ignore", ""),
        "invalidation_triggers":  meta.get("invalidation_triggers", []),
        "mirofish_confidence":    meta.get("mirofish_confidence", 0),
        "mirofish_weight":        weight,
    }
