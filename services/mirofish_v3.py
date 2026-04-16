"""
SesomNod MiroFish V3
6-agent hierarchical validation system for betting picks.
"""

import json
import asyncio
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class AgentDecision(Enum):
    """Possible decisions from an agent."""
    ACCEPT = "ACCEPT"
    REJECT = "REJECT"
    ABSTAIN = "ABSTAIN"


@dataclass
class AgentOutput:
    """Output from a single agent."""
    agent_name: str
    decision: AgentDecision
    confidence: float  # 0-1
    score: float  # 0-100
    data: Dict[str, Any] = field(default_factory=dict)
    reasoning: str = ""


@dataclass
class MiroFishResult:
    """Final result from MiroFish V3."""
    decision: AgentDecision
    final_kelly: float
    omega_score: int  # 0-100
    tier: str  # ATOMIC, EDGE, MONITORED
    why: str
    warn: Optional[str]
    confidence: float
    agent_outputs: List[AgentOutput] = field(default_factory=list)


class MarketSpecialistAgent:
    """
    Layer 1 Agent: Evaluates if the selected market is appropriate for the match.
    """
    
    PROMPT_TEMPLATE = """
Du er en ekspert på fotballmarkeder. Evaluer dette markedsvalget:

Kamp: {match}
Valgt marked: {market} @ {odds}
Modell-sannsynlighet: {model_prob:.1%}
Markedsimplisitt: {market_prob:.1%}
Edge: {edge:.1%}

Vurder:
1. Er dette det riktige markedet for denne kampen? (0-10)
2. Hvilke andre markeder burde vurderes?
3. Er oddsen riktig priset?

Output KUN JSON:
{{"market_quality": 0-10, "alternatives": [], "odds_fair": true/false, "reasoning": ""}}
"""
    
    def evaluate(self, match: str, market: str, odds: float, 
                 model_prob: float, market_prob: float, edge: float) -> AgentOutput:
        """
        Evaluate the market selection.
        
        Returns simulated output (in production, this would call LLM).
        """
        # Simulated evaluation logic
        market_quality = min(int(edge * 50), 10)  # Edge * 50, capped at 10
        market_quality = max(market_quality, 3)  # Minimum 3
        
        odds_fair = edge > 0.05  # Fair if edge > 5%
        
        reasoning = f"Edge på {edge:.1%} indikerer {'verdi' if edge > 0.05 else 'begrenset verdi'}."
        
        return AgentOutput(
            agent_name="MarketSpecialist",
            decision=AgentDecision.ACCEPT if market_quality >= 5 else AgentDecision.ABSTAIN,
            confidence=market_quality / 10.0,
            score=market_quality * 10,
            data={"market_quality": market_quality, "odds_fair": odds_fair},
            reasoning=reasoning
        )


class ContrarianAgent:
    """
    Layer 1 Agent: Devil's advocate - finds everything that could go wrong.
    """
    
    PROMPT_TEMPLATE = """
Du er djevelens advokat. Finn ALT som kan gå galt med denne picken:

Kamp: {match}
Pick: {market} @ {odds}
Modell sier: {model_prob:.1%} sannsynlighet

Finn:
1. De 3 sterkeste argumentene MOT denne picken
2. Hvilke data mangler modellen?
3. Hva er worst-case scenario?

Output KUN JSON:
{{"risk_score": 0-10, "arguments_against": [], "missing_data": [], "worst_case": ""}}
"""
    
    def evaluate(self, match: str, market: str, odds: float,
                 model_prob: float, context: Dict) -> AgentOutput:
        """Find counter-arguments to the pick."""
        # Simulated contrarian analysis
        risk_factors = []
        
        if model_prob > 0.65:
            risk_factors.append("Høy modell-sannsynlighet kan indikere overconfidence")
        
        if odds < 1.80:
            risk_factors.append("Lave odds = liten margin for feil")
        
        risk_score = min(len(risk_factors) * 2 + 3, 10)
        
        return AgentOutput(
            agent_name="Contrarian",
            decision=AgentDecision.ACCEPT if risk_score < 7 else AgentDecision.ABSTAIN,
            confidence=(10 - risk_score) / 10.0,
            score=(10 - risk_score) * 10,
            data={"risk_score": risk_score, "arguments_against": risk_factors},
            reasoning=f"Identifiserte {len(risk_factors)} risikofaktorer."
        )


class ContextAgent:
    """
    Layer 1 Agent: Analyzes match context (injuries, motivation, H2H).
    """
    
    PROMPT_TEMPLATE = """
Analyser konteksten for denne kampen:

Kamp: {match}
Liga: {league}
Dato: {kickoff}

Sjekk:
1. Skader/suspensioner på begge lag
2. Lagmotivasjon (tabellposisjon, cup-kamp?)
3. Historisk head-to-head i dette markedet
4. Hjemme/borte-form siste 5 kamper

Output KUN JSON:
{{"context_score": 0-10, "key_factors": [], "h2h_relevant": true/false, "recommendation": ""}}
"""
    
    def evaluate(self, match: str, league: str, kickoff: str,
                 home_form: List[str], away_form: List[str],
                 injuries: Dict) -> AgentOutput:
        """Analyze match context."""
        # Simulated context analysis
        context_score = 7  # Default good context
        
        # Adjust based on form
        home_wins = home_form.count('W')
        if home_wins >= 3:
            context_score += 1
        
        # Adjust based on injuries
        key_injuries = len([i for i in injuries.get('home', []) if i.get('key', False)])
        if key_injuries > 0:
            context_score -= key_injuries
        
        context_score = max(3, min(10, context_score))
        
        return AgentOutput(
            agent_name="Context",
            decision=AgentDecision.ACCEPT if context_score >= 5 else AgentDecision.ABSTAIN,
            confidence=context_score / 10.0,
            score=context_score * 10,
            data={"context_score": context_score, "h2h_relevant": True},
            reasoning=f"Kontekst-score: {context_score}/10. Hjemmeform: {home_wins} seire på siste 5."
        )


class ValueValidatorAgent:
    """
    Layer 2 Agent: Validates value based on Layer 1 agent outputs.
    """
    
    PROMPT_TEMPLATE = """
Valider verdien i denne picken basert på agent-rapportene:

Market Specialist score: {market_quality}/10
Contrarian risk score: {risk_score}/10
Context score: {context_score}/10
Edge: {edge:.1%}

Er dette en valid pick? Svar strengt.

Output KUN JSON:
{{"valid": true/false, "confidence": 0-1, "value_score": 0-10, "reason": ""}}
"""
    
    def evaluate(self, market_quality: int, risk_score: int, 
                 context_score: int, edge: float) -> AgentOutput:
        """Validate the pick value."""
        # Weighted scoring
        weighted_score = (
            market_quality * 0.35 +
            (10 - risk_score) * 0.30 +
            context_score * 0.20 +
            min(edge * 100, 10) * 0.15
        )
        
        is_valid = weighted_score >= 6.0 and edge >= 0.05
        confidence = weighted_score / 10.0
        
        return AgentOutput(
            agent_name="ValueValidator",
            decision=AgentDecision.ACCEPT if is_valid else AgentDecision.REJECT,
            confidence=confidence,
            score=weighted_score * 10,
            data={"valid": is_valid, "value_score": weighted_score},
            reasoning=f"Valid pick: {is_valid}. Vektet score: {weighted_score:.1f}/10."
        )


class RiskAssessorAgent:
    """
    Layer 2 Agent: Assesses risk profile and recommends Kelly stake.
    """
    
    PROMPT_TEMPLATE = """
Vurder risikoprofilen:

Pick: {market} @ {odds}
Kelly: {kelly:.1%}
Bankroll exposure: {exposure:.1%}
Correlation med andre aktive picks: {correlation}

Output KUN JSON:
{{"risk_level": "LOW/MEDIUM/HIGH", "max_kelly": 0.05, "warnings": []}}
"""
    
    def evaluate(self, market: str, odds: float, kelly: float,
                 exposure: float, correlation: float) -> AgentOutput:
        """Assess risk and recommend stake."""
        warnings = []
        
        # Determine risk level
        if kelly > 0.04:
            risk_level = "HIGH"
            warnings.append("Høy Kelly-stake")
        elif kelly > 0.025:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        if exposure > 0.20:
            warnings.append("Høy bankroll-eksponering")
        
        if correlation > 0.5:
            warnings.append("Høy korrelasjon med andre picks")
        
        # Adjust max Kelly based on risk
        if risk_level == "HIGH":
            max_kelly = 0.02
        elif risk_level == "MEDIUM":
            max_kelly = 0.035
        else:
            max_kelly = 0.05
        
        confidence = 1.0 - (len(warnings) * 0.15)
        
        return AgentOutput(
            agent_name="RiskAssessor",
            decision=AgentDecision.ACCEPT,
            confidence=confidence,
            score=80 if risk_level == "LOW" else (60 if risk_level == "MEDIUM" else 40),
            data={"risk_level": risk_level, "max_kelly": max_kelly, "warnings": warnings},
            reasoning=f"Risiko: {risk_level}. {len(warnings)} advarsler."
        )


class FinalSynthesizerAgent:
    """
    Layer 3 Agent: Synthesizes all agent outputs into final decision.
    """
    
    PROMPT_TEMPLATE = """
Syntesiser alle agent-rapporter til en endelig beslutning:

{all_agent_outputs}

Regler:
- Hvis risk_level = HIGH: MAX kelly 2%
- Hvis valid = false: AVVIS pick
- Hvis confidence < 0.6: AVVIS pick
- Hvis market_quality < 6: VURDER alternativt marked

Output KUN JSON:
{{
    "decision": "ACCEPT/REJECT",
    "final_kelly": 0.0-0.05,
    "omega_score": 0-100,
    "tier": "ATOMIC/EDGE/MONITORED",
    "why": "Konkret begrunnelse på norsk",
    "warn": "Advarsel på norsk eller null",
    "confidence": 0.0-1.0
}}
"""
    
    def synthesize(self, agent_outputs: List[AgentOutput], 
                   base_kelly: float, edge: float) -> MiroFishResult:
        """
        Synthesize all agent outputs into final decision.
        
        Args:
            agent_outputs: List of outputs from all agents
            base_kelly: Base Kelly stake from calculations
            edge: Edge percentage
            
        Returns:
            Final MiroFishResult
        """
        # Extract key scores
        market_quality = 5
        risk_score = 5
        context_score = 5
        valid = True
        risk_level = "MEDIUM"
        max_kelly = 0.05
        warnings = []
        
        for output in agent_outputs:
            if output.agent_name == "MarketSpecialist":
                market_quality = output.data.get("market_quality", 5)
            elif output.agent_name == "Contrarian":
                risk_score = output.data.get("risk_score", 5)
            elif output.agent_name == "Context":
                context_score = output.data.get("context_score", 5)
            elif output.agent_name == "ValueValidator":
                valid = output.data.get("valid", True)
            elif output.agent_name == "RiskAssessor":
                risk_level = output.data.get("risk_level", "MEDIUM")
                max_kelly = output.data.get("max_kelly", 0.05)
                warnings = output.data.get("warnings", [])
        
        # Calculate final Kelly
        final_kelly = min(base_kelly, max_kelly)
        
        # Determine decision
        if not valid:
            decision = AgentDecision.REJECT
            tier = "REJECTED"
        elif risk_level == "HIGH":
            decision = AgentDecision.ACCEPT
            tier = "MONITORED"
            final_kelly = min(final_kelly, 0.02)
        elif market_quality >= 8 and risk_score <= 4:
            decision = AgentDecision.ACCEPT
            tier = "ATOMIC"
        elif market_quality >= 6:
            decision = AgentDecision.ACCEPT
            tier = "EDGE"
        else:
            decision = AgentDecision.ABSTAIN
            tier = "MONITORED"
        
        # Calculate omega score
        omega = int(
            market_quality * 8 +
            (10 - risk_score) * 5 +
            context_score * 3 +
            min(edge * 100, 10)
        )
        omega = min(100, omega)
        
        # Generate why text
        why = self._generate_why(market_quality, risk_score, context_score, edge)
        
        # Generate warning text
        warn = self._generate_warning(warnings) if warnings else None
        
        # Calculate overall confidence
        confidence = sum(o.confidence for o in agent_outputs) / len(agent_outputs)
        
        return MiroFishResult(
            decision=decision,
            final_kelly=final_kelly,
            omega_score=omega,
            tier=tier,
            why=why,
            warn=warn,
            confidence=confidence,
            agent_outputs=agent_outputs
        )
    
    def _generate_why(self, market_quality: int, risk_score: int, 
                      context_score: int, edge: float) -> str:
        """Generate human-readable explanation."""
        parts = []
        
        if market_quality >= 8:
            parts.append("Sterkt markedsvalg")
        elif market_quality >= 6:
            parts.append("Godt markedsvalg")
        else:
            parts.append("Akseptabelt markedsvalg")
        
        if edge >= 0.15:
            parts.append(f"med {edge:.1%} edge")
        elif edge >= 0.10:
            parts.append(f"med solid {edge:.1%} edge")
        else:
            parts.append(f"med {edge:.1%} edge")
        
        if risk_score <= 4:
            parts.append("og lav risikoprofil.")
        elif risk_score <= 6:
            parts.append("og moderat risikoprofil.")
        else:
            parts.append("men høy risikoprofil.")
        
        return " ".join(parts)
    
    def _generate_warning(self, warnings: List[str]) -> str:
        """Generate warning text."""
        if not warnings:
            return None
        
        if len(warnings) == 1:
            return f"Advarsel: {warnings[0]}"
        else:
            return f"Advarsler: {'; '.join(warnings)}"


class MiroFishV3:
    """
    MiroFish V3: 6-agent hierarchical validation system.
    
    Architecture:
    - Layer 1 (parallel): MarketSpecialist + ContrarianAgent + ContextAgent
    - Layer 2 (sequential): ValueValidator + RiskAssessor
    - Layer 3 (sequential): FinalSynthesizer
    """
    
    def __init__(self):
        """Initialize all agents."""
        self.market_specialist = MarketSpecialistAgent()
        self.contrarian = ContrarianAgent()
        self.context = ContextAgent()
        self.value_validator = ValueValidatorAgent()
        self.risk_assessor = RiskAssessorAgent()
        self.synthesizer = FinalSynthesizerAgent()
    
    async def validate(self, 
                       match: str,
                       league: str,
                       market: str,
                       odds: float,
                       model_prob: float,
                       market_prob: float,
                       edge: float,
                       kelly: float,
                       home_form: List[str],
                       away_form: List[str],
                       injuries: Dict,
                       exposure: float = 0.0,
                       correlation: float = 0.0) -> MiroFishResult:
        """
        Run full MiroFish validation pipeline.
        
        Args:
            match: Match identifier (e.g., "Napoli vs Lazio")
            league: League name
            market: Selected market
            odds: Decimal odds
            model_prob: Model probability
            market_prob: Market implied probability
            edge: Edge percentage
            kelly: Base Kelly stake
            home_form: List of last 5 home results (W/D/L)
            away_form: List of last 5 away results (W/D/L)
            injuries: Dict with injury information
            exposure: Current bankroll exposure
            correlation: Correlation with existing picks
            
        Returns:
            MiroFishResult with final decision
        """
        # Layer 1: Parallel evaluation
        layer1_tasks = [
            asyncio.create_task(
                self._run_market_specialist(match, market, odds, model_prob, market_prob, edge)
            ),
            asyncio.create_task(
                self._run_contrarian(match, market, odds, model_prob, {})
            ),
            asyncio.create_task(
                self._run_context(match, league, "", home_form, away_form, injuries)
            )
        ]
        
        layer1_results = await asyncio.gather(*layer1_tasks)
        
        # Extract scores for Layer 2
        market_quality = layer1_results[0].data.get("market_quality", 5)
        risk_score = layer1_results[1].data.get("risk_score", 5)
        context_score = layer1_results[2].data.get("context_score", 5)
        
        # Layer 2: Sequential evaluation
        value_result = self.value_validator.evaluate(
            market_quality=market_quality,
            risk_score=risk_score,
            context_score=context_score,
            edge=edge
        )
        
        risk_result = self.risk_assessor.evaluate(
            market=market,
            odds=odds,
            kelly=kelly,
            exposure=exposure,
            correlation=correlation
        )
        
        # Combine all agent outputs
        all_outputs = list(layer1_results) + [value_result, risk_result]
        
        # Layer 3: Final synthesis
        final_result = self.synthesizer.synthesize(
            agent_outputs=all_outputs,
            base_kelly=kelly,
            edge=edge
        )
        
        return final_result
    
    async def _run_market_specialist(self, match: str, market: str, odds: float,
                                      model_prob: float, market_prob: float, 
                                      edge: float) -> AgentOutput:
        """Run Market Specialist agent."""
        return self.market_specialist.evaluate(match, market, odds, model_prob, market_prob, edge)
    
    async def _run_contrarian(self, match: str, market: str, odds: float,
                               model_prob: float, context: Dict) -> AgentOutput:
        """Run Contrarian agent."""
        return self.contrarian.evaluate(match, market, odds, model_prob, context)
    
    async def _run_context(self, match: str, league: str, kickoff: str,
                           home_form: List[str], away_form: List[str],
                           injuries: Dict) -> AgentOutput:
        """Run Context agent."""
        return self.context.evaluate(match, league, kickoff, home_form, away_form, injuries)


# === UNIT TESTS ===
async def test_mirofish_v3():
    """Unit tests for MiroFishV3."""
    mirofish = MiroFishV3()
    
    result = await mirofish.validate(
        match="Napoli vs Lazio",
        league="Serie A",
        market="over_25",
        odds=2.10,
        model_prob=0.673,
        market_prob=0.476,
        edge=0.197,
        kelly=0.042,
        home_form=['W', 'W', 'D', 'W', 'L'],
        away_form=['L', 'D', 'W', 'L', 'W'],
        injuries={'home': [], 'away': []},
        exposure=0.15,
        correlation=0.2
    )
    
    assert result.decision in [AgentDecision.ACCEPT, AgentDecision.REJECT, AgentDecision.ABSTAIN]
    assert 0 <= result.omega_score <= 100
    assert result.tier in ["ATOMIC", "EDGE", "MONITORED", "REJECTED"]
    assert result.why != ""
    assert len(result.agent_outputs) == 5
    
    print(f"MiroFishV3 test passed!")
    print(f"Decision: {result.decision.value}")
    print(f"Tier: {result.tier}")
    print(f"Omega: {result.omega_score}")
    print(f"Why: {result.why}")
    
    return True


if __name__ == "__main__":
    asyncio.run(test_mirofish_v3())
