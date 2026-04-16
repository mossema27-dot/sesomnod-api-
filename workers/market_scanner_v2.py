"""
SesomNod Market Scanner V2
Complete integration of Market Intelligence Layer.
"""

import asyncio
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

# Import our modules
import sys
sys.path.insert(0, '/app')

from services.market_extractor import MarketExtractor, MarketProbabilities
from services.market_selection_engine import MarketSelectionEngine, MarketSelectionResult
from services.context_engine import ContextEngine
from services.mirofish_v3 import MiroFishV3, MiroFishResult
from services.pick_formatter import PickFormatter, Pick
from services.kelly_v2 import KellyV2


@dataclass
class MatchData:
    """Input data for a match."""
    match_id: str
    home_team: str
    away_team: str
    league: str
    kickoff: str
    xg_home: float
    xg_away: float
    form_home: List[str]
    form_away: List[str]
    odds: Dict[str, float]
    bookmaker_counts: Optional[Dict[str, int]] = None
    context: Optional[Dict] = None
    injuries: Optional[Dict] = None


@dataclass
class ScanResult:
    """Result from scanning a match."""
    match_id: str
    match: str
    has_pick: bool
    pick: Optional[Pick]
    rejection_reason: Optional[str]
    processing_time_ms: float


class MarketScannerV2:
    """
    Market Scanner V2 - Full Market Intelligence Layer integration.
    
    Pipeline:
    1. Extract all markets from Poisson matrix
    2. Adjust lambda via ContextEngine
    3. Select best market via MarketSelectionEngine
    4. Validate via MiroFishV3
    5. Format via PickFormatter
    6. Return or reject
    """
    
    def __init__(self):
        """Initialize all components."""
        self.extractor = MarketExtractor(max_goals=10)
        self.selection_engine = MarketSelectionEngine()
        self.context_engine = ContextEngine()
        self.mirofish = MiroFishV3()
        self.formatter = PickFormatter()
        self.kelly_calc = KellyV2()
    
    async def scan_match(self, match: MatchData) -> ScanResult:
        """
        Scan a single match and generate a pick if valid.
        
        Args:
            match: MatchData object with all match information
            
        Returns:
            ScanResult with pick or rejection reason
        """
        import time
        start_time = time.time()
        
        try:
            # Step 1: Apply context adjustments to lambda
            lambda_h, lambda_a = match.xg_home, match.xg_away
            
            if match.context:
                lambda_h, lambda_a, _ = self.context_engine.apply_all(
                    lambda_h, lambda_a, match.context
                )
            
            # Step 2: Extract all market probabilities from Poisson matrix
            market_probs_obj = self.extractor.extract_all_markets(lambda_h, lambda_a)
            
            # Convert to dict format for selection engine
            market_probs = {
                'home_win': market_probs_obj.P_home_win,
                'draw': market_probs_obj.P_draw,
                'away_win': market_probs_obj.P_away_win,
                'over_05': market_probs_obj.P_over_05,
                'over_15': market_probs_obj.P_over_15,
                'over_25': market_probs_obj.P_over_25,
                'over_35': market_probs_obj.P_over_35,
                'over_45': market_probs_obj.P_over_45,
                'under_05': market_probs_obj.P_under_05,
                'under_15': market_probs_obj.P_under_15,
                'under_25': market_probs_obj.P_under_25,
                'under_35': market_probs_obj.P_under_35,
                'btts': market_probs_obj.P_btts,
                'home_scores': market_probs_obj.P_home_scores,
                'away_scores': market_probs_obj.P_away_scores,
                'home_cs': market_probs_obj.P_home_cs,
                'away_cs': market_probs_obj.P_away_cs,
                'ah_home_minus_05': market_probs_obj.P_ah_home_minus_05,
                'ah_home_minus_15': market_probs_obj.P_ah_home_minus_15,
                'ah_away_plus_05': market_probs_obj.P_ah_away_plus_05,
                'ah_away_plus_15': market_probs_obj.P_ah_away_plus_15,
            }
            
            # Step 3: Calculate hours to kickoff
            kickoff_dt = datetime.fromisoformat(match.kickoff.replace('Z', '+00:00'))
            now = datetime.now(kickoff_dt.tzinfo)
            hours_to_kickoff = (kickoff_dt - now).total_seconds() / 3600
            
            # Step 4: Select best market
            selection_result = self.selection_engine.select_best_market(
                market_probs=market_probs,
                market_odds=match.odds,
                league=match.league,
                hours_to_kickoff=hours_to_kickoff,
                bookmaker_counts=match.bookmaker_counts,
                data_quality=0.85
            )
            
            # Step 5: Calculate Kelly
            kelly_result = self.kelly_calc.calculate(
                edge=selection_result.edge,
                odds=selection_result.implied_odds,
                confidence=selection_result.confidence,
                volatility=0.2,
                correlation_penalty=0.1
            )
            
            # Step 6: Validate via MiroFish
            mirofish_result = await self.mirofish.validate(
                match=f"{match.home_team} vs {match.away_team}",
                league=match.league,
                market=selection_result.market,
                odds=selection_result.implied_odds,
                model_prob=selection_result.model_prob,
                market_prob=selection_result.market_prob,
                edge=selection_result.edge,
                kelly=kelly_result.kelly_fraction,
                home_form=match.form_home,
                away_form=match.form_away,
                injuries=match.injuries or {'home': [], 'away': []},
                exposure=0.15,
                correlation=0.1
            )
            
            # Step 7: Check if pick is accepted
            if mirofish_result.decision.value == "REJECT":
                processing_time = (time.time() - start_time) * 1000
                return ScanResult(
                    match_id=match.match_id,
                    match=f"{match.home_team} vs {match.away_team}",
                    has_pick=False,
                    pick=None,
                    rejection_reason="MiroFish rejected pick",
                    processing_time_ms=processing_time
                )
            
            # Step 8: Format the pick
            # Get context adjustment summaries
            context_adjustments = []
            if match.context:
                context_adjustments = self.context_engine.get_adjustment_summary()
            
            pick = self.formatter.format_pick(
                match=f"{match.home_team} vs {match.away_team}",
                league=match.league,
                kickoff=match.kickoff,
                market_type=selection_result.market,
                selection=selection_result.selection,
                odds=selection_result.implied_odds,
                model_prob=selection_result.model_prob,
                market_prob=selection_result.market_prob,
                edge=selection_result.edge,
                kelly_pct=mirofish_result.final_kelly,
                omega_score=mirofish_result.omega_score,
                tier=mirofish_result.tier,
                xg_home=lambda_h,
                xg_away=lambda_a,
                context_adjustments=context_adjustments,
                rejected_markets=selection_result.rejected_markets,
                why=mirofish_result.why,
                warn=mirofish_result.warn
            )
            
            processing_time = (time.time() - start_time) * 1000
            
            return ScanResult(
                match_id=match.match_id,
                match=f"{match.home_team} vs {match.away_team}",
                has_pick=True,
                pick=pick,
                rejection_reason=None,
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            return ScanResult(
                match_id=match.match_id,
                match=f"{match.home_team} vs {match.away_team}",
                has_pick=False,
                pick=None,
                rejection_reason=f"Error: {str(e)}",
                processing_time_ms=processing_time
            )
    
    async def scan_matches(self, matches: List[MatchData]) -> List[ScanResult]:
        """
        Scan multiple matches in parallel.
        
        Args:
            matches: List of MatchData objects
            
        Returns:
            List of ScanResult objects
        """
        tasks = [self.scan_match(match) for match in matches]
        results = await asyncio.gather(*tasks)
        return list(results)
    
    def get_picks(self, scan_results: List[ScanResult]) -> List[Pick]:
        """
        Extract valid picks from scan results.
        
        Args:
            scan_results: List of ScanResult objects
            
        Returns:
            List of valid Pick objects
        """
        return [r.pick for r in scan_results if r.has_pick and r.pick]
    
    def get_rejections(self, scan_results: List[ScanResult]) -> List[Dict]:
        """
        Get rejection information from scan results.
        
        Args:
            scan_results: List of ScanResult objects
            
        Returns:
            List of rejection dicts with match info and reasons
        """
        return [
            {
                "match_id": r.match_id,
                "match": r.match,
                "reason": r.rejection_reason
            }
            for r in scan_results if not r.has_pick
        ]


# === EXAMPLE USAGE ===
async def example_usage():
    """Example of how to use MarketScannerV2."""
    scanner = MarketScannerV2()
    
    # Example match data
    match = MatchData(
        match_id="nap_laz_20260417",
        home_team="Napoli",
        away_team="Lazio",
        league="serie_a",
        kickoff="2026-04-17T19:45:00Z",
        xg_home=1.89,
        xg_away=1.32,
        form_home=['W', 'W', 'D', 'W', 'L'],
        form_away=['L', 'D', 'W', 'L', 'W'],
        odds={
            'home_win': 1.75,
            'draw': 3.60,
            'away_win': 4.50,
            'over_25': 2.10,
            'under_25': 1.75,
            'btts': 1.80,
        },
        bookmaker_counts={
            'home_win': 8,
            'draw': 8,
            'away_win': 8,
            'over_25': 12,
            'under_25': 12,
            'btts': 10,
        },
        context={
            'weather': {'wind_ms': 8, 'rain_mm': 0, 'temp_c': 18},
            'travel': {'travel_km': 0, 'hours_since_arrival': 72},
            'european': {'home_played_europe': False, 'away_played_europe': True, 'days_since_european': 3},
            'referee': {'avg_cards': 4.2, 'avg_fouls': 22, 'penalty_rate': 0.18},
            'derby': {'is_derby': False},
            'motivation': {'home_position': 3, 'away_position': 8, 'games_remaining': 8,
                          'home_points_from_safety': 20, 'away_points_from_safety': 12}
        },
        injuries={
            'home': [{'player': 'Osimhen', 'key': True, 'reason': 'ankle'}],
            'away': []
        }
    )
    
    # Scan the match
    result = await scanner.scan_match(match)
    
    if result.has_pick:
        print(f"✅ Pick generated for {result.match}")
        print(f"   Market: {result.pick.selection}")
        print(f"   Odds: {result.pick.odds}")
        print(f"   Edge: {result.pick.edge:.1%}")
        print(f"   Omega: {result.pick.omega_score}")
        print(f"   Tier: {result.pick.tier}")
        print(f"   Processing time: {result.processing_time_ms:.1f}ms")
    else:
        print(f"❌ No pick for {result.match}")
        print(f"   Reason: {result.rejection_reason}")
    
    return result


# === UNIT TESTS ===
async def test_market_scanner_v2():
    """Unit tests for MarketScannerV2."""
    scanner = MarketScannerV2()
    
    # Test match
    match = MatchData(
        match_id="test_match_001",
        home_team="Team A",
        away_team="Team B",
        league="premier_league",
        kickoff=(datetime.utcnow() + timedelta(hours=24)).isoformat() + "Z",
        xg_home=1.8,
        xg_away=1.2,
        form_home=['W', 'W', 'L', 'D', 'W'],
        form_away=['L', 'D', 'W', 'L', 'L'],
        odds={
            'home_win': 1.90,
            'over_25': 2.00,
            'btts': 1.85,
        },
        context={
            'weather': {'wind_ms': 5, 'rain_mm': 0, 'temp_c': 20},
            'travel': {'travel_km': 100, 'hours_since_arrival': 72},
            'european': {'home_played_europe': False, 'away_played_europe': False, 'days_since_european': 7},
            'referee': {'avg_cards': 3.5, 'avg_fouls': 20, 'penalty_rate': 0.2},
            'derby': {'is_derby': False},
            'motivation': {'home_position': 5, 'away_position': 12, 'games_remaining': 10,
                          'home_points_from_safety': 15, 'away_points_from_safety': 8}
        },
        injuries={'home': [], 'away': []}
    )
    
    result = await scanner.scan_match(match)
    
    assert result.processing_time_ms < 1000, "Processing should be under 1 second"
    
    if result.has_pick:
        assert result.pick is not None
        assert result.pick.match == "Team A vs Team B"
        assert result.pick.edge > 0
        assert result.pick.omega_score > 0
        print(f"✅ Generated pick: {result.pick.selection} @ {result.pick.odds}")
    
    print(f"Processing time: {result.processing_time_ms:.1f}ms")
    print("\nMarketScannerV2 tests passed!")
    return True


if __name__ == "__main__":
    # Run example
    print("Running example usage...")
    asyncio.run(example_usage())
    
    # Run tests
    print("\nRunning tests...")
    asyncio.run(test_market_scanner_v2())
