"""
SesomNod Pick Formatter
Formats picks for different output channels (Telegram, Notion, JSON API).
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
import hashlib
import json


@dataclass
class Pick:
    """Complete pick data structure."""
    # Identity
    match: str
    league: str
    kickoff: str
    match_id: str = ""
    
    # Selected market
    market_type: str = ""
    selection: str = ""
    odds: float = 0.0
    bookmaker: str = ""
    
    # Model data
    model_prob: float = 0.0
    market_prob: float = 0.0
    edge: float = 0.0
    kelly_pct: float = 0.0
    omega_score: int = 0
    tier: str = ""
    
    # xG data
    xg_home: float = 0.0
    xg_away: float = 0.0
    combined_xg: float = 0.0
    
    # Context
    context_adjustments: List[str] = field(default_factory=list)
    
    # Alternative markets
    rejected_markets: List[Dict] = field(default_factory=list)
    
    # MiroFish output
    why: str = ""
    warn: Optional[str] = None
    
    # Proof
    sha256_hash: str = ""
    timestamp: str = ""
    
    def __post_init__(self):
        """Generate hash and timestamp if not provided."""
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()
        if not self.sha256_hash:
            self.sha256_hash = self._generate_hash()
    
    def _generate_hash(self) -> str:
        """Generate SHA256 hash of pick data for verification."""
        data = f"{self.match}:{self.market_type}:{self.odds}:{self.model_prob}:{self.timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]


class PickFormatter:
    """Formats picks for various output channels."""
    
    def __init__(self):
        """Initialize the formatter."""
        pass
    
    def format_pick(self, 
                    match: str,
                    league: str,
                    kickoff: str,
                    market_type: str,
                    selection: str,
                    odds: float,
                    model_prob: float,
                    market_prob: float,
                    edge: float,
                    kelly_pct: float,
                    omega_score: int,
                    tier: str,
                    xg_home: float,
                    xg_away: float,
                    context_adjustments: List[str],
                    rejected_markets: List[Dict],
                    why: str,
                    warn: Optional[str] = None,
                    bookmaker: str = "Pinnacle") -> Pick:
        """
        Create a complete Pick object from raw data.
        
        Args:
            match: Match name (e.g., "Napoli vs Lazio")
            league: League name
            kickoff: Kickoff time (ISO format)
            market_type: Market type code
            selection: Human-readable selection
            odds: Decimal odds
            model_prob: Model probability
            market_prob: Market implied probability
            edge: Edge as decimal
            kelly_pct: Kelly stake percentage
            omega_score: Omega score (0-100)
            tier: Tier (ATOMIC, EDGE, MONITORED)
            xg_home: Home team xG
            xg_away: Away team xG
            context_adjustments: List of context adjustment descriptions
            rejected_markets: List of rejected market dicts
            why: Explanation text
            warn: Warning text (optional)
            bookmaker: Bookmaker name
            
        Returns:
            Complete Pick object
        """
        match_id = f"{match.replace(' ', '_').lower()}_{kickoff[:10]}"
        
        return Pick(
            match=match,
            league=league,
            kickoff=kickoff,
            match_id=match_id,
            market_type=market_type,
            selection=selection,
            odds=odds,
            bookmaker=bookmaker,
            model_prob=model_prob,
            market_prob=market_prob,
            edge=edge,
            kelly_pct=kelly_pct,
            omega_score=omega_score,
            tier=tier,
            xg_home=xg_home,
            xg_away=xg_away,
            combined_xg=xg_home + xg_away,
            context_adjustments=context_adjustments,
            rejected_markets=rejected_markets,
            why=why,
            warn=warn
        )
    
    def format_telegram(self, pick: Pick) -> str:
        """
        Format pick for Telegram message.
        
        Args:
            pick: Pick object
            
        Returns:
            Formatted Telegram message string
        """
        lines = []
        
        # Header
        tier_emoji = {"ATOMIC": "🎯", "EDGE": "⚡", "MONITORED": "👁"}.get(pick.tier, "📊")
        lines.append(f"{tier_emoji} <b>{pick.match}</b>")
        lines.append(f"📅 {pick.league} | {pick.kickoff[:10]} {pick.kickoff[11:16]}")
        lines.append("")
        
        # Pick
        lines.append(f"🎲 <b>{pick.selection}</b> @ {pick.odds:.2f}")
        lines.append("")
        
        # Stats
        lines.append(f"📊 Modell: {pick.model_prob:.1%} | Marked: {pick.market_prob:.1%}")
        lines.append(f"📈 Edge: {pick.edge:.1%} | Kelly: {pick.kelly_pct:.1%}")
        lines.append(f"🎯 Omega: {pick.omega_score}/100 | Tier: {pick.tier}")
        lines.append("")
        
        # xG
        lines.append(f"⚽ xG: {pick.xg_home:.2f} vs {pick.xg_away:.2f} (Total: {pick.combined_xg:.2f})")
        lines.append("")
        
        # Why
        lines.append(f"💡 {pick.why}")
        
        # Warning
        if pick.warn:
            lines.append("")
            lines.append(f"⚠️ {pick.warn}")
        
        # Context adjustments
        if pick.context_adjustments:
            lines.append("")
            lines.append("🔧 Justeringer:")
            for adj in pick.context_adjustments:
                lines.append(f"  • {adj}")
        
        # Rejected markets
        if pick.rejected_markets:
            lines.append("")
            lines.append("❌ Avvist:")
            for rej in pick.rejected_markets[:3]:  # Max 3
                lines.append(f"  • {rej['market']}: {rej['reason']}")
        
        # Footer
        lines.append("")
        lines.append(f"🆔 {pick.sha256_hash}")
        
        return "\n".join(lines)
    
    def format_notion(self, pick: Pick) -> Dict[str, Any]:
        """
        Format pick for Notion database entry.
        
        Args:
            pick: Pick object
            
        Returns:
            Dict formatted for Notion API
        """
        return {
            "parent": {"database_id": "YOUR_DATABASE_ID"},
            "properties": {
                "Match": {"title": [{"text": {"content": pick.match}}]},
                "League": {"select": {"name": pick.league}},
                "Kickoff": {"date": {"start": pick.kickoff}},
                "Market": {"select": {"name": pick.market_type}},
                "Selection": {"rich_text": [{"text": {"content": pick.selection}}]},
                "Odds": {"number": pick.odds},
                "Model Prob": {"number": pick.model_prob},
                "Market Prob": {"number": pick.market_prob},
                "Edge": {"number": pick.edge},
                "Kelly": {"number": pick.kelly_pct},
                "Omega": {"number": pick.omega_score},
                "Tier": {"select": {"name": pick.tier}},
                "xG Home": {"number": pick.xg_home},
                "xG Away": {"number": pick.xg_away},
                "Why": {"rich_text": [{"text": {"content": pick.why}}]},
                "Warning": {"rich_text": [{"text": {"content": pick.warn or ""}}]},
                "Hash": {"rich_text": [{"text": {"content": pick.sha256_hash}}]},
                "Timestamp": {"date": {"start": pick.timestamp}},
                "Status": {"select": {"name": "Pending"}}
            }
        }
    
    def format_json_api(self, pick: Pick) -> Dict[str, Any]:
        """
        Format pick for JSON API response.
        
        Args:
            pick: Pick object
            
        Returns:
            Dict for JSON serialization
        """
        return {
            "identity": {
                "match": pick.match,
                "match_id": pick.match_id,
                "league": pick.league,
                "kickoff": pick.kickoff
            },
            "pick": {
                "market_type": pick.market_type,
                "selection": pick.selection,
                "odds": pick.odds,
                "bookmaker": pick.bookmaker
            },
            "model": {
                "model_prob": pick.model_prob,
                "market_prob": pick.market_prob,
                "edge": pick.edge,
                "kelly_pct": pick.kelly_pct,
                "omega_score": pick.omega_score,
                "tier": pick.tier
            },
            "xg": {
                "home": pick.xg_home,
                "away": pick.xg_away,
                "combined": pick.combined_xg
            },
            "context": {
                "adjustments": pick.context_adjustments
            },
            "alternatives": pick.rejected_markets,
            "explanation": {
                "why": pick.why,
                "warning": pick.warn
            },
            "proof": {
                "hash": pick.sha256_hash,
                "timestamp": pick.timestamp
            }
        }
    
    def format_csv_row(self, pick: Pick) -> Dict[str, str]:
        """
        Format pick as CSV row.
        
        Args:
            pick: Pick object
            
        Returns:
            Dict with string values for CSV export
        """
        return {
            "match": pick.match,
            "league": pick.league,
            "kickoff": pick.kickoff,
            "market": pick.market_type,
            "selection": pick.selection,
            "odds": str(pick.odds),
            "model_prob": f"{pick.model_prob:.3f}",
            "market_prob": f"{pick.market_prob:.3f}",
            "edge": f"{pick.edge:.3f}",
            "kelly": f"{pick.kelly_pct:.3f}",
            "omega": str(pick.omega_score),
            "tier": pick.tier,
            "xg_home": f"{pick.xg_home:.2f}",
            "xg_away": f"{pick.xg_away:.2f}",
            "why": pick.why,
            "warning": pick.warn or "",
            "hash": pick.sha256_hash,
            "timestamp": pick.timestamp
        }


# === UNIT TESTS ===
def test_pick_formatter():
    """Unit tests for PickFormatter."""
    formatter = PickFormatter()
    
    # Create test pick
    pick = formatter.format_pick(
        match="Napoli vs Lazio",
        league="Serie A",
        kickoff="2026-04-17T19:45:00Z",
        market_type="over_25",
        selection="Over 2.5 mål",
        odds=2.10,
        model_prob=0.673,
        market_prob=0.476,
        edge=0.197,
        kelly_pct=0.042,
        omega_score=78,
        tier="ATOMIC",
        xg_home=1.89,
        xg_away=1.67,
        context_adjustments=[
            "Vind 12 m/s: lambda redusert 8%",
            "Lazio spilte Europa for 68t siden: lambda_away redusert 7%"
        ],
        rejected_markets=[
            {"market": "1X2 Napoli", "edge": "8.2%", "reason": "Under terskel"},
            {"market": "BTTS", "edge": "11.4%", "reason": "Lavere composite score"}
        ],
        why="Napoli scorer 1.89 xG hjemme. Lazio slipper inn 1.67 borte. Kombinert 3.56 xG.",
        warn="Lazio holdt under 2.5 i 3 av siste 5 bortekamper."
    )
    
    # Test Telegram format
    telegram = formatter.format_telegram(pick)
    assert "Napoli vs Lazio" in telegram
    assert "Over 2.5 mål" in telegram
    assert "78/100" in telegram
    assert "ATOMIC" in telegram
    print("Telegram format OK")
    
    # Test Notion format
    notion = formatter.format_notion(pick)
    assert "properties" in notion
    assert notion["properties"]["Match"]["title"][0]["text"]["content"] == "Napoli vs Lazio"
    print("Notion format OK")
    
    # Test JSON API format
    json_api = formatter.format_json_api(pick)
    assert json_api["identity"]["match"] == "Napoli vs Lazio"
    assert json_api["model"]["omega_score"] == 78
    print("JSON API format OK")
    
    # Test CSV format
    csv_row = formatter.format_csv_row(pick)
    assert csv_row["match"] == "Napoli vs Lazio"
    assert csv_row["omega"] == "78"
    print("CSV format OK")
    
    # Test hash generation
    assert len(pick.sha256_hash) == 16
    assert pick.timestamp != ""
    print("Hash/timestamp OK")
    
    print("\nAll PickFormatter tests passed!")
    return True


if __name__ == "__main__":
    test_pick_formatter()
