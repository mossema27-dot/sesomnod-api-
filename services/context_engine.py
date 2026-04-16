"""
SesomNod Context Engine
Adjusts lambda (expected goals) based on contextual factors.
"""

from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class AdjustmentLog:
    """Log entry for a context adjustment."""
    factor: str
    original_lambda_h: float
    original_lambda_a: float
    adjusted_lambda_h: float
    adjusted_lambda_a: float
    reason: str


class ContextEngine:
    """
    Adjusts expected goals (lambda) based on contextual factors.
    
    Each adjustment method takes lambda_home and lambda_away, applies
    a contextual adjustment, and returns the adjusted values.
    """
    
    def __init__(self):
        """Initialize the Context Engine."""
        self.adjustment_log: List[AdjustmentLog] = []
    
    def adjust_for_weather(self, lambda_h: float, lambda_a: float,
                           wind_ms: float, rain_mm: float,
                           temp_c: float) -> Tuple[float, float]:
        """
        Adjust lambda based on weather conditions.
        
        Args:
            lambda_h: Original home expected goals
            lambda_a: Original away expected goals
            wind_ms: Wind speed in m/s
            rain_mm: Rainfall in mm
            temp_c: Temperature in Celsius
            
        Returns:
            Tuple of (adjusted_lambda_h, adjusted_lambda_a)
        """
        factor = 1.0
        reasons = []
        
        # Wind adjustments
        if wind_ms > 15:
            factor *= 0.85
            reasons.append(f"Vind {wind_ms:.0f} m/s: -15%")
        elif wind_ms > 10:
            factor *= 0.92
            reasons.append(f"Vind {wind_ms:.0f} m/s: -8%")
        
        # Rain adjustments
        if rain_mm > 5:
            factor *= 0.95
            reasons.append(f"Regn {rain_mm:.1f}mm: -5%")
        
        # Temperature adjustments
        if temp_c < 0:
            factor *= 0.97
            reasons.append(f"Kulde {temp_c:.0f}C: -3%")
        elif temp_c > 35:
            factor *= 0.90
            reasons.append(f"Ekstrem varme {temp_c:.0f}C: -10%")
        
        return lambda_h * factor, lambda_a * factor
    
    def adjust_for_travel(self, lambda_h: float, lambda_a: float,
                          travel_km: float,
                          hours_since_arrival: float) -> Tuple[float, float]:
        """
        Adjust lambda based on travel distance and recovery time.
        
        Args:
            lambda_h: Original home expected goals
            lambda_a: Original away expected goals
            travel_km: Distance traveled by away team in km
            hours_since_arrival: Hours since away team arrived
            
        Returns:
            Tuple of (adjusted_lambda_h, adjusted_lambda_a)
        """
        away_factor = 1.0
        
        # Distance-based adjustment
        if travel_km > 5000:  # Intercontinental
            away_factor *= 0.85
        elif travel_km > 3000:
            away_factor *= 0.90
        elif travel_km > 1500:
            away_factor *= 0.95
        
        # Recovery time adjustment (if arrived recently)
        if hours_since_arrival < 48:
            away_factor *= 0.95
        
        return lambda_h, lambda_a * away_factor
    
    def adjust_for_european_hangover(self, lambda_h: float, lambda_a: float,
                                     home_played_europe: bool,
                                     away_played_europe: bool,
                                     days_since_european: int) -> Tuple[float, float]:
        """
        Adjust lambda for European competition fatigue.
        
        Args:
            lambda_h: Original home expected goals
            lambda_a: Original away expected goals
            home_played_europe: Whether home team played in Europe recently
            away_played_europe: Whether away team played in Europe recently
            days_since_european: Days since European match
            
        Returns:
            Tuple of (adjusted_lambda_h, adjusted_lambda_a)
        """
        home_factor = 1.0
        away_factor = 1.0
        
        # Home team European fatigue
        if home_played_europe and days_since_european < 4:  # Less than 96 hours
            home_factor *= 0.93  # -7% for < 96h
            if days_since_european < 3:  # Less than 72 hours
                home_factor *= 0.97  # Additional -3%
        
        # Away team European fatigue
        if away_played_europe and days_since_european < 4:
            away_factor *= 0.93
            if days_since_european < 3:
                away_factor *= 0.97
        
        return lambda_h * home_factor, lambda_a * away_factor
    
    def adjust_for_referee(self, lambda_h: float, lambda_a: float,
                           ref_avg_cards: float,
                           ref_avg_fouls: float,
                           ref_penalty_rate: float) -> Tuple[float, float]:
        """
        Adjust lambda based on referee tendencies.
        
        Args:
            lambda_h: Original home expected goals
            lambda_a: Original away expected goals
            ref_avg_cards: Average cards per game for this referee
            ref_avg_fouls: Average fouls per game
            ref_penalty_rate: Penalties awarded per game
            
        Returns:
            Tuple of (adjusted_lambda_h, adjusted_lambda_a)
        """
        factor = 1.0
        
        # High card rate = more set pieces = potentially more goals
        if ref_avg_cards > 5.0:
            factor *= 1.03
        
        # Low penalty rate = fewer goals from penalties
        if ref_penalty_rate < 0.1:
            factor *= 0.98
        
        # High foul rate = more interruptions = fewer goals
        if ref_avg_fouls > 25:
            factor *= 0.97
        
        return lambda_h * factor, lambda_a * factor
    
    def adjust_for_derby(self, lambda_h: float, lambda_a: float,
                         is_derby: bool,
                         historical_derby_avg_goals: Optional[float] = None) -> Tuple[float, float]:
        """
        Adjust lambda for derby matches.
        
        Args:
            lambda_h: Original home expected goals
            lambda_a: Original away expected goals
            is_derby: Whether this is a derby match
            historical_derby_avg_goals: Historical average goals in this derby
            
        Returns:
            Tuple of (adjusted_lambda_h, adjusted_lambda_a)
        """
        if not is_derby:
            return lambda_h, lambda_a
        
        factor = 1.0
        
        # Use historical data if available
        if historical_derby_avg_goals is not None:
            if historical_derby_avg_goals < 2.0:
                factor = 0.90  # Tight derbies
            elif historical_derby_avg_goals > 3.0:
                factor = 1.05  # Open derbies
        else:
            # Default derby adjustment
            factor = 0.92  # Slight reduction for intensity
        
        return lambda_h * factor, lambda_a * factor
    
    def adjust_for_motivation(self, lambda_h: float, lambda_a: float,
                              home_position: int,
                              away_position: int,
                              games_remaining: int,
                              home_points_from_safety: int,
                              away_points_from_safety: int) -> Tuple[float, float]:
        """
        Adjust lambda based on team motivation (table position, relegation battle).
        
        Args:
            lambda_h: Original home expected goals
            lambda_a: Original away expected goals
            home_position: Home team league position
            away_position: Away team league position
            games_remaining: Games remaining in season
            home_points_from_safety: Points from relegation zone (negative = in danger)
            away_points_from_safety: Points from relegation zone
            
        Returns:
            Tuple of (adjusted_lambda_h, adjusted_lambda_a)
        """
        home_factor = 1.0
        away_factor = 1.0
        
        # Relegation battle (within 6 points of drop zone, less than 10 games left)
        if games_remaining <= 10:
            if home_points_from_safety < 6:
                home_factor *= 1.08  # Desperation factor
            if away_points_from_safety < 6:
                away_factor *= 1.08
        
        # Title race (top 3 positions, less than 5 games left)
        if games_remaining <= 5:
            if home_position <= 3:
                home_factor *= 1.05
            if away_position <= 3:
                away_factor *= 1.05
        
        # Mid-table with nothing to play for (positions 8-14, last 5 games)
        if games_remaining <= 5:
            if 8 <= home_position <= 14 and home_points_from_safety > 10:
                home_factor *= 0.95  # Less motivation
            if 8 <= away_position <= 14 and away_points_from_safety > 10:
                away_factor *= 0.95
        
        return lambda_h * home_factor, lambda_a * away_factor
    
    def apply_all(self, lambda_h: float, lambda_a: float,
                  context: Dict) -> Tuple[float, float, List[AdjustmentLog]]:
        """
        Apply all context adjustments in the correct order.
        
        Order matters: Weather -> Travel -> European -> Referee -> Derby -> Motivation
        
        Args:
            lambda_h: Original home expected goals
            lambda_a: Original away expected goals
            context: Dictionary containing all context data
            
        Returns:
            Tuple of (adjusted_lambda_h, adjusted_lambda_a, adjustment_log)
        """
        self.adjustment_log = []
        original_h, original_a = lambda_h, lambda_a
        
        # 1. Weather adjustment
        if 'weather' in context:
            w = context['weather']
            new_h, new_a = self.adjust_for_weather(
                lambda_h, lambda_a,
                wind_ms=w.get('wind_ms', 0),
                rain_mm=w.get('rain_mm', 0),
                temp_c=w.get('temp_c', 20)
            )
            self.adjustment_log.append(AdjustmentLog(
                factor='weather',
                original_lambda_h=original_h,
                original_lambda_a=original_a,
                adjusted_lambda_h=new_h,
                adjusted_lambda_a=new_a,
                reason=f"Wind: {w.get('wind_ms', 0)}m/s, Rain: {w.get('rain_mm', 0)}mm"
            ))
            lambda_h, lambda_a = new_h, new_a
        
        # 2. Travel adjustment
        if 'travel' in context:
            t = context['travel']
            new_h, new_a = self.adjust_for_travel(
                lambda_h, lambda_a,
                travel_km=t.get('travel_km', 0),
                hours_since_arrival=t.get('hours_since_arrival', 72)
            )
            self.adjustment_log.append(AdjustmentLog(
                factor='travel',
                original_lambda_h=original_h,
                original_lambda_a=original_a,
                adjusted_lambda_h=new_h,
                adjusted_lambda_a=new_a,
                reason=f"Travel: {t.get('travel_km', 0)}km"
            ))
            lambda_h, lambda_a = new_h, new_a
        
        # 3. European hangover
        if 'european' in context:
            e = context['european']
            new_h, new_a = self.adjust_for_european_hangover(
                lambda_h, lambda_a,
                home_played_europe=e.get('home_played_europe', False),
                away_played_europe=e.get('away_played_europe', False),
                days_since_european=e.get('days_since_european', 7)
            )
            self.adjustment_log.append(AdjustmentLog(
                factor='european_hangover',
                original_lambda_h=original_h,
                original_lambda_a=original_a,
                adjusted_lambda_h=new_h,
                adjusted_lambda_a=new_a,
                reason=f"Home EU: {e.get('home_played_europe', False)}, Away EU: {e.get('away_played_europe', False)}"
            ))
            lambda_h, lambda_a = new_h, new_a
        
        # 4. Referee adjustment
        if 'referee' in context:
            r = context['referee']
            new_h, new_a = self.adjust_for_referee(
                lambda_h, lambda_a,
                ref_avg_cards=r.get('avg_cards', 3.5),
                ref_avg_fouls=r.get('avg_fouls', 20),
                ref_penalty_rate=r.get('penalty_rate', 0.2)
            )
            self.adjustment_log.append(AdjustmentLog(
                factor='referee',
                original_lambda_h=original_h,
                original_lambda_a=original_a,
                adjusted_lambda_h=new_h,
                adjusted_lambda_a=new_a,
                reason=f"Cards: {r.get('avg_cards', 3.5)}/game"
            ))
            lambda_h, lambda_a = new_h, new_a
        
        # 5. Derby adjustment
        if 'derby' in context:
            d = context['derby']
            new_h, new_a = self.adjust_for_derby(
                lambda_h, lambda_a,
                is_derby=d.get('is_derby', False),
                historical_derby_avg_goals=d.get('historical_avg_goals')
            )
            if d.get('is_derby', False):
                self.adjustment_log.append(AdjustmentLog(
                    factor='derby',
                    original_lambda_h=original_h,
                    original_lambda_a=original_a,
                    adjusted_lambda_h=new_h,
                    adjusted_lambda_a=new_a,
                    reason="Derby match"
                ))
            lambda_h, lambda_a = new_h, new_a
        
        # 6. Motivation adjustment
        if 'motivation' in context:
            m = context['motivation']
            new_h, new_a = self.adjust_for_motivation(
                lambda_h, lambda_a,
                home_position=m.get('home_position', 10),
                away_position=m.get('away_position', 10),
                games_remaining=m.get('games_remaining', 10),
                home_points_from_safety=m.get('home_points_from_safety', 10),
                away_points_from_safety=m.get('away_points_from_safety', 10)
            )
            self.adjustment_log.append(AdjustmentLog(
                factor='motivation',
                original_lambda_h=original_h,
                original_lambda_a=original_a,
                adjusted_lambda_h=new_h,
                adjusted_lambda_a=new_a,
                reason=f"Home pos: {m.get('home_position', 10)}, Away pos: {m.get('away_position', 10)}"
            ))
            lambda_h, lambda_a = new_h, new_a
        
        return lambda_h, lambda_a, self.adjustment_log
    
    def get_adjustment_summary(self) -> List[str]:
        """
        Get human-readable summary of all adjustments made.
        
        Returns:
            List of adjustment descriptions
        """
        summaries = []
        for log in self.adjustment_log:
            h_change = (log.adjusted_lambda_h / log.original_lambda_h - 1) * 100
            a_change = (log.adjusted_lambda_a / log.original_lambda_a - 1) * 100
            summaries.append(
                f"{log.factor}: Home {h_change:+.1f}%, Away {a_change:+.1f}% - {log.reason}"
            )
        return summaries


# === UNIT TESTS ===
def test_context_engine():
    """Unit tests for ContextEngine."""
    engine = ContextEngine()
    
    # Test weather adjustment
    h, a = engine.adjust_for_weather(1.5, 1.2, wind_ms=12, rain_mm=0, temp_c=20)
    assert h < 1.5  # Wind should reduce lambda
    assert a < 1.2
    
    # Test travel adjustment
    h, a = engine.adjust_for_travel(1.5, 1.2, travel_km=2000, hours_since_arrival=24)
    assert h == 1.5  # Home unchanged
    assert a < 1.2  # Away reduced due to travel
    
    # Test European hangover
    h, a = engine.adjust_for_european_hangover(1.5, 1.2, True, False, 2)
    assert h < 1.5  # Home reduced (played Europe 2 days ago)
    assert a == 1.2  # Away unchanged
    
    # Test referee adjustment
    h, a = engine.adjust_for_referee(1.5, 1.2, ref_avg_cards=6, ref_avg_fouls=15, ref_penalty_rate=0.15)
    # High cards might increase goals slightly
    
    # Test derby adjustment
    h, a = engine.adjust_for_derby(1.5, 1.2, True, 1.8)
    assert h < 1.5  # Derby should reduce lambda
    assert a < 1.2
    
    # Test motivation adjustment
    h, a = engine.adjust_for_motivation(1.5, 1.2, 18, 15, 8, 3, 8)
    # Both teams near relegation should increase lambda (desperation)
    
    # Test apply_all
    context = {
        'weather': {'wind_ms': 12, 'rain_mm': 3, 'temp_c': 15},
        'travel': {'travel_km': 1500, 'hours_since_arrival': 48},
        'european': {'home_played_europe': False, 'away_played_europe': True, 'days_since_european': 3},
        'referee': {'avg_cards': 4, 'avg_fouls': 18, 'penalty_rate': 0.2},
        'derby': {'is_derby': False},
        'motivation': {'home_position': 8, 'away_position': 16, 'games_remaining': 8, 
                      'home_points_from_safety': 12, 'away_points_from_safety': 4}
    }
    
    h, a, logs = engine.apply_all(1.5, 1.2, context)
    assert len(logs) == 6  # All 6 adjustments applied
    
    print("All ContextEngine tests passed!")
    return True


if __name__ == "__main__":
    test_context_engine()
