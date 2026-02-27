"""
Bankroll Management Module for SesomNod Engine v3.4
Handles play money tracking and bankroll state
"""

from typing import Callable, Any, List, Dict
from datetime import datetime

# Constants
BANKROLL_START = 100.0 # Startbeløp i kroner
BANKROLL_GOAL = 10000.0 # Mål for bankroll

async def ensure_bankroll_tables(db_execute: Callable[[str], Any]):
    """
    Creates bankroll table if it doesn't exist.
    Initializes with starting amount if empty.
    """
    # Create table
    await db_execute("""
        CREATE TABLE IF NOT EXISTS bankroll (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            balance NUMERIC(12,2) DEFAULT 100.00,
            change NUMERIC(12,2) DEFAULT 0,
            source TEXT DEFAULT 'initial'
        )
    """)
    
    # Insert starting bankroll if table is empty
    await db_execute("""
        INSERT INTO bankroll (balance, change, source)
        SELECT 100.00, 0, 'initial'
        WHERE NOT EXISTS (SELECT 1 FROM bankroll LIMIT 1)
    """)

async def get_current_bankroll(db_execute: Callable[[str], Any]) -> float:
    """
    Gets current bankroll balance.
    Returns BANKROLL_START (100.0) if no records found.
    """
    rows = await db_execute("""
        SELECT balance 
        FROM bankroll 
        ORDER BY timestamp DESC 
        LIMIT 1
    """)
    
    if rows and len(rows) > 0:
        # Handle both dict and object formats
        if isinstance(rows[0], dict):
            return float(rows[0].get('balance', BANKROLL_START))
        else:
            # If it's a tuple/object from database driver
            return float(rows[0][0]) if rows[0] else BANKROLL_START
    
    return BANKROLL_START

async def update_bankroll(db_execute: Callable[[str], Any], amount_change: float, source: str = "bet_result"):
    """
    Updates bankroll with a win or loss.
    Positive amount = win, negative = loss.
    """
    current = await get_current_bankroll(db_execute)
    new_balance = current + amount_change
    
    await db_execute(f"""
        INSERT INTO bankroll (balance, change, source)
        VALUES ({new_balance}, {amount_change}, '{source}')
    """)
    
    return new_balance

async def get_bankroll_history(db_execute: Callable[[str], Any], limit: int = 50) -> List[Dict[str, Any]]:
    """
    Returns bankroll history for charting.
    """
    rows = await db_execute(f"""
        SELECT 
            timestamp,
            balance,
            change,
            source
        FROM bankroll 
        ORDER BY timestamp DESC 
        LIMIT {limit}
    """)
    
    return rows if rows else []
