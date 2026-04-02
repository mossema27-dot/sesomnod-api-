"""
Team name normalizer for mapping API team names to football-data.co.uk format.
Supports EPL, La Liga, Bundesliga, Serie A, and Ligue 1.
"""

NORMALIZATION_MAP: dict[str, str] = {
    # EPL
    "man united": "Man United",
    "manchester united": "Man United",
    "man city": "Man City",
    "manchester city": "Man City",
    "spurs": "Tottenham",
    "tottenham hotspur": "Tottenham",
    "wolverhampton": "Wolves",
    "wolves": "Wolves",
    "newcastle united": "Newcastle",
    "brighton & hove albion": "Brighton",
    "brighton": "Brighton",
    "west ham united": "West Ham",
    "nottingham forest": "Nott'm Forest",
    "nottm forest": "Nott'm Forest",
    "leicester city": "Leicester",
    "leeds united": "Leeds",
    # Bundesliga
    "bayer leverkusen": "Leverkusen",
    "borussia dortmund": "Dortmund",
    "borussia m'gladbach": "M'gladbach",
    "rb leipzig": "Leipzig",
    "eintracht frankfurt": "Ein Frankfurt",
    "sc freiburg": "Freiburg",
    "vfb stuttgart": "Stuttgart",
    "hamburger sv": "Hamburg",
    # La Liga
    "atletico madrid": "Ath Madrid",
    "athletic bilbao": "Ath Bilbao",
    "athletic club": "Ath Bilbao",
    "real betis": "Betis",
    "real sociedad": "Sociedad",
    "real valladolid": "Valladolid",
    "rayo vallecano": "Vallecano",
    "deportivo alaves": "Alaves",
    # Serie A
    "inter milan": "Inter",
    "internazionale": "Inter",
    "ac milan": "Milan",
    "as roma": "Roma",
    "hellas verona": "Verona",
    "us sassuolo": "Sassuolo",
    # Ligue 1
    "paris saint-germain": "Paris SG",
    "psg": "Paris SG",
    "olympique marseille": "Marseille",
    "olympique lyonnais": "Lyon",
    "stade rennais": "Rennes",
    "rc lens": "Lens",
    "rc strasbourg": "Strasbourg",
    "ogc nice": "Nice",
    "montpellier hsc": "Montpellier",
}


def normalize_team_name(name: str) -> str:
    """Normalize team name to match football-data.co.uk format."""
    if not name:
        return name
    cleaned = name.lower().strip()
    return NORMALIZATION_MAP.get(cleaned, name)


def find_best_team_match(
    name: str,
    available_teams: list[str],
) -> str | None:
    """
    Fuzzy match team name against available teams in dataset.
    Uses exact match, then case-insensitive, then substring as fallback.
    Returns None if no match found.
    """
    normalized = normalize_team_name(name)

    # Exact match
    if normalized in available_teams:
        return normalized

    # Case-insensitive match
    name_lower = normalized.lower()
    for team in available_teams:
        if team.lower() == name_lower:
            return team

    # Substring match (both directions)
    for team in available_teams:
        if name_lower in team.lower() or team.lower() in name_lower:
            return team

    return None
