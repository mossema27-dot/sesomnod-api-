"""
Team name normalizer for mapping API team names to football-data.co.uk format.
Supports EPL, La Liga, Bundesliga, Serie A, and Ligue 1.
"""

NORMALIZATION_MAP: dict[str, str] = {
    # ── EPL ──────────────────────────────────────────────────────────
    "man united": "Man United",
    "manchester united": "Man United",
    "manchester united fc": "Man United",
    "man city": "Man City",
    "manchester city": "Man City",
    "manchester city fc": "Man City",
    "spurs": "Tottenham",
    "tottenham hotspur": "Tottenham",
    "tottenham hotspur fc": "Tottenham",
    "wolverhampton wanderers": "Wolves",
    "wolverhampton": "Wolves",
    "wolves": "Wolves",
    "newcastle united": "Newcastle",
    "newcastle united fc": "Newcastle",
    "brighton and hove albion": "Brighton",
    "brighton & hove albion": "Brighton",
    "brighton": "Brighton",
    "west ham united": "West Ham",
    "west ham united fc": "West Ham",
    "west ham": "West Ham",
    "nottingham forest": "Nott'm Forest",
    "nottm forest": "Nott'm Forest",
    "nott'm forest": "Nott'm Forest",
    "leicester city": "Leicester",
    "leicester city fc": "Leicester",
    "leeds united": "Leeds",
    "afc bournemouth": "Bournemouth",
    "ipswich town": "Ipswich",
    "southampton fc": "Southampton",
    "crystal palace fc": "Crystal Palace",
    "everton fc": "Everton",
    "fulham fc": "Fulham",
    "brentford fc": "Brentford",
    # ── Bundesliga ──────────────────────────────────────────────────
    "bayer leverkusen": "Leverkusen",
    "bayer 04 leverkusen": "Leverkusen",
    "borussia dortmund": "Dortmund",
    "borussia monchengladbach": "M'gladbach",
    "borussia m'gladbach": "M'gladbach",
    "borussia mönchengladbach": "M'gladbach",
    "rb leipzig": "RB Leipzig",
    "rasenballsport leipzig": "RB Leipzig",
    "eintracht frankfurt": "Ein Frankfurt",
    "sc freiburg": "Freiburg",
    "sport-club freiburg": "Freiburg",
    "vfb stuttgart": "Stuttgart",
    "tsg hoffenheim": "Hoffenheim",
    "tsg 1899 hoffenheim": "Hoffenheim",
    "fc augsburg": "Augsburg",
    "1. fc union berlin": "Union Berlin",
    "fc union berlin": "Union Berlin",
    "sv werder bremen": "Werder Bremen",
    "werder bremen": "Werder Bremen",
    "vfl wolfsburg": "Wolfsburg",
    "1. fsv mainz 05": "Mainz",
    "fsv mainz 05": "Mainz",
    "mainz 05": "Mainz",
    "fc bayern munich": "Bayern Munich",
    "fc bayern münchen": "Bayern Munich",
    "vfl bochum 1848": "Bochum",
    "vfl bochum": "Bochum",
    "1. fc heidenheim 1846": "Heidenheim",
    "fc heidenheim 1846": "Heidenheim",
    "fc heidenheim": "Heidenheim",
    "fc st. pauli": "St Pauli",
    "fc st pauli": "St Pauli",
    "holstein kiel": "Holstein Kiel",
    "hamburger sv": "Hamburg",
    # ── La Liga ─────────────────────────────────────────────────────
    "atletico madrid": "Ath Madrid",
    "atletico de madrid": "Ath Madrid",
    "club atletico de madrid": "Ath Madrid",
    "athletic bilbao": "Ath Bilbao",
    "athletic club": "Ath Bilbao",
    "real betis": "Betis",
    "real betis balompie": "Betis",
    "real sociedad": "Sociedad",
    "real valladolid": "Valladolid",
    "real valladolid cf": "Valladolid",
    "rayo vallecano": "Vallecano",
    "deportivo alaves": "Alaves",
    "cd alaves": "Alaves",
    "cd leganes": "Leganes",
    "rcd espanyol": "Espanol",
    "rc celta": "Celta",
    "rc celta de vigo": "Celta",
    "celta vigo": "Celta",
    "ca osasuna": "Osasuna",
    "ud las palmas": "Las Palmas",
    "rcd mallorca": "Mallorca",
    "girona fc": "Girona",
    "villarreal cf": "Villarreal",
    "sevilla fc": "Sevilla",
    "valencia cf": "Valencia",
    "fc barcelona": "Barcelona",
    "real madrid cf": "Real Madrid",
    # ── Serie A ─────────────────────────────────────────────────────
    "inter milan": "Inter",
    "fc internazionale milano": "Inter",
    "internazionale": "Inter",
    "ac milan": "Milan",
    "as roma": "Roma",
    "ss lazio": "Lazio",
    "s.s. lazio": "Lazio",
    "hellas verona": "Verona",
    "hellas verona fc": "Verona",
    "udinese calcio": "Udinese",
    "us sassuolo": "Sassuolo",
    "us lecce": "Lecce",
    "uc sampdoria": "Sampdoria",
    "ssc napoli": "Napoli",
    "acf fiorentina": "Fiorentina",
    "torino fc": "Torino",
    "genoa cfc": "Genoa",
    "atalanta bc": "Atalanta",
    "cagliari calcio": "Cagliari",
    "juventus fc": "Juventus",
    "bologna fc 1909": "Bologna",
    "bologna fc": "Bologna",
    "como 1907": "Como",
    "ac monza": "Monza",
    "parma calcio 1913": "Parma",
    "venezia fc": "Venezia",
    "empoli fc": "Empoli",
    # ── Ligue 1 ─────────────────────────────────────────────────────
    "paris saint-germain": "Paris SG",
    "paris saint germain": "Paris SG",
    "paris sg": "Paris SG",
    "psg": "Paris SG",
    "olympique de marseille": "Marseille",
    "olympique marseille": "Marseille",
    "olympique lyonnais": "Lyon",
    "olympique lyon": "Lyon",
    "stade rennais fc": "Rennes",
    "stade rennais": "Rennes",
    "rc lens": "Lens",
    "rc strasbourg alsace": "Strasbourg",
    "rc strasbourg": "Strasbourg",
    "ogc nice": "Nice",
    "montpellier hsc": "Montpellier",
    "as monaco": "Monaco",
    "as saint-etienne": "St Etienne",
    "as saint etienne": "St Etienne",
    "saint-etienne": "St Etienne",
    "losc lille": "Lille",
    "stade brestois 29": "Brest",
    "stade brest": "Brest",
    "fc nantes": "Nantes",
    "aj auxerre": "Auxerre",
    "angers sco": "Angers",
    "le havre ac": "Le Havre",
    "toulouse fc": "Toulouse",
    "stade de reims": "Reims",
    # ── Champions League / Europa League common names ───────────────
    "club brugge kv": "Club Brugge",
    "psv eindhoven": "PSV",
    "feyenoord rotterdam": "Feyenoord",
    "sporting cp": "Sporting",
    "sl benfica": "Benfica",
    "fc porto": "Porto",
    "galatasaray sk": "Galatasaray",
    "red bull salzburg": "Salzburg",
    "celtic fc": "Celtic",
    "rangers fc": "Rangers",
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
