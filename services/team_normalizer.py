"""
Team name normalizer for mapping API team names to football-data.co.uk format.
Supports EPL, La Liga, Bundesliga, Serie A, and Ligue 1.

2026-09-16: diakritika-folding, ordbasert entydig matching (ingen delstreng), aliaser for 2026/27-opprykk.
"""
import re
import unicodedata


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
    "espanyol": "Espanol",
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
    "lille osc": "Lille",  # sniper_live.TEAM_NORMALIZER skriver «Lille» → «Lille OSC»
    "stade brestois 29": "Brest",
    "stade brest": "Brest",
    "fc nantes": "Nantes",
    "aj auxerre": "Auxerre",
    "angers sco": "Angers",
    "le havre ac": "Le Havre",
    "toulouse fc": "Toulouse",
    "stade de reims": "Reims",
    # ── 2026/27 opprykk + diakritiske varianter (incident 2026-09-16) ──
    "coventry city": "Coventry",
    "hull city": "Hull",
    "deportivo la coruna": "La Coruna",
    "deportivo de la coruna": "La Coruna",
    "rc deportivo": "La Coruna",
    "malaga cf": "Malaga",
    "racing santander": "Santander",
    "real racing club": "Santander",
    "sv elversberg": "Elversberg",
    "sc paderborn 07": "Paderborn",
    "sc paderborn": "Paderborn",
    "fc schalke 04": "Schalke 04",
    "schalke": "Schalke 04",
    "le mans fc": "Le Mans",
    "estac troyes": "Troyes",
    "es troyes ac": "Troyes",
    "bayern munchen": "Bayern Munich",
    "bayern munich": "Bayern Munich",
    "1. fc koln": "FC Koln",
    "fc koln": "FC Koln",
    "koln": "FC Koln",
    "atletico": "Ath Madrid",
    "ipswich town": "Ipswich",
    "leicester city": "Leicester",
    "leeds united": "Leeds",
    "real oviedo": "Oviedo",
    "real valladolid": "Valladolid",
    "sporting gijon": "Sp Gijon",
    "hull": "Hull",
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


def _fold(s: str) -> str:
    """Diakritika fjernes (Atlético → atletico, Köln → koln), små bokstaver, tegnsetting → mellomrom."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().replace("&", " ").replace("'", " ").replace("\u2019", " ")
    s = re.sub(r"[.\-_/,()]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


_FOLDED_MAP: dict[str, str] = {_fold(k): v for k, v in NORMALIZATION_MAP.items()}

# Ord som får skille et innkommende navn fra datasettets kortform uten å gjøre treffet tvetydig
# («Hull City» → Hull, «Deportivo La Coruna» → La Coruna). Alle andre ekstraord (f.eks. «Miami» i
# «Inter Miami») gjør at treffet nektes: ingen vilkårlig match.
GENERIC_TOKENS = frozenset({
    "fc", "cf", "sc", "sv", "ac", "as", "us", "ss", "ssc", "cd", "ca", "ud", "rcd", "rc", "afc", "cfc", "bc",
    "bk", "if", "fk", "sk", "kv", "hsc", "sco", "losc", "ogc", "aj", "tsg", "vfl", "vfb", "fsv", "bsc", "spvgg",
    "club", "calcio", "borussia", "bayer", "eintracht", "estac", "olympique", "stade",
    "1", "04", "05", "07", "09", "96", "98", "1846", "1848", "1899", "1904", "1907", "1909", "1913",
})
# Bevisst IKKE generiske (de skiller ekte klubber): real, deportivo, racing, sporting, athletic, city, town, united, utd,
# de/la/le. «Real Santander», «Deportivo Español», «Coventry United», «Malaga City» får dermed ingen match; de kjente
# Big5-formene dekkes av eksplisitte aliaser (hull city, coventry city, deportivo la coruna, racing santander, …).


def normalize_team_name(name: str) -> str:
    """Normalize team name to match football-data.co.uk format (alias-oppslag med diakritika-folding)."""
    if not name:
        return name
    return _FOLDED_MAP.get(_fold(name), name)


def find_best_team_match(
    name: str,
    available_teams: list[str],
) -> str | None:
    """
    Entydig match mot lagene i datasettet. Rekkefølge: alias → eksakt (foldet) → alle datasett-ord finnes som
    hele ord i navnet og resten er rene klubbtype-ord (fc, sc, 04, borussia …).
    Aldri delstreng («Remo» matcher ikke «Cremonese», «Internacional» matcher ikke «Inter»), og aldri et
    vilkårlig valg ved flere kandidater («Paris» → None når både Paris SG og Paris FC finnes).
    Returns None if no unambiguous match found.
    """
    if not name:
        return None
    normalized = normalize_team_name(name)
    if normalized in available_teams:
        return normalized
    fn = _fold(normalized)
    if not fn:
        return None
    folded = {team: _fold(team) for team in available_teams}
    exact = [team for team, f in folded.items() if f == fn]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        return None  # duplikat i datasettet — aldri vilkårlig valg
    tokens = set(fn.split())
    # A) datasett-navnets ord ⊆ innkommende navn, og alle ekstra ord er generiske
    cands = []
    for team, f in folded.items():
        tt = set(f.split())
        if tt and tt <= tokens and (tokens - tt) <= GENERIC_TOKENS:
            cands.append((len(tt), team))
    if cands:
        best = max(n for n, _ in cands)
        top = [team for n, team in cands if n == best]
        return top[0] if len(top) == 1 else None
    return None  # kortformer («Frankfurt», «Coruña») dekkes kun av eksplisitte aliaser, aldri gjetting
