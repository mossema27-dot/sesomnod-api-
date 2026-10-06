"""
Team-normalizer: diakritika, 2026/27-opprykk, og kollisjonssikkerhet (incident docs/incidents/2026-09-16-dc-season-coverage.md).
Ren test, ingen nettverk. Universet under = lagene i football-data.co.uk 2026/27 (E0, SP1, D1, I1, F1) + kjente lag fra 2023/24–2025/26.
Kjør: python3 -m pytest tests/test_team_normalizer.py -q
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.team_normalizer import NORMALIZATION_MAP, find_best_team_match as match, normalize_team_name  # noqa: E402

SEASON_2627 = [
    'Arsenal', 'Aston Villa', 'Bournemouth', 'Brentford', 'Brighton', 'Chelsea', 'Coventry', 'Crystal Palace', 'Everton', 'Fulham',
    'Hull', 'Ipswich', 'Leeds', 'Liverpool', 'Man City', 'Man United', 'Newcastle', "Nott'm Forest", 'Sunderland', 'Tottenham',
    'Alaves', 'Ath Bilbao', 'Ath Madrid', 'Barcelona', 'Betis', 'Celta', 'Elche', 'Espanol', 'Getafe', 'La Coruna', 'Levante',
    'Malaga', 'Osasuna', 'Real Madrid', 'Santander', 'Sevilla', 'Sociedad', 'Valencia', 'Vallecano', 'Villarreal',
    'Augsburg', 'Bayern Munich', 'Dortmund', 'Ein Frankfurt', 'Elversberg', 'FC Koln', 'Freiburg', 'Hamburg', 'Hoffenheim',
    'Leverkusen', "M'gladbach", 'Mainz', 'Paderborn', 'RB Leipzig', 'Schalke 04', 'Stuttgart', 'Union Berlin', 'Werder Bremen',
    'Atalanta', 'Bologna', 'Cagliari', 'Como', 'Fiorentina', 'Frosinone', 'Genoa', 'Inter', 'Juventus', 'Lazio', 'Lecce', 'Milan',
    'Monza', 'Napoli', 'Parma', 'Roma', 'Sassuolo', 'Torino', 'Udinese', 'Venezia',
    'Angers', 'Auxerre', 'Brest', 'Le Havre', 'Le Mans', 'Lens', 'Lille', 'Lorient', 'Lyon', 'Marseille', 'Monaco', 'Nice',
    'Paris FC', 'Paris SG', 'Rennes', 'Strasbourg', 'Toulouse', 'Troyes',
]
OLDER_SEASONS = ['Leicester', 'Southampton', 'Luton', 'Burnley', 'Sheffield United', 'West Ham', 'Wolves',
                 'Girona', 'Las Palmas', 'Valladolid', 'Leganes', 'Mallorca', 'Cadiz', 'Granada', 'Almeria', 'Oviedo',
                 'Empoli', 'Verona', 'Salernitana', 'Cremonese', 'Pisa',
                 'Darmstadt', 'Bochum', 'Heidenheim', 'Holstein Kiel', 'St Pauli', 'Wolfsburg',
                 'Metz', 'Montpellier', 'St Etienne', 'Reims', 'Nantes', 'Clermont']
UNIVERSE = SEASON_2627 + OLDER_SEASONS


class TestDiacriticsAndPromoted(unittest.TestCase):
    def test_diacritics_resolve(self):
        self.assertEqual(match("Atlético Madrid", UNIVERSE), "Ath Madrid")
        self.assertEqual(match("Atletico Madrid", UNIVERSE), "Ath Madrid")
        self.assertEqual(match("Bayern München", UNIVERSE), "Bayern Munich")
        self.assertEqual(match("1. FC Köln", UNIVERSE), "FC Koln")
        self.assertEqual(match("1. FC Koln", UNIVERSE), "FC Koln")

    def test_promoted_2627_resolve_to_csv_names(self):
        expected = {"Coventry": "Coventry", "Hull City": "Hull", "Deportivo La Coruna": "La Coruna", "Malaga": "Malaga",
                    "Racing Santander": "Santander", "SV Elversberg": "Elversberg", "SC Paderborn 07": "Paderborn",
                    "FC Schalke 04": "Schalke 04", "Le Mans": "Le Mans", "Estac Troyes": "Troyes"}
        self.assertEqual({k: match(k, UNIVERSE) for k in expected}, expected)


class TestNoArbitraryMatch(unittest.TestCase):
    def test_substring_collisions_are_rejected(self):
        self.assertIsNone(match("Internacional", UNIVERSE))       # ikke Inter
        self.assertIsNone(match("Remo", UNIVERSE))                # ikke Cremonese
        self.assertIsNone(match("Inter Miami", UNIVERSE))         # ikke Inter
        self.assertIsNone(match("Celta de Vigo II", UNIVERSE))    # ikke Celta (B-lag)
        self.assertIsNone(match("Real Sociedad II", UNIVERSE))    # ikke Sociedad

    def test_ambiguous_short_names_get_no_match(self):
        self.assertIsNone(match("Paris", UNIVERSE))               # Paris SG og Paris FC
        self.assertIsNone(match("Madrid", UNIVERSE))              # Real Madrid og Ath Madrid
        self.assertIsNone(match("United", UNIVERSE))
        self.assertIsNone(match("Frankfurt", UNIVERSE))           # kortform uten alias → aldri gjetting

    def test_cross_league_lookalikes_are_rejected(self):
        """Funnet i prod-navnene 2026-09-16: samme kjerneord, annen klubb i annen liga."""
        for n in ("Deportivo Español", "Coventry United", "Malaga City", "Real Santander", "Stoke City", "Bristol City",
                  "Liverpool Montevideo", "Everton de Vina", "Inter San Carlos", "Arsenal U21", "Lillestrom", "ADT", "Este"):
            self.assertIsNone(match(n, UNIVERSE), n)

    def test_unknown_leagues_never_match(self):
        for n in ("Sparta Praha", "Ajax", "Sporting CP", "Benfica", "Bodo/Glimt", "Start", "Viking", "Molde", "Malmo FF"):
            self.assertIsNone(match(n, UNIVERSE), n)


class TestCompatibility(unittest.TestCase):
    def test_every_universe_team_resolves_to_itself(self):
        for t in UNIVERSE:
            self.assertEqual(match(t, UNIVERSE), t, t)

    def test_every_alias_whose_target_exists_resolves(self):
        targets = set(UNIVERSE)
        for key, value in NORMALIZATION_MAP.items():
            if value in targets:
                self.assertEqual(match(key, UNIVERSE), value, key)
                self.assertEqual(match(key.upper(), UNIVERSE), value, key)

    def test_api_football_style_names(self):
        cases = {"Manchester United": "Man United", "Manchester City": "Man City", "Nottingham Forest": "Nott'm Forest",
                 "Ipswich Town": "Ipswich", "Newcastle": "Newcastle", "Wolves": "Wolves", "West Ham": "West Ham",
                 "Athletic Club": "Ath Bilbao", "Real Betis": "Betis", "Real Sociedad": "Sociedad", "Celta Vigo": "Celta",
                 "Rayo Vallecano": "Vallecano", "Espanyol": "Espanol", "Borussia Dortmund": "Dortmund",
                 "Borussia Mönchengladbach": "M'gladbach", "Eintracht Frankfurt": "Ein Frankfurt", "Bayer Leverkusen": "Leverkusen",
                 "VfB Stuttgart": "Stuttgart", "SC Freiburg": "Freiburg", "FSV Mainz 05": "Mainz", "1899 Hoffenheim": "Hoffenheim",
                 "Hamburger SV": "Hamburg", "FC Augsburg": "Augsburg", "AC Milan": "Milan", "Inter": "Inter",
                 "Paris Saint Germain": "Paris SG", "Stade Brestois 29": "Brest", "Leicester City": "Leicester"}
        self.assertEqual({k: match(k, UNIVERSE) for k in cases}, cases)

    def test_normalize_is_stable_for_unknown(self):
        self.assertEqual(normalize_team_name("Ukjent Lag FK"), "Ukjent Lag FK")
        self.assertIsNone(match("", UNIVERSE))


if __name__ == "__main__":
    unittest.main()


class TestProdNamesRegression(unittest.TestCase):
    """Alle Big5-lagnavn som API-Football faktisk leverte 2026-08-01..09-16 (tests/fixtures) må løses entydig mot 2026/27-universet."""

    def test_every_big5_api_name_resolves(self):
        import json
        fx = json.loads((Path(__file__).resolve().parent / "fixtures" / "api_football_big5_names_2026-09.json").read_text(encoding="utf-8"))
        unresolved = {lg: [n for n in names if match(n, UNIVERSE) is None] for lg, names in fx["leagues"].items()}
        self.assertEqual({lg: v for lg, v in unresolved.items() if v}, {})
        total = sum(len(v) for v in fx["leagues"].values())
        self.assertGreaterEqual(total, 90)


class TestSniperPath(unittest.TestCase):
    """Sniper skriver om lagnavn (sniper_live.TEAM_NORMALIZER, f.eks. «Lille» → «Lille OSC») FØR matcheren får dem.
    Hele kjeden må løses, ikke bare rånavnet. Funnet i stresstest 2026-10-06: «Lille OSC» ga None uten delstreng-matching."""

    @staticmethod
    def _sniper_rewrite() -> dict:
        import ast
        src = (Path(__file__).resolve().parents[1] / "services" / "sniper_live.py").read_text(encoding="utf-8")
        for node in ast.walk(ast.parse(src)):   # leses fra kildefilen: ingen import av sniper_live (nettverk/DB-avhengigheter)
            if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "TEAM_NORMALIZER" for t in node.targets):
                return ast.literal_eval(node.value)
        raise AssertionError("TEAM_NORMALIZER ikke funnet i services/sniper_live.py")

    def test_every_big5_api_name_resolves_after_sniper_rewrite(self):
        import json
        rewrite = self._sniper_rewrite()
        fx = json.loads((Path(__file__).resolve().parent / "fixtures" / "api_football_big5_names_2026-09.json").read_text(encoding="utf-8"))
        wrong = {}
        for names in fx["leagues"].values():
            for n in names:
                sent = rewrite.get(n, n)
                if match(n, UNIVERSE) is None or match(sent, UNIVERSE) != match(n, UNIVERSE):
                    wrong[n] = (sent, match(sent, UNIVERSE), match(n, UNIVERSE))
        self.assertEqual(wrong, {})

    def test_every_sniper_rewrite_target_resolves_like_its_source(self):
        """Alt Sniper kan skrive om TIL må løses til samme lag som navnet det ble skrevet om FRA (når det er et kjent lag)."""
        wrong = {src: (dst, match(dst, UNIVERSE), match(src, UNIVERSE)) for src, dst in self._sniper_rewrite().items()
                 if match(src, UNIVERSE) is not None and match(dst, UNIVERSE) != match(src, UNIVERSE)}
        self.assertEqual(wrong, {})
