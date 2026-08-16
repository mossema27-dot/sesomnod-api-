#!/usr/bin/env python3
"""
Force Edge Discovery — bevis eller drep modellen.
Single script, no DB, no endpoints. Just truth.

Korrigert for penaltyblog 1.9.0:
- pred.home_win / pred.draw / pred.away_win (ikke _probability)
- pred.home_goal_expectation / pred.away_goal_expectation
- pred.totals(2.5) returnerer (under, push, over)

ADVARSEL (audit 2026-08-16):
    IN-SAMPLE. EDGE_THRESHOLD 0.09 ble valgt ETTER gridsøket i
    scripts/sniper_validation.py samme dag (2026-04-28).
    Test-CSV-en er samme datasett terskelen ble optimalisert på.
    +14.62 % ROI og 62.4 % hit rate SKAL ALDRI publiseres som bevis.
    Live-verifikasjon krever n ≥ 30 signaler fra data som ikke var
    i denne CSV-en — det er hele poenget med kalibrerings-vinduet.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ── STEG 1: LAST NED DATA ────────────────────────────────────────────────────
print("=" * 60)
print("STEG 1: Laster ned historisk data")
print("=" * 60)

URLS = {
    "Premier League": [
        "https://www.football-data.co.uk/mmz4281/2223/E0.csv",
        "https://www.football-data.co.uk/mmz4281/2324/E0.csv",
        "https://www.football-data.co.uk/mmz4281/2425/E0.csv",
    ],
    "La Liga": [
        "https://www.football-data.co.uk/mmz4281/2223/SP1.csv",
        "https://www.football-data.co.uk/mmz4281/2324/SP1.csv",
        "https://www.football-data.co.uk/mmz4281/2425/SP1.csv",
    ],
    "Bundesliga": [
        "https://www.football-data.co.uk/mmz4281/2223/D1.csv",
        "https://www.football-data.co.uk/mmz4281/2324/D1.csv",
        "https://www.football-data.co.uk/mmz4281/2425/D1.csv",
    ],
    "Serie A": [
        "https://www.football-data.co.uk/mmz4281/2223/I1.csv",
        "https://www.football-data.co.uk/mmz4281/2324/I1.csv",
        "https://www.football-data.co.uk/mmz4281/2425/I1.csv",
    ],
    "Ligue 1": [
        "https://www.football-data.co.uk/mmz4281/2223/F1.csv",
        "https://www.football-data.co.uk/mmz4281/2324/F1.csv",
        "https://www.football-data.co.uk/mmz4281/2425/F1.csv",
    ],
}

all_dfs = []
for league, urls in URLS.items():
    for url in urls:
        try:
            df = pd.read_csv(url, encoding="utf-8", on_bad_lines="skip")
            df["league"] = league
            df["source_url"] = url
            all_dfs.append(df)
            label = "/".join(url.rsplit("/", 2)[-2:])
            print(f"  ok  {label}: {len(df)} kamper")
        except Exception as e:
            print(f"  err {url}: {e}")

if not all_dfs:
    print("ERROR: ingen CSV-er lastet")
    sys.exit(1)

df = pd.concat(all_dfs, ignore_index=True)
df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
df["FTHG"] = pd.to_numeric(df["FTHG"], errors="coerce")
df["FTAG"] = pd.to_numeric(df["FTAG"], errors="coerce")
df = df.dropna(subset=["Date", "FTHG", "FTAG", "HomeTeam", "AwayTeam"])
df["FTHG"] = df["FTHG"].astype(int)
df["FTAG"] = df["FTAG"].astype(int)

print(f"\nTotal kamper: {len(df)}")
print(f"Datointervall: {df['Date'].min().date()} → {df['Date'].max().date()}")

# ── STEG 2: TEAM NORMALIZER ──────────────────────────────────────────────────
TEAM_NORMALIZER = {
    "Man United": "Manchester United", "Manchester Utd": "Manchester United",
    "Man City": "Manchester City",
    "Spurs": "Tottenham", "Tottenham Hotspur": "Tottenham",
    "Wolves": "Wolverhampton",
    "Nott'm Forest": "Nottingham Forest",
    "Brighton": "Brighton & Hove Albion", "Brighton & Hove": "Brighton & Hove Albion",
    "West Ham": "West Ham United",
    "Newcastle": "Newcastle United",
    "Leicester": "Leicester City",
    "Sheffield Utd": "Sheffield United",
    "Leeds": "Leeds United",
    "Atletico Madrid": "Atlético Madrid", "Atletico": "Atlético Madrid",
    "Sociedad": "Real Sociedad",
    "Athletic Bilbao": "Athletic Club",
    "Vallecano": "Rayo Vallecano",
    "Cadiz": "Cádiz",
    "Almeria": "Almería",
    "Bayern Munich": "Bayern München", "Bayern": "Bayern München",
    "Dortmund": "Borussia Dortmund", "BVB": "Borussia Dortmund",
    "Leverkusen": "Bayer Leverkusen",
    "M'gladbach": "Borussia Mönchengladbach",
    "Mönchengladbach": "Borussia Mönchengladbach",
    "Frankfurt": "Eintracht Frankfurt",
    "Hoffenheim": "TSG Hoffenheim",
    "Köln": "1. FC Köln",
    "Wolfsburg": "VfL Wolfsburg",
    "Milan": "AC Milan",
    "Inter": "Inter Milan", "Internazionale": "Inter Milan",
    "Roma": "AS Roma",
    "PSG": "Paris Saint-Germain", "Paris SG": "Paris Saint-Germain",
    "Marseille": "Olympique Marseille",
    "Lyon": "Olympique Lyonnais",
    "Monaco": "AS Monaco",
    "Lille": "Lille OSC",
    "Rennes": "Stade Rennais",
}


def norm(team: str) -> str:
    return TEAM_NORMALIZER.get(team, team)


df["home_norm"] = df["HomeTeam"].apply(norm)
df["away_norm"] = df["AwayTeam"].apply(norm)


# ── STEG 3: ODDS-HELPERS (Pinnacle foretrukket) ──────────────────────────────
def get_odds_1x2(row):
    if pd.notna(row.get("PSH")) and pd.notna(row.get("PSD")) and pd.notna(row.get("PSA")):
        return float(row["PSH"]), float(row["PSD"]), float(row["PSA"]), "pinnacle"
    if pd.notna(row.get("AvgH")) and pd.notna(row.get("AvgD")) and pd.notna(row.get("AvgA")):
        return float(row["AvgH"]), float(row["AvgD"]), float(row["AvgA"]), "avg"
    if pd.notna(row.get("B365H")) and pd.notna(row.get("B365D")) and pd.notna(row.get("B365A")):
        return float(row["B365H"]), float(row["B365D"]), float(row["B365A"]), "b365"
    return None, None, None, None


def get_odds_over25(row):
    """Closing Pinnacle eller Avg over/under 2.5."""
    if pd.notna(row.get("PSCO")) and pd.notna(row.get("PSCU")):
        return float(row["PSCO"]), float(row["PSCU"]), "pinnacle_close"
    if pd.notna(row.get("Avg>2.5")) and pd.notna(row.get("Avg<2.5")):
        return float(row["Avg>2.5"]), float(row["Avg<2.5"]), "avg"
    if pd.notna(row.get("B365>2.5")) and pd.notna(row.get("B365<2.5")):
        return float(row["B365>2.5"]), float(row["B365<2.5"]), "b365"
    return None, None, None


def implied_1x2(h, d, a):
    raw_h, raw_d, raw_a = 1 / h, 1 / d, 1 / a
    total = raw_h + raw_d + raw_a
    return raw_h / total, raw_d / total, raw_a / total


def implied_ou(over, under):
    raw_o, raw_u = 1 / over, 1 / under
    total = raw_o + raw_u
    return raw_o / total, raw_u / total


# ── STEG 4: TRAIN/TEST SPLIT ─────────────────────────────────────────────────
TEST_CUTOFF = pd.to_datetime("2024-01-01")

train_df = df[df["Date"] < TEST_CUTOFF].copy()
test_df = df[df["Date"] >= TEST_CUTOFF].copy()

print(f"\nTrain: {len(train_df)} kamper (før {TEST_CUTOFF.date()})")
print(f"Test:  {len(test_df)} kamper (fra {TEST_CUTOFF.date()})")


# ── STEG 5: TREN DIXON-COLES PER LIGA ────────────────────────────────────────
print("\n" + "=" * 60)
print("STEG 5: Trener Dixon-Coles per liga (out-of-sample)")
print("=" * 60)

try:
    from penaltyblog.models import DixonColesGoalModel
except ImportError:
    print("ERROR: penaltyblog ikke installert. Kjør: pip install penaltyblog")
    sys.exit(1)

models = {}
for league in train_df["league"].unique():
    league_train = train_df[train_df["league"] == league]
    if len(league_train) < 100:
        print(f"  skip {league}: kun {len(league_train)} kamper (trenger ≥100)")
        continue
    try:
        model = DixonColesGoalModel(
            league_train["FTHG"].tolist(),
            league_train["FTAG"].tolist(),
            league_train["home_norm"].tolist(),
            league_train["away_norm"].tolist(),
        )
        model.fit()
        models[league] = model
        team_count = len(set(league_train["home_norm"]) | set(league_train["away_norm"]))
        print(f"  ok   {league}: {len(league_train)} kamper, {team_count} lag")
    except Exception as e:
        print(f"  err  {league}: {e}")


# ── STEG 6: BATCH PREDICT TEST-SET ───────────────────────────────────────────
print("\n" + "=" * 60)
print("STEG 6: Predicter test-set")
print("=" * 60)

test_results = []
skipped_no_team = 0
skipped_no_odds = 0
skipped_predict_err = 0

for _, row in test_df.iterrows():
    league = row["league"]
    if league not in models:
        continue

    model = models[league]
    teams_in_model = set(model.teams)

    if row["home_norm"] not in teams_in_model or row["away_norm"] not in teams_in_model:
        skipped_no_team += 1
        continue

    odds_h, odds_d, odds_a, odds_src = get_odds_1x2(row)
    if not odds_h:
        skipped_no_odds += 1
        continue

    try:
        pred = model.predict(row["home_norm"], row["away_norm"])
        prob_home = float(pred.home_win)
        prob_draw = float(pred.draw)
        prob_away = float(pred.away_win)

        ih, id_, ia = implied_1x2(odds_h, odds_d, odds_a)

        # Outcome
        if row["FTHG"] > row["FTAG"]:
            outcome = "HOME"
        elif row["FTHG"] == row["FTAG"]:
            outcome = "DRAW"
        else:
            outcome = "AWAY"

        btts = (row["FTHG"] > 0) and (row["FTAG"] > 0)
        over_25 = (row["FTHG"] + row["FTAG"]) > 2.5

        # Over/Under 2.5 odds + edge
        odds_o25, odds_u25, ou_src = get_odds_over25(row)
        edge_o25 = edge_u25 = None
        prob_over_25 = None
        if odds_o25 and odds_u25:
            io, iu = implied_ou(odds_o25, odds_u25)
            try:
                under, push, over = pred.totals(2.5)
                prob_over_25 = float(over)
                prob_under_25 = float(under)
                edge_o25 = prob_over_25 - io
                edge_u25 = prob_under_25 - iu
            except Exception:
                pass

        test_results.append({
            "date": row["Date"],
            "league": league,
            "home": row["home_norm"],
            "away": row["away_norm"],
            "home_goals": int(row["FTHG"]),
            "away_goals": int(row["FTAG"]),
            "odds_h": odds_h, "odds_d": odds_d, "odds_a": odds_a,
            "odds_src": odds_src,
            "prob_home": prob_home, "prob_draw": prob_draw, "prob_away": prob_away,
            "implied_home": ih, "implied_draw": id_, "implied_away": ia,
            "edge_home": prob_home - ih,
            "edge_draw": prob_draw - id_,
            "edge_away": prob_away - ia,
            "outcome_1x2": outcome,
            "btts": btts, "over_25": over_25,
            "odds_o25": odds_o25, "odds_u25": odds_u25,
            "prob_over_25": prob_over_25,
            "edge_over_25": edge_o25, "edge_under_25": edge_u25,
        })
    except Exception:
        skipped_predict_err += 1
        continue

results = pd.DataFrame(test_results)
print(f"\nTest-events: {len(results)}")
print(f"  skipped (no team in model): {skipped_no_team}")
print(f"  skipped (no odds): {skipped_no_odds}")
print(f"  skipped (predict err): {skipped_predict_err}")


# ── STEG 7: REALITY TEST ─────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEG 7: REALITY TEST — bevis eller drep")
print("=" * 60)

EDGE_THRESHOLDS = [0.02, 0.03, 0.05, 0.08]


def evaluate_market(df_in, edge_col, target_match_fn, odds_col, threshold):
    bets = df_in[df_in[edge_col] >= threshold].copy()
    if len(bets) == 0:
        return None
    bets["win"] = target_match_fn(bets)
    bets["profit"] = np.where(bets["win"], bets[odds_col] - 1, -1)
    n = len(bets)
    wins = int(bets["win"].sum())
    return {
        "n": n,
        "wins": wins,
        "hit_rate_pct": round(wins / n * 100, 1),
        "total_profit": round(bets["profit"].sum(), 2),
        "roi_pct": round(bets["profit"].mean() * 100, 2),
        "avg_edge_pp": round(bets[edge_col].mean() * 100, 2),
    }


markets = [
    ("Hjemmeseier", "edge_home", lambda d: d["outcome_1x2"] == "HOME", "odds_h"),
    ("Uavgjort",    "edge_draw", lambda d: d["outcome_1x2"] == "DRAW", "odds_d"),
    ("Borteseier",  "edge_away", lambda d: d["outcome_1x2"] == "AWAY", "odds_a"),
    ("Over 2.5",    "edge_over_25",  lambda d: d["over_25"], "odds_o25"),
    ("Under 2.5",   "edge_under_25", lambda d: ~d["over_25"], "odds_u25"),
]

print(f"\n{'Market':<14} {'Edge≥':>6} {'N':>6} {'Hit%':>7} {'ROI%':>7} {'AvgEdge':>9}")
print("-" * 60)

best_market = None
best_roi = -100.0

for name, edge_col, match_fn, odds_col in markets:
    if edge_col not in results.columns or results[edge_col].isna().all():
        print(f"{name:<14}  NO DATA")
        continue
    valid = results.dropna(subset=[edge_col, odds_col])
    for thr in EDGE_THRESHOLDS:
        r = evaluate_market(valid, edge_col, match_fn, odds_col, thr)
        if r is None:
            continue
        flag = ""
        if r["n"] < 30:
            flag = " (small n)"
        print(f"{name:<14} {thr*100:>5.0f}% {r['n']:>6} {r['hit_rate_pct']:>6.1f}% "
              f"{r['roi_pct']:>+6.2f}% {r['avg_edge_pp']:>+8.2f}pp{flag}")
        if r["n"] >= 100 and r["roi_pct"] > best_roi:
            best_roi = r["roi_pct"]
            best_market = (name, thr, r)

print("-" * 60)


# ── STEG 8: HARD VERDICT ─────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEG 8: HARD VERDICT")
print("=" * 60)

if best_market is None:
    print("\n[NOT ENOUGH DATA] — alle markets har <100 bets ved testede thresholds")
    print("  → vurder lavere threshold eller utvid test-set")
elif best_roi > 3.0:
    name, thr, r = best_market
    print(f"\n[EDGE BEKREFTET] Best market: {name} @ edge≥{thr*100:.0f}%")
    print(f"  N={r['n']}  Hit={r['hit_rate_pct']}%  ROI={r['roi_pct']:+.2f}%  AvgEdge={r['avg_edge_pp']:+.2f}pp")
    print("  RECOMMENDATION: Bygg sniper-strategi rundt dette markedet")
elif best_roi > 1.0:
    name, thr, r = best_market
    print(f"\n[MARGINAL] Best: {name} @ edge≥{thr*100:.0f}% — ROI {r['roi_pct']:+.2f}%")
    print(f"  N={r['n']}  Hit={r['hit_rate_pct']}%  AvgEdge={r['avg_edge_pp']:+.2f}pp")
    print("  RECOMMENDATION: Observer mer, vurder CLV-tracking før commitment")
else:
    name, thr, r = best_market if best_market else (None, None, None)
    label = f"{name} (best ROI {best_roi:+.2f}%)" if name else "ingen market"
    print(f"\n[NO EDGE] 1X2 + Over/Under på Big5 — {label}")
    print("  RECOMMENDATION: Pivot til #33 (BTTS, AH, eller utvidede markeder)")
    print("  Big5 1X2 er for effektivt for denne modellen")


# ── STEG 9: LAGRE CSV ────────────────────────────────────────────────────────
out_path = Path("/Users/don/sesomnod-api/scripts/force_edge_discovery_results.csv")
results.to_csv(out_path, index=False)
print(f"\nDetaljert data: {out_path} ({len(results)} rows)")
print(f"Kjørt: {datetime.utcnow().isoformat()}Z")
