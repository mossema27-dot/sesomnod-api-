#!/usr/bin/env python3
"""
Sniper Validation — bygger på force_edge_discovery_results.csv (2368 events).
Tester edge × odds-range × Kelly sizing for Over 2.5-markedet.

Inputs : scripts/force_edge_discovery_results.csv
Output : print til stdout
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

CSV_PATH = Path("/Users/don/sesomnod-api/scripts/force_edge_discovery_results.csv")

if not CSV_PATH.exists():
    print(f"ERROR: {CSV_PATH} mangler — kjør force_edge_discovery.py først")
    sys.exit(1)

df = pd.read_csv(CSV_PATH)
ou_df = df.dropna(subset=["edge_over_25", "odds_o25", "over_25"]).copy()
ou_df["over_25"] = ou_df["over_25"].astype(bool)
print(f"Total Over 2.5 events: {len(ou_df)}")


# ── MOVE 1: SNIPER-FILTER (edge × odds range) ───────────────────────────────
print("\n" + "=" * 70)
print("MOVE 1: Sniper-filter test (edge × odds range)")
print("=" * 70)

EDGE_THRESHOLDS = [0.05, 0.07, 0.08, 0.09, 0.10]
ODDS_RANGES = [
    ("all",       1.0,  99.0),
    ("1.40-1.70", 1.40, 1.70),
    ("1.70-2.00", 1.70, 2.00),
    ("1.70-2.30", 1.70, 2.30),
    ("2.00-2.50", 2.00, 2.50),
    ("2.30-3.00", 2.30, 3.00),
]

print(f"\n{'Edge≥':<8}{'Odds':<14}{'N':>6}  {'Hit%':>7}  {'ROI%':>8}  {'AvgEdge':>9}")
print("-" * 70)

best_filter = None
best_roi = -100.0

for edge_th in EDGE_THRESHOLDS:
    for range_name, odds_min, odds_max in ODDS_RANGES:
        bets = ou_df[
            (ou_df["edge_over_25"] >= edge_th)
            & (ou_df["odds_o25"] >= odds_min)
            & (ou_df["odds_o25"] <= odds_max)
        ].copy()
        if len(bets) < 30:
            continue

        bets["win"] = bets["over_25"]
        bets["profit"] = np.where(bets["win"], bets["odds_o25"] - 1, -1)
        n = len(bets)
        wins = int(bets["win"].sum())
        hit = wins / n * 100
        roi = bets["profit"].mean() * 100
        avg_edge = bets["edge_over_25"].mean() * 100

        marker = " ⭐" if (n >= 100 and roi > 5) else ""
        print(f"{edge_th*100:>4.0f}%  {range_name:<14}{n:>6}  {hit:>6.1f}%  "
              f"{roi:>+7.2f}%  {avg_edge:>+8.2f}pp{marker}")

        if n >= 100 and roi > best_roi:
            best_roi = roi
            best_filter = (edge_th, range_name, odds_min, odds_max, n, hit, roi)

print("-" * 70)

if best_filter:
    edge_th, rng, omin, omax, n, hit, roi = best_filter
    print(f"\nBEST SNIPER-FILTER:")
    print(f"  Edge ≥ {edge_th*100:.0f}%")
    print(f"  Odds range: {rng} ({omin}–{omax})")
    print(f"  N={n}  Hit={hit:.1f}%  ROI={roi:+.2f}%")
else:
    print("\nNo filter met N≥100 + ROI threshold.")
    sys.exit(0)


# ── MOVE 2: KELLY SIZING SIMULATION ─────────────────────────────────────────
print("\n" + "=" * 70)
print("MOVE 2: Kelly Sizing Simulation (kronologisk over best filter)")
print("=" * 70)

edge_th, rng, omin, omax, _, _, _ = best_filter
sniper = ou_df[
    (ou_df["edge_over_25"] >= edge_th)
    & (ou_df["odds_o25"] >= omin)
    & (ou_df["odds_o25"] <= omax)
].copy()
sniper["date"] = pd.to_datetime(sniper["date"])
sniper = sniper.sort_values("date").reset_index(drop=True)

KELLY_FRACTIONS = [
    ("Flat 1u",   "flat"),
    ("1/8 Kelly", 0.125),
    ("1/4 Kelly", 0.25),
    ("1/2 Kelly", 0.50),
    ("Full Kelly", 1.00),
]

print(f"\n{'Strategy':<13}{'Final %':>11}  {'Max DD %':>10}  {'Max LossStreak':>16}")
print("-" * 60)

for name, fraction in KELLY_FRACTIONS:
    bankroll = 100.0
    peak = 100.0
    max_dd_pct = 0.0
    loss_streak = 0
    max_loss_streak = 0

    for _, bet in sniper.iterrows():
        prob = float(bet["prob_over_25"])
        odds = float(bet["odds_o25"])
        won = bool(bet["over_25"])

        if fraction == "flat":
            stake = 1.0
        else:
            kelly = ((odds * prob) - 1) / (odds - 1) if odds > 1 else 0
            kelly = max(0.0, kelly)
            stake = bankroll * kelly * fraction
            stake = min(stake, bankroll * 0.10)

        if won:
            bankroll += stake * (odds - 1)
            loss_streak = 0
        else:
            bankroll -= stake
            loss_streak += 1
            max_loss_streak = max(max_loss_streak, loss_streak)

        peak = max(peak, bankroll)
        if peak > 0:
            dd_pct = (peak - bankroll) / peak * 100
            max_dd_pct = max(max_dd_pct, dd_pct)

    final_pct = (bankroll - 100) / 100 * 100
    print(f"{name:<13}{final_pct:>+10.2f}%  {max_dd_pct:>9.2f}%  {max_loss_streak:>16}")

print("-" * 60)
print("\nINSTITUSJONELL ANBEFALING: 1/4 Kelly første 200 bets, vurder 1/2 Kelly etter 500+.")


# ── MOVE 3: CLV-PROXY (ærlig begrensning) ───────────────────────────────────
print("\n" + "=" * 70)
print("MOVE 3: CLV-Proxy Analysis (begrensning: ikke ekte closing odds)")
print("=" * 70)

print("""
ÆRLIG BEGRENSNING:
  Vi har ikke separat opening + closing odds for over_2.5 i CSV.
  PSCO/PSCU er Pinnacle CLOSING (ikke opening).
  "CLV-proxy" her = (model_prob − implied_prob) ved entry.
  Måler model-vs-implied, IKKE entry-vs-close. Ekte CLV krever
  live entry-odds + senere closing-fetch (uke 2-arbeid).
""")

print(f"Sniper sub-set CLV-proxy ({len(sniper)} bets):")
print(f"  Median model-vs-implied: {sniper['edge_over_25'].median()*100:+.2f} pp")
print(f"  Mean   model-vs-implied: {sniper['edge_over_25'].mean()*100:+.2f} pp")
print(f"  % positive proxy:        {(sniper['edge_over_25'] > 0).mean()*100:.1f}%")
print("\nNOTE: Disse tallene er per definisjon positive (filter krever edge ≥%).")
print("      Ikke valid CLV-bevis. Krever MOVE 5 live simulation.")


# ── MOVE 4: 1X2 STATUS ──────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("MOVE 4: 1X2 status (output-layer-only fjerning)")
print("=" * 70)

print("""
ANBEFALT IMPLEMENTASJON (krever separat kode-endring):
  - Decision Engine V2 output : KUN Over 2.5 events
  - Telegram-format            : KUN Over 2.5 Sniper signal
  - sesomnod.com narrativ      : spesialisert på Over 2.5
  - picks_v2 + settlement      : UENDRET (1X2 fortsatt logget for compat)

GRUNN: Settlement (1fbe4db) + auto-settle 7d + Pinnacle CLV (mirofish_agent)
       bruker 1X2-result. Fjerning fra picks_v2 ville bryte alle disse.
""")


# ── MOVE 5: LIVE SIMULATION SETUP ───────────────────────────────────────────
print("\n" + "=" * 70)
print("MOVE 5: Live Simulation Setup (uke 2-arbeid)")
print("=" * 70)

print("""
KAN IKKE GJØRE I DAG: Krever live-pipeline + tid.

ARKITEKTUR (uke 2):
  Daglig:
   - Big5 kamper hentes
   - Live Over 2.5-odds fetch (Pinnacle/API-Football)
   - Dixon-Coles predict per kamp
   - Filter: edge ≥{best_edge}%, odds {best_range}
   - Logg entry-odds + timestamp + model-prob

  Match-tid (90 min etter kickoff):
   - Hent closing-odds (siste 5 min før kickoff)
   - Beregn ekte CLV = (entry − close) / close

  Etter 100+ logged bets:
   - Aggregate % positive CLV
   - Validate edge er reell (ikke åpningsfluk)
""".format(best_edge=int(edge_th*100), best_range=rng))


# ── ENDELIG VERDICT ─────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("ENDELIG VERDICT")
print("=" * 70)

edge_th, rng, omin, omax, n, hit, roi = best_filter
if roi >= 7.0 and n >= 100:
    verdict = "INSTITUTIONAL GRADE"
elif roi >= 5.0 and n >= 100:
    verdict = "STRONG SNIPER"
elif roi >= 3.0 and n >= 100:
    verdict = "DECENT EDGE — kan optimaliseres"
else:
    verdict = "MARGINAL — ikke kohort 1-klar"

print(f"\n{verdict}")
print(f"Best sniper : Over 2.5, edge≥{edge_th*100:.0f}%, odds {rng}")
print(f"N={n}  Hit={hit:.1f}%  ROI={roi:+.2f}%")

print("\nNESTE STEG:")
print("  1. Hvis ROI ≥5%: bygg sniper-arkitektur (uke 2)")
print("  2. Live simulation (uke 2-3): ekte CLV-bevis")
print("  3. 1/4 Kelly sizing for første 200 bets")
print("  4. Telegram + sesomnod.com pivot til 'Over 2.5 Sniper'")
