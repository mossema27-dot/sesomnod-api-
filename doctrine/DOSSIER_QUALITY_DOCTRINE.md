# Dossier Quality Doctrine — SesomNod v1

## Kritiske felter (mangler ett = -20% completeness)
- home_xg_avg_last10
- away_xg_avg_last10
- current_odds
- line_movement_pct
- core_edge
- core_confidence
- lineup_confirmed (boolean)

## Viktige felter (mangler ett = -10% completeness)
- home_form_last5
- away_form_last5
- sharp_money_indicator
- key_absences_home/away
- dossier_generated_at

## Valgfrie felter (mangler = -5% completeness)
- referee_data
- weather
- head_to_head
- narrative_state

## Completeness → Confidence mapping
| Completeness | Konsekvens |
|---|---|
| 90-100% | Ingen påvirkning |
| 75-89% | Legg til WARN |
| 60-74% | Downgrade confidence |
| 50-59% | Automatisk REVIEW |
| <50% | Automatisk NO-BET |

## Prinsipp
Et dårlig dossier er verre enn intet dossier.
Aldri simuler med <50% completeness.
