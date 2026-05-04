# Arkitektur — Kjente Mismatches og Bugs

Versjon: 1.0
Dato: 2026-05-04
Eier: Don
Status: Venter på Don's strategisk beslutning (B1, B2)

## KJENT MISMATCH 1 — DOKTRINE vs KODE (ATOMIC-tier)

**CLAUDE.md / doktrine sier:**
ATOMIC = atomic_score 7-9 (ren atomic_score)

**Kode sier (main.py:1853-1868):**
ATOMIC = atomic_score >= 5 AND soft_edge >= 7

**Innført:**
Commit 17f8ead, 2026-04-02, "Fixed duplicate atomic_score >= 1
condition" — silent strengelse uten doktrine-oppdatering.

**Original-doktrine commit:**
ab4d6b0, 2026-03-18, "ATOMIC(>=7) / EDGE(>=4) / MONITORED(<4)"

**Effekt:**
Den ekte gaten er soft_edge >= 7, ikke atomic_score >= 5.
30-dagers vindu produserer 0 ATOMIC av 148 picks.

**Status:**
Venter på Don's B1-beslutning (a/b/c).
IKKE endre kode eller doktrine før beslutning.

## KJENT BUG 1 — EDGE-BONUS DEAD CODE

**Lokasjon:** main.py:1815-1821

**Problem:**

```python
if soft_edge >= 0.5:
    atomic_score += 2
elif soft_edge >= 0.5:   # ← identical condition, unreachable
    atomic_score += 1
```

**Effekt:**
Alltid +2 atomic-poeng ved edge>=0.5%, aldri +1.
Forklarer hvorfor atomic_score>=5 er trivielt
mens soft_edge>=7 er den ekte gaten.

**Status:**
Skal IKKE fixes før Don godkjenner B1. Hvis fixes nå
samtidig som B1-beslutning, blander vi to variabler
i datainnsamling.

## KJENT MISMATCH 2 — PHASE 0-TELLING

**/dashboard/stats** (main.py:6058-6082):
Inkluderer alle tier all-time + backfilled picks. Viser 28 settled, 50%.

**/admin/phase0-stats** (main.py:10660-10665):
KUN ATOMIC+EDGE 30d + dummy-eksklusjon. Viser 2 settled, 0%.

**Status:**
By design. /admin/phase0-stats er kanonisk gate-kilde.
Venter på Don's B2-beslutning om backfilled picks (4 (unknown)-liga)
skal ekskluderes fra Phase 0-telling.

## KJENT GAP 1 — (UNKNOWN)-LIGA BACKFILL

**Lokasjon:** main.py:8918-8950 (legacy backfill INSERT-path)

**Problem:**
INSERT inkluderer ikke home_team, away_team, league.
4 picks i picks_v2 har league=(unknown). 2 av disse er
wins som teller mot Phase 0-stats før dummy-filter.

**Status:**
Skal IKKE fixes før Don godkjenner B2. Datakvalitet
isolert til 4 historiske picks.

## KJENT MISMATCH 3 — pinnacle_clv FELT-NAVN vs INNHOLD

**Lokasjon:** main.py:2415-2417

**Problem:**

```python
pinnacle_clv = round((soft_model_prob - 1 / pin_ref_odds) * 100, 2)
```

picks_v2.pinnacle_clv-kolonnen lagrer **pre-game model-edge mot
Pinnacle no-vig opening odds**, ikke klassisk CLV (close - open).
Riktig CLV finnes i clv_records-tabellen og MiroFish /summary.

**Effekt:**
/admin/phase0-stats labler verdien korrekt som
model_edge_pinnacle_pre_pct. Men kolonne-navnet i schema
er villedende for nye agenter eller eksterne reviewere.

**Status:**
Ikke en aktiv bug — bare et navnings-faremoment.
Skal IKKE rename kolonnen før Don godkjenner schema-migrasjon.

## GOVERNANCE-REGEL (ny, fra 4. mai 2026)

Hver fremtidig endring av tier-logikk, signal-vekter,
gate-kriterier, eller atomic-arkitektur krever:

1. Doktrine-doc-oppdatering i SAMME commit
2. Eksplisitt Don-godkjennelse i commit-meldingen
   (format: "Approved by Don: [reason]")
3. ARKITEKTUR_KJENT.md-oppdatering hvis mismatch beholdes
