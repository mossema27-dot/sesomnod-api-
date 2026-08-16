-- Oraklion Brain — verifisering av append-only chain (2026-08-16).
-- Kjør etter oraklion_brain_schema.sql for å bekrefte at
-- immutabilitets-triggerne fungerer.

\echo ==== verify_chain (skal returnere 0 rader hvis chain er intakt) ====
SELECT * FROM oraklion.verify_chain();

\echo ==== forsøker UPDATE — skal FEILE med 42501 ====
UPDATE oraklion.events SET stars_after = stars_after + 1 WHERE seq = 1;

\echo ==== forsøker DELETE — skal FEILE ====
DELETE FROM oraklion.events WHERE seq = 1;

\echo ==== events-antall (uendret hvis triggere fungerte) ====
SELECT COUNT(*) AS total_events FROM oraklion.events;
