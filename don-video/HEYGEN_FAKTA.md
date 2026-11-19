# HeyGen – kontrollerte fakta

Lest førstehånds 2026-11-19 fra `developers.heygen.com` og `help.heygen.com`. Priser og motorer endres ofte: les live-skjema og `get_current_user` før hvert kredittbrukende kall.

## Betalingsvei

- Remote MCP (`https://mcp.heygen.com/mcp/v1/`, OAuth) trekker fra abonnementets kreditter: «Usage draws on your existing HeyGen plan's credits», «Does this cost extra? No.» Kilde: `/mcp/overview`.
- API-nøkkel, HeyGen CLI med nøkkel og Direct API faktureres til en separat API-plan/lommebok. Kilde: `/llms.txt`. **Ikke bruk dem.**
- `get_current_user` gir `billing_type`: `subscription` (les `subscription.plan` og `subscription.credits.premium_credits.remaining` / `resets_at`), `wallet` eller `usage_based`. Bare `subscription` er innenfor 0 kr-rammen. Kilde: `/user-profile`.
- Mobilabonnement (Apple/Google): plankreditter utløper etter 30 dager og ruller ikke over. Kilde: help-artikkel 10207693.
- Tilkobling fra Claude: **+ → Connectors → Manage connectors → + Add custom connector**, navn `HeyGen`, URL over. Krever betalt Claude-plan. Kilde: `/mcp/claude-web`.

## Kreditter per minutt (help-artikkel 15126059)

| Motor | Kreditter/min |
|---|---|
| Avatar III, videolook / fotolook | 4 / 7 |
| Avatar 6 Turbo (720p/1080p) | 20 |
| Avatar IV, videolook / fotolook | 31 / 16 |
| Avatar 6 Pro (720p/1080p) | 40 |
| Avatar V | 48 |
| Videooversettelse Speed / Precision | 6 / 10 |
| Video Agent standard | 80 |

Minstebelastning og avrunding per jobb er ikke dokumentert. Mål saldo før og etter prøveklippet.

## MCP-verktøy som trengs

`get_current_user` · `list_avatar_groups` · `list_avatar_looks` (look-ID er `avatar_id`) · `get_avatar_look` (viser støttede motorer) · `list_voices` (standard `type=private` = egne stemmer) · `get_voice` · `get_video_voice_capabilities` · `create_speech` (bare lyd) · `create_video_from_avatar` · `get_video` · `create_video_translation` (måte A).

`create_video_from_avatar` bruker Avatar IV når `engine` utelates. Send alltid `engine` eksplisitt. Avatar 6 velges med `engine.type` `heygen-avatar-6` eller `heygen-avatar-6-turbo` på looks som lister dem i `supported_api_engines`.

Privat avatar må ha fullført samtykke (`create_avatar_consent`) før den kan generere video.

## Norsk tale – viktigste risiko

- Motoren **HeyGen Voice** (`heygen_voice`, modell `heygen-voice-1`) har 39 språk. Dansk, svensk og finsk er med. **Norsk er ikke med.** Kilde: `/docs/voices/heygen-voice-speech#supported-languages`.
- Språket leses fra manuset og settes ikke av oss. Norsk tekst på HeyGen Voice kan derfor bli avvist eller lest som dansk/svensk.
- En instant stemmeklon kan bruke alle seks motorer (`heygen_voice`, `elevenlabs`, `fish`, `cartesia`, `starfish`, `mockingbird`). Kall `get_video_voice_capabilities`, finn oppføringen for norsk, og velg en motor og modell som står der. Send alltid `voice_settings.engine_settings` med `engine_type` og `model`.
- Står norsk ikke oppført for noen motor stemmen kan bruke: stopp og rapporter. Ikke bytt til en generisk stemme.
- `voice_settings.locale` er aksent, ikke språk. Send bare en locale som capabilities-listen tilbyr.
- Fart: `voice_settings.speed` 0,5–1,5.

## Utdata

- `aspect_ratio`: `9:16` må settes (standard er `16:9`). `resolution`: be om `1080p`.
- `get_video` gir `video_url`, `thumbnail_url`, `subtitle_url` (SRT følger alltid med) og `captioned_video_url` når `caption.style` er satt. Lenkene er signerte og utløper: hent på nytt ved behov.
- Filverter sett i dokumentasjonen: `files2.heygen.ai`, `files.heygen.ai`, `resource2.heygen.ai`. Alle er tillatt i miljøet «don-video».
