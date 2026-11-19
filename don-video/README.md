# don-video

Norsk video med Don Moses' egen avatar og stemme fra en YouTube-lenke. 0 kr i nye utgifter.

## Slik starter du neste video

Skriv i en Claude Code-økt der HeyGen-connectoren er på:

> Lag norsk Don-video av denne: https://youtube.com/shorts/...

Skillen `.claude/skills/don-video/SKILL.md` styrer resten. Den ligger på grenen `claude/sweet-cerf-pv0trw` til den er flettet inn i `main`.

## Engangsoppsett

1. claude.ai → Innstillinger → Connectors → «Add custom connector» → navn `HeyGen`, URL `https://mcp.heygen.com/mcp/v1/` → logg inn og godkjenn.
2. Start en ny økt. Connectors leses bare ved oppstart.

Ikke bruk HeyGen API-nøkkel: den trekker fra en separat lommebok, ikke fra abonnementet.

## Kommandoer

```bash
python3 don-video/donvideo.py new "<youtube-lenke>"          # jobbmappe jobs/<dato>_<video-id>/
python3 don-video/donvideo.py source <jobb>                  # metadata + teksting (eller --video-file / --transcript-file)
python3 don-video/donvideo.py status <jobb>
python3 don-video/donvideo.py extjob <jobb> --kind final --id <ekstern-id>
python3 don-video/donvideo.py ledger <jobb> --what "..." --credits N [--estimated]
python3 don-video/donvideo.py finish <jobb> --master <video.mp4> [--srt <fil.srt>] [--music <fil>]
python3 don-video/donvideo.py verify <jobb>
```

Krever `ffmpeg`/`ffprobe` og Python 3.10+. `yt-dlp` installeres ved behov.

## Hva som ligger hvor

| Sti | Innhold | I git |
|---|---|---|
| `donvideo.py` | arbeidsflyten | ja |
| `profile/don_moses.example.json` | profilmal | ja |
| `profile/don_moses.json` | Dons avatar, stemme og innstillinger | **nei** |
| `jobs/<id>/` | kilde, manus, jobbstatus, eksport, kontroll | **nei** |

Repoet er offentlig. Profil, jobber og media lagres privat (Drive-mappen «Don-video»), ikke i git.

## Kjente begrensninger

- YouTube blokkerer ofte skyservere («Sign in to confirm you're not a bot»). Da trengs videofilen.
- Uten SRT eller ordtidsstempler estimeres tekstingens timing fra pauser i lyden. Kontroller visuelt.
- Maskinell kontroll beviser ikke stemmelikhet eller leppesynk. Det må ses og høres.
