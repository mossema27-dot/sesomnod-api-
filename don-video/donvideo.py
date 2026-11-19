#!/usr/bin/env python3
"""donvideo - liten, gjenbrukbar arbeidsflyt for norske avatarvideoer fra en YouTube-lenke.

Dekker den lokale, deterministiske delen: jobbmappe, kildehenting, teksting,
eksport og kontroll. Selve avatar-/stemmegenereringen skjer i HeyGen (MCP) og
styres av skillen .claude/skills/don-video/SKILL.md.

Kun standardbibliotek + ffmpeg/ffprobe. yt-dlp brukes hvis den er installert.
"""
import argparse
import datetime as dt
import difflib
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
JOBS = Path(os.environ.get("DONVIDEO_JOBS") or ROOT / "jobs")
PROFILE = ROOT / "profile" / "don_moses.json"
STEPS = ["source", "manus", "budget", "test_clip", "generation", "subs", "export", "verify"]


def _file_prefix():
    """Filnavn-prefiks står i den private profilen (ikke i git); uten profil brukes et nøytralt navn."""
    try:
        return json.loads(PROFILE.read_text(encoding="utf-8")).get("export", {}).get("file_prefix") or "Video"
    except (OSError, ValueError):
        return "Video"


_P = _file_prefix()
EXPORT_NAMES = {
    "burned": f"{_P}_Norsk_Med_Tekst.mp4",
    "clean": f"{_P}_Norsk_Ren.mp4",
    "srt": "Undertekster_nb.srt",
    "manus": "Manus_nb.txt",
    "voice": "Norsk_stemme.wav",
    "note": "Produksjonsnotat.md",
}
DELIVERABLES = ("clean", "burned", "srt", "manus", "voice")


# ------------------------------------------------------------------ helpers
def now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(cmd, cwd=None, check=False):
    p = subprocess.run([str(c) for c in cmd], cwd=cwd, capture_output=True, text=True, errors="replace")
    if check and p.returncode != 0:
        sys.exit(f"FEIL ({p.returncode}): {' '.join(str(c) for c in cmd)}\n{p.stderr[-2000:]}")
    return p


def die(msg):
    sys.exit(f"FEIL: {msg}")


def video_id(url):
    m = re.search(r"(?:youtube\.com/(?:shorts/|watch\?(?:.*&)?v=|embed/|live/)|youtu\.be/)([A-Za-z0-9_-]{11})(?![A-Za-z0-9_-])", url)
    if not m:
        die(f"Fant ingen YouTube-video-ID i: {url} (bare youtube.com- og youtu.be-lenker støttes)")
    return m.group(1)


def job_dir(ref):
    """Finn jobbmappe fra jobb-ID, video-ID, URL eller sti."""
    p = Path(ref)
    if p.is_dir() and (p / "job.json").exists():
        return p.resolve()
    name = ref.rstrip("/").split("/")[-1]
    if (JOBS / name / "job.json").exists():   # jobs/<id>, <id>/ og lignende
        return (JOBS / name).resolve()
    vid = ref
    if "youtu" in ref:
        vid = video_id(ref)
    hits = sorted(d for d in JOBS.glob("*") if d.is_dir() and (d.name == ref or d.name.endswith("_" + vid)))
    if not hits:
        die(f"Ingen jobb funnet for '{ref}'. Kjør: donvideo.py new <youtube-lenke>")
    return hits[-1]


def load(jd):
    return json.loads((jd / "job.json").read_text(encoding="utf-8"))


def save(jd, job):
    job["updated"] = now()
    tmp = jd / "job.json.tmp"
    tmp.write_text(json.dumps(job, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(jd / "job.json")


def set_step(job, name, status, note=None):
    st = job["steps"].setdefault(name, {})
    st["status"] = status
    st["at"] = now()
    if note:
        st["note"] = note
    else:
        st.pop("note", None)  # et gammelt notat skal ikke bli hengende ved ny status


def probe(path):
    p = run(["ffprobe", "-v", "error", "-show_format", "-show_streams", "-of", "json", path])
    if p.returncode != 0:
        die(f"ffprobe klarte ikke å lese {path}: {p.stderr.strip()[:300]}")
    d = json.loads(p.stdout)
    v = next((s for s in d["streams"] if s["codec_type"] == "video"), None)
    a = next((s for s in d["streams"] if s["codec_type"] == "audio"), None)
    info = {"duration": float(d["format"].get("duration", 0)), "size_bytes": int(d["format"].get("size", 0)),
            "format": d["format"].get("format_name"), "video": None, "audio": None}
    if v:
        num, den = (v.get("avg_frame_rate") or "0/1").split("/")
        info["video"] = {"codec": v.get("codec_name"), "width": v.get("width"), "height": v.get("height"),
                         "fps": round(float(num) / float(den), 3) if float(den) else 0,
                         "pix_fmt": v.get("pix_fmt"), "duration": float(v.get("duration") or info["duration"])}
    if a:
        info["audio"] = {"codec": a.get("codec_name"), "sample_rate": int(a.get("sample_rate") or 0),
                         "channels": a.get("channels"), "duration": float(a.get("duration") or info["duration"])}
    return info


def audio_duration(path):
    """Lydsporets egen lengde (ikke beholderens: video kan vare lenger enn lyden)."""
    info = probe(path)
    return (info["audio"] or {}).get("duration") or info["duration"]


def silences(path, noise_db=-35, min_d=0.18):
    """Stille partier [(start, slutt)] i lydsporet."""
    p = run(["ffmpeg", "-hide_banner", "-nostats", "-i", path, "-vn", "-af",
             f"silencedetect=noise={noise_db}dB:d={min_d}", "-f", "null", "-"])
    out, start = [], None
    for line in p.stderr.splitlines():
        m = re.search(r"silence_start: (-?[\d.]+(?:e[-+]?\d+)?)", line)
        if m:
            start = max(0.0, float(m.group(1)))
        m = re.search(r"silence_end: (-?[\d.]+(?:e[-+]?\d+)?)", line)
        if m and start is not None:
            out.append((start, float(m.group(1))))
            start = None
    if start is not None:
        out.append((start, audio_duration(path)))
    return out


# ------------------------------------------------------------------ new / status
def cmd_new(a):
    vid = video_id(a.url)
    JOBS.mkdir(exist_ok=True)
    existing = sorted(d for d in JOBS.glob(f"*_{vid}") if d.is_dir())
    if existing:
        print(f"Jobb finnes allerede, gjenbruker: {existing[-1]}")
        return cmd_status(argparse.Namespace(job=str(existing[-1])))
    jd = JOBS / f"{dt.date.today():%Y%m%d}_{vid}"
    (jd / "eksport").mkdir(parents=True)
    job = {
        "job_id": jd.name, "created": now(),
        "source": {"url": a.url, "video_id": vid, "canonical_url": f"https://www.youtube.com/watch?v={vid}",
                   "title": None, "channel": None, "duration_s": None, "language": None,
                   "speaker": None, "speaker_is_don": None, "transcript_method": None, "unclear_parts": []},
        "production_mode": None,  # A=dubbing av eget opptak, B=ny fremføring med avatar, C=fortellerfilm
        "steps": {s: {"status": "pending"} for s in STEPS},
        "external_jobs": [], "resources": [], "settings": {}, "profile": str(PROFILE.relative_to(ROOT)),
    }
    save(jd, job)
    print(f"Opprettet jobb: {jd}")


def cmd_status(a):
    jd = job_dir(a.job)
    job = load(jd)
    s = job["source"]
    print(f"Jobb {job['job_id']}  ({jd})")
    print(f"  Kilde: {s['url']}\n  Tittel: {s['title']}  Kanal: {s['channel']}  Varighet: {s['duration_s']}  Språk: {s['language']}")
    print(f"  Produksjonsmåte: {job['production_mode']}")
    for name in STEPS:
        st = job["steps"].get(name, {})
        print(f"  [{st.get('status', 'pending'):>8}] {name}" + (f" - {st['note']}" if st.get("note") else ""))
    for e in job["external_jobs"]:
        print(f"  ekstern jobb: {e['service']} {e['kind']} {e['id']} ({e.get('status', '?')})")
    spent = [r for r in job["resources"]]
    for r in spent:
        tag = "estimat" if r.get("estimated") else "faktisk"
        print(f"  ressurs: {r['service']} - {r['what']}: {r['credits']} kreditter ({tag})")
    files = sorted(p.name for p in (jd / "eksport").glob("*")) if (jd / "eksport").exists() else []
    print(f"  Eksport: {', '.join(files) if files else '(ingen ennå)'}")


def cmd_step(a):
    jd = job_dir(a.job)
    job = load(jd)
    if a.name not in STEPS:
        die(f"Ukjent trinn '{a.name}'. Gyldige: {', '.join(STEPS)}")
    set_step(job, a.name, a.status, a.note)
    save(jd, job)
    print(f"{a.name} -> {a.status}")


def cmd_set(a):
    """Sett felt i job.json: source.*, settings.*, billing.* eller production_mode. Trinn settes med «step»."""
    jd = job_dir(a.job)
    job = load(jd)
    done = []
    for pair in a.pairs:
        key, sep, val = pair.partition("=")
        parts = key.split(".")
        if not sep or not all(parts):
            die(f"Ugyldig «{pair}». Bruk nøkkel=verdi, f.eks. source.title='\"Tittel\"'.")
        if not (key == "production_mode" or (parts[0] in ("source", "settings", "billing") and len(parts) >= 2)):
            die(f"«{key}» kan ikke settes her. Tillatt: source.*, settings.*, billing.*, production_mode. "
                "Trinn settes med: donvideo.py step <jobb> <trinn> <status>.")
        try:
            val = json.loads(val)
        except json.JSONDecodeError:
            pass
        node = job
        for k in parts[:-1]:
            if not isinstance(node.setdefault(k, {}), dict):
                die(f"«{k}» i «{key}» er ikke en gruppe av felt.")
            node = node[k]
        if isinstance(node.get(parts[-1]), dict) and not isinstance(val, dict):
            die(f"«{key}» er en gruppe av felt. Sett ett felt om gangen, f.eks. {key}.<felt>=verdi.")
        node[parts[-1]] = val
        done.append(f"  {key} = {json.dumps(val, ensure_ascii=False)}")
    save(jd, job)
    print("Oppdatert:\n" + "\n".join(done))


def cmd_extjob(a):
    """Lagre ekstern jobb-ID umiddelbart etter opprettelse (unngår dobbel belastning ved avbrudd)."""
    jd = job_dir(a.job)
    job = load(jd)
    for e in job["external_jobs"]:
        if e["id"] == a.id:
            e.update({"status": a.status or e.get("status"), "updated": now(), "note": a.note or e.get("note")})
            break
    else:
        job["external_jobs"].append({"service": a.service, "kind": a.kind, "id": a.id,
                                     "status": a.status or "submitted", "created": now(), "note": a.note})
    save(jd, job)
    print(f"Ekstern jobb lagret: {a.service} {a.kind} {a.id}")


def cmd_ledger(a):
    jd = job_dir(a.job)
    job = load(jd)
    job["resources"].append({"service": a.service, "what": a.what, "credits": a.credits,
                             "estimated": bool(a.estimated), "at": now()})
    save(jd, job)
    print("Ressursbruk ført.")


# ------------------------------------------------------------------ source
def parse_json3(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    lines = []
    for ev in data.get("events", []):
        txt = "".join(seg.get("utf8", "") for seg in ev.get("segs", []) or []).replace("\n", " ").strip()
        if txt:
            lines.append((ev.get("tStartMs", 0) / 1000.0, txt))
    return lines


def cmd_source(a):
    jd = job_dir(a.job)
    job = load(jd)
    src = job["source"]
    kdir = jd / "kilde"
    kdir.mkdir(exist_ok=True)
    if a.transcript_file and a.video_file:
        die("Velg enten --transcript-file eller --video-file, ikke begge.")
    if a.transcript_file:
        shutil.copy(Path(a.transcript_file).resolve(), kdir / "transkripsjon_kilde.txt")
        src["transcript_method"] = a.method or "levert som fil av bruker"
        set_step(job, "source", "done", "transkripsjon importert fra fil")
        save(jd, job)
        return print("Transkripsjon importert.")
    if a.video_file:
        vfile = Path(a.video_file).resolve()   # tåler filnavn som begynner med bindestrek
        info = probe(vfile)
        run(["ffmpeg", "-y", "-v", "error", "-i", vfile, "-vn", "-ac", "1", "-ar", "16000",
             kdir / "kilde_lyd_16k.wav"], check=True)
        src["duration_s"] = round(info["duration"], 2)
        src["transcript_method"] = "lokal fil levert - lyd hentet ut, transkripsjon gjenstår"
        set_step(job, "source", "partial", "lyd hentet fra levert videofil; transkriber og sett done")
        save(jd, job)
        return print(f"Lyd hentet ut: {kdir / 'kilde_lyd_16k.wav'} ({info['duration']:.1f} s)")
    if run([sys.executable, "-m", "yt_dlp", "--version"]).returncode != 0:
        print("yt-dlp mangler - installerer (PyPI)...")
        run([sys.executable, "-m", "pip", "install", "-q", "yt-dlp"], check=True)
    p = run([sys.executable, "-m", "yt_dlp", "--dump-single-json", "--skip-download", "--no-warnings",
             src["canonical_url"]])
    if p.returncode != 0:
        err = p.stderr.strip()
        low, status = err.lower(), "blocked"
        if "403" in err and ("proxy" in low or "tunnel" in low):
            reason = "blokkert av nettverksregel i miljøet (proxy 403 for youtube.com)"
        elif "not a bot" in low:
            reason = "YouTube krever bot-kontroll fra denne IP-adressen"
        elif "private video" in low:
            reason, status = "videoen er privat", "failed"
        elif "confirm your age" in low or "age-restricted" in low:
            reason, status = "videoen er aldersbegrenset og krever innlogging", "failed"
        elif "unavailable" in low or "removed" in low or "does not exist" in low:
            reason, status = "videoen er utilgjengelig eller fjernet", "failed"
        else:
            reason, status = "feil fra yt-dlp: " + (err.splitlines()[-1][:160] if err else "ukjent"), "failed"
        set_step(job, "source", status, reason)
        (kdir / "ytdlp_feil.txt").write_text(err[-4000:], encoding="utf-8")
        save(jd, job)
        print(f"{'BLOKKERT' if status == 'blocked' else 'FEILET'}: {reason}\nDetaljer: {kdir / 'ytdlp_feil.txt'}\n"
              "Alternativ: donvideo.py source <jobb> --video-file <fil> eller --transcript-file <fil>")
        sys.exit(2)
    meta = json.loads(p.stdout)
    (kdir / "metadata.json").write_text(json.dumps(
        {k: meta.get(k) for k in ("id", "title", "channel", "uploader", "channel_url", "upload_date", "duration",
                                  "language", "description", "webpage_url", "width", "height")},
        ensure_ascii=False, indent=2), encoding="utf-8")
    src.update({"title": meta.get("title"), "channel": meta.get("channel") or meta.get("uploader"),
                "duration_s": meta.get("duration"), "language": meta.get("language")})
    manual = {k: v for k, v in (meta.get("subtitles") or {}).items() if k != "live_chat"}
    auto = meta.get("automatic_captions") or {}
    origs = [k for k in auto if k.endswith("-orig")]
    lang = meta.get("language") or (origs[0][:-5] if len(origs) == 1 else None)
    pick, kind, why_none = None, "manuell teksting", "ingen teksting funnet"
    if lang:
        base_lang = lang.split("-")[0]  # f.eks. en-US -> en; "-orig" er ASR av originalsporet
        pick = next((k for k in manual if k.split("-")[0] == base_lang), None)
        if not pick:
            pick = next((k for k in (f"{base_lang}-orig", f"{lang}-orig", base_lang, lang) if k in auto), None)
            kind = "automatisk teksting (ASR fra YouTube)"
    elif len(manual) == 1:
        pick = next(iter(manual))
    else:
        why_none = "originalspråket er ukjent - velger ikke et maskinoversatt spor"
    if pick:
        # Tekstsporets signerte URL står allerede i svaret: ett YouTube-oppslag holder (flere utløser bot-sperre).
        tracks = (manual if kind == "manuell teksting" else auto).get(pick) or []
        url = next((t.get("url") for t in tracks if t.get("ext") == "json3"), None)
        raw = kdir / f"yt.{pick}.json3"
        got = run(["curl", "-sS", "-f", "-m", "40", "-o", raw, url]) if url else None
        try:
            lines = parse_json3(raw) if got and got.returncode == 0 else []
        except (ValueError, OSError):
            lines = []
        if not lines:
            why_none = (f"tekstsporet '{pick}' finnes, men kunne ikke lastes ned eller leses"
                        + (f" ({got.stderr.strip()[:120]})" if got and got.stderr.strip() else ""))
        else:
            (kdir / "transkripsjon_kilde.txt").write_text(
                "\n".join(f"[{t:7.2f}] {x}" for t, x in lines) + "\n", encoding="utf-8")
            src["transcript_method"] = (f"YouTube {kind}, spor '{pick}' - ikke kontrollert mot lyd "
                                        "(lyd ligger på googlevideo-verter som ikke er tillatt)")
            set_step(job, "source", "done", f"{kind} hentet ({len(lines)} linjer)")
            save(jd, job)
            return print(f"Kilde hentet: «{src['title']}» ({src['channel']}), {src['duration_s']} s, {kind}.")
    set_step(job, "source", "partial", f"metadata hentet; {why_none} - trenger lyd + lokal transkripsjon")
    save(jd, job)
    print(f"Metadata hentet, men {why_none}. Hent lyd og transkriber lokalt.")


# ------------------------------------------------------------------ subtitles
WORD_RE = re.compile(r"[^ ]+")  # deler bare på vanlig mellomrom; tallgrupper holdes samlet med hardt mellomrom
ABBREV = {"bl.a.", "f.eks.", "ca.", "osv.", "nr.", "dvs.", "kl.", "jf.", "mv.", "evt.", "inkl.", "ekskl.", "pga.",
          "mht.", "m.m.", "o.l.", "e.l.", "t.o.m.", "f.o.m.", "dr.", "prof.", "st.", "tlf.", "etc.", "vs."}


def tokens(text):
    return WORD_RE.findall(re.sub(r"[\n\t\r]+", " ", text))


def read_manus(path):
    """Talt tekst: kommentarlinjer (#) og [regi] fjernes."""
    return read_manus_text(Path(path).read_text(encoding="utf-8-sig"))


def read_manus_text(content):
    lines = []
    for raw in content.splitlines():
        if raw.strip().startswith("#"):
            continue
        lines.append(re.sub(r"\[[^\]]*\]", " ", raw))
    text = re.sub(r"\s+", " ", " ".join(lines)).strip()
    text = re.sub(r" +([,.;:!?…])", r"\1", text)                 # ingen løse skilletegn etter fjernet [regi]
    return re.sub(r"(?<=\d) (?=\d{3}(?!\d))", "\u00a0", text)   # «10 000» holdes samlet


def norm(w):
    return re.sub(r"[^\wæøåÆØÅ]", "", w.lower())


def sentence_end(w, nxt=None):
    """Ekte setningsslutt? Ordenstall (17. mai), forkortelser (f.eks.) og initialer teller ikke."""
    if not re.search(r"[.!?…][\"»”)]*$", w):
        return False
    if re.search(r"[!?…][\"»”)]*$", w):
        return True
    core = w.lower().strip("\"»«”“()")
    if re.fullmatch(r"\d+\.", core):
        return nxt is None or not nxt[:1].islower()
    if core in ABBREV or re.fullmatch(r"[a-zæøå]\.", core):
        return nxt is None
    return True


FUNC_WORDS = r"^(og|i|på|til|av|en|et|ei|å|som|er|for|med|at|det|den|de|du|jeg|vi|men|så|om|fra|har|kan|skal|vil|ikke)$"


def split_balanced(idx, words, max_chars):
    """Del en for lang setningsdel i jevne blokker (unngår enslige ord og heng på småord)."""
    def tlen(i, j):
        return len(" ".join(words[k] for k in idx[i:j]))
    m = len(idx)
    if tlen(0, m) <= max_chars or m == 1:
        return [idx]
    n = math.ceil(tlen(0, m) / max_chars)
    while n <= m:
        target = tlen(0, m) / n
        INF = float("inf")
        cost = [[INF] * (m + 1) for _ in range(n + 1)]
        back = [[0] * (m + 1) for _ in range(n + 1)]
        cost[0][0] = 0.0
        for parts in range(1, n + 1):
            for j in range(parts, m + 1):
                for i in range(parts - 1, j):
                    if cost[parts - 1][i] == INF:
                        continue
                    ln = tlen(i, j)
                    if ln > max_chars and j - i > 1:
                        continue
                    c = cost[parts - 1][i] + (ln - target) ** 2
                    last = words[idx[j - 1]]
                    if j < m and (re.match(FUNC_WORDS, norm(last), re.I) or last.lower() in ABBREV
                                  or re.fullmatch(r"\d+\.", last)):
                        c += 60  # ikke la en blokk ende på småord, forkortelse eller ordenstall
                    if c < cost[parts][j]:
                        cost[parts][j], back[parts][j] = c, i
        if cost[n][m] < INF:
            cuts, j = [], m
            for parts in range(n, 0, -1):
                i = back[parts][j]
                cuts.append((i, j))
                j = i
            return [idx[i:j] for i, j in reversed(cuts)]
        n += 1
    return [[k] for k in idx]


def build_cues(words, max_chars=42, max_line=22):
    """Del ord i korte, lesbare tekstblokker med naturlige setningsbrudd."""
    segs, cur = [], []
    for i, w in enumerate(words):
        cur.append(i)
        text = " ".join(words[j] for j in cur)
        hard = sentence_end(w, words[i + 1] if i + 1 < len(words) else None)
        soft = re.search(r"[,;:–—]$", w) is not None and len(text) >= 14
        if hard or soft or i == len(words) - 1:
            segs.append(cur)
            cur = []
    cues = [part for seg in segs for part in split_balanced(seg, words, max_chars)]
    return [{"idx": c, "text": wrap(" ".join(words[j] for j in c), max_line)} for c in cues]


def wrap(text, max_line):
    ws = text.split(" ")
    if len(text) <= max_line or len(ws) < 2:
        return text
    best, best_score = None, None
    for k in range(1, len(ws)):
        l1, l2 = " ".join(ws[:k]), " ".join(ws[k:])
        over = max(0, len(l1) - max_line) + max(0, len(l2) - max_line)
        score = over * 100 + abs(len(l1) - len(l2)) + (4 if re.match(FUNC_WORDS, norm(ws[k - 1]), re.I) else 0)
        if best_score is None or score < best_score:
            best, best_score = (l1, l2), score
    return "\n".join(best)


def parse_srt(path):
    cues = []
    for block in re.split(r"\n\s*\n", Path(path).read_text(encoding="utf-8-sig").strip()):
        m = re.search(r"(\d+):(\d+):(\d+)[,.](\d+)\s*-->\s*(\d+):(\d+):(\d+)[,.](\d+)", block)
        if not m:
            continue
        g = [int(x) for x in m.groups()]
        st = g[0] * 3600 + g[1] * 60 + g[2] + g[3] / 1000
        en = g[4] * 3600 + g[5] * 60 + g[6] + g[7] / 1000
        text = re.sub(r"<[^>]+>", "", block[m.end():]).strip().replace("\n", " ")
        if text:
            cues.append((st, en, text))
    return cues


def timed_words_from_srt(path):
    out = []
    for st, en, text in parse_srt(path):
        ws = text.split()
        tot = sum(len(w) + 1 for w in ws) or 1
        t = st
        for w in ws:
            d = (en - st) * (len(w) + 1) / tot
            out.append({"word": w, "start": t, "end": t + d})
            t += d
    return out


def align_to_timed(words, timed, dur):
    """Legg manusord på tidsstemplede ord (fra tjenestens teksting eller ASR)."""
    def end_of(t):
        return t.get("end", t["start"] + 0.25)
    a, b = [norm(w) for w in words], [norm(t["word"]) for t in timed]
    times = [None] * len(words)
    sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
    matched = 0
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for k in range(i2 - i1):
                times[i1 + k] = (timed[j1 + k]["start"], end_of(timed[j1 + k]))
                matched += 1
        elif tag == "replace":  # avvikende ordlyd: del tidsrommet til de erstattede ordene
            st, en = timed[j1]["start"], end_of(timed[j2 - 1])
            n = i2 - i1
            for k in range(n):
                times[i1 + k] = (st + (en - st) * k / n, st + (en - st) * (k + 1) / n)
    cov = matched / max(1, len(words))
    if cov < 0.5:
        die(f"Bare {cov:.0%} av manusordene finnes i tidsstemplene - feil fil, eller talen følger ikke manus.")
    # Manusord uten treff: del tid med naboene, slik at ingen ord får null eller negativ varighet.
    i, missing = 0, 0
    while i < len(times):
        if times[i]:
            i += 1
            continue
        j = i
        while j < len(times) and not times[j]:
            j += 1
        missing += j - i
        lo, hi = (i - 1 if i > 0 else None), (j if j < len(times) else None)
        need = 0.22 * (j - i)
        start = times[lo][0] if lo is not None else max(0.0, times[hi][0] - need)
        end = times[hi][1] if hi is not None else min(dur, times[lo][1] + need)
        grp = list(range(lo if lo is not None else i, (hi if hi is not None else j - 1) + 1))
        if end - start < 0.12 * len(grp):
            end = min(dur, start + 0.12 * len(grp))
            start = max(0.0, end - 0.12 * len(grp))
        tot = sum(len(norm(words[k])) + 2 for k in grp)
        acc = 0.0
        for k in grp:
            wgt = len(norm(words[k])) + 2
            times[k] = (start + (end - start) * acc / tot, start + (end - start) * (acc + wgt) / tot)
            acc += wgt
        i = j
    return times, cov, missing


def align_by_pauses(words, media):
    """Reserve uten tidsstempler: fordel ord over tale, og fest setningsslutt til reelle pauser."""
    dur = audio_duration(media)
    sil = silences(media)
    speech, t = [], 0.0
    for s, e in sil:
        if s - t > 0.05:
            speech.append((t, s))
        t = e
    if dur - t > 0.05:
        speech.append((t, dur))
    if not speech:
        die("Fant ingen tale i lydsporet.")
    total = sum(e - s for s, e in speech)

    def w8(w):  # tall leses lenger enn de skrives
        n = norm(w)
        return len(n) + 2 + 3 * sum(c.isdigit() for c in n)
    tot_w = sum(w8(w) for w in words)
    # Fest setningsslutt til pauser. Kostnaden regnes fra forrige anker (taletid mot ordvekt med lokal talefart),
    # så ett feilvektet ord forskyver ikke alt som kommer etter. En pause uten forklaring koster også.
    gaps = [(speech[i][1], speech[i + 1][0]) for i in range(len(speech) - 1)]
    S, acc = [], 0.0  # taletid før hver pause
    for i in range(len(gaps)):
        acc += speech[i][1] - speech[i][0]
        S.append(acc)
    cand, acc = [], 0.0  # (ordindeks, akkumulert vekt)
    for i, w in enumerate(words[:-1]):
        acc += w8(w)
        if sentence_end(w, words[i + 1]) or w.endswith(":"):
            cand.append((i, acc))
    K, M, SKIP, INF, LK, LJ = len(cand), len(gaps), 0.9, float("inf"), 6, 12
    cum = [0.0]
    for g in gaps:
        cum.append(cum[-1] + min(g[1] - g[0], 1.0))

    def stretch(k0, j0, k, j):
        w0, s0 = (cand[k0][1], S[j0]) if k0 >= 0 else (0.0, 0.0)
        rate = (total - s0) / max(1e-9, tot_w - w0)
        return (abs((S[j] - s0) - (cand[k][1] - w0) * rate) + SKIP * (k - k0 - 1) + (cum[j] - cum[j0 + 1])
                - 0.4 * min(gaps[j][1] - gaps[j][0], 1.0))
    f = [[INF] * M for _ in range(K)]
    back = [[None] * M for _ in range(K)]
    for k in range(K):
        for j in range(M):
            best, bp = (stretch(-1, -1, k, j), (-1, -1)) if k < LK else (INF, None)
            for k0 in range(max(0, k - LK), k):
                for j0 in range(max(0, j - LJ), j):
                    if f[k0][j0] < INF:
                        c = f[k0][j0] + stretch(k0, j0, k, j)
                        if c < best:
                            best, bp = c, (k0, j0)
            f[k][j], back[k][j] = best, bp
    best, end = SKIP * K + cum[M], None
    for k in range(K):
        for j in range(M):
            if f[k][j] < INF:
                c = f[k][j] + SKIP * (K - 1 - k) + (cum[M] - cum[j + 1])
                if c < best:
                    best, end = c, (k, j)
    bounds = []
    while end and end[0] >= 0:
        bounds.append((cand[end[0]][0], gaps[end[1]]))
        end = back[end[0]][end[1]]
    bounds.reverse()
    times, start_i, start_t = [None] * len(words), 0, speech[0][0]
    segs = [(i, g) for i, g in bounds] + [(len(words) - 1, (speech[-1][1], speech[-1][1]))]
    last_i = -1
    for end_i, gap in segs:
        if end_i <= last_i:
            continue
        chunk = list(range(start_i, end_i + 1))
        cw = sum(w8(words[k]) for k in chunk)
        seg_speech = [(max(s, start_t), min(e, gap[0])) for s, e in speech if min(e, gap[0]) - max(s, start_t) > 0.01]
        seg_total = sum(e - s for s, e in seg_speech) or 0.01

        def seg_at(sp):
            a2 = 0.0
            for s, e in seg_speech:
                if sp <= a2 + (e - s) + 1e-9:
                    return s + (sp - a2)
                a2 += e - s
            return seg_speech[-1][1] if seg_speech else start_t
        a3 = 0.0
        for k in chunk:
            w = w8(words[k])
            times[k] = (seg_at(seg_total * a3 / cw), seg_at(seg_total * (a3 + w) / cw))
            a3 += w
        start_i, start_t, last_i = end_i + 1, gap[1], end_i
    return times, len(bounds)


def fmt_srt(t):
    ms = int(round(max(0, t) * 1000))
    return f"{ms // 3600000:02d}:{ms // 60000 % 60:02d}:{ms // 1000 % 60:02d},{ms % 1000:03d}"


def fmt_ass(t):
    cs = int(round(max(0, t) * 100))
    return f"{cs // 360000}:{cs // 6000 % 60:02d}:{cs // 100 % 60:02d}.{cs % 100:02d}"


def write_subs(cues, times, dur, srt_path, ass_path, width, height, style):
    timed = []
    for c in cues:
        st, en = times[c["idx"][0]][0], times[c["idx"][-1]][1] + 0.10
        timed.append([st, en, c["text"]])
    for i, c in enumerate(timed):              # starttider strengt stigende og innenfor filmen
        floor = timed[i - 1][0] + 0.2 if i else 0.0
        c[0] = max(0.0, min(max(c[0], floor), dur - 0.1))
    for i, c in enumerate(timed):
        nxt = timed[i + 1][0] if i + 1 < len(timed) else dur
        c[1] = max(c[1], c[0] + 0.55)          # minste lesetid
        if nxt - c[1] < 0.30:                  # lukk små hull, aldri overlapp
            c[1] = nxt
        c[1] = min(c[1], nxt, dur)
        if c[1] <= c[0]:
            c[1] = min(dur, c[0] + 0.1)
    srt = "".join(f"{i}\n{fmt_srt(s)} --> {fmt_srt(e)}\n{t}\n\n" for i, (s, e, t) in enumerate(timed, 1))
    Path(srt_path).write_text(srt, encoding="utf-8")
    k = height / 1920.0
    font = style.get("font", "Inter")
    try:
        if run(["fc-match", "-f", "%{family}", f"{font}:bold"]).stdout.split(",")[0].strip().lower() != font.lower():
            font = "DejaVu Sans"
    except FileNotFoundError:  # fontconfig mangler: libass velger selv en reservefont
        pass
    head = (
        "[Script Info]\nScriptType: v4.00+\nWrapStyle: 2\nScaledBorderAndShadow: yes\n"
        f"PlayResX: {width}\nPlayResY: {height}\n\n[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, "
        "Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, "
        "MarginR, MarginV, Encoding\n"
        f"Style: Don,{font},{round(style.get('size', 66) * k)},&H00FFFFFF,&H00FFFFFF,&H00000000,&H96000000,-1,0,0,0,"
        f"100,100,0,0,1,{round(style.get('outline', 5) * k, 1)},{round(style.get('shadow', 2) * k, 1)},2,"
        f"{round(style.get('margin_x', 100) * k)},{round(style.get('margin_x', 100) * k)},"
        f"{round(style.get('margin_bottom', 430) * k)},1\n\n[Events]\n"
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n")
    body = "".join(f"Dialogue: 0,{fmt_ass(s)},{fmt_ass(e)},Don,,0,0,0,,{t.replace(chr(10), chr(92) + 'N')}\n"
                   for s, e, t in timed)
    Path(ass_path).write_text(head + body, encoding="utf-8")
    return timed


def make_subs(jd, job, media, manus, srt=None, words_json=None):
    text = read_manus(manus)
    words = tokens(text)
    if not words:
        die(f"Manus er tomt: {manus}")
    info = probe(media)
    if words_json or srt:
        if words_json:
            timed = json.loads(Path(words_json).read_text(encoding="utf-8"))
            if not (isinstance(timed, list) and timed and all(
                    isinstance(t, dict) and isinstance(t.get("word"), str) and isinstance(t.get("start"), (int, float))
                    for t in timed)):
                die('--words må være en JSON-liste: [{"word": "Hei", "start": 0.42, "end": 0.61}, ...] med sekunder.')
            if max(t["start"] for t in timed) > info["duration"] + 2:
                die("--words har tider utenfor filmens lengde. Tidene skal være i sekunder, ikke millisekunder.")
        else:
            timed = timed_words_from_srt(srt)
        times, cov, missing = align_to_timed(words, timed, info["duration"])
        src_name = "ordtidsstempler fra fil" if words_json else "tidskoder fra levert teksting"
        method = f"{src_name} ({cov:.0%} av manusordene traff)"
        job["settings"].update({"subtitle_coverage": round(cov, 4), "subtitle_missing_words": missing,
                                "subtitle_words": len(words)})
        if missing:
            method += f"; AVVIK: {missing} manusord manglet i tidsstemplene og er plassert ved interpolasjon"
            print(f"ADVARSEL: {missing} manusord finnes ikke i tidsstemplene. Sier stemmen noe annet enn manus? "
                  "Rett Manus_nb.txt til det som faktisk sies, eller regenerer.")
    else:
        times, anchors = align_by_pauses(words, media)
        job["settings"].update({"subtitle_coverage": None, "subtitle_missing_words": None, "subtitle_words": len(words)})
        method = f"ESTIMERT fra pauser i lyden ({anchors} setningsgrenser festet) - må kontrolleres visuelt"
    profile = json.loads(PROFILE.read_text(encoding="utf-8")) if PROFILE.exists() else {}
    style = profile.get("subtitle_style", {})
    cues = build_cues(words, style.get("max_chars", 42), style.get("max_line", 22))
    w = (info["video"] or {}).get("width") or 1080
    h = (info["video"] or {}).get("height") or 1920
    if h < 1920 and abs(w / h - 9 / 16) < 0.02:
        w, h = 1080, 1920
    exp = jd / "eksport"
    exp.mkdir(exist_ok=True)
    timed = write_subs(cues, times, info["duration"], exp / EXPORT_NAMES["srt"], exp / "undertekster.ass", w, h, style)
    job["settings"]["subtitle_timing"] = method
    set_step(job, "subs", "done", f"{len(timed)} tekstblokker; {method}")
    return timed, method


def cmd_subs(a):
    jd = job_dir(a.job)
    job = load(jd)
    manus = Path(a.manus) if a.manus else jd / EXPORT_NAMES["manus"]
    if not manus.exists():
        die(f"Mangler manus: {manus}")
    timed, method = make_subs(jd, job, a.media, manus, a.srt, a.words)
    save(jd, job)
    print(f"{len(timed)} tekstblokker skrevet. Timing: {method}")


# ------------------------------------------------------------------ asr
def cmd_asr(a):
    """Lokal talegjenkjenning: hvilket språk snakkes, hva sies, og når (ordtider til --words)."""
    jd = job_dir(a.job)
    media = Path(a.media).resolve()
    if not media.exists():
        die(f"Finner ikke filen: {media}")
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        print("faster-whisper mangler - installerer (PyPI)...")
        run([sys.executable, "-m", "pip", "install", "-q", "faster-whisper"], check=True)
        from faster_whisper import WhisperModel
    try:
        model = WhisperModel(a.model, device="cpu", compute_type="int8", cpu_threads=os.cpu_count() or 4)
    except Exception as e:  # modellen hentes fra huggingface.co første gang
        die(f"Kunne ikke laste Whisper-modellen «{a.model}»: {str(e)[:200]}\n"
            "Modellen lastes ned fra huggingface.co, som bare er tillatt i miljøet «don-video».")
    import numpy as np  # følger med faster-whisper
    pcm = subprocess.run(["ffmpeg", "-v", "error", "-i", str(media), "-vn", "-ac", "1", "-ar", "16000", "-f", "s16le", "-"],
                         capture_output=True)  # egen dekoding: uavhengig av PyAV-versjonen
    if pcm.returncode != 0 or not pcm.stdout:
        die(f"Kunne ikke lese lyd fra {media}: {pcm.stderr.decode(errors='replace')[:200]}")
    audio = np.frombuffer(pcm.stdout, dtype=np.int16).astype(np.float32) / 32768.0
    _, info = model.transcribe(audio, language=None)
    probs = sorted(info.all_language_probs or [], key=lambda x: -x[1])[:4]
    lang = a.language or info.language
    segments, _ = model.transcribe(audio, language=lang, word_timestamps=True, vad_filter=True,
                                   condition_on_previous_text=False)
    words = [{"word": w.word.strip(), "start": round(w.start, 3), "end": round(w.end, 3)}
             for seg in segments for w in (seg.words or []) if w.word.strip()]
    out = jd / "asr"
    out.mkdir(exist_ok=True)
    (out / f"{media.stem}.words.json").write_text(json.dumps(words, ensure_ascii=False), encoding="utf-8")
    heard = " ".join(w["word"] for w in words)
    (out / f"{media.stem}.txt").write_text(heard + "\n", encoding="utf-8")
    result = {"media": str(media), "model": a.model, "detected_language": info.language,
              "detected_probability": round(info.language_probability, 3),
              "top_languages": [[k, round(v, 3)] for k, v in probs], "transcribed_as": lang, "words": len(words)}
    print(f"Oppdaget språk: {info.language} ({info.language_probability:.0%}). Mest sannsynlige: "
          + ", ".join(f"{k} {v:.0%}" for k, v in probs))
    print(f"Hørt ({len(words)} ord): {heard[:400]}{'...' if len(heard) > 400 else ''}")
    ref_path = Path(a.manus) if a.manus else jd / EXPORT_NAMES["manus"]
    if ref_path.exists():
        ref = [norm(w) for w in tokens(read_manus(ref_path)) if norm(w)]
        hyp = [norm(w["word"]) for w in words if norm(w["word"])]
        sm = difflib.SequenceMatcher(None, ref, hyp, autojunk=False)
        same = sum(i2 - i1 for tag, i1, i2, _, _ in sm.get_opcodes() if tag == "equal")
        diffs = [(tag, " ".join(ref[i1:i2]), " ".join(hyp[j1:j2])) for tag, i1, i2, j1, j2 in sm.get_opcodes() if tag != "equal"]
        result.update({"manus_words": len(ref), "matched": same, "match_share": round(same / max(1, len(ref)), 3),
                       "differences": diffs[:60]})
        print(f"Mot manus: {same} av {len(ref)} ord gjenkjent ({same / max(1, len(ref)):.0%}). Avvik (manus -> hørt):")
        for tag, r, h in diffs[:15]:
            print(f"  {'mangler' if tag == 'delete' else 'ekstra' if tag == 'insert' else 'ulikt'}: «{r}» -> «{h}»")
        print("NB: talegjenkjenning gjør egne feil. Avvik er noe å lytte etter, ikke bevis på feil i talen.")
    (out / f"{media.stem}.asr.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Ordtider: {out / (media.stem + '.words.json')}  (kan brukes med finish --words)")


# ------------------------------------------------------------------ finish
def loudnorm_filter(path, target=-14.0):
    p = run(["ffmpeg", "-hide_banner", "-nostats", "-i", path, "-vn", "-af",
             f"loudnorm=I={target}:TP=-1.5:LRA=11:print_format=json", "-f", "null", "-"])
    m = re.search(r"\{[^{}]*\"input_i\"[^{}]*\}", p.stderr, re.S)
    if not m:
        return f"loudnorm=I={target}:TP=-1.5:LRA=11", None
    d = json.loads(m.group(0))
    if d["input_i"] in ("-inf", "inf"):
        return "anull", d
    return (f"loudnorm=I={target}:TP=-1.5:LRA=11:measured_I={d['input_i']}:measured_TP={d['input_tp']}:"
            f"measured_LRA={d['input_lra']}:measured_thresh={d['input_thresh']}:offset={d['target_offset']}:"
            "linear=true"), d


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def cmd_finish(a):
    jd = job_dir(a.job)
    try:
        _finish(a, jd)
    except BaseException:  # også die() og avbrudd: en halv eksport skal aldri se ferdig ut
        job = load(jd)
        if job["steps"].get("export", {}).get("status") != "done":
            set_step(job, "export", "failed", "eksporten ble avbrutt - kjør finish på nytt")
            job["settings"].pop("export_files", None)
            save(jd, job)
        raise


def _finish(a, jd):
    job = load(jd)
    exp = jd / "eksport"
    exp.mkdir(exist_ok=True)
    master = Path(a.master).resolve()
    if not master.exists():
        die(f"Finner ikke master: {master}")
    if master.parent == exp.resolve() and master.name in EXPORT_NAMES.values():
        die("Master kan ikke være en av eksportfilene. Bruk den originale filen fra genereringen.")
    music = Path(a.music).resolve() if a.music else None
    if music and not music.exists():
        die(f"Finner ikke musikkfilen: {music}")
    manus = Path(a.manus) if a.manus else jd / EXPORT_NAMES["manus"]
    if not manus.exists():
        die(f"Mangler manus: {manus}")
    if manus.resolve() != (exp / EXPORT_NAMES["manus"]).resolve():
        shutil.copy(manus, exp / EXPORT_NAMES["manus"])
    info = probe(master)
    if not info["video"] or not info["audio"]:
        die("Master mangler video- eller lydstrøm.")
    # Ny eksport gjør gammel kontroll og gammelt notat ugyldig.
    set_step(job, "export", "running")
    set_step(job, "verify", "pending")
    for stale in (exp / EXPORT_NAMES["note"], jd / "kontroll" / "verification.json", exp / ".ny_ren.mp4",
                  exp / ".ny_tekst.mp4"):
        stale.unlink(missing_ok=True)
    job["settings"].pop("export_files", None)
    save(jd, job)
    w, h = info["video"]["width"], info["video"]["height"]
    scale, note = None, []
    if abs(w / h - 9 / 16) < 0.02:
        if h < 1920:
            scale = "scale=1080:1920:flags=lanczos"
            note.append(f"Master var {w}x{h}; skalert til 1080x1920. Oppskalering gir ingen ny bildedetalj.")
        ow, oh = (1080, 1920) if h <= 1920 else (w, h)
    else:
        ow, oh = w - w % 2, h - h % 2
        if (ow, oh) != (w, h):
            scale = f"scale={ow}:{oh}"
        note.append(f"Master er {w}x{h} (ikke 9:16). Beholdt format for å unngå tilfeldig beskjæring.")
    vdur, adur = info["video"]["duration"], info["audio"]["duration"]
    target = max(vdur, adur)  # aldri kutt tale; fyll heller ut det korteste sporet
    pad_v = target - vdur if target - vdur > 0.05 else 0.0
    if abs(vdur - adur) > 0.15:
        note.append(f"Master hadde ulik lengde på bilde ({vdur:.2f} s) og lyd ({adur:.2f} s); korteste spor er fylt ut.")
    # 1) ren talelyd (før eventuell musikk)
    run(["ffmpeg", "-y", "-v", "error", "-i", master, "-vn", "-ac", "2", "-ar", "48000", "-c:a", "pcm_s16le",
         exp / EXPORT_NAMES["voice"]], check=True)
    # 2) teksting fra faktisk ferdig lyd
    timed, method = make_subs(jd, job, master, manus, a.srt, a.words)
    # 3) lydkjede: skånsom lydstyrke-normalisering, ingen støyfjerning
    ln, measured = loudnorm_filter(master)
    inputs, amap = ["-i", master], f"[0:a]{ln},aresample=48000,apad[a]"
    if music:
        inputs += ["-stream_loop", "-1", "-i", music]
        fade = max(0.0, target - 1.5)
        amap = (f"[0:a]{ln},aresample=48000,apad[v];[1:a]volume={a.music_db}dB,afade=t=in:d=1,"
                f"afade=t=out:st={fade:.2f}:d=1.5,aresample=48000[m];[v][m]amix=inputs=2:duration=first:normalize=0[a]")
        note.append(f"Bakgrunnsmusikk mikset på {a.music_db} dB under talen: {music.name}")
    aenc = ["-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", "-t", f"{target:.3f}"]
    venc = ["-c:v", "libx264", "-preset", "medium", "-crf", "18", "-profile:v", "high"]
    pre = (f"{scale}," if scale else "") + (f"tpad=stop_mode=clone:stop_duration={pad_v:.3f}," if pad_v else "")
    copy_ok = not pre and info["video"]["codec"] == "h264" and info["video"]["pix_fmt"] == "yuv420p"
    tmp_clean, tmp_burned = ".ny_ren.mp4", ".ny_tekst.mp4"  # byttes inn først når begge er ferdige
    if copy_ok:  # ren versjon: originalt videospor urørt, bare lyden normaliseres
        run(["ffmpeg", "-y", "-v", "error", *inputs, "-filter_complex", amap, "-map", "0:v:0", "-map", "[a]",
             "-c:v", "copy", *aenc, tmp_clean], cwd=exp, check=True)
        note.append("Ren versjon: videosporet er kopiert uendret fra master (ingen ny komprimering).")
    else:
        run(["ffmpeg", "-y", "-v", "error", *inputs, "-filter_complex", f"[0:v]{pre}format=yuv420p[vo];{amap}",
             "-map", "[vo]", "-map", "[a]", *venc, *aenc, tmp_clean], cwd=exp, check=True)
    run(["ffmpeg", "-y", "-v", "error", *inputs, "-filter_complex",
         f"[0:v]{pre}ass=undertekster.ass,format=yuv420p[vo];{amap}",
         "-map", "[vo]", "-map", "[a]", *venc, *aenc, tmp_burned], cwd=exp, check=True)
    (exp / tmp_clean).replace(exp / EXPORT_NAMES["clean"])
    (exp / tmp_burned).replace(exp / EXPORT_NAMES["burned"])
    job["settings"].update({"master": str(master), "master_probe": info, "output_size": f"{ow}x{oh}",
                            "loudness_in_lufs": (measured or {}).get("input_i"), "notes": note,
                            "voice_wav_contains_music": False, "music": str(music) if music else None})
    job["settings"]["export_files"] = {EXPORT_NAMES[k]: {"size": (exp / EXPORT_NAMES[k]).stat().st_size,
                                                        "sha256": sha256(exp / EXPORT_NAMES[k])} for k in DELIVERABLES}
    set_step(job, "export", "done", f"{EXPORT_NAMES['clean']} + {EXPORT_NAMES['burned']}")
    save(jd, job)
    print(f"Eksportert til {exp}\n  Teksting: {len(timed)} blokker ({method})")
    for n in note:
        print("  NB:", n)


# ------------------------------------------------------------------ verify
def detect(path, vf, pattern):
    p = run(["ffmpeg", "-hide_banner", "-nostats", "-i", path, "-vf", vf, "-an", "-f", "null", "-"])
    return re.findall(pattern, p.stderr)


def band_psnr(burned, clean, t):
    """Likhet i tekstfeltet (nedre midtre del) mellom tekstet og ren versjon. Høy verdi = ingen synlig tekst."""
    crop = "crop=iw:ih*0.35:0:ih*0.5"
    p = run(["ffmpeg", "-hide_banner", "-nostats", "-ss", f"{t:.2f}", "-i", burned, "-ss", f"{t:.2f}", "-i", clean,
             "-frames:v", "1", "-lavfi", f"[0:v]{crop}[a];[1:v]{crop}[b];[a][b]psnr", "-f", "null", "-"])
    m = re.search(r"average:(\S+)", p.stderr)
    if not m:
        return None
    return float("inf") if m.group(1) == "inf" else float(m.group(1))


def cmd_verify(a):
    jd = job_dir(a.job)
    job = load(jd)
    exp = jd / "eksport"
    # Kontroller bare en fullført eksport, og bare de filene den eksporten skrev (stopp før noe skrives).
    if job["steps"].get("export", {}).get("status") != "done":
        die("Eksporten er ikke fullført (status: " + str(job["steps"].get("export", {}).get("status"))
            + "). Kjør finish på nytt før verify.")
    for name, fp in (job["settings"].get("export_files") or {"": None}).items():
        f = exp / name
        if fp is None or not f.exists() or f.stat().st_size != fp["size"] or sha256(f) != fp["sha256"]:
            die(f"Eksportfilen «{name or '?'}» mangler eller er endret etter finish. Kjør finish på nytt før verify.")
    kdir = jd / "kontroll"
    kdir.mkdir(exist_ok=True)
    for old in kdir.glob("*.jpg"):
        old.unlink()
    report, hard = {"at": now(), "files": {}, "checks": []}, []

    def check(name, ok, detail, is_hard=True):
        report["checks"].append({"check": name, "ok": bool(ok), "detail": detail, "hard": is_hard})
        if not ok and is_hard:
            hard.append(name)
        print(f"  [{'OK ' if ok else 'FEIL' if is_hard else 'OBS'}] {name}: {detail}")

    speech = []
    for key in ("clean", "burned"):
        f = exp / EXPORT_NAMES[key]
        print(f"{f.name}")
        if not f.exists():
            check(f"{key}: finnes", False, "filen mangler")
            continue
        info = probe(f)
        report["files"][f.name] = info
        v, au = info["video"], info["audio"]
        check(f"{key}: strømmer", bool(v and au), f"video={v and v['codec']} lyd={au and au['codec']}")
        if not (v and au):
            continue
        dur = info["duration"]
        err = run(["ffmpeg", "-v", "error", "-i", f, "-f", "null", "-"]).stderr.strip()
        check(f"{key}: dekoder uten feil", not err, err[:200] or "hele filen dekodet")
        check(f"{key}: bildeformat", abs(v["width"] / v["height"] - 9 / 16) < 0.02,
              f"{v['width']}x{v['height']} @ {v['fps']} fps, {v['pix_fmt']}", is_hard=False)
        check(f"{key}: lyd/bilde samme lengde", abs(v["duration"] - au["duration"]) < 0.15,
              f"video {v['duration']:.2f} s, lyd {au['duration']:.2f} s")
        black = detect(f, "blackdetect=d=0.4:pix_th=0.10", r"black_start:([\d.]+) black_end:([\d.]+)")
        check(f"{key}: ingen svarte partier", not black, f"{black}" if black else "ingen >= 0,4 s")
        if key == "clean":
            voice = exp / EXPORT_NAMES["voice"]  # ren tale: musikk skal ikke skjule et kutt
            sil_src = voice if voice.exists() else f
            sil = silences(sil_src, noise_db=-38, min_d=0.25)
            sdur = audio_duration(sil_src)
            t0 = 0.0
            for s, e in sil:
                if s - t0 > 0.05:
                    speech.append((t0, s))
                t0 = e
            if sdur - t0 > 0.05:
                speech.append((t0, sdur))
            talk = sum(e - s for s, e in speech)
            check("clean: det er tale i lydsporet", talk >= max(1.0, 0.2 * sdur),
                  f"{talk:.1f} s lyd over stillhetsgrensen av {sdur:.1f} s")
            head = sil[0][1] if sil and sil[0][0] <= 0.02 else 0.0
            tail = sdur - sil[-1][0] if sil and sil[-1][1] >= sdur - 0.05 else 0.0
            check("clean: tale kuttes ikke i starten", head >= 0.03, f"{head:.2f} s luft før første lyd", is_hard=False)
            check("clean: tale kuttes ikke i slutten", tail >= 0.10, f"{tail:.2f} s luft etter siste lyd", is_hard=False)
            check("clean: talen stopper ikke midt i filmen", tail <= 4.0,
                  f"{tail:.1f} s stillhet til slutt")
            long_sil = [(round(s, 2), round(e, 2)) for s, e in sil if e - s > 2.5 and s > 0.1 and e < sdur - 0.1]
            check("clean: ingen lange stille hull", not long_sil, f"{long_sil}" if long_sil else "ingen > 2,5 s", is_hard=False)
            frz, open_start = [], None  # en frys som varer ut filmen får ingen freeze_end
            for kind, val in detect(f, "freezedetect=n=0.001:d=2.5", r"freeze_(start|end): ([\d.]+)"):
                if kind == "start":
                    open_start = float(val)
                elif open_start is not None:
                    frz.append((open_start, float(val)))
                    open_start = None
            if open_start is not None:
                frz.append((open_start, dur))
            during = [(round(s, 1), round(e, 1)) for s, e in frz
                      if sum(max(0.0, min(e, b) - max(s, x)) for x, b in speech) > 1.0]
            check("clean: bildet fryser ikke mens det snakkes", not during, f"{during}" if during else "ingen frys under tale")
            check("clean: ingen frys ellers", len(frz) == len(during), f"{frz}" if frz else "ingen >= 2,5 s", is_hard=False)
            _, m = loudnorm_filter(f)
            if m and m["input_i"] not in ("-inf", "inf"):
                lufs = float(m["input_i"])
                check("clean: lydstyrke", -17 <= lufs <= -12, f"{lufs:.1f} LUFS, topp {m['input_tp']} dBTP", is_hard=False)
            else:
                check("clean: lydstyrke", False, "lydsporet er stille")
            pts = [0.4, dur * 0.25, dur * 0.5, dur * 0.75, max(0.4, dur - 0.5)]
            for seg in job["settings"].get("segment_boundaries", []):
                pts += [max(0, seg - 0.08), seg + 0.08]
        else:
            pts = [dur * x for x in (0.1, 0.5, 0.9)]
        for i, t in enumerate(sorted(pts)):
            run(["ffmpeg", "-y", "-v", "error", "-ss", f"{t:.2f}", "-i", f, "-frames:v", "1", "-vf", "scale=540:-2",
                 kdir / f"{key}_{i:02d}_{t:06.2f}s.jpg"])
    srt, manus = exp / EXPORT_NAMES["srt"], exp / EXPORT_NAMES["manus"]
    clean, burned = exp / EXPORT_NAMES["clean"], exp / EXPORT_NAMES["burned"]
    if srt.exists() and manus.exists():
        cues = parse_srt(srt)
        mw = [norm(w) for w in tokens(read_manus(manus)) if norm(w)]
        sw = [norm(w) for _, _, t in cues for w in tokens(t) if norm(w)]
        check("teksting: alle manusord med", mw == sw, f"{len(sw)} ord i teksting / {len(mw)} i manus")
        dur = next(iter(report["files"].values()), {}).get("duration", 0)
        bad = [(round(s, 2), round(e, 2)) for s, e, _ in cues if not (0 <= s < e <= dur + 0.05)]
        order = all(cues[i][0] < cues[i + 1][0] and cues[i][1] <= cues[i + 1][0] + 0.001 for i in range(len(cues) - 1))
        check("teksting: gyldige tider, stigende, uten overlapp", bool(cues) and not bad and order,
              f"{len(cues)} blokker, siste slutter {cues[-1][1]:.2f} s av {dur:.2f} s" if cues and not bad else f"ugyldige: {bad[:3]}")
        short = [(round(c[0], 2), c[2]) for c in cues if c[1] - c[0] < 0.2]
        check("teksting: ingen blokk for kort til å leses", not short, f"{short[:3]}" if short else "alle >= 0,2 s")
        cps = max((len(t) / max(0.05, e - s) for s, e, t in cues), default=0)
        check("teksting: lesehastighet", cps <= 40, f"høyest {cps:.0f} tegn/s")
        check("teksting: behagelig lesehastighet", cps <= 25, f"høyest {cps:.0f} tegn/s", is_hard=False)
        tm = job["settings"].get("subtitle_timing") or ""
        check("teksting: timing fra faktiske tidsstempler", "ESTIMERT" not in tm and "AVVIK" not in tm, tm or "ukjent",
              is_hard=False)
        outside = [(round(s, 2), t) for s, e, t in cues
                   if speech and sum(max(0.0, min(e, b) - max(s, x)) for x, b in speech) < 0.05]
        check("teksting: hver blokk vises mens det er lyd", not outside, f"{outside[:3]}" if outside else "alle overlapper tale")
        if cues and speech:
            over = cues[-1][1] - speech[-1][1]
            check("teksting: siste blokk slutter ikke lenge etter siste lyd", over <= 1.5,
                  f"siste blokk slutter {max(0.0, over):.2f} s etter siste lyd")
        cov, miss, nw = (job["settings"].get(k) for k in ("subtitle_coverage", "subtitle_missing_words", "subtitle_words"))
        if cov is not None:
            check("teksting: talen dekker manus", miss <= max(2, round(0.05 * nw)) and cov >= 0.85,
                  f"{miss} av {nw} manusord fantes ikke i tidsstemplene ({cov:.0%} traff)")
        longest = max((len(line) for _, _, t in parse_srt_raw(srt) for line in t.split("\n")), default=0)
        check("teksting: korte linjer", longest <= 26, f"lengste linje {longest} tegn", is_hard=False)
        if cues and clean.exists() and burned.exists():
            mids = [(c[0] + c[1]) / 2 for c in (cues[0], cues[len(cues) // 2], cues[-1])]
            vals = [band_psnr(burned, clean, t) for t in mids]
            seen = [x for x in vals if x is not None and x < 30]
            check("burned: teksten er synlig i bildet", len(seen) >= 2,
                  "likhet med ren versjon i tekstfeltet: " + ", ".join("?" if x is None else f"{x:.0f} dB" for x in vals))
        check("teksting: første og siste setning", bool(cues) and cues[0][0] < 3.0,
              f"første blokk {cues[0][0]:.2f} s: «{cues[0][2]}» / siste: «{cues[-1][2]}»" if cues else "-", is_hard=False)
    else:
        check("teksting: filer finnes", False, "mangler SRT eller manus")
    todo = [n for n in STEPS[:5] if job["steps"].get(n, {}).get("status") != "done"]
    check("arbeidsflyt: tidligere trinn er markert ferdige", not todo,
          "ikke markert ferdig: " + ", ".join(todo) if todo else "kilde, manus, budsjett, prøve og generering er ferdige",
          is_hard=False)
    report["passed"] = not hard
    report["subtitle_timing"] = job["settings"].get("subtitle_timing")
    report["human_review_required"] = [
        "Se kontrollbildene i kontroll/ (ansikt, utsnitt, tekst dekker ikke munn).",
        "Stemmelikhet, norsk uttale og leppesynk kan ikke bevises maskinelt - må vurderes ved avspilling.",
    ]
    (kdir / "verification.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    set_step(job, "verify", "done" if not hard else "failed", "maskinell kontroll OK" if not hard else f"feil: {hard}")
    save(jd, job)
    write_note(jd, job, report)
    soft = [c["check"] for c in report["checks"] if not c["ok"] and not c["hard"]]
    print(f"\nKontrollbilder: {kdir}\nResultat: " + ("BESTÅTT (maskinell del)" if not hard else "IKKE BESTÅTT: " + ", ".join(hard))
          + (f"\nMerknader: {', '.join(soft)}" if soft else ""))
    sys.exit(0 if not hard else 1)


def parse_srt_raw(path):
    out = []
    for block in re.split(r"\n\s*\n", Path(path).read_text(encoding="utf-8-sig").replace("\r", "").strip()):
        lines = block.split("\n")
        if len(lines) >= 3:
            out.append((0, 0, "\n".join(lines[2:])))
    return out


def write_note(jd, job, report):
    s, st = job["source"], job["settings"]
    none = "ikke registrert i jobben"
    bill = job.get("billing") or {}
    hard = [c["check"] for c in report["checks"] if not c["ok"] and c.get("hard", True)]
    soft = [c["check"] for c in report["checks"] if not c["ok"] and not c.get("hard", True)]
    L = [f"# Produksjonsnotat - {job['job_id']}", "", f"Generert: {now()}", "",
         "Maskinell kontroll: " + ("BESTÅTT" if not hard else "IKKE BESTÅTT (" + ", ".join(hard) + ")")
         + (f". Merknader: {', '.join(soft)}" if soft else ""), "", "## Kilde",
         f"- URL: {s['url']}", f"- Video-ID: {s['video_id']}", f"- Tittel: {s['title']}", f"- Kanal: {s['channel']}",
         f"- Varighet: {s['duration_s']} s, språk: {s['language']}", f"- Taler: {s['speaker']} (er Don: {s['speaker_is_don']})",
         f"- Transkripsjon: {s['transcript_method']}", f"- Uklare partier: {s['unclear_parts'] or 'ingen notert'}", "",
         "## Identitet og produksjonsvei", f"- Produksjonsmåte: {job['production_mode'] or none}",
         f"- Avatar: {st.get('avatar_name', none)} (ID {st.get('avatar_id', none)})",
         f"- Stemme: {st.get('voice_name', none)} (ID {st.get('voice_id', none)})",
         f"- Motor/innstillinger: {st.get('engine', none)}", f"- Tilkobling: {st.get('connection', none)}",
         f"- Ny leppesynk: {st.get('new_lipsync', none)}", f"- Utdata: {st.get('output_size')}", "", "## Eksterne jobber"]
    L += [f"- {e['service']} {e['kind']}: {e['id']} ({e.get('status')})" for e in job["external_jobs"]] or ["- ingen"]
    L += ["", "## Ressursbruk",
          f"- Betalingstype sett hos tjenesten: {bill.get('billing_type', none)}",
          f"- Saldo før / etter: {bill.get('balance_before', none)} / {bill.get('balance_after', none)}",
          f"- Nye betalingsbelastninger: {bill.get('new_charges_nok', 'ikke kontrollert - ' + none)}"]
    L += [f"- {r['service']} - {r['what']}: {r['credits']} kreditter ({'estimat' if r.get('estimated') else 'faktisk'})"
          for r in job["resources"]] or ["- ingen kreditter ført"]
    L += ["", "## Maskinell kontroll av eksporten"]
    L += [f"- [{'OK' if c['ok'] else 'FEIL' if c.get('hard', True) else 'MERKNAD'}] {c['check']}: {c['detail']}"
          for c in report["checks"]]
    L += ["", f"- Tekstingens timing: {report.get('subtitle_timing')}", "", "## Krever menneskelig/visuell vurdering"]
    L += [f"- {x}" for x in report["human_review_required"]]
    L += ["", "## Merknader"] + [f"- {n}" for n in st.get("notes", [])] + [f"- {n}" for n in st.get("limitations", [])]
    L += ["", f"- Norsk_stemme.wav inneholder musikk: {'ja' if st.get('voice_wav_contains_music') else 'nei'}", ""]
    (jd / "eksport" / EXPORT_NAMES["note"]).write_text("\n".join(L), encoding="utf-8")


# ------------------------------------------------------------------ cli
def main():
    ap = argparse.ArgumentParser(prog="donvideo", description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("new", help="opprett jobb fra YouTube-lenke"); p.add_argument("url"); p.set_defaults(f=cmd_new)
    p = sub.add_parser("status", help="vis jobbstatus"); p.add_argument("job"); p.set_defaults(f=cmd_status)
    p = sub.add_parser("source", help="hent metadata + teksting, eller importer fil")
    p.add_argument("job"); p.add_argument("--transcript-file"); p.add_argument("--video-file"); p.add_argument("--method")
    p.set_defaults(f=cmd_source)
    p = sub.add_parser("step", help="sett status for et trinn")
    p.add_argument("job"); p.add_argument("name"); p.add_argument("status"); p.add_argument("--note"); p.set_defaults(f=cmd_step)
    p = sub.add_parser("set", help="sett felt i job.json (nøkkel=verdi)")
    p.add_argument("job"); p.add_argument("pairs", nargs="+"); p.set_defaults(f=cmd_set)
    p = sub.add_parser("extjob", help="lagre ekstern jobb-ID")
    p.add_argument("job"); p.add_argument("--service", required=True); p.add_argument("--kind", required=True)
    p.add_argument("--id", required=True); p.add_argument("--status"); p.add_argument("--note"); p.set_defaults(f=cmd_extjob)
    p = sub.add_parser("ledger", help="før ressursbruk")
    p.add_argument("job"); p.add_argument("--service", required=True); p.add_argument("--what", required=True)
    p.add_argument("--credits", type=float, required=True); p.add_argument("--estimated", action="store_true")
    p.set_defaults(f=cmd_ledger)
    p = sub.add_parser("subs", help="lag SRT/ASS som følger ferdig tale")
    p.add_argument("job"); p.add_argument("--media", required=True); p.add_argument("--manus"); p.add_argument("--srt")
    p.add_argument("--words"); p.set_defaults(f=cmd_subs)
    p = sub.add_parser("asr", help="lokal talegjenkjenning: språk, ordlyd og ordtider")
    p.add_argument("job"); p.add_argument("--media", required=True); p.add_argument("--model", default="medium")
    p.add_argument("--language", help="tving språk, f.eks. no; ellers brukes det som oppdages")
    p.add_argument("--manus", help="sammenlign mot denne teksten (standard: jobbens Manus_nb.txt)")
    p.set_defaults(f=cmd_asr)
    p = sub.add_parser("finish", help="eksporter ren + tekstet versjon, SRT, WAV fra godkjent master")
    p.add_argument("job"); p.add_argument("--master", required=True); p.add_argument("--manus"); p.add_argument("--srt")
    p.add_argument("--words"); p.add_argument("--music"); p.add_argument("--music-db", type=float, default=-24.0)
    p.set_defaults(f=cmd_finish)
    p = sub.add_parser("verify", help="kontroller de eksporterte filene"); p.add_argument("job"); p.set_defaults(f=cmd_verify)
    a = ap.parse_args()
    # Én kommando om gangen per jobb: samtidige kall skal ikke overskrive hverandres endringer i job.json.
    JOBS.mkdir(exist_ok=True)
    lock_dir = job_dir(a.job) if hasattr(a, "job") else JOBS
    with open(lock_dir / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        a.f(a)


if __name__ == "__main__":
    main()
