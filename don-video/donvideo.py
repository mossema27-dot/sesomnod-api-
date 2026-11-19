#!/usr/bin/env python3
"""donvideo - liten, gjenbrukbar arbeidsflyt for norske Don Moses-videoer.

Dekker den lokale, deterministiske delen: jobbmappe, kildehenting, teksting,
eksport og kontroll. Selve avatar-/stemmegenereringen skjer i HeyGen (MCP) og
styres av skillen .claude/skills/don-video/SKILL.md.

Kun standardbibliotek + ffmpeg/ffprobe. yt-dlp brukes hvis den er installert.
"""
import argparse
import datetime as dt
import difflib
import json
import math
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
JOBS = ROOT / "jobs"
PROFILE = ROOT / "profile" / "don_moses.json"
STEPS = ["source", "manus", "budget", "test_clip", "generation", "subs", "export", "verify"]
EXPORT_NAMES = {
    "burned": "Don_Moses_Norsk_Med_Tekst.mp4",
    "clean": "Don_Moses_Norsk_Ren.mp4",
    "srt": "Undertekster_nb.srt",
    "manus": "Manus_nb.txt",
    "voice": "Norsk_stemme.wav",
    "note": "Produksjonsnotat.md",
}


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
    m = re.search(r"(?:shorts/|watch\?(?:.*&)?v=|youtu\.be/|embed/|live/)([A-Za-z0-9_-]{11})", url)
    if not m:
        die(f"Fant ingen YouTube-video-ID i: {url}")
    return m.group(1)


def job_dir(ref):
    """Finn jobbmappe fra jobb-ID, video-ID, URL eller sti."""
    p = Path(ref)
    if p.is_dir() and (p / "job.json").exists():
        return p.resolve()
    vid = ref
    if "/" in ref or "youtu" in ref:
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


def silences(path, noise_db=-35, min_d=0.18):
    """Stille partier [(start, slutt)] i lydsporet."""
    p = run(["ffmpeg", "-hide_banner", "-nostats", "-i", path, "-vn", "-af",
             f"silencedetect=noise={noise_db}dB:d={min_d}", "-f", "null", "-"])
    out, start = [], None
    for line in p.stderr.splitlines():
        m = re.search(r"silence_start: (-?[\d.]+)", line)
        if m:
            start = max(0.0, float(m.group(1)))
        m = re.search(r"silence_end: (-?[\d.]+)", line)
        if m and start is not None:
            out.append((start, float(m.group(1))))
            start = None
    if start is not None:
        out.append((start, probe(path)["duration"]))
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
    """Sett felt i job.json, f.eks. source.title='...' eller production_mode=B."""
    jd = job_dir(a.job)
    job = load(jd)
    for pair in a.pairs:
        key, _, val = pair.partition("=")
        try:
            val = json.loads(val)
        except json.JSONDecodeError:
            pass
        node = job
        parts = key.split(".")
        for k in parts[:-1]:
            node = node.setdefault(k, {})
        node[parts[-1]] = val
    save(jd, job)
    print("Oppdatert.")


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
                             "estimated": bool(a.estimated), "new_payment_nok": 0, "at": now()})
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
    if a.transcript_file:
        shutil.copy(a.transcript_file, kdir / "transkripsjon_kilde.txt")
        src["transcript_method"] = a.method or "levert som fil av bruker"
        set_step(job, "source", "done", "transkripsjon importert fra fil")
        save(jd, job)
        return print("Transkripsjon importert.")
    if a.video_file:
        info = probe(a.video_file)
        run(["ffmpeg", "-y", "-v", "error", "-i", a.video_file, "-vn", "-ac", "1", "-ar", "16000",
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
        if "403" in err and ("proxy" in err.lower() or "tunnel" in err.lower()):
            reason = "blokkert av nettverksregel i miljøet (proxy 403 for youtube.com)"
        elif "not a bot" in err or "Sign in" in err:
            reason = "YouTube krever innlogging/bot-kontroll fra denne IP-adressen"
        else:
            reason = "ukjent feil fra yt-dlp"
        set_step(job, "source", "blocked", reason)
        (kdir / "ytdlp_feil.txt").write_text(err[-4000:], encoding="utf-8")
        save(jd, job)
        print(f"BLOKKERT: {reason}\nDetaljer: {kdir / 'ytdlp_feil.txt'}\n"
              "Alternativ: donvideo.py source <jobb> --video-file <fil> eller --transcript-file <fil>")
        sys.exit(2)
    meta = json.loads(p.stdout)
    (kdir / "metadata.json").write_text(json.dumps(
        {k: meta.get(k) for k in ("id", "title", "channel", "uploader", "channel_url", "upload_date", "duration",
                                  "language", "description", "webpage_url", "width", "height")},
        ensure_ascii=False, indent=2), encoding="utf-8")
    src.update({"title": meta.get("title"), "channel": meta.get("channel") or meta.get("uploader"),
                "duration_s": meta.get("duration"), "language": meta.get("language")})
    lang = meta.get("language") or "en"
    manual, auto = meta.get("subtitles") or {}, meta.get("automatic_captions") or {}
    pick = next((k for k in manual if k.split("-")[0] == lang.split("-")[0] and k != "live_chat"), None)
    kind = "manuell teksting"
    if not pick:
        pick = next((k for k in (f"{lang}-orig", lang) if k in auto), None)
        kind = "automatisk teksting (ASR fra YouTube)"
    if pick:
        q = run([sys.executable, "-m", "yt_dlp", "--skip-download", "--no-warnings", "--sub-format", "json3",
                 "--sub-langs", pick, "--write-subs", "--write-auto-subs", "-o", str(kdir / "yt"),
                 src["canonical_url"]])
        files = list(kdir.glob("yt*.json3"))
        if q.returncode == 0 and files:
            lines = parse_json3(files[0])
            (kdir / "transkripsjon_kilde.txt").write_text(
                "\n".join(f"[{t:7.2f}] {x}" for t, x in lines) + "\n", encoding="utf-8")
            src["transcript_method"] = f"YouTube {kind}, spor '{pick}' - kontroller mot lyd"
            set_step(job, "source", "done", f"{kind} hentet ({len(lines)} linjer)")
            save(jd, job)
            return print(f"Kilde hentet: «{src['title']}» ({src['channel']}), {src['duration_s']} s, {kind}.")
    set_step(job, "source", "partial", "metadata hentet, men ingen teksting - trenger lyd + lokal transkripsjon")
    save(jd, job)
    print("Metadata hentet, men ingen teksting funnet. Hent lyd og transkriber lokalt.")


# ------------------------------------------------------------------ subtitles
WORD_RE = re.compile(r"\S+")


def read_manus(path):
    """Talt tekst: kommentarlinjer (#) og [regi] fjernes."""
    lines = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        if raw.strip().startswith("#"):
            continue
        lines.append(re.sub(r"\[[^\]]*\]", " ", raw))
    return re.sub(r"\s+", " ", " ".join(lines)).strip()


def norm(w):
    return re.sub(r"[^\wæøåÆØÅ]", "", w.lower())


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
                    if j < m and re.match(FUNC_WORDS, norm(words[idx[j - 1]]), re.I):
                        c += 60
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
        hard = re.search(r"[.!?…][\"»”)]*$", w) is not None
        soft = re.search(r"[,;:–—]$", w) is not None and len(text) >= 14
        if hard or soft or i == len(words) - 1:
            segs.append(cur)
            cur = []
    cues = [part for seg in segs for part in split_balanced(seg, words, max_chars)]
    return [{"idx": c, "text": wrap(" ".join(words[j] for j in c), max_line)} for c in cues]


def wrap(text, max_line):
    if len(text) <= max_line:
        return text
    ws = text.split(" ")
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


def align_to_timed(words, timed):
    """Legg manusord på tidsstemplede ord (fra tjenestens teksting eller ASR)."""
    a, b = [norm(w) for w in words], [norm(t["word"]) for t in timed]
    times = [None] * len(words)
    sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
    matched = 0
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for k in range(i2 - i1):
                times[i1 + k] = (timed[j1 + k]["start"], timed[j1 + k]["end"])
                matched += 1
        elif tag == "replace" and j2 > j1:
            st, en = timed[j1]["start"], timed[j2 - 1]["end"]
            n = i2 - i1
            for k in range(n):
                times[i1 + k] = (st + (en - st) * k / n, st + (en - st) * (k + 1) / n)
    # interpoler ord uten treff
    known = [i for i, t in enumerate(times) if t]
    if not known:
        die("Ingen ord i manus kunne plasseres på tidsstemplene - feil fil?")
    for i in range(len(times)):
        if times[i]:
            continue
        prev = max((k for k in known if k < i), default=None)
        nxt = min((k for k in known if k > i), default=None)
        t0 = times[prev][1] if prev is not None else 0.0
        t1 = times[nxt][0] if nxt is not None else t0 + 0.3 * (i - (prev or 0))
        span = (nxt if nxt is not None else i + 1) - (prev if prev is not None else -1)
        pos = i - (prev if prev is not None else -1)
        times[i] = (t0 + (t1 - t0) * (pos - 1) / max(1, span - 1), t0 + (t1 - t0) * pos / max(1, span - 1))
    return times, matched / max(1, len(words))


def align_by_pauses(words, media):
    """Reserve uten tidsstempler: fordel ord over tale, og fest setningsslutt til reelle pauser."""
    dur = probe(media)["duration"]
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

    def at(sp):  # tale-sekunder -> tidslinje
        acc = 0.0
        for s, e in speech:
            if sp <= acc + (e - s) + 1e-9:
                return s + (sp - acc)
            acc += e - s
        return speech[-1][1]

    def w8(w):
        return len(norm(w)) + 2
    tot_w = sum(w8(w) for w in words)
    # forventede setningsgrenser, festet til nærmeste pause
    gaps = [(speech[i][1], speech[i + 1][0]) for i in range(len(speech) - 1)]
    bounds, acc = [], 0.0
    for i, w in enumerate(words[:-1]):
        acc += w8(w)
        if re.search(r"[.!?…:][\"»”)]*$", w):
            exp = at(total * acc / tot_w)
            near = min(gaps, key=lambda g: abs((g[0] + g[1]) / 2 - exp), default=None)
            if near and abs((near[0] + near[1]) / 2 - exp) <= 0.9 and near not in [b[1] for b in bounds]:
                bounds.append((i, near))
    bounds.sort(key=lambda b: b[1][0])
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
    for i, c in enumerate(timed):
        nxt = timed[i + 1][0] if i + 1 < len(timed) else dur
        c[1] = max(c[1], c[0] + 0.55)          # minste lesetid
        if nxt - c[1] < 0.30:                  # lukk små hull, aldri overlapp
            c[1] = nxt
        c[1] = min(c[1], nxt, dur)
    srt = "".join(f"{i}\n{fmt_srt(s)} --> {fmt_srt(e)}\n{t}\n\n" for i, (s, e, t) in enumerate(timed, 1))
    Path(srt_path).write_text(srt, encoding="utf-8")
    k = height / 1920.0
    font = style.get("font", "Inter")
    if run(["fc-match", "-f", "%{family}", f"{font}:bold"]).stdout.split(",")[0].strip().lower() != font.lower():
        font = "DejaVu Sans"
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
    words = WORD_RE.findall(text)
    if not words:
        die(f"Manus er tomt: {manus}")
    info = probe(media)
    if words_json:
        timed = json.loads(Path(words_json).read_text(encoding="utf-8"))
        times, cov = align_to_timed(words, timed)
        method = f"ordtidsstempler fra fil ({cov:.0%} av manusordene traff)"
    elif srt:
        times, cov = align_to_timed(words, timed_words_from_srt(srt))
        method = f"tidskoder fra levert teksting ({cov:.0%} av manusordene traff)"
        if cov < 0.85:
            print(f"ADVARSEL: bare {cov:.0%} av manusordene finnes i levert teksting - talen avviker fra manus?")
    else:
        times, anchors = align_by_pauses(words, media)
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


def cmd_finish(a):
    jd = job_dir(a.job)
    job = load(jd)
    exp = jd / "eksport"
    exp.mkdir(exist_ok=True)
    master = Path(a.master).resolve()
    manus = Path(a.manus) if a.manus else jd / EXPORT_NAMES["manus"]
    if not manus.exists():
        die(f"Mangler manus: {manus}")
    if manus.resolve() != (exp / EXPORT_NAMES["manus"]).resolve():
        shutil.copy(manus, exp / EXPORT_NAMES["manus"])
    info = probe(master)
    if not info["video"] or not info["audio"]:
        die("Master mangler video- eller lydstrøm.")
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
    # 1) ren talelyd (før eventuell musikk)
    run(["ffmpeg", "-y", "-v", "error", "-i", master, "-vn", "-ac", "2", "-ar", "48000", "-c:a", "pcm_s16le",
         exp / EXPORT_NAMES["voice"]], check=True)
    # 2) teksting fra faktisk ferdig lyd
    timed, method = make_subs(jd, job, master, manus, a.srt, a.words)
    # 3) lydkjede: skånsom lydstyrke-normalisering, ingen støyfjerning
    ln, measured = loudnorm_filter(master)
    inputs, amap = ["-i", master], f"[0:a]{ln},aresample=48000[a]"
    if a.music:
        inputs += ["-stream_loop", "-1", "-i", a.music]
        fade = max(0.0, info["duration"] - 1.5)
        amap = (f"[0:a]{ln},aresample=48000[v];[1:a]volume={a.music_db}dB,afade=t=in:d=1,"
                f"afade=t=out:st={fade:.2f}:d=1.5,aresample=48000[m];[v][m]amix=inputs=2:duration=first:normalize=0[a]")
        note.append(f"Bakgrunnsmusikk mikset på {a.music_db} dB under talen: {Path(a.music).name}")
    aenc = ["-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", "-t", f"{info['duration']:.3f}"]
    venc = ["-c:v", "libx264", "-preset", "medium", "-crf", "18", "-profile:v", "high"]
    pre = f"{scale}," if scale else ""
    copy_ok = not scale and info["video"]["codec"] == "h264" and info["video"]["pix_fmt"] == "yuv420p"
    if copy_ok:  # ren versjon: originalt videospor urørt, bare lyden normaliseres
        run(["ffmpeg", "-y", "-v", "error", *inputs, "-filter_complex", amap, "-map", "0:v:0", "-map", "[a]",
             "-c:v", "copy", *aenc, EXPORT_NAMES["clean"]], cwd=exp, check=True)
        note.append("Ren versjon: videosporet er kopiert uendret fra master (ingen ny komprimering).")
    else:
        run(["ffmpeg", "-y", "-v", "error", *inputs, "-filter_complex", f"[0:v]{pre}format=yuv420p[vo];{amap}",
             "-map", "[vo]", "-map", "[a]", *venc, *aenc, EXPORT_NAMES["clean"]], cwd=exp, check=True)
    run(["ffmpeg", "-y", "-v", "error", *inputs, "-filter_complex",
         f"[0:v]{pre}ass=undertekster.ass,format=yuv420p[vo];{amap}",
         "-map", "[vo]", "-map", "[a]", *venc, *aenc, EXPORT_NAMES["burned"]], cwd=exp, check=True)
    job["settings"].update({"master": str(master), "master_probe": info, "output_size": f"{ow}x{oh}",
                            "loudness_in_lufs": (measured or {}).get("input_i"), "notes": note,
                            "voice_wav_contains_music": False, "music": a.music})
    set_step(job, "export", "done", f"{EXPORT_NAMES['clean']} + {EXPORT_NAMES['burned']}")
    save(jd, job)
    print(f"Eksportert til {exp}\n  Teksting: {len(timed)} blokker ({method})")
    for n in note:
        print("  NB:", n)


# ------------------------------------------------------------------ verify
def detect(path, vf=None, af=None, pattern=""):
    cmd = ["ffmpeg", "-hide_banner", "-nostats", "-i", path]
    cmd += ["-vf", vf, "-an"] if vf else ["-af", af, "-vn"]
    p = run(cmd + ["-f", "null", "-"])
    return re.findall(pattern, p.stderr)


def cmd_verify(a):
    jd = job_dir(a.job)
    job = load(jd)
    exp = jd / "eksport"
    kdir = jd / "kontroll"
    kdir.mkdir(exist_ok=True)
    report, hard = {"at": now(), "files": {}, "checks": []}, []

    def check(name, ok, detail, is_hard=True):
        report["checks"].append({"check": name, "ok": bool(ok), "detail": detail})
        if not ok and is_hard:
            hard.append(name)
        print(f"  [{'OK ' if ok else 'FEIL' if is_hard else 'OBS'}] {name}: {detail}")

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
        err = run(["ffmpeg", "-v", "error", "-i", f, "-f", "null", "-"]).stderr.strip()
        check(f"{key}: dekoder uten feil", not err, err[:200] or "hele filen dekodet")
        check(f"{key}: bildeformat", abs(v["width"] / v["height"] - 9 / 16) < 0.02,
              f"{v['width']}x{v['height']} @ {v['fps']} fps, {v['pix_fmt']}", is_hard=False)
        check(f"{key}: lyd/bilde samme lengde", abs(v["duration"] - au["duration"]) < 0.15,
              f"video {v['duration']:.2f} s, lyd {au['duration']:.2f} s")
        black = detect(f, vf="blackdetect=d=0.4:pix_th=0.10", pattern=r"black_start:([\d.]+) black_end:([\d.]+)")
        check(f"{key}: ingen svarte partier", not black, f"{black}" if black else "ingen >= 0,4 s")
        if key == "clean":
            frz = detect(f, vf="freezedetect=n=0.003:d=1.5", pattern=r"freeze_start: ([\d.]+)")
            check("clean: ingen frys", not frz, f"frys ved {frz} s" if frz else "ingen >= 1,5 s")
            voice = exp / EXPORT_NAMES["voice"]  # ren tale: musikk skal ikke skjule et kutt
            sil_src = voice if voice.exists() else f
            sil = silences(sil_src, noise_db=-38, min_d=0.25)
            dur = probe(sil_src)["duration"]
            head = sil[0][1] if sil and sil[0][0] <= 0.02 else 0.0
            tail = dur - sil[-1][0] if sil and sil[-1][1] >= dur - 0.05 else 0.0
            check("clean: tale kuttes ikke i starten", head >= 0.03, f"{head:.2f} s luft før første lyd", is_hard=False)
            check("clean: tale kuttes ikke i slutten", tail >= 0.10, f"{tail:.2f} s luft etter siste lyd", is_hard=False)
            long_sil = [(round(s, 2), round(e, 2)) for s, e in sil if e - s > 2.5 and s > 0.1 and e < dur - 0.1]
            check("clean: ingen lange stille hull", not long_sil, f"{long_sil}" if long_sil else "ingen > 2,5 s", is_hard=False)
            _, m = loudnorm_filter(f)
            if m:
                lufs = float(m["input_i"])
                check("clean: lydstyrke", -17 <= lufs <= -12, f"{lufs:.1f} LUFS, topp {m['input_tp']} dBTP", is_hard=False)
            dur = info["duration"]
            pts = [0.4, dur * 0.25, dur * 0.5, dur * 0.75, max(0.4, dur - 0.5)]
            for seg in job["settings"].get("segment_boundaries", []):
                pts += [max(0, seg - 0.08), seg + 0.08]
        else:
            pts = [dur * x for x in (0.1, 0.5, 0.9)] if (dur := info["duration"]) else []
        for i, t in enumerate(sorted(pts)):
            run(["ffmpeg", "-y", "-v", "error", "-ss", f"{t:.2f}", "-i", f, "-frames:v", "1", "-vf", "scale=540:-2",
                 kdir / f"{key}_{i:02d}_{t:06.2f}s.jpg"])
    srt = exp / EXPORT_NAMES["srt"]
    manus = exp / EXPORT_NAMES["manus"]
    if srt.exists() and manus.exists():
        cues = parse_srt(srt)
        mw = [norm(w) for w in WORD_RE.findall(read_manus(manus)) if norm(w)]
        sw = [norm(w) for _, _, t in cues for w in t.split() if norm(w)]
        check("teksting: alle manusord med", mw == sw, f"{len(sw)} ord i teksting / {len(mw)} i manus")
        dur = next(iter(report["files"].values()), {}).get("duration", 0)
        over = [c for i, c in enumerate(cues[:-1]) if c[1] > cues[i + 1][0] + 0.001]
        check("teksting: ingen overlapp, innenfor filmen", not over and (not cues or cues[-1][1] <= dur + 0.05),
              f"{len(cues)} blokker, siste slutter {cues[-1][1]:.2f} s av {dur:.2f} s" if cues else "ingen blokker")
        longest = max((len(line) for _, _, t in parse_srt_raw(srt) for line in t.split("\n")), default=0)
        check("teksting: korte linjer", longest <= 26, f"lengste linje {longest} tegn", is_hard=False)
        check("teksting: første og siste setning", bool(cues) and cues[0][0] < 3.0, f"første blokk starter {cues[0][0]:.2f} s: «{cues[0][2]}» / siste: «{cues[-1][2]}»" if cues else "-", is_hard=False)
    else:
        check("teksting: filer finnes", False, "mangler SRT eller manus")
    report["passed"] = not hard
    report["subtitle_timing"] = job["settings"].get("subtitle_timing")
    report["human_review_required"] = [
        "Se kontrollbildene i kontroll/ (ansikt, utsnitt, tekst dekker ikke munn).",
        "Stemmelikhet og leppesynk kan ikke bevises maskinelt - må vurderes ved avspilling.",
    ]
    (jd / "kontroll" / "verification.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    set_step(job, "verify", "done" if not hard else "failed", "maskinell kontroll OK" if not hard else f"feil: {hard}")
    save(jd, job)
    write_note(jd, job, report)
    print(f"\nKontrollbilder: {kdir}\nResultat: {'BESTÅTT (maskinell del)' if not hard else 'IKKE BESTÅTT: ' + ', '.join(hard)}")
    sys.exit(0 if not hard else 1)


def parse_srt_raw(path):
    out = []
    for block in re.split(r"\n\s*\n", Path(path).read_text(encoding="utf-8-sig").strip()):
        lines = block.split("\n")
        if len(lines) >= 3:
            out.append((0, 0, "\n".join(lines[2:])))
    return out


def write_note(jd, job, report):
    s, st = job["source"], job["settings"]
    prof = json.loads(PROFILE.read_text(encoding="utf-8")) if PROFILE.exists() else {}
    hey = prof.get("heygen", {})
    L = [f"# Produksjonsnotat - {job['job_id']}", "", f"Generert: {now()}", "", "## Kilde",
         f"- URL: {s['url']}", f"- Video-ID: {s['video_id']}", f"- Tittel: {s['title']}", f"- Kanal: {s['channel']}",
         f"- Varighet: {s['duration_s']} s, språk: {s['language']}", f"- Taler: {s['speaker']} (er Don: {s['speaker_is_don']})",
         f"- Transkripsjon: {s['transcript_method']}", f"- Uklare partier: {s['unclear_parts'] or 'ingen notert'}", "",
         "## Identitet og produksjonsvei", f"- Produksjonsmåte: {job['production_mode']}",
         f"- Avatar: {st.get('avatar_name', hey.get('avatar_name'))} (ID {st.get('avatar_id', hey.get('avatar_id'))})",
         f"- Stemme: {st.get('voice_name', hey.get('voice_name'))} (ID {st.get('voice_id', hey.get('voice_id'))})",
         f"- Motor/innstillinger: {st.get('engine')}", f"- Tilkobling: {st.get('connection', prof.get('connection'))}",
         f"- Ny leppesynk: {st.get('new_lipsync')}", f"- Utdata: {st.get('output_size')}", "", "## Eksterne jobber"]
    L += [f"- {e['service']} {e['kind']}: {e['id']} ({e.get('status')})" for e in job["external_jobs"]] or ["- ingen"]
    L += ["", "## Ressursbruk", "- Nye betalingsbelastninger: 0 kr"]
    L += [f"- {r['service']} - {r['what']}: {r['credits']} kreditter ({'estimat' if r.get('estimated') else 'faktisk'})"
          for r in job["resources"]] or ["- ingen kreditter ført"]
    L += ["", "## Maskinell kontroll av eksporten"]
    L += [f"- [{'OK' if c['ok'] else 'AVVIK'}] {c['check']}: {c['detail']}" for c in report["checks"]]
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
    p.add_argument("job"); p.add_argument("--service", default="heygen"); p.add_argument("--kind", required=True)
    p.add_argument("--id", required=True); p.add_argument("--status"); p.add_argument("--note"); p.set_defaults(f=cmd_extjob)
    p = sub.add_parser("ledger", help="før ressursbruk")
    p.add_argument("job"); p.add_argument("--service", default="heygen"); p.add_argument("--what", required=True)
    p.add_argument("--credits", type=float, required=True); p.add_argument("--estimated", action="store_true")
    p.set_defaults(f=cmd_ledger)
    p = sub.add_parser("subs", help="lag SRT/ASS som følger ferdig tale")
    p.add_argument("job"); p.add_argument("--media", required=True); p.add_argument("--manus"); p.add_argument("--srt")
    p.add_argument("--words"); p.set_defaults(f=cmd_subs)
    p = sub.add_parser("finish", help="eksporter ren + tekstet versjon, SRT, WAV fra godkjent master")
    p.add_argument("job"); p.add_argument("--master", required=True); p.add_argument("--manus"); p.add_argument("--srt")
    p.add_argument("--words"); p.add_argument("--music"); p.add_argument("--music-db", type=float, default=-24.0)
    p.set_defaults(f=cmd_finish)
    p = sub.add_parser("verify", help="kontroller de eksporterte filene"); p.add_argument("job"); p.set_defaults(f=cmd_verify)
    a = ap.parse_args()
    a.f(a)


if __name__ == "__main__":
    main()
