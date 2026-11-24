#!/usr/bin/env python3
"""Selvtest for donvideo.py på syntetisk media. Krever bare ffmpeg. Kjør: python3 don-video/selftest.py"""
import json
import math
import shutil
import struct
import subprocess
import sys
import tempfile
import types
import wave
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import donvideo as d  # noqa: E402

TEXT = ("Du trenger ikke å vente på at alt skal bli perfekt. Begynn der du står, med det du har. "
        "Hver dag du utsetter det, blir drømmen litt mindre. Så reis deg, og ta det første steget nå!")
FAILS = []


def check(name, ok, detail=""):
    print(f"  [{'OK ' if ok else 'FEIL'}] {name}" + (f": {detail}" if detail else ""))
    if not ok:
        FAILS.append(name)


def synth(tmp, text=TEXT):
    """Lag «tale» som tonestøt med kjente ordtider + 720x1280 testvideo."""
    sr, t, samples, truth = 48000, 0.6, [0.0] * int(48000 * 0.6), []
    for w in d.tokens(text):
        core = w.strip(".,!?")
        dur = 0.05 + 0.055 * (len(core) + 4 * sum(c.isdigit() for c in core))  # tall tar lengre tid å si
        truth.append({"word": w, "start": round(t, 3), "end": round(t + dur, 3)})
        n = int(sr * dur)
        samples += [0.3 * min(1, i / (sr * 0.01), (n - i) / (sr * 0.01)) * math.sin(2 * math.pi * (180 + 7 * len(w)) * i / sr)
                    for i in range(n)]
        # mellomrom inne i en setning er kortere enn stillhetsgrensen, så bare tegnsetting gir pauser
        gap = 0.55 if w[-1] in ".!?" else (0.28 if w[-1] == "," else 0.04)
        samples += [0.0] * int(sr * gap)
        t += dur + gap
    samples += [0.0] * int(sr * 0.4)
    tmp.mkdir(parents=True, exist_ok=True)
    with wave.open(str(tmp / "voice.wav"), "w") as f:
        f.setnchannels(1)
        f.setsampwidth(2)
        f.setframerate(sr)
        f.writeframes(b"".join(struct.pack("<h", int(x * 32767)) for x in samples))
    total = len(samples) / sr
    subprocess.run(["ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i", f"testsrc2=s=720x1280:r=25:d={total}", "-i",
                    str(tmp / "voice.wav"), "-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p", "-c:a",
                    "aac", "-shortest", str(tmp / "master.mp4")], check=True)
    return truth, total


def srt_from(truth, path, drop=()):
    def f(x):
        ms = int(round(x * 1000))
        return f"{ms // 3600000:02d}:{ms // 60000 % 60:02d}:{ms // 1000 % 60:02d},{ms % 1000:03d}"
    cues, cur = [], []
    for i, w in enumerate(truth):
        if i not in drop:
            cur.append(w)
        if w["word"][-1] in ".!?" and cur:
            cues.append(cur)
            cur = []
    Path(path).write_text("".join(f"{n}\n{f(c[0]['start'])} --> {f(c[-1]['end'])}\n{' '.join(x['word'] for x in c)}\n\n"
                                  for n, c in enumerate(cues, 1)), encoding="utf-8")


def main():
    tmp = Path(tempfile.mkdtemp(prefix="donvideo_selftest_"))
    d.JOBS = tmp / "jobs"
    try:
        truth, total = synth(tmp)
        words = [t["word"] for t in truth]
        (tmp / "manus.txt").write_text("﻿# kommentar som ikke skal tekstes\n" + TEXT + "\n", encoding="utf-8")
        master = str(tmp / "master.mp4")

        print("Tekst og linjedeling")
        check("BOM + kommentar fjernes", d.read_manus(tmp / "manus.txt") == TEXT)
        (tmp / "regi.txt").write_text("Stopp [pause] . Tenk 10 000 ganger [smil] , så gå.", encoding="utf-8")
        check("[regi] etterlater ikke løse skilletegn, tall holdes samlet",
              d.tokens(d.read_manus(tmp / "regi.txt")) == ["Stopp.", "Tenk", "10 000", "ganger,", "så", "gå."])
        w2 = d.tokens("Den 17. mai kom f.eks. hele familien til Oslo. Etterpå dro vi hjem.")
        cues = d.build_cues(w2)
        check("ordenstall og forkortelser deler ikke setningen",
              not any(c["text"].replace("\n", " ").endswith(("17.", "f.eks.")) for c in cues),
              " | ".join(c["text"].replace("\n", " ") for c in cues))
        check("ett veldig langt ord krasjer ikke", d.wrap("Menneskerettighetsorganisasjonene", 22) != "")
        cues = d.build_cues(words)
        check("alle ord med, i rekkefølge", [w for c in cues for w in c["text"].split()] == words)
        check("korte linjer", max(len(line) for c in cues for line in c["text"].split("\n")) <= 26)

        print("Timing")
        times, anchors = d.align_by_pauses(words, master)
        err = [abs(times[i][0] - truth[i]["start"]) for i in range(len(words))]
        check("pause-reserve treffer", sum(err) / len(err) < 0.15 and max(err) < 0.6,
              f"snitt {sum(err) / len(err):.3f} s, maks {max(err):.3f} s, {anchors} ankre")
        num_text = ("Hør her. Hver eneste dag får du 86 400 sekunder. Hva bruker du dem på? Bekymring? Frykt? "
                    "Eller handling? Jeg har sett mennesker miste alt, og reise seg igjen. Så reis deg. Begynn nå!")
        ntruth, _ = synth(tmp / "tall", d.read_manus_text(num_text))
        nwords = [t["word"] for t in ntruth]
        ntimes, _ = d.align_by_pauses(nwords, str(tmp / "tall" / "master.mp4"))
        nerr = [abs(ntimes[i][0] - ntruth[i]["start"]) for i in range(len(nwords))]
        check("pause-reserve tåler tall i manus", max(nerr) < 0.6, f"maks {max(nerr):.3f} s")
        srt_from(truth, tmp / "full.srt")
        times, cov, missing = d.align_to_timed(words, d.timed_words_from_srt(tmp / "full.srt"), total)
        err = [abs(times[i][0] - truth[i]["start"]) for i in range(len(words))]
        check("SRT-vei treffer", cov == 1 and missing == 0 and max(err) < 0.35, f"maks {max(err):.3f} s")
        srt_from(truth, tmp / "hull.srt", drop=set(range(12, 16)) | set(range(len(truth) - 3, len(truth))))
        times, cov, missing = d.align_to_timed(words, d.timed_words_from_srt(tmp / "hull.srt"), total)
        check("manglende ord får positiv varighet innenfor filmen",
              missing == 7 and all(e - s > 0.02 for s, e in times) and max(e for _, e in times) <= total + 0.01,
              f"{missing} interpolert")
        srt_from([{"word": w, "start": i, "end": i + 0.5} for i, w in enumerate("helt annen tekst uten treff her.".split())],
                 tmp / "feil.srt")
        try:
            d.align_to_timed(words, d.timed_words_from_srt(tmp / "feil.srt"), total)
            check("urelatert SRT avvises", False)
        except SystemExit:
            check("urelatert SRT avvises", True)

        print("Eksport og kontroll")
        d.cmd_new(types.SimpleNamespace(url="https://youtu.be/SELFtest001"))
        job = next(d.JOBS.iterdir())
        shutil.copy(tmp / "manus.txt", job / "Manus_nb.txt")
        ns = dict(job=str(job), master=master, manus=None, words=None, music=None, music_db=-24.0)
        try:
            d.cmd_verify(types.SimpleNamespace(job=str(job)))
            check("verify nekter når eksporten ikke er fullført", False)
        except SystemExit as e:
            check("verify nekter når eksporten ikke er fullført", e.code not in (0, None))
        for step in d.STEPS[:5]:
            d.cmd_step(types.SimpleNamespace(job=str(job), name=step, status="done", note=None))
        cli = [sys.executable, str(Path(d.__file__)), ]
        procs = [subprocess.Popen(cli + c, stdout=subprocess.DEVNULL, env={**__import__("os").environ, "DONVIDEO_JOBS": str(d.JOBS)})
                 for i in range(6) for c in (["extjob", str(job), "--service", "test", "--kind", "k", "--id", f"id{i}"],
                                             ["ledger", str(job), "--service", "test", "--what", f"w{i}", "--credits", "1"])]
        for pr in procs:
            pr.wait()
        state = json.loads((job / "job.json").read_text())
        check("samtidige kall mister ingen endringer", len(state["external_jobs"]) == 6 and len(state["resources"]) == 6,
              f"{len(state['external_jobs'])} jobber, {len(state['resources'])} ressurslinjer")
        d.cmd_finish(types.SimpleNamespace(srt=str(tmp / "full.srt"), **ns))
        try:
            d.cmd_verify(types.SimpleNamespace(job=str(job)))
        except SystemExit as e:
            check("verify består ren eksport", e.code == 0)
        rep = json.loads((job / "kontroll" / "verification.json").read_text())
        check("ingen avvik på god eksport", all(c["ok"] for c in rep["checks"]),
              ", ".join(c["check"] for c in rep["checks"] if not c["ok"]))
        good = json.loads((job / "job.json").read_text())
        good_hash = good["settings"]["export_files"][d.EXPORT_NAMES["clean"]]["sha256"]
        try:
            d.cmd_finish(types.SimpleNamespace(srt=str(tmp / "feil.srt"), **ns))
            check("feil tekstfil avbryter finish", False)
        except SystemExit as e:
            check("feil tekstfil avbryter finish", e.code not in (0, None))
        state = json.loads((job / "job.json").read_text())
        clean = job / "eksport" / d.EXPORT_NAMES["clean"]
        kept = (state["steps"]["export"]["status"] == "done"
                and state["settings"].get("export_files", {}).get(d.EXPORT_NAMES["clean"], {}).get("sha256") == good_hash
                and d.sha256(clean) == good_hash
                and (job / "kontroll" / "verification.json").exists())
        check("avbrutt finish beholder ferdig eksport", kept, f"status={state['steps']['export']['status']}")
        try:
            d.cmd_verify(types.SimpleNamespace(job=str(job)))
        except SystemExit as e:
            check("verify består fortsatt etter avbrutt finish", e.code == 0)
        d.cmd_finish(types.SimpleNamespace(srt=str(tmp / "hull.srt"), **ns))
        try:
            d.cmd_verify(types.SimpleNamespace(job=str(job)))
        except SystemExit:
            pass
        rep = json.loads((job / "kontroll" / "verification.json").read_text())
        flagged = [c["check"] for c in rep["checks"] if not c["ok"]]
        check("verify feiler når talen ikke dekker manus", "teksting: talen dekker manus" in flagged and not rep["passed"],
              ", ".join(flagged))
        (job / "eksport" / d.EXPORT_NAMES["srt"]).write_text("1\n00:00:00,000 --> 00:00:01,000\nendret\n", encoding="utf-8")
        try:
            d.cmd_verify(types.SimpleNamespace(job=str(job)))
            check("verify nekter filer som er endret etter finish", False)
        except SystemExit as e:
            check("verify nekter filer som er endret etter finish", e.code not in (0, None))
        for name in d.EXPORT_NAMES.values():
            check(f"leveransefil {name}", (job / "eksport" / name).exists())
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nSELVTEST " + ("BESTÅTT" if not FAILS else "FEILET: " + ", ".join(FAILS)))
    sys.exit(0 if not FAILS else 1)


if __name__ == "__main__":
    main()
