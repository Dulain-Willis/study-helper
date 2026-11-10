import re, sys, pathlib
R = pathlib.Path("/home/dulain/projects/study-helper/lesson_preview")
OUT = pathlib.Path("/tmp/claude-1000/-home-dulain/a4720c86-5d38-441e-a4be-7704bd9223cf/scratchpad/eval2/arms/V")
SKIP = {"What you will be able to do", "Putting It All Together", "Key Takeaways"}
LESSONS = ["pa_pattern_batch_or_stream_intermediate", "pa_pattern_backpressure_scaling", "pa_pattern_idempotency", "pa_pattern_cdc_dual_writes"]
for stem in LESSONS:
    lines = (R/f"{stem}.md").read_text().splitlines()
    hs = [(len(m.group(1)), m.group(2), i) for i, l in enumerate(lines, 1) if (m := re.match(r"^(##|###) (.*\S)", l))]
    out = [f"# Index: {stem}.md", f"Source: `lesson_preview/{stem}.md` ({len(lines)} lines). Read just the line range you need (Read offset/limit).", ""]
    for idx, (lvl, t, s) in enumerate(hs):
        end = len(lines)
        for l2, _, s2 in hs[idx+1:]:
            if l2 <= lvl: end = s2 - 1; break
        if t in SKIP: continue
        out.append(("## " if lvl == 2 else "### ") + f"{t} (L{s}-{end})"); out.append("")
    (OUT/f"{stem}.md").write_text("\n".join(out))
    print(stem, sum(1 for h in hs if h[0]==3 and h[1] not in SKIP), "### entries")
