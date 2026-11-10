import re, pathlib
R = pathlib.Path("/home/dulain/projects/study-helper")
E = pathlib.Path("/tmp/claude-1000/-home-dulain/a4720c86-5d38-441e-a4be-7704bd9223cf/scratchpad/eval/indexes")
LESSON = R/"lesson_preview/pa_pattern_batch_or_stream_intermediate.md"
HEAD = "# Index: pa_pattern_batch_or_stream_intermediate.md\nSource: `lesson_preview/pa_pattern_batch_or_stream_intermediate.md` (490 lines). Read just the line range you need (Read offset/limit).\n"

def parse(path):
    groups, cur = [], None
    for line in path.read_text().splitlines():
        m = re.match(r"^(##|###) (.*) \(L(\d+)-(\d+)\)$", line)
        if not m: continue
        lvl, t, s, e = m.group(1), m.group(2), int(m.group(3)), int(m.group(4))
        if lvl == "##": cur = {"t": t, "s": s, "e": e, "kids": []}; groups.append(cur)
        else: cur["kids"].append([t, s, e])
    return groups

def write(groups, name):
    out = [HEAD]
    for g in groups:
        out.append(f"## {g['t']} (L{g['s']}-{g['e']})\n")
        for t, s, e in g["kids"]: out.append(f"### {t} (L{s}-{e})\n")
    (E/name).write_text("\n".join(out))
    print(name, sum(len(g["kids"]) for g in groups), "### entries")

# B and C
for src, name in [(R/"docs/agents/index-lesson/batch_or_stream_v1_first_run.md","B.md"),(R/"lesson_preview/index/pa_pattern_batch_or_stream_intermediate.md","C.md")]:
    write(parse(src), name)

# D: merge entries shorter than 10 lines into neighbour
gs = parse(R/"lesson_preview/index/pa_pattern_batch_or_stream_intermediate.md")
for g in gs:
    kids = g["kids"]; out = []
    i = 0
    while i < len(kids):
        t, s, e = kids[i]
        if e - s + 1 < 10:
            if out: out[-1][2] = e
            elif i + 1 < len(kids): kids[i+1][1] = s
            i += 1; continue
        out.append([t, s, e]); i += 1
    g["kids"] = out
write(gs, "D.md")

# E: raw lesson headings
lines = LESSON.read_text().splitlines()
hs = [(len(m.group(1)), m.group(2), i) for i, l in enumerate(lines, 1) if (m := re.match(r"^(##|###) (.*\S)", l)) for _ in [0] if True]
hs = [(len(re.match(r"^#+", lines[i-1]).group(0)), t, i) for _, t, i in hs]
skip = {"What you will be able to do", "Putting It All Together", "Key Takeaways"}
groups = []
for idx, (lvl, t, s) in enumerate(hs):
    end = len(lines)
    for l2, _, s2 in hs[idx+1:]:
        if l2 <= lvl: end = s2 - 1; break
    if t in skip: continue
    if lvl == 2: groups.append({"t": t, "s": s, "e": end, "kids": []})
    elif groups: groups[-1]["kids"].append([t, s, end])
write(groups, "E.md")
