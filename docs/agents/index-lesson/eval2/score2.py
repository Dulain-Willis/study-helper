import json, re, glob, collections
BASE = "/tmp/claude-1000/-home-dulain/a4720c86-5d38-441e-a4be-7704bd9223cf"
TASKS, E2 = BASE + "/tasks", BASE + "/scratchpad/eval2"

def text_of(c):
    if isinstance(c, str): return c
    if isinstance(c, list): return "".join(b.get("text", "") for b in c if isinstance(b, dict))
    return ""

runs = {}
for f in glob.glob(TASKS + "/*.output"):
    tag = None; uses = {}; seen = set(); res_chars = collections.Counter(); order = []
    try:
        for line in open(f):
            try: d = json.loads(line)
            except: continue
            c = (d.get("message") or {}).get("content")
            if isinstance(c, str) and tag is None:
                m = re.search(r"EVAL2 ([VRFG])_(q\d+)", c)
                if m: tag = m.groups()
            if not isinstance(c, list): continue
            for b in c:
                t = b.get("type")
                if t == "text" and tag is None:
                    m = re.search(r"EVAL2 ([VRFG])_(q\d+)", b.get("text", ""))
                    if m: tag = m.groups()
                elif t == "tool_use" and b["id"] not in seen:
                    seen.add(b["id"]); uses[b["id"]] = (b["name"], b.get("input", {})); order.append(b["id"])
                elif t == "tool_result":
                    res_chars[b["tool_use_id"]] = len(text_of(b.get("content")))
    except Exception: continue
    if tag: runs[tag] = (uses, res_chars, order)

questions = {q["id"]: q for q in json.load(open(E2 + "/questions.json"))}
rows = []
for (arm, qid), (uses, rc, order) in sorted(runs.items()):
    chars = 0; idx_chars = 0; calls = 0; bash = 0; reads_lesson = 0; whole = 0; idx_reads = 0
    q = questions[qid]
    for i in order:
        name, inp = uses[i]
        if name in ("Write", "SubagentHandback"): continue
        p = inp.get("file_path", "") if name == "Read" else ""
        if name == "Read" and "/eval2/q/" in p: continue
        calls += 1; chars += rc[i]
        if name == "Bash": bash += 1
        if name == "Read" and "/arms/" in p: idx_chars += rc[i]; idx_reads += 1
        if name == "Read" and p.endswith(q["lesson"]):
            reads_lesson += 1
            if not inp.get("limit") and not inp.get("offset"): whole += 1
    rows.append(dict(arm=arm, q=qid, lesson=q["lesson"], chars=chars, idx_chars=idx_chars, calls=calls, bash=bash,
                     whole_reads=whole, idx_reads=idx_reads))
json.dump(rows, open(E2 + "/scores2.json", "w"), indent=1)
by = collections.defaultdict(list)
for r in rows: by[r["arm"]].append(r)
print(f"{'arm':3} {'n':>2} {'chars':>7} {'~tok':>6} {'idx':>6} {'calls':>6} {'bash':>5} {'wholeRead':>9}")
for a in "GVRF":
    rs = by.get(a, [])
    if not rs: continue
    n = len(rs); c = sum(r["chars"] for r in rs) / n
    print(f"{a:3} {n:>2} {c:7.0f} {c/4:6.0f} {sum(r['idx_chars'] for r in rs)/n:6.0f} {sum(r['calls'] for r in rs)/n:6.1f} "
          f"{sum(r['bash'] for r in rs)/n:5.1f} {sum(r['whole_reads'] for r in rs):>9}")
