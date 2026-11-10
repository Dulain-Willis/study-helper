import json, re, glob, os, collections
BASE = "/tmp/claude-1000/-home-dulain/a4720c86-5d38-441e-a4be-7704bd9223cf"
TASKS = BASE + "/tasks"
EVAL = BASE + "/scratchpad/eval"
LESSON = "pa_pattern_batch_or_stream_intermediate.md"
LESSON_LINES = 490
questions = {q["id"]: q for q in json.load(open(EVAL + "/questions.json"))}
index_len = {v: sum(1 for _ in open(f"{EVAL}/indexes/{v}.md")) for v in "BCDE"}

def exp_lines(q):
    s = set()
    for a, b in q["expected_ranges"]: s.update(range(a, b + 1))
    return s

runs = {}
for f in glob.glob(TASKS + "/*.output"):
    tag = None; calls = []; seen = set()
    try:
        for line in open(f):
            try: d = json.loads(line)
            except: continue
            msg = d.get("message") or {}
            c = msg.get("content")
            if isinstance(c, str) and tag is None:
                m = re.search(r"RUN ([A-E])_(q\d+)", c)
                if m: tag = m.groups()
            if isinstance(c, list):
                for b in c:
                    if b.get("type") == "text" and tag is None:
                        m = re.search(r"RUN ([A-E])_(q\d+)", b.get("text", ""))
                        if m: tag = m.groups()
                    if b.get("type") == "tool_use" and b.get("id") not in seen:
                        seen.add(b.get("id")); calls.append((b["name"], b.get("input", {})))
    except Exception as e:
        continue
    if tag: runs[tag] = calls

def analyze(var, qid, calls):
    lesson_read = set(); idx_read = 0; ncalls = 0; bash = 0; other_files = 0
    for name, inp in calls:
        if name in ("Write", "SubagentHandback"): continue
        ncalls += 1
        if name == "Read":
            p = inp.get("file_path", "")
            off = max(1, int(inp.get("offset") or 1)); lim = int(inp.get("limit") or 2000)
            if p.endswith(LESSON):
                lesson_read.update(range(off, min(off + lim - 1, LESSON_LINES) + 1))
            elif "/indexes/" in p:
                idx_read += min(lim, index_len.get(var, 0) - off + 1)
            elif "/eval/q/" in p: pass
            else: other_files += 1
        elif name == "Bash":
            bash += 1
            cmd = inp.get("command", "")
            if LESSON in cmd:
                m = re.findall(r"sed -n ['\"]?(\d+),(\d+)p", cmd)
                if m:
                    for a, b in m: lesson_read.update(range(int(a), int(b) + 1))
                elif re.search(r"\bcat\b", cmd): lesson_read.update(range(1, LESSON_LINES + 1))
        elif name == "Grep": pass
    q = questions[qid]; e = exp_lines(q)
    cov = len(e & lesson_read) / len(e)
    return dict(var=var, q=qid, lesson_lines=len(lesson_read), idx_lines=idx_read,
                total=len(lesson_read) + idx_read, coverage=cov, full=int(cov == 1.0),
                calls=ncalls, bash=bash, other=other_files, type=q["type"])

rows = [analyze(v, q, c) for (v, q), c in sorted(runs.items())]
json.dump(rows, open(EVAL + "/scores.json", "w"), indent=1)
by = collections.defaultdict(list)
for r in rows: by[r["var"]].append(r)
print(f"{'var':3} {'n':>2} {'full%':>6} {'cov%':>6} {'lesson':>7} {'index':>6} {'total':>6} {'calls':>6} {'whole-file':>10}")
for v in sorted(by):
    rs = by[v]; n = len(rs)
    whole = sum(r["lesson_lines"] >= 480 for r in rs)
    print(f"{v:3} {n:>2} {100*sum(r['full'] for r in rs)/n:6.0f} {100*sum(r['coverage'] for r in rs)/n:6.0f} "
          f"{sum(r['lesson_lines'] for r in rs)/n:7.0f} {sum(r['idx_lines'] for r in rs)/n:6.0f} "
          f"{sum(r['total'] for r in rs)/n:6.0f} {sum(r['calls'] for r in rs)/n:6.1f} {whole:>10}")
