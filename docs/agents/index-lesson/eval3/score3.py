import json, glob, os, collections, statistics, random
E3 = "/tmp/claude-1000/-home-dulain/a4720c86-5d38-441e-a4be-7704bd9223cf/scratchpad/eval3"
Q = {q["id"]: q for q in json.load(open(E3 + "/questions.json"))}
rows = []
for f in sorted(glob.glob(E3 + "/probe_results/*.json")):
    name = os.path.basename(f)[:-5]; arm, rep = name.rsplit("_", 1)
    try: data = json.load(open(f))
    except Exception as e: print("bad json", name, e); continue
    got = {d["qid"]: d.get("picks", []) for d in data}
    for qid, q in Q.items():
        picks = got.get(qid, [])
        a, b = q["expected_ranges"][0]
        exp = set(range(a, b + 1))
        def cov(p):  # lines of expected covered by this pick
            if p["lesson"] != q["lesson"]: return set()
            return exp & set(range(int(p["start"]), int(p["end"]) + 1))
        top1 = cov(picks[0]) if picks else set()
        allc = set().union(*[cov(p) for p in picks]) if picks else set()
        lines = sum(max(0, int(p["end"]) - int(p["start"]) + 1) for p in picks)
        l1 = max(0, int(picks[0]["end"]) - int(picks[0]["start"]) + 1) if picks else 0
        rows.append(dict(arm=arm, rep=rep, qid=qid, lesson=q["lesson"],
                         top1_full=int(len(top1) == len(exp)), any_full=int(len(allc) == len(exp)),
                         any_overlap=int(len(allc) > 0), lines=lines, lines_top1=l1,
                         right_lesson_top1=int(bool(picks) and picks[0]["lesson"] == q["lesson"])))
json.dump(rows, open(E3 + "/scores3.json", "w"), indent=1)
by = collections.defaultdict(list)
for r in rows: by[(r["arm"], r["rep"])].append(r)
print(f"{'arm':4}{'rep':>4}{'n':>4} {'top1 full%':>10} {'any full%':>9} {'overlap%':>8} {'lesson ok%':>10} {'avg lines':>9}")
for k in sorted(by):
    rs = by[k]; n = len(rs)
    print(f"{k[0]:4}{k[1]:>4}{n:>4} {100*sum(r['top1_full'] for r in rs)/n:10.0f} {100*sum(r['any_full'] for r in rs)/n:9.0f} "
          f"{100*sum(r['any_overlap'] for r in rs)/n:8.0f} {100*sum(r['right_lesson_top1'] for r in rs)/n:10.0f} {sum(r['lines'] for r in rs)/n:9.0f}")
# per-arm pooled + paired bootstrap vs V on per-question any_full
armq = collections.defaultdict(dict)  # arm -> qid -> mean any_full over reps
for r in rows: armq[r["arm"]].setdefault(r["qid"], []).append(r["any_full"])
for a in armq:
    for q in armq[a]: armq[a][q] = sum(armq[a][q]) / len(armq[a][q])
print()
random.seed(1)
qids = sorted(Q)
for a, b in [("R", "V"), ("R2", "V"), ("R", "R2")]:
    if a not in armq or b not in armq: continue
    d = [armq[a][q] - armq[b][q] for q in qids if q in armq[a] and q in armq[b]]
    if not d: continue
    boots = sorted(sum(random.choice(d) for _ in d) / len(d) for _ in range(5000))
    print(f"{a} - {b}: mean diff in any_full = {sum(d)/len(d):+.3f}, 95% CI [{boots[125]:+.3f}, {boots[4875]:+.3f}], n={len(d)} questions")
