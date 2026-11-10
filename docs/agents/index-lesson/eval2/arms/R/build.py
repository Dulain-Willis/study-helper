import re
src = "/tmp/claude-1000/-home-dulain/a4720c86-5d38-441e-a4be-7704bd9223cf/scratchpad/eval2/arms/V/pa_pattern_idempotency.md"
dst = "/tmp/claude-1000/-home-dulain/a4720c86-5d38-441e-a4be-7704bd9223cf/scratchpad/eval2/arms/R/pa_pattern_idempotency.md"
new = [
"Idempotent means a rerun leaves the same target state",
"Causes of repeated pipeline executions",
"Three points where a pipeline run can crash",
"Nondeterministic transforms break safe reruns",
"Partition replace needs DELETE and INSERT in one transaction",
"MERGE reruns safely with unique source keys and version guard",
"Partition replace versus MERGE: what each costs to rerun",
"Record a run's input offset range before reading it",
"Commit order sets whether a crash loses or repeats records",
"Timestamp watermarks skip late-committed rows",
"Dedup state grows with event rate times window length",
"Checkpoint-bound sinks delay output until checkpoint completes",
"Window results need an upsert sink keyed by window",
"Side effects need receiver-side keys derived from events",
"Answering whether a pipeline design is idempotent",
"Walking crash points to show a retry converges",
"Rerun fingerprint test: compare target state after two runs",
"Follow-up questions on backfill, exactly-once, overlap, and cost",
]
lines = open(src, encoding="utf-8").read().split("\n")
pat = re.compile(r"^### (.*) (\(L\d+-\d+\))$")
idx = [i for i, l in enumerate(lines) if pat.match(l)]
assert len(idx) == len(new) == 18, (len(idx), len(new))
for k, i in enumerate(idx):
    rng = pat.match(lines[i]).group(2)
    lines[i] = f"### {new[k]} {rng}"
open(dst, "w", encoding="utf-8").write("\n".join(lines))
print("ok", len(idx))
