# Handoff: iterating on the index-lesson skill

For a fresh leader agent picking this up. Read this, then `.claude/skills/index-lesson/SKILL.md`.

## Goal

Give a future data engineering agent a way to answer architecture questions from the lessons in `lesson_preview/` (26 markdown lessons, about 170k words). Chosen approach: agentic retrieval (grep, glob, read), helped by a hand-written two-level index. No RAG, no MCP for now.

- Level 1 (not built yet): a small top-level index, one entry per lesson, pointing to its detail index.
- Level 2: one file per lesson in `lesson_preview/index/<lesson-stem>.md`. Headers plus line ranges only, so the agent reads just the section it needs.

Lessons are immutable. Only the index and the skill change.

## Decisions so far (and why)

- Hand-written index, not script-generated. Headers need human judgment. The old generator `scripts/build_lesson_index.py` and `lesson_preview/INDEX.md` (generated, 766 lines) are stale and should be deleted.
- `##` titles copied word for word from the lesson. Vague ones are flagged to the human, never silently renamed.
- `###` headers rewritten so each stands alone as a general concept. Claims where one idea dominates, topic labels where a section lists options. Ten words or fewer. No example-specific numbers.
- No descriptions under headers. They summarize the lesson and tempt the agent to skip reading it. Fix the header or add an entry instead.
- `###` entries are not strictly one-to-one with lesson `###`. Extra entries are allowed when content would be unfindable from the header (unheaded block, several topics under one heading).
- Skipped sections: "What you will be able to do", "Key Takeaways", "Putting It All Together".

## State of the files

- Skill: `.claude/skills/index-lesson/SKILL.md` (user-invoked).
- Done and treated as the worked example: `lesson_preview/index/pa_pattern_backpressure_scaling.md` (hand-built with the human; 18 lesson `###` plus 2 extra entries from splitting L65-106).
- Second lesson, indexed by a fresh subagent with the current skill: `lesson_preview/index/pa_pattern_batch_or_stream_intermediate.md` (5 `##`, 29 `###` vs 14 in the lesson).
- First-run output from the earlier skill version, kept for comparison: `docs/agents/index-lesson/batch_or_stream_v1_first_run.md` (25 `###`, `##` titles reworded).

## How we test the skill

Spawn a general-purpose subagent with no other context, have it follow the skill on a lesson, and ask it to report ambiguities. Compare its output to the rules. Prompts used, verbatim:

First run:

```
Read /home/dulain/projects/study-helper/.claude/skills/index-lesson/SKILL.md and follow it literally to index the lesson /home/dulain/projects/study-helper/lesson_preview/pa_pattern_batch_or_stream_intermediate.md. Write the result to /home/dulain/projects/study-helper/lesson_preview/index/pa_pattern_batch_or_stream_intermediate.md.

Do not edit the lesson or the skill file, and do not touch any other file. You have no other context; rely only on the skill and the files it points to.

When done, reply with: (1) the path you wrote, (2) any step or rule in the skill that was ambiguous, contradictory, or that you had to guess at, quoting the text, and (3) any place where you were tempted to add a description or break a rule, and why. Keep the reply under 300 words.
```

Redo after the skill was revised:

```
Read /home/dulain/projects/study-helper/.claude/skills/index-lesson/SKILL.md and follow it literally to index the lesson /home/dulain/projects/study-helper/lesson_preview/pa_pattern_batch_or_stream_intermediate.md. Write the result to /home/dulain/projects/study-helper/lesson_preview/index/pa_pattern_batch_or_stream_intermediate.md, replacing any existing file there; do not read the existing file first.

Do not edit the lesson or the skill file, and do not touch any other file. You have no other context; rely only on the skill and the files it points to.

When done, reply with: (1) the path you wrote, (2) the Flagged list the skill asks for, (3) any step or rule in the skill that was ambiguous, contradictory, or that you had to guess at, quoting the text, and (4) any place where you were tempted to break a rule, and why. Keep the reply under 300 words.
```

What to check in each result: `##` titles verbatim, `###` count versus the lesson's, semicolons in headers, interview coaching or scenario vocabulary in headers, ranges contiguous, and the subagent's reported ambiguities.

## Findings from the redo (second run)

Better: `##` verbatim, vague title flagged ("Design for Both Batch and Stream"), no semicolon headers, no interview coaching. Worse: 29 entries for 14 lesson headings, several 2 to 5 line entries (L47-48, L345-346, L275-279). The "unfindable otherwise" test does not limit splitting.

## Open issues

1. Skill contradiction: "Entries cover every line... with no gaps" conflicts with the worked example, which leaves text before the first `###` uncovered. Allow that gap explicitly.
2. Skill's last example (testing do's and don'ts) confused the subagent. Delete it.
3. Entry granularity: tighten the extra-entry rule (a distinct, named concept a data engineer would look up by itself; short runs stay inside a neighbor's range) or accept 29. Undecided.
4. Some headers carry detail after a colon, close to a description ("Streaming test do's and don'ts: watermark from fixture, no sleep()").
5. Top-level index design is undecided. Per-lesson titles are broad; a concept-based top level (concept pointing to sections across lessons) may serve a data engineering agent better, since one concept can span several lessons.

## Next steps (agreed direction, not yet done)

1. Build a small eval before tuning more. Draft about 8 questions answerable from the two indexed lessons, each with the expected sections and key points. For each question run a fresh no-context subagent in three setups: A) lessons folder only (grep and read), B) the first-run index, C) the current index. Have it report which line ranges it read. Score section hit rate, lines read, and answer correctness. This only tests finding a section inside a known lesson, not choosing a lesson; the top-level index tests that later.
2. Use the results to settle issue 3, then fix issues 1, 2, and 4 in the skill and re-run once.
3. Add mechanical validation as a script: `##` titles match the lesson, ranges fall within the file, sibling ranges neither overlap nor leave gaps. Reserve the human or model review for header wording.
4. Delete `scripts/build_lesson_index.py` and the old `lesson_preview/INDEX.md`.
5. Design and build the top-level index, then index the remaining lessons in batches of 4 to 5 for human review.

## Working preferences observed

The human wants discussion before edits on judgment calls ("let's talk first"), does not want the lessons modified, and wants the writing-for-agents skill used whenever the skill is edited. Subagent results are the test of the skill, not the leader's own opinion.

## Eval results (batch_or_stream lesson, 10 questions, 5 setups)

Data and scripts: `docs/agents/index-lesson/eval/` (questions with ground truth, the five indexes, all 50 answers, scores.json, grades.json, score.py).

Setup: 10 LLM-written questions (types: scenario 3, numeric 2, comparison 2, buried 3), each answered by a fresh no-context Haiku subagent under five setups. A = no index. B = first-run index (25 `###`). C = redo index (29). D = C with entries under 10 lines merged into a neighbour (18). E = the lesson's own `##`/`###` headings verbatim, mechanically generated (14). The agent was told which lesson to use, so this tests finding the section inside a known lesson, not choosing a lesson. Cost and reads come from the subagent transcripts (Read offset/limit), not self-report. Correctness graded by a separate subagent against 2 to 4 key points per question.

| Setup | Expected lines covered | Chars read, lesson + index | ~Tokens | Key points hit | Runs that read the whole lesson |
| --- | --- | --- | --- | --- | --- |
| A none | 100% | 22,743 + 0 | 5,700 | 36/39 | 6 of 10 |
| B 25 entries | 100% | 2,926 + 2,442 | 1,340 | 38/39 | 0 |
| C 29 entries | 100% | 2,790 + 2,747 | 1,380 | 38/39 | 0 |
| D 18 entries | 100% | 3,781 + 1,843 | 1,410 | 37/39 | 0 |
| E 14, raw headings | 100% | 3,994 + 1,081 | 1,270 | 36/39 | 0 |

What it shows:
1. Any index cut reading cost about 4x versus none, and no indexed run read the whole lesson.
2. Among B to E the spread is under 10% in tokens and 2 of 39 key points. That is within noise at n=10 with one run per cell. No setup missed a section.
3. Finer entries read less lesson (C 2,790 chars vs E 3,994) but cost more index (2,747 vs 1,081), and the two cancel.
4. Raw lesson headings (E, free to generate) matched the hand-rewritten indexes on this lesson. Nothing measured here supports the extra entries, header rewriting, or the 10-line merge rule.
5. Five of the ten missed key points are one q8 point no setup mentioned, which is a question issue, not an index issue.

Caveats: one lesson, 10 questions, one run per cell, Haiku. Grep was unavailable to the subagents, so setup A fell back to reading the whole file; with grep the A cost would be lower and the gap smaller. The questions were written from the lesson text by a model. Every setup reached 100% coverage, so the questions did not discriminate between indexes. The lesson-selection hop (top-level index) is untested.

## Updated next steps

1. Do not hand-index the remaining 24 lessons yet. The evidence says verbatim headings plus ranges are as good as hand-tuned ones at single-lesson scale.
2. Test what could discriminate: a multi-lesson corpus where the agent must pick the lesson and then the section, comparing (a) verbatim-heading indexes and (b) hand-tuned indexes, with questions phrased far from the headings, and a top-level index variant. Include Grep in the baseline.
3. If verbatim-heading indexes hold up, replace the skill's per-header rewriting with a generator plus a short list of exceptions for misleading headings. Keep the validation script idea.

## Eval 2 results (4 lessons, grep available, questions far from headings)

Data and scripts: `docs/agents/index-lesson/eval2/`. Lessons: batch_or_stream, backpressure, idempotency, cdc_dual_writes. 8 questions (2 per lesson, one "buried" and one "scenario" each) written by a subagent to avoid heading wording. Each question run once per arm by a fresh Haiku subagent that could Read and use Bash grep/sed over the whole 26-lesson folder and was told where the 4 index files were. Cost = characters returned by all tool calls (from transcripts); correctness = key points hit, graded by a separate subagent.

Arms: G = no index. V = lesson headings verbatim (generated). R = one-to-one rewrite: `##` verbatim, each `###` rewritten per the skill's header rules, no extra entries (Haiku). F = full current skill with extra entries (Haiku for idempotency and cdc, earlier agents for the other two).

| Arm | Index entries (### per lesson) | ~Tokens per question | Key points hit |
| --- | --- | --- | --- |
| G none | - | 4,700 | 15/16 |
| V verbatim | 17, 14, 18, 18 | 4,000 | 16/16 |
| R one-to-one rewrite | 17, 14, 18, 18 | 3,700 | 16/16 |
| F full skill | 20, 29, 35, 38 | 5,000 | 15/16 |

Reading: with grep available, no index costs only about 13% more than verbatim headings (versus about 4x when grep was unavailable in eval 1). R was cheapest (26% under F, 8% under V) and fully correct. F cost more than no index: Haiku over-split two lessons to about twice the lesson's own `###` count, so the index itself grew (up to 3,006 chars vs about 1,700 for R). The correctness spread is one key point (q6, G and F).

Caveats: 8 questions, one run per cell, Haiku. Differences between V and R are within noise; the F overhead is the clearest effect. Runs sometimes cited lessons outside the four indexed ones, so a no-index arm can still answer correctly from related lessons. Lesson selection was among only 4 indexed lessons. An attempted "right lesson opened" metric was unreliable (folder-wide grep never names the file) and is not used.

Conclusion for the skill: the extra-entry rule is unsupported and harmful as executed, so default to one-to-one (`##` verbatim, one rewritten `###` per lesson `###`). Whether rewriting beats verbatim headings is unproven at this sample size.

## Eval 3: index-only selection probe (10 lessons, 50 questions, 2 replicates)

Data and scripts: `docs/agents/index-lesson/eval3/`. The question: given only an index of 10 lessons, can an agent name the line range that holds the answer? This isolates header wording from reading noise.

Setup: 10 mostly distinct lessons, 5 questions each (50), written by Haiku subagents from the lesson text, phrased as a data engineer's situation, only 4% of question words (5+ letters) appear in that lesson's headings. Ground truth is 1 to 2 lines per question (spot-checked). Arms: V = lesson headings verbatim (generated). R and R2 = two independent Haiku one-to-one rewrites (`##` verbatim, each `###` rewritten per the skill's Header rules, same ranges, no extra entries). Each arm was probed by 2 fresh Haiku agents with the questions in different shuffled orders; each agent saw only the arm's combined index and named up to 2 line ranges per question. Scoring is mechanical: the picked ranges either cover the ground-truth lines or they do not.

| Arm | Section found (rep 1, rep 2) | Mean | Right lesson (top pick) | Mean lines picked |
| --- | --- | --- | --- | --- |
| V verbatim | 42%, 58% | 50% | 84%, 86% | 58 |
| R rewrite | 66%, 60% | 63% | 84%, 86% | 48 |
| R2 rewrite (independent) | 58%, 62% | 60% | 88%, 84% | 46 |

Paired bootstrap over the 50 questions: R minus V = +13 points (95% CI +2 to +25); R2 minus V = +10 (CI 0 to +21); R minus R2 = +3 (CI -5 to +12).

Reading: both independent rewrites beat verbatim headings by roughly 10 to 13 points at finding the right section, and the two rewrites are indistinguishable from each other, so the gain comes from rewriting, not from one lucky rewrite. Picking the right lesson was about 85% for every arm; the difference is entirely in choosing the section inside the lesson.

Caveats: the bootstrap resamples questions only, but the two V replicates differ by 16 points, so agent-to-agent noise is as large as the effect and the CI is optimistic. One model family (Haiku) wrote the indexes, wrote the questions and ran the probes. The probe saw only the index; a real agent can fall back to grep when the index misses, so a miss costs a second lookup, not a failure. Full-skill indexes with extra entries (arm F in eval 2) were not in this probe. Lines picked are noisy and show no clear cost difference.

## Decision

Default the skill to one-to-one: `##` titles verbatim, one rewritten `###` per lesson `###`, no extra entries. Eval 2 showed extra entries raise cost; eval 3 shows rewritten headers beat verbatim ones at section finding.

## Status after eval 3

The skill (`.claude/skills/index-lesson/SKILL.md`) is now one-to-one: `##` verbatim, one rewritten `###` per lesson `###`, no extra entries, and the backpressure worked example was brought in line (17 `###`, 5 `##`). Still stale: `lesson_preview/index/pa_pattern_batch_or_stream_intermediate.md` (29 entries, old skill) and the old generated `lesson_preview/INDEX.md` and `scripts/build_lesson_index.py`.

Next: re-index batch_or_stream with the new skill and confirm its `###` count is 14, then index the remaining lessons in batches of 4 to 5 for review, then build the top-level index and delete the stale generated files.

## Status: all 26 lessons indexed

All 26 lessons have an index in `lesson_preview/index/`, written by Haiku subagents with the one-to-one skill and checked by `scripts/validate_lesson_index.py` (all 26 print OK). The stale generated `INDEX.md` and `build_lesson_index.py` were deleted. Vague `##` titles are listed in `flagged-titles.md` for review. Not done yet: a top-level index (one entry per lesson, ideally by concept), and nothing is committed to git.

## MCP server

A small MCP server lives in `~/projects/data-eng-knowledge/` (own `.venv`, one file `server.py`). Five tools: list_lessons, get_lesson_index, read_lesson, search (headers), grep_lessons (full text). Its `instructions` tell the agent to treat the lessons as the source of truth and cite lesson and lines. Tested directly and over a real stdio MCP handshake. Measured: the top-level `INDEX.md` alone picked the right lesson first 57% of the time (top 2: 69%); header keyword search with the raw question as the query was weak (12% first, 54% anywhere in the top 8 hits), so it is a supplement to the agent's own keyword queries and `grep_lessons`, not a replacement. Agent-written queries were not measured.

The `index-lesson` skill now also adds the lesson's line to `lesson_preview/INDEX.md`, and the validator checks it exists.
