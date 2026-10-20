Q: When you hear these in an interview, which concept is being tested?
- How do you orchestrate your pipelines?
- What is a DAG?
- Why not just use cron?

A: DAG Orchestration

---

Q: How do you orchestrate your pipelines?
- What is a DAG?
- Why not just use cron?

A: A DAG is a directed acyclic graph that defines tasks and their dependencies. Airflow is the standard orchestrator. Each task is a node. Edges define ordering. If task B depends on task A, Airflow won't start B until A succeeds.

---

Q: What is the difference between a cron job and an orchestrator like Airflow?

A: Cron can schedule a job, but it can't manage dependencies between jobs, retry failed tasks, or show you the state of your entire pipeline at a glance. This is why orchestrators like Airflow exist.

---

Q: When asked about Airflow DAG orchestration, a common follow-up is: what happens when a task in the middle fails?

A: Downstream tasks do not run. Airflow marks them as upstream_failed. The on-call fixes the failed task and reruns it. Airflow picks up from that task, not from the beginning.

---

Q: When asked about Airflow DAG orchestration, a common follow-up is: what other orchestrators exist?

A: 
- Dagster — more opinionated, better testing
- Prefect — Python-native, cloud-first
- Mage — newer, built for ELT
- Airflow is most common, but Dagster is gaining fast

---

Q: When asked about Airflow DAG orchestration, a common follow-up is: what is the "DAG of DAGs" problem?

A: When you have 200+ DAGs that depend on each other across team boundaries. Cross-DAG dependencies are harder than within-DAG dependencies.

---

Q: When you hear these in an interview, which concept is being tested?
- How do you handle dependencies between pipelines?
- What if pipeline B needs data from pipeline A?
- Sensor versus trigger, when do you use each?

A: Task Dependencies

---

Q: How do you handle dependencies between pipelines?
- What if pipeline B needs data from pipeline A?
- Sensor versus trigger, when do you use each?

A: Within a DAG, dependencies are edges — task A runs before task B. Across DAGs, it gets harder. Sensors poll for a signal (a file exists, a partition is populated). Event triggers fire when the data is ready. The trade-off: sensors waste compute by polling; event triggers are more efficient but require the producer to publish a signal.

---

Q: When asked about task dependencies, a common follow-up is: what if the producer DAG is late?

A: The sensor times out. You need an SLA — if the data is not ready by X time, alert the producer team and serve stale data to the consumer.

---

Q: When asked about task dependencies, a common follow-up is: how do you avoid a web of cross-DAG dependencies?

A: Limit cross-DAG dependencies to published interfaces (tables with SLAs). Teams should depend on tables, not on other teams' DAGs directly.

---

Q: When asked about task dependencies, a common follow-up is: what are Airflow datasets?

A: Data-aware scheduling, introduced in Airflow 2.4. A producer DAG declares it updates a dataset. Consumer DAGs trigger automatically when that dataset is updated. No polling needed.

---

Q: When you hear these in an interview, what is the concept being tested?
- What happens when a pipeline fails?
- How do you make retry safe?
- What is exponential backoff?

A: Safe Retries

---

Q: What happens when a pipeline fails?
- How do you make retry safe?
- What is exponential backoff?

A: Two things make retry safe. Idempotency (running twice produces the same result) and exponential backoff (wait longer between each retry). Classify failures as transient (network timeout, API rate limit: retry with backoff) or permanent (schema mismatch, bad data): alert human, do not retry.

---

Q: When asked about safe retries, a common follow-up is: how many times do you retry?

A: 3 retries for transient failures with exponential backoff (1 minute, 5 minutes, 30 minutes). After that, alert the on-call.

---

Q: When asked about safe retries, a common follow-up is: what is exponential backoff?

A: Double the wait between each retry (1 second, 2 seconds, 4 seconds, 8 seconds). Add random jitter to avoid thundering herd (all retries firing at the same time).

---

Q: When asked about safe retries, a common follow-up is: what if the pipeline is not idempotent and you retry?

A: You get duplicate data. This is why idempotency is the FIRST thing to build. Without it, retries are dangerous.

---

Q: What are the key takeaways when asked about DAG orchestration?

A:
- Say: Airflow orchestrates tasks as a DAG: define dependencies, retry on failure, track state
- Why not cron? No dependency management, no retry logic, no visibility into pipeline state
- Name-drop alternatives: Dagster (better testing), Prefect (Python-native)

---

Q: When asked about task dependencies, what are the key takeaways?

A:
- Say: within a DAG, edges. Across DAGs: sensors or event triggers. Depend on tables, not on other teams' DAGs.
- Sensors poll (waste compute). Event triggers react (efficient, but need producer cooperation).
- Pro tip: Airflow datasets (v2.4+) eliminate polling for cross-DAG dependencies.

---

Q: When asked about safe retries, what are the key takeaways?

A:
- Say: idempotent writes + exponential backoff + classifying transient versus permanent failures
- Transient: retry with backoff. Permanent: alert human, stop retrying.
- Without idempotency, retries create duplicate data. Always build idempotency first.

---

Q: When you hear these in an interview, which concept is being tested?
- What environments do you use?
- How do you test pipeline changes before production?
- What is thin staging?

A: dev / staging / prod

---

Q: What environments do you use?
- How do you test pipeline changes before production?
- What is thin staging?

A: Three environments: dev (fast iteration, sample data), staging (production-like infrastructure, recent data snapshot), prod (real data, real consumers). Staging catches issues that dev misses: permission errors, data volume differences, infrastructure differences. A common cost optimization is thin staging: production-like infrastructure, but only the last 7 days of data instead of the full history.

---

Q: When asked about dev/staging/prod, a common follow-up is: is staging worth the cost?

A: Yes, for data pipelines. Data bugs are silent. They do not throw errors. They produce wrong numbers. Staging catches these before consumers see them.

---

Q: When asked about dev/staging/prod, a common follow-up is: what is thin staging?

A: Same infrastructure as prod, but with a subset of data (last 7 days instead of 5 years). This reduces storage cost by 95% while still catching infrastructure and data volume issues.

---

Q: When asked about dev/staging/prod, a common follow-up is: how do you promote changes from staging to prod?

A: Same CI/CD pipeline: merge to main, automated tests in staging, deploy to prod if all pass. Never manual promotion.

---

Q: When asked about dev/staging/prod, what are the key takeaways?

A:
- Say: dev for iteration, staging for validation, prod for real data. Thin staging saves 95% of staging costs.
- Data bugs are silent. Staging catches wrong numbers before consumers see them.
- Never manual promotion. CI/CD pipelines handle dev → staging → prod.

---

Q: When you hear these in an interview, what concept is being tested?
- Do you have CI/CD for your pipelines?
- What tests run before a pipeline change ships?
- How do you deploy pipeline changes safely?

A: CI/CD for data

---

Q: Do you have CI/CD for your pipelines?
- What tests run before a pipeline change ships?
- How do you deploy pipeline changes safely?

A: Every PR triggers: linting, unit tests, schema validation (fast, seconds). On merge to main, integration tests in staging (minutes). Before release, data diff on a staging subset (optional, hours). On deploy, canary rollout if the change affects critical pipelines. The key insight: fast checks on every push, slow checks before release. Never skip the fast checks.

---

Q: When asked about CI/CD for data, a common follow-up is: what if a test passes in staging but fails in prod?

A: Data volume differences. Staging has 7 days of data. Prod has 5 years. The test might pass on small data but fail at scale. This is why data diff on a prod-like subset matters for critical changes.

---

Q: When asked about CI/CD for data, a common follow-up is: how do you roll back a bad deployment?

A: Revert the PR and redeploy. If the pipeline is idempotent, the old code reruns on the same data and produces the correct output.

---

Q: When asked about CI/CD for data, a common follow-up is: what about schema migrations?

A: Schema changes need their own deployment process: backwards-compatible change first (add column), then code change (use new column), then clean up (remove old column). Never deploy a breaking schema change and a code change in the same release.

---

Q: When asked about CI/CD for data, what are the key takeaways?

A:
- Say: lint and unit test on every PR (seconds). Integration test on merge (minutes). Deploy if all pass.
- Fast checks on every push, slow checks before release. Never skip the fast checks.
- Schema changes: backwards-compatible first, code change second, clean up third.
