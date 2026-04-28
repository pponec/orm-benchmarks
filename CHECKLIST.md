# ORM Benchmark Methodology Checklist

This checklist tracks follow-up actions based on methodological review feedback.

## 1) Fix benchmark methodology first (core correctness)

- [x] Review insert benchmark flow and ensure `persist` timing includes `flush` and transaction `commit`.
- [x] Document exactly what each measured metric includes (ORM work, SQL execution, DB roundtrip, mapping).
- [x] Add assertions/validation to ensure inserts are actually flushed to DB within measured scope.
- [x] Separate benchmark phases clearly: setup, warm-up, measurement, validation.

## 2) Add PostgreSQL benchmark profile (production-like baseline)

- [x] Introduce a dedicated Postgres profile/config next to H2.
- [x] Provide reproducible PostgreSQL startup via `run-database-pg.sh` script workflow.
- [x] Add DB warm-up phase before measured iterations.
- [x] Ensure schema setup and teardown are deterministic per run.

## 3) Expand benchmark realism (beyond simple CRUD)

- [ ] Add at least one join-heavy read scenario (multi-table, relation-rich query).
- [ ] Add a mixed read/write scenario with realistic transaction boundaries.
- [ ] Add a scenario with pagination/filtering/sorting on indexed columns.
- [ ] Evaluate a concurrent workload variant (optional first pass).
- [ ] Keep scenario definitions explicit: what layer they primarily stress (ORM vs DB).

## 4) Apply consistent framework tuning

- [ ] Create a tuning matrix document for all frameworks (especially Hibernate).
- [ ] Record key settings: batch size, flush strategy, fetch size, caching, statement settings.
- [ ] Ensure "reasonable best-practice" tuning is applied consistently.
- [ ] Include exact framework and driver versions in benchmark output.

## 5) Re-run and validate results

- [ ] Re-run baseline results after methodology fixes.
- [ ] Run H2 and PostgreSQL benchmarks with identical scenario definitions.
- [ ] Verify result stability (repeat runs) and capture variance.
- [ ] Capture before/after deltas versus previous published numbers.
- [x] Keep deterministic DB reset policy: reset once before each framework run.
- [x] Do not reset before each individual operation (would distort operation-level workload continuity).

## 6) Standardized workload path (TPC-C / HammerDB, optional phase)

- [ ] Evaluate feasibility of HammerDB TPROC-C integration for reference testing.
- [ ] Define minimum viable standardized workload scope for phase 1.
- [ ] Document gaps between current microbenchmarks and standardized workload behavior.
- [ ] Decide whether to publish as separate report or part of this repository.

## 7) Update README and external communication last

- [ ] Add temporary disclaimer immediately if needed while fixes are in progress.
- [ ] Update `README.md` to clearly separate H2 microbenchmark from PostgreSQL results.
- [ ] Separate measured facts from interpretation in result commentary.
- [ ] Add a "Threats to validity" section with clear limitations.
- [ ] Prepare a "Methodology update" note summarizing changes and rationale.
- [ ] Include before/after notes where methodology changed measured outcomes.
- [ ] Share a concise follow-up with reviewer (Vlad) and request quick sanity check.
- [ ] Tag release notes with benchmark methodology version/date.

## 8) Execution plan (short horizon, implementation-first)

### Week 1

- [x] `persist + flush + commit` measurement fix for Hibernate path.
- [x] Harness cleanup for warm-up vs measurement separation.
- [x] PostgreSQL profile + Docker setup.
- [ ] Baseline rerun on H2 and first run on PostgreSQL.

### Week 2

- [ ] Add at least 1-2 more realistic scenarios.
- [ ] Apply and verify framework tuning matrix.
- [ ] Repeat runs and capture variance/deltas.
- [ ] Final README update + methodology update summary.

