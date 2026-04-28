# Phase 1 Status Report

Date: 2026-04-28

Repository: https://github.com/Bhavzzzzzz/Multipipeline-ETL

Summary
- Phase: 1 (Apache Pig)
- Scope completed:
  - Orchestrator / controller: [src/controllers/main.py](src/controllers/main.py#L1-L200)
  - Log parsing & batching: [src/controllers/utils.py](src/controllers/utils.py#L1-L300)
  - Pig pipelines: [src/pipelines/pig/queries.pig](src/pipelines/pig/queries.pig#L1)
  - PostgreSQL ingestion client: [src/controllers/db_client.py](src/controllers/db_client.py#L1-L400)
  - Reporting/CLI scaffolding: [src/controllers/reporting.py](src/controllers/reporting.py#L1-L200)

What's implemented (concise)
- Physical batching of raw logs into batch files (works for .gz and plain text).
- Pig job invocation (local mode) from the Python orchestrator.
- Pig queries that compute the three required aggregations (daily traffic, top resources, hourly errors).
- Database schema + ingestion code to persist query outputs to PostgreSQL.
- Basic reporting CLI that can run the orchestrator and fetch results from the DB.

Member contributions
- Member 1 — Data & Controller:
  - Designed the log parsing regex and implemented `process_and_batch_logs` in `src/controllers/utils.py`.
  - Built the initial orchestrator in `src/controllers/main.py` to coordinate batching and pipeline triggers.

- Member 2 — Pig Pipeline:
  - Implemented the Pig ETL queries in `src/pipelines/pig/queries.pig` covering Query 1, Query 2 and Query 3.

- Member 3 — Database & Ingestion:
  - Designed the PostgreSQL schema and reset script in `database/reset_and_create.sql` and `database/schema.sql`.
  - Implemented `src/controllers/db_client.py` to load Pig outputs into the reporting DB and provide query helpers.

- Member 4 — Reporting UI & Integration:
  - Built the interactive CLI/ reporting layer `src/controllers/reporting.py` to run pipelines and render fetched results.
  - Documented setup and run instructions in `README.md` and supported integration with `db_client`.
