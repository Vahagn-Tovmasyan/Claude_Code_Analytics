# Claude Code Usage Analytics Platform

An end-to-end analytics platform that processes Claude Code telemetry data, transforming raw event streams into actionable insights through an interactive dashboard, REST API, and predictive analytics.

## Architecture

```
┌───────────────────────────────────────────────┐
│            Presentation Layer                 │
│   Streamlit Dashboard  │  FastAPI Endpoints   │
├───────────────────────────────────────────────┤
│            Analytics / Service Layer          │
│   Core Metrics │ User Patterns │ Predictions  │
├───────────────────────────────────────────────┤
│            Repository / Data Access Layer     │
│   SQLAlchemy Models │ Query Functions         │
├───────────────────────────────────────────────┤
│            Data / ETL Layer                   │
│   Ingestion │ Validation │ Transformation     │
├───────────────────────────────────────────────┤
│            Storage: SQLite                    │
└───────────────────────────────────────────────┘
```

**Design choice — Clean Layered + Repository Pattern** over Hexagonal or Command Pattern:
- Data flows linearly (ingest → store → analyze → present), making layers a natural fit
- Repository Pattern isolates all DB queries, providing the core Hexagonal benefit without boilerplate
- Each layer is independently testable via clearly defined function interfaces

## Project Structure

```
├── generate_fake_data.py        # Synthetic data generator
├── run_pipeline.py              # CLI: generate → ETL → DB load
├── requirements.txt             # Python dependencies
│
├── src/
│   ├── config.py                # Central configuration
│   ├── etl/                     # Ingest + validate + transform
│   ├── db/                      # SQLAlchemy models + repository
│   ├── analytics/               # Core metrics, user patterns, tools, ML
│   └── api/                     # FastAPI REST endpoints
│
├── dashboard/
│   └── app.py                   # Streamlit interactive dashboard
│
├── tests/                       # pytest test suite
└── output/                      # Generated data + SQLite DB
```

## Quick Start

### Prerequisites

- Python 3.10+
- pip

### Setup

```bash
# 1. Clone and enter the project
git clone <repo-url>
cd Provectus

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the full pipeline (generate data → ETL → load DB)
python run_pipeline.py

# 5. Launch the dashboard
streamlit run dashboard/app.py

# 6. (Optional) Launch the API
uvicorn src.api.main:app --reload
```

### Pipeline Options

| Flag | Default | Description |
|------|---------|-------------|
| `--num-users` | 100 | Number of synthetic engineers |
| `--num-sessions` | 5000 | Total coding sessions |
| `--days` | 60 | Time span in days |
| `--skip-generate` | false | Use existing data files |
| `--seed` | 42 | Random seed for reproducibility |

### Run Tests

```bash
pytest tests/ -v
```

## Output Files

| File | Format | Description |
|------|--------|-------------|
| `output/telemetry_logs.jsonl` | JSONL | Raw telemetry batches |
| `output/employees.csv` | CSV | Employee directory |
| `output/analytics.db` | SQLite | Processed analytics database |

## Database Schema

Six normalized tables with indexes for efficient querying:

| Table | Key Columns | Description |
|-------|-------------|-------------|
| `employees` | email, practice, level, location | User metadata |
| `sessions` | session_id, total_cost, started_at | Session aggregates |
| `api_requests` | model, cost_usd, input/output_tokens | LLM API call events |
| `tool_events` | tool_name, decision, success, duration | Tool usage events |
| `user_prompts` | prompt_length, timestamp | User prompt events |
| `api_errors` | error, status_code, attempt | API error events |

## Dashboard Pages

1. **Overview** — KPIs, daily activity, hourly heatmap, day-of-week charts
2. **Cost & Token Analysis** — Trends with moving averages, cumulative cost, model/practice/level breakdowns, token stacking
3. **Tool Usage** — Popularity, acceptance/success rates, execution durations, decision source sunburst
4. **User Patterns** — Power users, Gini/Pareto distribution, practice/level/location comparisons
5. **Predictions** — Cost forecasting with confidence intervals, anomaly detection, growth projections

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/metrics/summary` | GET | Overall KPIs |
| `/api/metrics/costs/daily` | GET | Daily cost trends |
| `/api/metrics/costs/by-model` | GET | Cost by model |
| `/api/metrics/costs/by-practice` | GET | Cost by practice |
| `/api/metrics/costs/by-level` | GET | Cost by level |
| `/api/metrics/tools` | GET | Tool usage stats |
| `/api/metrics/tools/efficiency` | GET | Tool efficiency scores |
| `/api/users` | GET | All user activity |
| `/api/users/top?n=10` | GET | Top-N power users |
| `/api/users/distribution` | GET | Gini, Pareto stats |
| `/api/metrics/activity/hourly` | GET | Hourly activity |
| `/api/metrics/errors` | GET | Error distribution |
| `/api/predictions/forecast?days=14` | GET | Cost forecast |
| `/api/predictions/anomalies` | GET | Anomaly detection |
| `/api/predictions/growth` | GET | Growth projections |

Full API docs available at `http://localhost:8000/docs` (Swagger UI).

## Key Analytics & Insights

- **Token Consumption Trends** — Daily input/output token usage with 7/14-day moving averages
- **Cost Attribution** — Breakdown by model, engineering practice, seniority level, location
- **Peak Usage Patterns** — Hourly and day-of-week activity heatmaps
- **Tool Behavior** — Acceptance rates, success rates, execution duration, efficiency scoring
- **User Segmentation** — Power user identification, Gini coefficient, Pareto analysis
- **Predictive Forecasting** — Polynomial regression / optional Prophet with 95% confidence
- **Anomaly Detection** — Rolling z-score based spike/drop identification

## Technical Highlights

- **Error Handling** — Malformed JSON lines skip gracefully with logging; type coercion prevents crashes
- **Validation** — Schema checks per event type before DB insertion
- **Data Integrity** — Foreign key constraints, unique indexes, transactional loads
- **Clean Architecture** — Repository pattern isolates queries; analytics are pure functions
- **Performance** — Bulk ORM operations, SQLite WAL mode, indexed columns for common queries

## Dependencies

| Package | Purpose |
|---------|---------|
| pandas | Data processing and analysis |
| sqlalchemy | ORM and database management |
| streamlit | Interactive dashboard |
| plotly | Chart visualizations |
| fastapi + uvicorn | REST API |
| scipy, scikit-learn | Statistical analysis & ML |
| prophet (optional) | Advanced time-series forecasting |
| pytest, httpx | Testing |

## LLM Usage Log

This project was built with AI assistance (Gemini / Antigravity). Key areas where LLM was used:

1. **Architecture Design** — Evaluated Hexagonal, Command Pattern, and Clean Layered approaches; recommended Clean Layered with Repository Pattern based on data pipeline characteristics
2. **Database Schema** — Generated normalized 6-table schema with SQLAlchemy ORM models and strategic index placement
3. **ETL Pipeline** — Built ingestion (generator-based JSONL reader), validation (per-event-type schema checks), and transformation (classify → derive sessions → bulk load)
4. **Analytics Layer** — Implemented core metrics, user segmentation (Gini/Pareto), tool efficiency scoring, and predictive analytics (polynomial regression forecasting, z-score anomaly detection)
5. **Dashboard** — Created 5-page Streamlit app with 20+ Plotly charts, custom dark theme with glassmorphism CSS
6. **API** — Generated 17 FastAPI endpoints with auto-documentation
7. **Tests** — Created 40+ tests with mock fixtures covering ETL, analytics, and API layers

**Validation approach**: All AI-generated code was reviewed for correctness, tested with the synthetic dataset, and validated via pytest and manual dashboard inspection.
