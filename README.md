# Port Tariff Engine

Automates port tariff calculation from PDF documents. Upload a tariff PDF once, then query vessel dues instantly with full audit trails.

## How It Works

The system splits the problem into two phases:

**Ingestion** — Parse the tariff PDF, extract rates and rules using Gemini, persist them as structured JSON. This runs once per document.

**Calculation** — Given a vessel profile and port, look up the applicable rules and compute dues with pure arithmetic. No LLM involved at query time, so results are fast, reproducible, and auditable.

```
  Tariff PDF ──▶ Parser ──▶ Gemini Extraction ──▶ Rule Store (JSON)
                                                        │
  Vessel Query ──────────────────────────────────▶ Calculator ──▶ Result + Audit
```

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
echo "GEMINI_API_KEY=your-key" > .env
```

## Usage

**Step 1 — Ingest the tariff document:**

```bash
python -m scripts.ingest data/port_tariff.pdf
```

**Step 2 — Start the API:**

```bash
uvicorn src.main:app --reload
```

**Step 3 — Calculate dues:**

```bash
curl -X POST http://localhost:8000/calculate \
  -H "Content-Type: application/json" \
  -d '{
    "port": "Durban",
    "vessel": {
      "vessel_metadata": { "name": "SUDESTADA", "flag": "MLT - Malta" },
      "technical_specs": {
        "type": "Bulk Carrier", "dwt": 93274,
        "gross_tonnage": 51300, "net_tonnage": 31192,
        "loa_meters": 229.2, "beam_meters": 38.0, "lbp_meters": 222.0,
        "draft_sw_s_w_t": [14.9, 0.0, 0.0], "suez_nt": 49069
      },
      "operational_data": {
        "cargo_quantity_mt": 40000, "days_alongside": 3.39,
        "arrival_time": "2024-11-15T10:12:00",
        "departure_time": "2024-11-22T13:00:00",
        "activity": "Exporting Iron Ore",
        "num_operations": 2, "num_holds": 7
      }
    }
  }'
```

Swagger docs available at `http://localhost:8000/docs`.

## API

| Method | Path | What it does |
|--------|------|--------------|
| `POST` | `/ingest` | Upload a tariff PDF, extract rules |
| `POST` | `/calculate` | Compute dues for a vessel at a port |
| `GET` | `/rules/{port}` | Inspect what was extracted |
| `GET` | `/ports` | List ports with available rules |
| `GET` | `/health` | Health check |

## Accuracy

Verified against SUDESTADA at the Port of Durban:

| Due | Calculated | Expected | Diff |
|-----|-----------|----------|------|
| Light Dues | 60,062.04 | 60,062.04 | 0.00% |
| Port Dues | 199,371.35 | 199,549.22 | 0.09% |
| Towage | 147,074.38 | 147,074.38 | 0.00% |
| VTS | 33,345.00 | 33,315.75 | 0.09% |
| Pilotage | 47,189.94 | 47,189.94 | 0.00% |
| Running Lines | 19,639.50 | 19,639.50 | 0.00% |

All within 1% tolerance. Four are exact.

## Project Layout

```
src/
  models/         Pydantic models — vessel, rules (typed rate structures), results
  ingestion/      PDF parsing, section splitting, Gemini extraction, rule persistence
  engine/         Rule matching, condition evaluation, deterministic calculator, audit trails
  llm/            Gemini client wrapper
  main.py         FastAPI app
tests/            Unit tests for calculator + integration golden test
scripts/          CLI ingestion script
data/             Source PDF + extracted rules
```

## Rate Structures

The engine handles five rate patterns found across port tariffs:

- **FlatRate** — fixed rate per unit (light dues, VTS)
- **TieredRate** — bracketed rates by tonnage (towage)
- **CompositeRate** — multiple components summed together (port dues = initial fee + daily fee)
- **PerServiceRate** — per-operation charge with base + unit rate (pilotage, berthing)
- **TimeBasedRate** — rate multiplied by duration (port dues daily component)

New rate types slot in without touching existing calculation logic.

## Docker

```bash
docker compose up --build
# or
docker build -t tariff-engine . && docker run -p 8000:8000 -e GEMINI_API_KEY=your-key tariff-engine
```

## Tests

```bash
pytest tests/ -v
```

## Key Decisions

- **No LLM at query time.** Tariff math is arithmetic, not language. Extract once, compute forever. Same input always gives the same output.
- **`decimal.Decimal` everywhere.** Floating point has no place in financial calculations.
- **Per-section Gemini prompts.** Each tariff type gets a focused prompt instead of dumping the whole PDF. Better extraction, no truncation issues.
- **No framework dependencies.** Raw Gemini SDK + Pydantic. Nothing opaque.

## Stack

Python 3.11+ / FastAPI / Pydantic v2 / Gemini 2.0 Flash / pdfplumber / pytest
