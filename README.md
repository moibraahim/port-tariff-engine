# Port Tariff Intelligence Engine

A CQRS-based system that ingests port tariff PDFs and calculates vessel dues deterministically — no LLM in the query path.

## Architecture: CQRS + Materialized Views

The core insight, borrowed from [Designing Data-Intensive Applications](https://dataintensive.net/) (DDIA), is **separating the write path from the read path**:

```
WRITE PATH (runs once per tariff document)     READ PATH (runs per query, no LLM)
──────────────────────────────────────────     ──────────────────────────────────────

  Port Tariff PDF                               Vessel Query (JSON)
       │                                             │
       ▼                                             ▼
  PDF Parser (pdfplumber)                       Rule Matcher
       │                                        (find rules for port + vessel)
       ▼                                             │
  Section Splitter                                   ▼
  (heading detection)                           Condition Evaluator
       │                                        (exemptions, reductions)
       ▼                                             │
  Rule Extractor                                     ▼
  (Gemini 2.0 Flash, temp=0)                    Calculator
       │                                        (pure arithmetic, Decimal)
       ▼                                             │
  Rule Store (JSON)                                  ▼
  ═══════════════                               Result + Audit Trail
  Materialized View                             (full formula trace)
```

### Why Not RAG?

| Aspect | RAG Approach | This System (CQRS) |
|--------|-------------|-------------------|
| LLM in query path? | Yes, every query | No — only during ingestion |
| Reproducible? | No (LLM variance) | Yes (deterministic math) |
| Speed per query | Slow (LLM round-trip) | Fast (pure computation) |
| Auditable? | LLM black box | Full formula trace |
| Testable? | Hard to unit test | Unit-testable per rate type |
| New tariff document? | Re-prompt everything | Re-run write path only |

### DDIA Concepts Applied

| Concept | Application |
|---------|-------------|
| **Source of Truth** (Ch 11) | The PDF is the source. Rules are derived data. |
| **Materialized View** (Ch 11) | Extracted rules = materialized view, re-derivable from PDF. |
| **CQRS** (Ch 11) | Write path (LLM extraction) separated from read path (calculation). |
| **Schema Evolution** (Ch 4) | Discriminated union rate structures — new types without breaking existing. |
| **Idempotency** (Ch 11) | Same PDF + same vessel = same result. Always. |
| **Data Lineage** (Ch 12) | Every ZAR amount traces to: page → section → rule → formula. |

## Quick Start

### Prerequisites
- Python 3.11+
- Gemini API key ([get one free](https://aistudio.google.com/apikey))

### Setup

```bash
cd port-tariff-engine

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set API key
echo "GEMINI_API_KEY=your-key-here" > .env
```

### 1. Ingest a Tariff Document (Write Path)

```bash
python -m scripts.ingest data/port_tariff.pdf
```

This runs the full pipeline: PDF parsing → section splitting → Gemini extraction → JSON persistence.

### 2. Start the API Server

```bash
uvicorn src.main:app --reload
```

### 3. Calculate Dues (Read Path)

```bash
curl -X POST http://localhost:8000/calculate \
  -H "Content-Type: application/json" \
  -d '{
    "port": "Durban",
    "vessel": {
      "vessel_metadata": {
        "name": "SUDESTADA",
        "built_year": 2010,
        "flag": "MLT - Malta"
      },
      "technical_specs": {
        "type": "Bulk Carrier",
        "dwt": 93274,
        "gross_tonnage": 51300,
        "net_tonnage": 31192,
        "loa_meters": 229.2,
        "beam_meters": 38.0,
        "lbp_meters": 222.0,
        "draft_sw_s_w_t": [14.9, 0.0, 0.0],
        "suez_nt": 49069
      },
      "operational_data": {
        "cargo_quantity_mt": 40000,
        "days_alongside": 3.39,
        "arrival_time": "2024-11-15T10:12:00",
        "departure_time": "2024-11-22T13:00:00",
        "activity": "Exporting Iron Ore",
        "num_operations": 2,
        "num_holds": 7
      }
    }
  }'
```

### 4. Inspect Extracted Rules

```bash
# List all ports with rules
curl http://localhost:8000/ports

# View rules for a specific port
curl http://localhost:8000/rules/Durban
```

## Verification Results

SUDESTADA @ Durban — all 6 tariff items within 1% of ground truth:

| Tariff Item | Calculated (ZAR) | Expected (ZAR) | Diff |
|---|---|---|---|
| Light Dues | 60,062.04 | 60,062.04 | 0.00% |
| Port Dues | 199,371.35 | 199,549.22 | 0.09% |
| Towage Dues | 147,074.38 | 147,074.38 | 0.00% |
| VTS Dues | 33,345.00 | 33,315.75 | 0.09% |
| Pilotage Dues | 47,189.94 | 47,189.94 | 0.00% |
| Running Lines | 19,639.50 | 19,639.50 | 0.00% |
| **Total** | **506,682.21** | **506,830.83** | **0.03%** |

4 out of 6 are exact matches. The remaining 2 are within 0.09%.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/ingest` | Upload a tariff PDF (write path) |
| `POST` | `/calculate` | Calculate dues for a vessel (read path) |
| `GET` | `/rules/{port}` | Inspect extracted rules for a port |
| `GET` | `/ports` | List all ports with extracted rules |
| `GET` | `/health` | Health check |

Interactive docs at `http://localhost:8000/docs` (Swagger UI).

## The Rule Model

The heart of the system is the discriminated union rate structure:

```
TariffRule
├── due_type, port, source (lineage)
├── conditions, exemptions, surcharges, reductions
└── rate_structure (discriminated union)
        ├── FlatRate         — ceil(GT/100) × rate
        ├── TieredRate       — bracket lookup + incremental
        ├── CompositeRate    — sum of components (e.g., flat + time-based)
        ├── PerServiceRate   — (base_fee + unit_rate × units) × operations
        └── TimeBasedRate    — rate × units × days
```

`CompositeRate` is the generalization insight: Port Dues = flat component + time-based component. This composition pattern handles arbitrarily complex rate structures.

## Project Structure

```
port-tariff-engine/
├── src/
│   ├── models/          # Pydantic data models (the contract)
│   │   ├── vessel.py    # VesselProfile
│   │   ├── rules.py     # TariffRule + discriminated unions
│   │   └── results.py   # CalculationResult + AuditTrail
│   ├── ingestion/       # === WRITE PATH ===
│   │   ├── pdf_parser.py        # pdfplumber extraction
│   │   ├── section_splitter.py  # Heading-based section detection
│   │   ├── rule_extractor.py    # Gemini-powered extraction
│   │   └── rule_store.py        # JSON materialized view
│   ├── engine/          # === READ PATH ===
│   │   ├── calculator.py        # Deterministic arithmetic
│   │   ├── condition_evaluator.py  # Exemptions/surcharges
│   │   ├── rule_matcher.py      # Rule lookup by port
│   │   └── audit.py             # Data lineage builder
│   ├── llm/
│   │   └── gemini_client.py     # Gemini API wrapper
│   └── main.py          # FastAPI application
├── tests/
│   ├── test_calculator.py       # Unit tests (9 tests)
│   └── test_integration.py     # Golden test (4 tests)
├── data/
│   ├── port_tariff.pdf          # Source document
│   └── extracted_rules/         # Materialized view output
├── scripts/
│   └── ingest.py                # CLI for write path
├── Dockerfile
└── docker-compose.yml
```

## Running with Docker

```bash
# Build and run
docker compose up --build

# Or just Docker
docker build -t tariff-engine .
docker run -p 8000:8000 -e GEMINI_API_KEY=your-key tariff-engine
```

## Running Tests

```bash
# Unit tests (no API key needed)
pytest tests/test_calculator.py -v

# Integration tests (requires extracted rules)
pytest tests/test_integration.py -v

# All tests
pytest tests/ -v
```

## Design Decisions

1. **CQRS over RAG**: Tariff calculation is fundamentally arithmetic, not Q&A. Extracting rules once and computing deterministically is more reliable than asking an LLM to calculate every time.

2. **Discriminated Unions**: Each rate structure type (`FlatRate`, `TieredRate`, `CompositeRate`, etc.) has its own Pydantic model with type-safe fields. New rate types can be added without breaking existing ones.

3. **`decimal.Decimal` for Money**: Floating-point arithmetic produces rounding errors that compound. `Decimal` gives exact financial precision.

4. **Focused Gemini Prompts**: Instead of one giant prompt, each tariff section gets a focused, type-specific prompt. This reduces output truncation and improves extraction accuracy.

5. **No Framework Lock-in**: Raw Gemini SDK + Pydantic, not LangChain. Every layer is transparent and debuggable.

## Tech Stack

| Component | Choice | Why |
|-----------|--------|-----|
| LLM | Gemini 2.0 Flash | Free tier, structured output |
| PDF Parsing | pdfplumber | Best table extraction |
| Data Models | Pydantic v2 | Type safety, serialization |
| API | FastAPI | Async, auto-docs, Pydantic-native |
| Math | decimal.Decimal | Financial precision |
| Testing | pytest | Standard, clear |
