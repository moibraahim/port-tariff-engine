# Port Tariff Intelligence Engine

> Feed it any port tariff PDF. Get back structured, auditable, deterministic vessel dues — instantly.

```
         PDF in                                           JSON out
  ┌──────────────┐                                 ┌──────────────────┐
  │ Port Tariff  │    WRITE once     READ forever   │  Calculated Dues │
  │  (any port)  │ ──────────────▶ ──────────────▶  │  + Audit Trail   │
  └──────────────┘   ~30 seconds    ~0.1 seconds    └──────────────────┘
       Gemini          extracts        pure            deterministic
       parses          rules           arithmetic      reproducible
```

**Live API** — [port-tariff-engine-production.up.railway.app](https://port-tariff-engine-production.up.railway.app/docs)

---

## Table of Contents

- [The Problem](#the-problem)
- [Architecture](#architecture)
- [Try It Now — Live API](#try-it-now--live-api)
- [Accuracy](#accuracy)
- [Generalizability — Tested on 5 Real-World Tariffs](#generalizability--tested-on-5-real-world-tariffs)
- [Gemini Model Benchmarks](#gemini-model-benchmarks)
- [Rate Structure Model](#rate-structure-model)
- [Project Structure](#project-structure)
- [Design Decisions](#design-decisions)
- [Local Setup](#local-setup)
- [API Reference](#api-reference)
- [Tech Stack](#tech-stack)

---

## The Problem

Port tariff documents are dense, multi-page PDFs filled with rate tables, tiered brackets, conditional surcharges, and per-operation multipliers. Today, DA-Desk analysts read these manually. Every. Single. Time.

This engine automates the entire pipeline: ingest a tariff PDF once, then calculate vessel dues instantly — with full traceability from every ZAR amount back to the exact page, section, and formula that produced it.

---

## Architecture

The core architectural insight — borrowed from *Designing Data-Intensive Applications* — is **CQRS (Command Query Responsibility Segregation)**. The tariff PDF is the source of truth. Everything else is derived.

```
══════════════════════════════════════════════════════════════════════════════════
  WRITE PATH  (slow, LLM-powered, runs once per tariff document)
══════════════════════════════════════════════════════════════════════════════════

  Port Tariff PDF
       │
       ▼
  ┌─────────────────────────┐
  │  1. PDF Parser           │  pdfplumber — extracts text + tables per page
  │     pdf_parser.py        │  preserves table structure as list-of-dicts
  └───────────┬─────────────┘
              ▼
  ┌─────────────────────────┐
  │  2. Section Splitter     │  regex pattern matching + LLM fallback
  │     section_splitter.py  │  finds tariff sections by keyword, not hardcoded IDs
  └───────────┬─────────────┘
              ▼
  ┌─────────────────────────┐
  │  3. Rule Extractor       │  Gemini (temp=0) extracts typed TariffRule objects
  │     rule_extractor.py    │  per-section prompts — focused, no truncation
  └───────────┬─────────────┘
              ▼
  ┌─────────────────────────┐
  │  4. Rule Store           │  JSON persistence — the "materialized view"
  │     rule_store.py        │  indexed by (port, due_type) for fast lookup
  └─────────────────────────┘

══════════════════════════════════════════════════════════════════════════════════
  READ PATH  (fast, deterministic, no LLM — pure arithmetic)
══════════════════════════════════════════════════════════════════════════════════

  Vessel Query (JSON)
       │
       ▼
  ┌─────────────────────────┐
  │  1. Rule Matcher         │  finds applicable rules for port + vessel type
  │     rule_matcher.py      │  dynamic due type discovery from store
  └───────────┬─────────────┘
              ▼
  ┌─────────────────────────┐
  │  2. Condition Evaluator  │  exemptions, reductions, surcharges
  │     condition_evaluator  │  evaluated against vessel profile
  └───────────┬─────────────┘
              ▼
  ┌─────────────────────────┐
  │  3. Calculator           │  decimal.Decimal arithmetic — ceil, tiers, brackets
  │     calculator.py        │  NO LLM. reproducible. unit-testable.
  └───────────┬─────────────┘
              ▼
  ┌─────────────────────────┐
  │  4. Audit Builder        │  data lineage: page → section → rule → formula → result
  │     audit.py             │  every output fully traceable
  └───────────┬─────────────┘
              ▼
  Result + Full Audit Trail
```

### Why This Wins Over RAG-at-Query-Time

| | Naive RAG | This Engine (CQRS) |
|---|---|---|
| LLM in every query? | Yes | **No** — only during ingestion |
| Same query = same answer? | No (LLM variance) | **Yes** — deterministic |
| Speed per query | Slow (~5s LLM call) | **Fast** (~100ms pure compute) |
| Unit-testable? | Hard | **Yes** — every calculator function |
| Auditable? | LLM black box | **Full formula trace** |
| New tariff document? | Re-prompt everything | **Re-run write path only** |

### DDIA Concepts Applied

| Concept | Application |
|---|---|
| **Source of Truth** (Ch 11) | The PDF is canonical. Everything else is derived and re-derivable. |
| **Materialized View** (Ch 11) | Extracted rules = pre-computed view of the tariff, queryable without the original. |
| **CQRS** (Ch 11) | Write path (LLM extraction) fully separated from read path (calculation). |
| **Schema Evolution** (Ch 4) | Discriminated unions — new rate types slot in without breaking existing ones. |
| **Idempotency** (Ch 11) | Same PDF + same vessel = same result. Always. Gemini temp=0. |
| **Batch Pipeline** (Ch 10) | Ingestion is a pipeline of transforms with clear contracts between stages. |
| **Data Lineage** (Ch 12) | Every output traces to: page → section → extracted text → rule → formula → result. |

---

## Try It Now — Live API

The engine is deployed on Railway with the Transnet Port Tariff pre-ingested. Try these `curl` commands directly:

### 1. Check what's loaded

```bash
curl https://port-tariff-engine-production.up.railway.app/health
```

### 2. List available ports

```bash
curl https://port-tariff-engine-production.up.railway.app/ports
```

### 3. Calculate dues for a vessel

```bash
curl -X POST https://port-tariff-engine-production.up.railway.app/calculate \
  -H "Content-Type: application/json" \
  -d '{
    "port": "Durban",
    "vessel": {
      "vessel_metadata": {
        "name": "SUDESTADA",
        "built_year": 2010,
        "flag": "MLT - Malta",
        "classification_society": "Registro Italiano Navale"
      },
      "technical_specs": {
        "type": "Bulk Carrier",
        "dwt": 93274,
        "gross_tonnage": 51300,
        "net_tonnage": 31192,
        "loa_meters": 229.2,
        "beam_meters": 38.0,
        "moulded_depth_meters": 20.7,
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

### 4. Inspect extracted rules (transparency)

```bash
curl https://port-tariff-engine-production.up.railway.app/rules/Durban
```

### 5. Ingest a new tariff PDF

```bash
curl -X POST https://port-tariff-engine-production.up.railway.app/ingest \
  -F "file=@your_tariff.pdf"
```

Interactive Swagger docs: [port-tariff-engine-production.up.railway.app/docs](https://port-tariff-engine-production.up.railway.app/docs)

---

## Accuracy

Verified against the SUDESTADA test case at the Port of Durban — ground truth provided by Marcura:

| Tariff Item | Expected (ZAR) | Calculated (ZAR) | Diff | Status |
|---|---|---|---|---|
| Light Dues | 60,062.04 | 60,062.04 | **0.00%** | Exact |
| Port Dues | 199,549.22 | 199,371.35 | **0.09%** | Pass |
| Towage Dues | 147,074.38 | 147,074.38 | **0.00%** | Exact |
| VTS Dues | 33,315.75 | 33,345.00 | **0.09%** | Pass |
| Pilotage Dues | 47,189.94 | 47,189.94 | **0.00%** | Exact |
| Running Lines | 19,639.50 | 19,639.50 | **0.00%** | Exact |
| **Total** | **506,830.83** | **506,682.21** | **0.03%** | **Pass** |

4 out of 6 are exact matches. All within 0.1% tolerance. 13/13 unit + integration tests pass.

Every calculated amount includes a full audit trail tracing back to the source page, section, rule, and arithmetic formula — so you can verify *why* each number is what it is.

---

## Generalizability — Tested on 5 Real-World Tariffs

The system was tested against **real port tariff PDFs downloaded from the internet** — no code changes between runs. These are PDFs the system has never seen before, from different countries, port authorities, formats, and currencies.

| PDF Source | Country | Pages | Tables | Sections Found | Rules Extracted | Ports Discovered | Time | Status |
|---|---|---|---|---|---|---|---|---|
| Transnet (Durban) | South Africa | 80 | 48 | 6 | 40 | 12 | 30.4s | **PASS** |
| DP World Jebel Ali | UAE | 64 | 90 | 3 | 2 | 2 | 8.0s | **PASS** |
| Port of Fujairah | UAE | 24 | 8 | 4 | 13 | 2 | 9.3s | **PASS** |
| Kenya Ports Authority | Kenya | 40 | 1 | 6 | 4 | 1 | 10.3s | **PASS** |
| Namport | Namibia | 72 | 99 | 5 | 5 | 4 | 12.4s | **PASS** |
| Tanzania Ports Authority | Tanzania | 106 | 89 | 5 | 8 | 1 | 16.9s | **PASS** |

**6/6 PDFs processed end-to-end.** Zero code changes between tariffs.

### What "generalizability" means concretely

The system handles:
- **Different section numbering** — Transnet uses "1.1.1", Kenya uses "Part IV", Tanzania uses chapter numbers
- **Different rate structures** — flat rates, tiered brackets, per-operation fees, time-based charges
- **Different currencies** — ZAR, AED, KES, NAD, USD, TZS
- **Different terminology** — "port dues" vs "harbour dues" vs "wharfage", "mooring" vs "running lines" vs "berthing"
- **Different table formats** — structured tables, inline text rates, nested brackets
- **Multi-port tariffs** — Namport PDF covers Walvis Bay + Luderitz, automatically discovered

### Sample: Tanzania calculation output

```
Tanzania Ports Authority — Vessel SUDESTADA:
  Light Dues:    TZS   3,078.00
  Pilotage:      TZS   1,196.20
  Towage:        TZS   5,540.40
  Wharfage:      TZS   5,130.00
  ─────────────────────────────
  Total:         TZS  14,944.60
```

---

## Gemini Model Benchmarks

Benchmarked 8 Gemini models on the same Transnet tariff PDF to find the optimal speed/accuracy trade-off:

| Model | Time | Rules | Ports | Avg Error | Light | Port | Towage | VTS | Pilotage | Lines | Status |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **gemini-3.1-flash-lite** | **30s** | **40** | **12** | **0.03%** | 0.00% | 0.09% | 0.00% | 0.09% | 0.00% | 0.00% | **BEST** |
| gemini-2.5-flash-lite | 45s | 87 | 15 | 0.03% | 0.00% | 0.09% | 0.00% | 0.09% | 0.00% | 0.00% | Pass |
| gemini-2.0-flash | 86s | 78 | 12 | 0.03% | 0.00% | 0.09% | 0.00% | 0.09% | 0.00% | 0.00% | Pass |
| gemini-2.5-flash | 216s | 27 | 13 | 0.04% | MISS | 0.09% | 0.00% | 0.09% | 0.00% | 0.00% | Fail |
| gemini-2.5-pro | 205s | 19 | 9 | 0.04% | 0.00% | 0.09% | MISS | 0.09% | 0.00% | 0.00% | Fail |
| gemini-3-flash | 440s | 10 | 10 | 0.03% | 0.00% | MISS | MISS | 0.09% | 0.00% | MISS | Fail |
| gemini-3.1-pro | 468s | 15 | 10 | 0.04% | 0.00% | 0.09% | MISS | 0.09% | 0.00% | MISS | Fail |
| gemini-2.0-flash-lite | 4s | 0 | 0 | — | — | — | — | — | — | — | Fail |

### Key Finding

**Bigger "thinking" models performed worse.** They spent tokens on chain-of-thought reasoning, hit output limits, and truncated the actual extraction. The sweet spot is `gemini-3.1-flash-lite-preview` — 3x faster than gemini-2.0-flash, same accuracy, all 6 due types extracted.

---

## Rate Structure Model

The heart of the system is a **discriminated union** of rate structures — type-safe, evolvable, handles any tariff pattern through composition:

```
TariffRule
├── due_type: str              "light_dues", "towage_dues", etc.
├── port: str                  "Durban", "Walvis Bay", etc.
├── source: RuleSource         page numbers, section title, text excerpt
├── conditions: [Condition]    vessel type, tonnage range, etc.
├── exemptions: [Exemption]    conditions that zero out the charge
├── surcharges: [Surcharge]    percentage add-ons (25%, 50%)
├── reductions: [Reduction]    percentage discounts (35%, 60%)
└── rate_structure: (one of)
        │
        ├── FlatRate            ceil(GT/100) x 117.08
        │                       → Light Dues, VTS Dues
        │
        ├── TieredRate          bracket lookup by GT, base + incremental
        │                       → Towage Dues
        │
        ├── CompositeRate       sum of multiple components
        │                       → Port Dues = initial fee + daily fee
        │
        ├── PerServiceRate      (base_fee + unit_rate x units) x operations
        │                       → Pilotage, Running Lines
        │
        └── TimeBasedRate       rate x units x days_alongside
                                → Port Dues daily component
```

New rate types slot in by adding a variant to the union — no changes to existing calculator logic.

---

## Project Structure

```
port-tariff-engine/
│
├── src/
│   ├── main.py                     FastAPI application (295 lines)
│   │
│   ├── models/
│   │   ├── vessel.py               VesselProfile — metadata, specs, operations
│   │   ├── rules.py                TariffRule + discriminated union types
│   │   └── results.py              CalculationResult + AuditEntry
│   │
│   ├── ingestion/                  ═══ WRITE PATH ═══
│   │   ├── pdf_parser.py           pdfplumber — text + table extraction
│   │   ├── section_splitter.py     keyword regex + LLM fallback discovery
│   │   ├── rule_extractor.py       Gemini-powered structured extraction
│   │   └── rule_store.py           JSON persistence (materialized view)
│   │
│   ├── engine/                     ═══ READ PATH ═══
│   │   ├── rule_matcher.py         find applicable rules by port + type
│   │   ├── condition_evaluator.py  exemptions, reductions, surcharges
│   │   ├── calculator.py           deterministic Decimal arithmetic
│   │   └── audit.py                full data lineage builder
│   │
│   └── llm/
│       └── gemini_client.py        Gemini API wrapper (temp=0, JSON mode)
│
├── tests/
│   ├── test_calculator.py          unit tests for each rate structure
│   └── test_integration.py         golden test: SUDESTADA @ Durban
│
├── scripts/
│   ├── ingest.py                   CLI ingestion pipeline
│   ├── benchmark_models.py         Gemini model comparison
│   └── test_generalizability.py    real-world PDF stress test
│
└── data/
    ├── port_tariff.pdf             source tariff document
    ├── extracted_rules/            materialized view (generated)
    ├── benchmark_results.json      model comparison data
    └── generalizability_results.json  cross-tariff test results
```

~2,700 lines of source code. No framework magic.

---

## Design Decisions

### 1. No LLM at Query Time

Tariff math is arithmetic, not language. Rates don't change between queries — so why call an LLM every time? Extract once, compute forever. Same input = same output. Always.

### 2. `decimal.Decimal` Everywhere

Floating point has no place in financial calculations. `0.1 + 0.2 != 0.3` in IEEE 754. Every rate, every intermediate value, every result uses `decimal.Decimal`.

### 3. Per-Section Gemini Prompts

Instead of dumping the entire 80-page PDF into one prompt (truncation, confusion), each tariff section gets a focused, type-specific prompt. Light dues get a prompt tuned for flat rates. Towage gets one tuned for tiered brackets. Better extraction, no truncation.

### 4. Composition Over Special-Casing

Port Dues isn't one rate — it's two rates composed together (initial per-tonnage + daily per-tonnage). `CompositeRate` handles this by summing child components. This pattern generalizes to arbitrarily complex rate structures without adding more code paths.

### 5. No Framework Lock-in

Raw Gemini SDK + Pydantic. No LangChain, no LlamaIndex, no opaque abstractions. Every line of code is visible, debuggable, and replaceable.

### 6. Flexible Section Discovery

Instead of hardcoding section numbers (which vary between tariff documents), the splitter uses keyword-based regex patterns that match regardless of numbering scheme, with an LLM fallback for documents that defy convention.

### 7. Dynamic Due Type Discovery

The system doesn't hardcode which due types to expect. It discovers what's in the rule store and calculates whatever was extracted — wharfage, harbour dues, anchorage, berth hire — anything the tariff contains.

---

## Local Setup

```bash
# Clone
git clone https://github.com/moibraahim/port-tariff-engine.git
cd port-tariff-engine

# Environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# API key
echo "GEMINI_API_KEY=your-key-here" > .env

# Ingest the tariff PDF (write path — runs once)
python -m scripts.ingest data/port_tariff.pdf

# Start the API (read path — query as many times as you want)
uvicorn src.main:app --reload

# Run tests
pytest tests/ -v
```

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/ingest` | Upload a tariff PDF, run the write path, extract rules |
| `POST` | `/calculate` | Calculate vessel dues (deterministic read path) |
| `GET` | `/rules/{port}` | Inspect extracted rules — full transparency |
| `GET` | `/ports` | List all ports with available rules |
| `GET` | `/health` | Health check + loaded rule count |

Swagger UI: `/docs`

---

## Tech Stack

| Component | Choice | Why |
|---|---|---|
| Language | Python 3.11+ | Industry standard for AI/ML pipelines |
| LLM | Gemini 3.1 Flash Lite | Best speed/accuracy ratio (benchmarked 8 models) |
| PDF Parsing | pdfplumber | Best table extraction for Python — preserves structure |
| Data Models | Pydantic v2 | Type safety, validation, serialization |
| API | FastAPI | Async, auto-docs, native Pydantic integration |
| Math | decimal.Decimal | Financial-grade precision — no float errors |
| Testing | pytest | 13 tests — unit + golden integration |
| Deployment | Railway | Live at production URL |

---

<p align="center">
  Built for the <strong>Marcura DA-Desk</strong> technical assessment.<br>
  <em>Extract once. Compute forever. Audit everything.</em>
</p>
