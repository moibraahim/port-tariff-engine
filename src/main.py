"""
Port Tariff Intelligence Engine — FastAPI Application.

Exposes the CQRS architecture as a REST API:
- Write Path: POST /ingest (upload PDF, extract rules)
- Read Path: POST /calculate (deterministic calculation)
- Inspection: GET /rules/{port} (view extracted rules)
"""

import logging
import tempfile
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from .models.vessel import VesselProfile
from .models.results import CalculationResult
from .ingestion.pdf_parser import parse_pdf
from .ingestion.section_splitter import split_into_sections
from .ingestion.rule_extractor import extract_all_rules
from .ingestion.rule_store import RuleStore
from .llm.gemini_client import GeminiClient
from .engine.audit import calculate_port_dues

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Port Tariff Intelligence Engine",
    description=(
        "CQRS-based system for automated port tariff calculation. "
        "Ingests port tariff PDFs and calculates vessel dues deterministically."
    ),
    version="1.0.0",
)

# Global state
rule_store = RuleStore()


class CalculateRequest(BaseModel):
    """Request to calculate port dues for a vessel."""
    vessel: VesselProfile
    port: str
    due_types: list[str] | None = None


class IngestResponse(BaseModel):
    """Response from the ingestion endpoint."""
    status: str
    document: str
    rules_extracted: int
    ports_found: list[str]
    due_types_found: list[str]


class DecimalEncoder:
    """Custom JSON serialization for Decimal types."""
    @staticmethod
    def default(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    available_ports = rule_store.get_available_ports()
    rules = rule_store.load_rules()
    return {
        "status": "healthy",
        "rules_loaded": len(rules),
        "available_ports": available_ports,
    }


@app.post("/ingest", response_model=IngestResponse)
async def ingest_tariff(file: UploadFile = File(...)):
    """
    WRITE PATH: Upload a port tariff PDF and extract rules.

    This runs the full ingestion pipeline:
    1. Parse PDF (text + tables)
    2. Split into sections
    3. Extract rules via Gemini
    4. Persist as materialized view
    """
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files are accepted")

    logger.info("Ingesting tariff document: %s", file.filename)

    # Save uploaded file to temp location
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        # Stage 1: Parse PDF
        logger.info("Stage 1: Parsing PDF...")
        parsed_doc = parse_pdf(tmp_path)

        # Stage 2: Split into sections
        logger.info("Stage 2: Splitting into sections...")
        sections = split_into_sections(parsed_doc)

        # Stage 3: Extract rules
        logger.info("Stage 3: Extracting rules via Gemini...")
        client = GeminiClient()
        rules = extract_all_rules(client, sections, file.filename)

        if not rules:
            raise HTTPException(422, "No tariff rules could be extracted from the document")

        # Stage 4: Persist rules
        logger.info("Stage 4: Persisting rules...")
        source_hash = RuleStore.compute_file_hash(tmp_path)
        rule_store.save_rules(rules, file.filename, source_hash)

        ports = sorted(set(r.port for r in rules))
        due_types = sorted(set(r.due_type for r in rules))

        logger.info(
            "Ingestion complete: %d rules for ports %s",
            len(rules), ports,
        )

        return IngestResponse(
            status="success",
            document=file.filename,
            rules_extracted=len(rules),
            ports_found=ports,
            due_types_found=due_types,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Ingestion failed")
        raise HTTPException(500, f"Ingestion failed: {str(e)}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)


@app.post("/calculate")
async def calculate_dues(request: CalculateRequest):
    """
    READ PATH: Calculate port dues for a vessel.

    This is the fast, deterministic path — no LLM involved:
    1. Match applicable rules from the materialized view
    2. Evaluate conditions and exemptions
    3. Calculate amounts with pure arithmetic
    4. Return results with full audit trail
    """
    logger.info(
        "Calculating dues for %s at %s",
        request.vessel.vessel_metadata.name,
        request.port,
    )

    available_ports = rule_store.get_available_ports()
    if not available_ports:
        raise HTTPException(
            404,
            "No tariff rules loaded. Please ingest a tariff document first via POST /ingest",
        )

    # Check if port exists (case-insensitive)
    port_match = None
    for p in available_ports:
        if p.lower() == request.port.lower():
            port_match = p
            break

    if not port_match:
        raise HTTPException(
            404,
            f"No rules found for port '{request.port}'. "
            f"Available ports: {available_ports}",
        )

    result = calculate_port_dues(
        rule_store,
        request.vessel,
        port_match,
        request.due_types,
    )

    # Convert to JSON-safe format (Decimals -> floats for JSON)
    response_data = _serialize_result(result)
    return JSONResponse(content=response_data)


@app.get("/rules/{port}")
async def get_rules(port: str):
    """
    Inspect extracted rules for a port.

    Transparency endpoint — allows evaluators to see exactly
    what the AI extracted from the tariff document.
    """
    rules = rule_store.get_rules_by_port(port)

    if not rules:
        available_ports = rule_store.get_available_ports()
        raise HTTPException(
            404,
            f"No rules for port '{port}'. Available: {available_ports}",
        )

    return {
        "port": port,
        "rule_count": len(rules),
        "rules": [
            {
                "id": r.id,
                "due_type": r.due_type,
                "description": r.description,
                "rate_structure": r.rate_structure.model_dump(mode="json"),
                "conditions": [c.model_dump() for c in r.conditions],
                "exemptions": [e.model_dump() for e in r.exemptions],
                "surcharges": [s.model_dump(mode="json") for s in r.surcharges],
                "reductions": [rd.model_dump(mode="json") for rd in r.reductions],
                "source": r.source.model_dump(),
                "notes": r.notes,
            }
            for r in rules
        ],
    }


@app.get("/ports")
async def list_ports():
    """List all ports with extracted rules."""
    ports = rule_store.get_available_ports()
    rules = rule_store.load_rules()

    port_summary = {}
    for rule in rules:
        if rule.port not in port_summary:
            port_summary[rule.port] = []
        port_summary[rule.port].append(rule.due_type)

    return {
        "ports": [
            {"name": p, "due_types": sorted(set(port_summary.get(p, [])))}
            for p in ports
        ]
    }


def _serialize_result(result: CalculationResult) -> dict:
    """Convert CalculationResult to JSON-safe dict (Decimal -> float)."""
    return {
        "vessel_name": result.vessel_name,
        "port": result.port,
        "currency": result.currency,
        "total": float(result.total),
        "lines": [
            {
                "due_type": line.due_type,
                "description": line.description,
                "amount": float(line.amount),
                "currency": line.currency,
                "rule_id": line.rule_id,
                "source_section": line.source_section,
                "source_pages": line.source_pages,
                "audit_trail": [
                    {
                        "step": a.step,
                        "description": a.description,
                        "formula": a.formula,
                        "values": a.values,
                        "result": a.result,
                    }
                    for a in line.audit_trail
                ],
            }
            for line in result.lines
        ],
        "warnings": result.warnings,
        "metadata": result.metadata,
    }
