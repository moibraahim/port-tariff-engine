#!/usr/bin/env python3
"""
CLI script to run the Write Path (ingestion pipeline).

Usage:
    python -m scripts.ingest data/port_tariff.pdf

This runs the full pipeline:
1. Parse PDF → structured text + tables
2. Split into sections by TOC
3. Extract rules via Gemini (LLM)
4. Persist extracted rules (JSON)
"""

import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.pdf_parser import parse_pdf
from src.ingestion.section_splitter import split_into_sections
from src.ingestion.rule_extractor import extract_all_rules
from src.ingestion.rule_store import RuleStore
from src.llm.gemini_client import GeminiClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main(pdf_path: str):
    pdf = Path(pdf_path)
    if not pdf.exists():
        logger.error("PDF not found: %s", pdf)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("PORT TARIFF INGESTION PIPELINE")
    logger.info("=" * 60)
    logger.info("Source: %s", pdf.name)

    # Stage 1: Parse PDF
    logger.info("\n--- Stage 1: PDF Parsing ---")
    parsed_doc = parse_pdf(pdf)
    logger.info("Pages: %d", parsed_doc.total_pages)
    logger.info("Tables found: %d", sum(len(p.tables) for p in parsed_doc.pages))

    # Stage 2: Section Splitting
    logger.info("\n--- Stage 2: Section Splitting ---")
    sections = split_into_sections(parsed_doc)
    for sec in sections:
        logger.info(
            "  [%s] %s (pages %d-%d, %d tables)",
            sec.section_number, sec.title,
            sec.start_page, sec.end_page,
            len(sec.tables),
        )

    # Stage 3: Rule Extraction
    logger.info("\n--- Stage 3: Rule Extraction (Gemini) ---")
    client = GeminiClient()
    rules = extract_all_rules(client, sections, pdf.name)

    if not rules:
        logger.error("No rules extracted! Check Gemini API key and PDF content.")
        sys.exit(1)

    # Stage 4: Persistence
    logger.info("\n--- Stage 4: Persisting Rules ---")
    store = RuleStore()
    source_hash = RuleStore.compute_file_hash(pdf)
    output_path = store.save_rules(rules, pdf.name, source_hash)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("INGESTION COMPLETE")
    logger.info("=" * 60)
    logger.info("Rules extracted: %d", len(rules))
    logger.info("Output: %s", output_path)

    ports = sorted(set(r.port for r in rules))
    logger.info("Ports: %s", ports)

    for port in ports:
        port_rules = [r for r in rules if r.port == port]
        due_types = sorted(set(r.due_type for r in port_rules))
        logger.info("  %s: %s", port, due_types)

    logger.info("\nExtracted rules are ready for querying.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.ingest <path-to-pdf>")
        print("Example: python -m scripts.ingest data/port_tariff.pdf")
        sys.exit(1)

    main(sys.argv[1])
