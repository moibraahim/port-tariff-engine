#!/usr/bin/env python3
"""
Generalizability Test — Run real-world port tariff PDFs through the system.

Downloads and processes tariff PDFs from different countries/regions to
verify the system handles diverse tariff formats without code changes.

Usage:
    python scripts/test_generalizability.py
"""

import sys
import time
import json
import logging
import traceback
from pathlib import Path
from dataclasses import dataclass, field

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.pdf_parser import parse_pdf
from src.ingestion.section_splitter import split_into_sections
from src.ingestion.rule_extractor import extract_all_rules
from src.ingestion.rule_store import RuleStore
from src.llm.gemini_client import GeminiClient
from src.models.vessel import VesselProfile, VesselMetadata, TechnicalSpecs, OperationalData
from src.engine.audit import calculate_port_dues

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

TEST_PDF_DIR = Path(__file__).parent.parent / "data" / "test_pdfs"


def get_test_vessel() -> VesselProfile:
    """Standard test vessel — same as benchmark."""
    return VesselProfile(
        vessel_metadata=VesselMetadata(
            name="SUDESTADA",
            built_year=2010,
            flag="MLT - Malta",
            classification_society="Registro Italiano Navale",
        ),
        technical_specs=TechnicalSpecs(
            type="Bulk Carrier",
            dwt=93274,
            gross_tonnage=51300,
            net_tonnage=31192,
            loa_meters=229.2,
            beam_meters=38.0,
            moulded_depth_meters=20.7,
            lbp_meters=222.0,
            draft_sw_s_w_t=[14.9, 0.0, 0.0],
            suez_nt=49069,
        ),
        operational_data=OperationalData(
            cargo_quantity_mt=40000,
            days_alongside=3.39,
            arrival_time="2024-11-15T10:12:00",
            departure_time="2024-11-22T13:00:00",
            activity="Exporting Iron Ore",
            num_operations=2,
            num_holds=7,
        ),
    )


@dataclass
class GeneralizabilityResult:
    pdf_name: str
    country: str
    parse_time: float = 0.0
    split_time: float = 0.0
    extract_time: float = 0.0
    total_time: float = 0.0
    pages: int = 0
    tables: int = 0
    sections_found: int = 0
    rules_extracted: int = 0
    ports_found: list = field(default_factory=list)
    due_types_found: dict = field(default_factory=dict)  # port -> [due_types]
    calculation_results: dict = field(default_factory=dict)  # port -> {due_type: amount}
    calculation_warnings: dict = field(default_factory=dict)  # port -> [warnings]
    error: str = ""
    stage_failed: str = ""


def run_test(pdf_path: Path, country: str, client: GeminiClient) -> GeneralizabilityResult:
    """Run full pipeline on a single PDF."""
    result = GeneralizabilityResult(pdf_name=pdf_path.name, country=country)

    try:
        # Stage 1: Parse PDF
        t0 = time.time()
        parsed_doc = parse_pdf(pdf_path)
        result.parse_time = time.time() - t0
        result.pages = parsed_doc.total_pages
        result.tables = sum(len(p.tables) for p in parsed_doc.pages)
        print(f"    Parse: {result.parse_time:.1f}s — {result.pages} pages, {result.tables} tables")

    except Exception as e:
        result.error = str(e)
        result.stage_failed = "parse"
        print(f"    FAILED at parse: {e}")
        return result

    try:
        # Stage 2: Section Splitting
        t0 = time.time()
        sections = split_into_sections(parsed_doc, gemini_client=client)
        result.split_time = time.time() - t0
        result.sections_found = len(sections)
        print(f"    Split: {result.split_time:.1f}s — {result.sections_found} sections")

        for sec in sections:
            print(f"      [{sec.section_number}] {sec.title} (p{sec.start_page}-{sec.end_page})")

    except Exception as e:
        result.error = str(e)
        result.stage_failed = "split"
        print(f"    FAILED at split: {e}")
        return result

    try:
        # Stage 3: Rule Extraction
        t0 = time.time()
        rules = extract_all_rules(client, sections, pdf_path.name)
        result.extract_time = time.time() - t0
        result.rules_extracted = len(rules)
        result.ports_found = sorted(set(r.port for r in rules))

        for port in result.ports_found:
            port_rules = [r for r in rules if r.port == port]
            due_types = sorted(set(r.due_type for r in port_rules))
            result.due_types_found[port] = due_types

        print(f"    Extract: {result.extract_time:.1f}s — {result.rules_extracted} rules")
        for port, types in result.due_types_found.items():
            print(f"      {port}: {types}")

    except Exception as e:
        result.error = str(e)
        result.stage_failed = "extract"
        print(f"    FAILED at extract: {e}")
        traceback.print_exc()
        return result

    try:
        # Stage 4: Calculation (test with each discovered port)
        if rules:
            store = RuleStore(
                store_dir=Path(f"/tmp/generalizability_{pdf_path.stem}")
            )
            source_hash = RuleStore.compute_file_hash(pdf_path)
            store.save_rules(rules, pdf_path.name, source_hash)

            vessel = get_test_vessel()
            for port in result.ports_found:
                try:
                    calc = calculate_port_dues(store, vessel, port)
                    amounts = {
                        line.due_type: str(line.amount)
                        for line in calc.lines
                    }
                    result.calculation_results[port] = amounts
                    result.calculation_warnings[port] = calc.warnings

                    total = sum(line.amount for line in calc.lines)
                    print(f"    Calc [{port}]: {len(amounts)} dues, total={total}")
                    for dt, amt in sorted(amounts.items()):
                        print(f"      {dt}: {amt}")
                    if calc.warnings:
                        for w in calc.warnings:
                            print(f"      WARN: {w}")
                except Exception as e:
                    result.calculation_warnings[port] = [f"Calc error: {e}"]
                    print(f"    Calc [{port}] ERROR: {e}")

    except Exception as e:
        result.error = str(e)
        result.stage_failed = "calculate"
        print(f"    FAILED at calculate: {e}")
        return result

    result.total_time = result.parse_time + result.split_time + result.extract_time
    return result


def print_summary(results: list[GeneralizabilityResult]):
    """Print a summary table of all results."""
    print("\n" + "=" * 100)
    print("GENERALIZABILITY TEST RESULTS")
    print("=" * 100)

    print(f"\n{'PDF':<35} {'Country':<12} {'Pages':>5} {'Sects':>5} {'Rules':>5} {'Ports':>5} {'Time':>8} {'Status':<15}")
    print("-" * 100)

    passed = 0
    for r in results:
        status = "PASS" if r.rules_extracted > 0 and not r.stage_failed else f"FAIL ({r.stage_failed})"
        if r.rules_extracted > 0:
            passed += 1

        print(
            f"{r.pdf_name:<35} {r.country:<12} {r.pages:>5} "
            f"{r.sections_found:>5} {r.rules_extracted:>5} "
            f"{len(r.ports_found):>5} {r.total_time:>7.1f}s {status:<15}"
        )

    print("-" * 100)
    print(f"\nResults: {passed}/{len(results)} PDFs successfully processed")

    # Detailed breakdown per PDF
    for r in results:
        print(f"\n--- {r.pdf_name} ({r.country}) ---")
        if r.stage_failed:
            print(f"  FAILED at stage: {r.stage_failed}")
            print(f"  Error: {r.error}")
            continue

        print(f"  Pipeline: parse={r.parse_time:.1f}s split={r.split_time:.1f}s extract={r.extract_time:.1f}s")
        print(f"  {r.pages} pages, {r.tables} tables, {r.sections_found} sections, {r.rules_extracted} rules")
        print(f"  Ports: {r.ports_found}")

        for port, types in r.due_types_found.items():
            print(f"  {port} due types: {types}")
            if port in r.calculation_results:
                for dt, amt in sorted(r.calculation_results[port].items()):
                    print(f"    {dt}: {amt}")
            if port in r.calculation_warnings and r.calculation_warnings[port]:
                for w in r.calculation_warnings[port]:
                    print(f"    WARN: {w}")

    print("\n" + "=" * 100)


# Map PDF filenames to countries
PDF_COUNTRY_MAP = {
    "namport_namibia_2024.pdf": "Namibia",
    "fujairah_uae_2024.pdf": "UAE",
    "kenya_kpa_2024.pdf": "Kenya",
    "dpworld_uae_2023.pdf": "UAE",
    "tanzania_tpa_2024.pdf": "Tanzania",
}


def main():
    print("=" * 80)
    print("GENERALIZABILITY TEST — Real-World Port Tariff PDFs")
    print("=" * 80)

    pdfs = sorted(TEST_PDF_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {TEST_PDF_DIR}")
        sys.exit(1)

    print(f"\nFound {len(pdfs)} test PDFs:")
    for p in pdfs:
        size_mb = p.stat().st_size / 1024 / 1024
        print(f"  {p.name} ({size_mb:.1f} MB)")

    client = GeminiClient()
    results = []

    for i, pdf in enumerate(pdfs, 1):
        country = PDF_COUNTRY_MAP.get(pdf.name, "Unknown")
        print(f"\n[{i}/{len(pdfs)}] Processing {pdf.name} ({country})...")
        print("-" * 60)
        result = run_test(pdf, country, client)
        results.append(result)
        print()

    print_summary(results)

    # Save raw results
    out_path = Path(__file__).parent.parent / "data" / "generalizability_results.json"
    raw = []
    for r in results:
        raw.append({
            "pdf": r.pdf_name,
            "country": r.country,
            "pages": r.pages,
            "tables": r.tables,
            "sections_found": r.sections_found,
            "rules_extracted": r.rules_extracted,
            "ports_found": r.ports_found,
            "due_types_found": r.due_types_found,
            "calculation_results": r.calculation_results,
            "calculation_warnings": r.calculation_warnings,
            "parse_time": round(r.parse_time, 2),
            "split_time": round(r.split_time, 2),
            "extract_time": round(r.extract_time, 2),
            "total_time": round(r.total_time, 2),
            "error": r.error,
            "stage_failed": r.stage_failed,
        })
    out_path.write_text(json.dumps(raw, indent=2))
    print(f"\nRaw results saved to {out_path}")


if __name__ == "__main__":
    main()
