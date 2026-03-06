#!/usr/bin/env python3
"""
Benchmark different Gemini models for tariff extraction.

Compares processing time and accuracy across models:
- gemini-2.0-flash (current baseline)
- gemini-2.0-flash-lite
- gemini-2.5-flash
- gemini-2.5-flash-lite
- gemini-2.5-pro

Usage:
    python scripts/benchmark_models.py
"""

import sys
import time
import json
import logging
from decimal import Decimal
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
    level=logging.WARNING,  # Quiet — only show errors
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

# Models to benchmark (cheapest → most capable)
MODELS = [
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-3.1-flash-lite-preview",
    "gemini-3-flash-preview",
    "gemini-3.1-pro-preview",
]

# Ground truth
EXPECTED = {
    "light_dues": Decimal("60062.04"),
    "port_dues": Decimal("199549.22"),
    "towage_dues": Decimal("147074.38"),
    "vts_dues": Decimal("33315.75"),
    "pilotage_dues": Decimal("47189.94"),
    "running_lines": Decimal("19639.50"),
}

PDF_PATH = Path(__file__).parent.parent / "data" / "port_tariff.pdf"


def get_test_vessel() -> VesselProfile:
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
class BenchmarkResult:
    model: str
    ingestion_time: float = 0.0
    section_split_time: float = 0.0
    extraction_time: float = 0.0
    total_time: float = 0.0
    rules_extracted: int = 0
    ports_found: int = 0
    accuracy: dict = field(default_factory=dict)  # due_type -> diff%
    missing_types: list = field(default_factory=list)
    error: str = ""
    overall_accuracy: float = 0.0  # average diff%


def run_benchmark(model_name: str, parsed_doc, vessel: VesselProfile) -> BenchmarkResult:
    """Run full ingestion + calculation for a single model."""
    result = BenchmarkResult(model=model_name)

    try:
        # Create client with specific model
        client = GeminiClient()
        client.model_name = model_name

        # Override the model
        import google.generativeai as genai
        client.model = genai.GenerativeModel(model_name)

        # Stage 2: Section Splitting (with LLM fallback)
        t0 = time.time()
        sections = split_into_sections(parsed_doc, gemini_client=client)
        result.section_split_time = time.time() - t0

        # Stage 3: Rule Extraction
        t0 = time.time()
        rules = extract_all_rules(client, sections, PDF_PATH.name)
        result.extraction_time = time.time() - t0

        result.total_time = result.section_split_time + result.extraction_time
        result.rules_extracted = len(rules)
        result.ports_found = len(set(r.port for r in rules))

        if not rules:
            result.error = "No rules extracted"
            return result

        # Stage 4: Save rules temporarily and calculate
        store = RuleStore(store_dir=Path(f"/tmp/benchmark_{model_name.replace('.', '_').replace('-', '_')}"))
        source_hash = RuleStore.compute_file_hash(PDF_PATH)
        store.save_rules(rules, PDF_PATH.name, source_hash)

        # Calculate
        calc_result = calculate_port_dues(store, vessel, "Durban")

        calculated = {line.due_type: line.amount for line in calc_result.lines}

        for due_type, expected_val in EXPECTED.items():
            if due_type in calculated:
                diff_pct = float(abs(calculated[due_type] - expected_val) / expected_val * 100)
                result.accuracy[due_type] = diff_pct
            else:
                result.missing_types.append(due_type)

        if result.accuracy:
            result.overall_accuracy = sum(result.accuracy.values()) / len(result.accuracy)

    except Exception as e:
        result.error = str(e)

    return result


def print_results(results: list[BenchmarkResult]):
    """Print benchmark results in a formatted table."""
    print("\n" + "=" * 120)
    print("GEMINI MODEL BENCHMARK — Port Tariff Extraction")
    print("=" * 120)

    # Header
    print(f"\n{'Model':<28} {'Time':>8} {'Rules':>6} {'Ports':>6} {'Avg Err%':>9} ", end="")
    for dt in EXPECTED:
        short = dt.replace("_dues", "").replace("_", " ")[:8]
        print(f" {short:>8}", end="")
    print(f"  {'Status':<20}")
    print("-" * 120)

    for r in results:
        if r.error:
            print(f"{r.model:<28} {'—':>8} {'—':>6} {'—':>6} {'—':>9} ", end="")
            for _ in EXPECTED:
                print(f" {'—':>8}", end="")
            print(f"  {r.error[:20]:<20}")
            continue

        print(
            f"{r.model:<28} {r.total_time:>7.1f}s {r.rules_extracted:>6} {r.ports_found:>6} {r.overall_accuracy:>8.2f}%",
            end=" ",
        )
        for dt in EXPECTED:
            if dt in r.accuracy:
                pct = r.accuracy[dt]
                marker = "  " if pct <= 1.0 else " !"
                print(f" {pct:>6.2f}%{marker[0]}", end="")
            elif dt in r.missing_types:
                print(f" {'MISS':>7}", end="")
            else:
                print(f" {'—':>8}", end="")

        status = "PASS" if r.overall_accuracy <= 1.0 and not r.missing_types else "FAIL"
        if r.missing_types:
            status += f" (missing {len(r.missing_types)})"
        print(f"  {status:<20}")

    print("-" * 120)

    # Detailed breakdown
    print("\nDETAILED TIMING:")
    for r in results:
        if r.error:
            print(f"  {r.model:<28} ERROR: {r.error}")
        else:
            print(
                f"  {r.model:<28} split={r.section_split_time:.1f}s  extract={r.extraction_time:.1f}s  total={r.total_time:.1f}s"
            )

    # Ranking
    valid = [r for r in results if not r.error and not r.missing_types]
    if valid:
        print("\nRANKING (by accuracy, then speed):")
        ranked = sorted(valid, key=lambda r: (r.overall_accuracy, r.total_time))
        for i, r in enumerate(ranked, 1):
            within = "ALL PASS" if all(v <= 1.0 for v in r.accuracy.values()) else "SOME FAIL"
            print(f"  #{i}  {r.model:<28} avg_err={r.overall_accuracy:.3f}%  time={r.total_time:.1f}s  [{within}]")

    print("\n" + "=" * 120)


def main():
    print("Parsing PDF (one-time cost, shared across all models)...")
    parsed_doc = parse_pdf(PDF_PATH)
    print(f"  {parsed_doc.total_pages} pages, {sum(len(p.tables) for p in parsed_doc.pages)} tables\n")

    vessel = get_test_vessel()
    results = []

    for i, model in enumerate(MODELS, 1):
        print(f"[{i}/{len(MODELS)}] Benchmarking {model}...", end=" ", flush=True)
        t0 = time.time()
        result = run_benchmark(model, parsed_doc, vessel)
        elapsed = time.time() - t0
        if result.error:
            print(f"ERROR ({elapsed:.1f}s): {result.error[:60]}")
        else:
            print(f"done ({elapsed:.1f}s) — {result.rules_extracted} rules, avg_err={result.overall_accuracy:.2f}%")
        results.append(result)

    print_results(results)

    # Save raw results
    out_path = Path(__file__).parent.parent / "data" / "benchmark_results.json"
    raw = []
    for r in results:
        raw.append({
            "model": r.model,
            "total_time": round(r.total_time, 2),
            "section_split_time": round(r.section_split_time, 2),
            "extraction_time": round(r.extraction_time, 2),
            "rules_extracted": r.rules_extracted,
            "ports_found": r.ports_found,
            "accuracy": {k: round(v, 4) for k, v in r.accuracy.items()},
            "overall_accuracy": round(r.overall_accuracy, 4),
            "missing_types": r.missing_types,
            "error": r.error,
        })
    out_path.write_text(json.dumps(raw, indent=2))
    print(f"\nRaw results saved to {out_path}")


if __name__ == "__main__":
    main()
