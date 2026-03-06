"""
Rule Extractor — Stage 3 of the Write Path.

Uses Gemini to transform document sections into structured TariffRule objects.
This is the LLM-heavy stage — runs once per tariff document during ingestion.

Key design: temperature=0 for reproducible extraction.
Each section is processed independently with a focused prompt.
"""

import json
import logging
import time
from decimal import Decimal

from ..llm.gemini_client import GeminiClient
from ..models.rules import (
    TariffRule, FlatRate, TieredRate, CompositeRate,
    PerServiceRate, TimeBasedRate, Tier, Condition,
    Exemption, Surcharge, Reduction, RuleSource,
)
from .section_splitter import DocumentSection

logger = logging.getLogger(__name__)

# Maps tariff section titles to due_type identifiers
DUE_TYPE_KEYWORDS = {
    "light": "light_dues",
    "port due": "port_dues",
    "port fee": "port_dues",
    "tug": "towage_dues",
    "towage": "towage_dues",
    "vessel assist": "towage_dues",
    "vts": "vts_dues",
    "vessel traffic": "vts_dues",
    "pilotage": "pilotage_dues",
    "running": "running_lines",
    "berthing": "running_lines",
    "mooring": "running_lines",
}


# Focused extraction prompts per due type — much shorter, more precise
LIGHT_DUES_PROMPT = """Extract the Light Dues tariff rule from this section.

The rate is "per 100 tons or part thereof" based on gross tonnage. Find the exact rate amount.

Return a JSON object (NOT an array):
{
  "due_type": "light_dues",
  "port": "All Ports",
  "description": "Light dues on vessels",
  "rate": "<exact rate per 100 GT, e.g. '117.08'>",
  "per_unit": "100",
  "base_field": "gross_tonnage",
  "effective_date": "2024-04-01",
  "source_pages": [<page numbers>],
  "notes": ["<any important notes>"]
}

IMPORTANT: Extract the rate for "All other vessels" (not self-propelled at registered port). The rate should be per 100 tons or part thereof of gross tonnage."""

VTS_PROMPT = """Extract the VTS (Vessel Traffic Services) charges from this section.

VTS charges are per GT (gross tonnage) per port call. Different ports may have different rates.

Return a JSON array of objects, one per rate variant:
[{
  "due_type": "vts_dues",
  "port": "<port name or 'All Ports'>",
  "description": "VTS charges on vessels",
  "rate": "<rate per GT>",
  "per_unit": "1",
  "base_field": "gross_tonnage",
  "minimum_fee": "<minimum fee if specified>",
  "effective_date": "2024-04-01",
  "source_pages": [<page numbers>]
}]

Note: Look for different rates for Durban/Saldanha vs other ports."""

PILOTAGE_PROMPT = """Extract the Pilotage tariff rules from this section.

Pilotage has a basic fee per service PLUS a per 100 GT rate. Different ports have different rates.
The table shows columns for each port with basic fee and per 100 tons rate.

Return a JSON array with one object per port:
[{
  "due_type": "pilotage_dues",
  "port": "<port name>",
  "description": "Pilotage services",
  "basic_fee": "<basic fee per service>",
  "unit_rate": "<rate per 100 GT>",
  "per_unit": "100",
  "base_field": "gross_tonnage",
  "service_count_field": "num_operations",
  "surcharges": [{"percentage": "<pct>", "description": "<desc>"}],
  "source_pages": [<page numbers>]
}]

IMPORTANT: Extract from the table. Look for "Basic Fee" and "Per 100 tons or part thereof" rows. Remove spaces from numbers (e.g. "18 608.61" becomes "18608.61")."""

TOWAGE_PROMPT = """Extract the Towage (Tugs/Vessel Assistance) tariff rules from this section.

Towage uses tiered/bracketed rates based on gross tonnage. Each port has its own rates.
The table has GT brackets as rows and ports as columns.

The tiers are typically:
- Up to 2,000 GT: flat fee
- 2,001 to 10,000 GT: base fee + rate per 100 GT above 2,000
- 10,001 to 50,000 GT: base fee + rate per 100 GT above 10,000
- 50,001 to 100,000 GT: base fee + rate per 100 GT above 50,000
- Above 100,000 GT: base fee + rate per 100 GT above 100,000

Return a JSON array with one object per port:
[{
  "due_type": "towage_dues",
  "port": "<port name>",
  "description": "Tugs/vessel assistance",
  "tiers": [
    {"min_value": "0", "max_value": "2000", "base_fee": "<fee>", "rate_per_unit": "0", "per_unit": "100"},
    {"min_value": "2001", "max_value": "10000", "base_fee": "<fee>", "rate_per_unit": "<rate>", "per_unit": "100"},
    {"min_value": "10001", "max_value": "50000", "base_fee": "<fee>", "rate_per_unit": "<rate>", "per_unit": "100"},
    {"min_value": "50001", "max_value": "100000", "base_fee": "<fee>", "rate_per_unit": "<rate>", "per_unit": "100"},
    {"min_value": "100001", "max_value": null, "base_fee": "<fee>", "rate_per_unit": "<rate>", "per_unit": "100"}
  ],
  "base_field": "gross_tonnage",
  "service_count_field": "num_operations",
  "surcharges": [{"percentage": "25", "description": "Outside ordinary working hours"}],
  "source_pages": [<page numbers>]
}]

IMPORTANT: Remove spaces from numbers. Towage is charged PER OPERATION (entering + leaving = 2 operations). Include surcharge info."""

RUNNING_LINES_PROMPT = """Extract the Berthing Services / Running Lines tariff rules from this section.

Berthing services have a basic fee per service PLUS a per 100 GT rate. Different ports have different rates.
The table shows columns for each port with basic fee and per 100 tons rate.

Return a JSON array with one object per port:
[{
  "due_type": "running_lines",
  "port": "<port name>",
  "description": "Berthing services (running of vessel lines)",
  "basic_fee": "<basic fee per service>",
  "unit_rate": "<rate per 100 GT>",
  "per_unit": "100",
  "base_field": "gross_tonnage",
  "service_count_field": "num_operations",
  "source_pages": [<page numbers>]
}]

IMPORTANT: Remove spaces from numbers. This is charged per service (entering/leaving = 2 services).
Look for "Other Ports" row which applies to Durban and East London.
Extract from the table with "Basic fee" and "Per 100 tons or part thereof" rows."""

PORT_DUES_PROMPT = """Extract the Port Dues tariff rules from this section.

Port Dues have TWO components:
1. Basic fee: per 100 tons or part thereof (one-time)
2. Daily fee: per 100 tons per 24-hour period (pro-rata)

The section should mention both rates. Also extract reductions and exemptions.

Return a JSON object (NOT an array):
{
  "due_type": "port_dues",
  "port": "All Ports",
  "description": "Port dues on vessels",
  "basic_rate": "<rate per 100 GT, e.g. '192.73'>",
  "daily_rate": "<rate per 100 GT per 24 hours, e.g. '57.79'>",
  "per_unit": "100",
  "base_field": "gross_tonnage",
  "reductions": [
    {"percentage": "<pct>", "description": "<condition for reduction>"}
  ],
  "surcharges": [
    {"percentage": "<pct>", "description": "<condition for surcharge>"}
  ],
  "effective_date": "2024-04-01",
  "source_pages": [<page numbers>]
}

IMPORTANT: Extract the exact rates. The basic fee and daily fee are separate lines in the text."""


PROMPT_MAP = {
    "light_dues": LIGHT_DUES_PROMPT,
    "vts_dues": VTS_PROMPT,
    "pilotage_dues": PILOTAGE_PROMPT,
    "towage_dues": TOWAGE_PROMPT,
    "running_lines": RUNNING_LINES_PROMPT,
    "port_dues": PORT_DUES_PROMPT,
}


def identify_due_type(section: DocumentSection) -> str | None:
    """Identify what type of due a section describes based on its title."""
    title_lower = section.title.lower()

    for keyword, due_type in DUE_TYPE_KEYWORDS.items():
        if keyword in title_lower:
            return due_type
    return None


def extract_rules_from_section(
    client: GeminiClient,
    section: DocumentSection,
    document_name: str = "",
) -> list[TariffRule]:
    """
    Extract tariff rules from a document section using Gemini.

    Uses a focused prompt specific to the due type for better accuracy.
    """
    due_type = identify_due_type(section)
    if not due_type:
        logger.debug("Skipping section '%s' — no matching due type", section.title)
        return []

    logger.info(
        "Extracting rules from section '%s' (pages %d-%d, due_type=%s)",
        section.title, section.start_page, section.end_page, due_type
    )

    prompt = PROMPT_MAP.get(due_type)
    if not prompt:
        logger.warning("No prompt template for due_type '%s'", due_type)
        return []

    content = section.get_full_content()

    # Retry up to 2 times for JSON parsing failures
    for attempt in range(3):
        try:
            raw_result = client.extract_structured(prompt, content)
            break
        except json.JSONDecodeError as e:
            logger.warning(
                "JSON parse error on attempt %d for '%s': %s",
                attempt + 1, section.title, e
            )
            if attempt == 2:
                logger.error("All attempts failed for section '%s'", section.title)
                return []
            time.sleep(1)
        except Exception as e:
            logger.error("Gemini extraction failed for section '%s': %s", section.title, e)
            return []

    # Convert raw result to TariffRule objects based on due type
    try:
        rules = _convert_to_rules(raw_result, due_type, section, document_name)
    except Exception as e:
        logger.error(
            "Failed to convert raw rules for '%s': %s\nRaw: %s",
            section.title, e, json.dumps(raw_result, indent=2)[:2000]
        )
        return []

    logger.info("Extracted %d rules from section '%s'", len(rules), section.title)
    return rules


def _clean_number(s: str) -> str:
    """Clean number strings: remove spaces, handle formatting."""
    if not isinstance(s, str):
        s = str(s)
    # Remove spaces within numbers (e.g., "18 608.61" -> "18608.61")
    s = s.strip()
    # Handle numbers with spaces as thousand separators
    parts = s.split(".")
    if len(parts) == 2:
        integer_part = parts[0].replace(" ", "")
        return f"{integer_part}.{parts[1]}"
    return s.replace(" ", "")


def _convert_to_rules(
    raw: dict | list,
    due_type: str,
    section: DocumentSection,
    document_name: str,
) -> list[TariffRule]:
    """Convert raw Gemini output to typed TariffRule objects."""

    if due_type == "light_dues":
        return _convert_light_dues(raw, section, document_name)
    elif due_type == "vts_dues":
        return _convert_vts(raw, section, document_name)
    elif due_type == "pilotage_dues":
        return _convert_pilotage(raw, section, document_name)
    elif due_type == "towage_dues":
        return _convert_towage(raw, section, document_name)
    elif due_type == "running_lines":
        return _convert_running_lines(raw, section, document_name)
    elif due_type == "port_dues":
        return _convert_port_dues(raw, section, document_name)
    else:
        raise ValueError(f"Unknown due_type: {due_type}")


def _make_source(section: DocumentSection, doc_name: str, pages: list[int] | None = None) -> RuleSource:
    return RuleSource(
        document=doc_name,
        section_number=section.section_number,
        section_title=section.title,
        page_numbers=pages or [section.start_page],
        text_excerpt=section.text_content[:300],
    )


def _convert_light_dues(raw: dict, section: DocumentSection, doc_name: str) -> list[TariffRule]:
    if isinstance(raw, list):
        raw = raw[0]
    rate_raw = raw.get("rate") or "117.08"
    per_unit_raw = raw.get("per_unit") or "100"
    rate = Decimal(_clean_number(str(rate_raw)))
    per_unit = Decimal(_clean_number(str(per_unit_raw)))

    return [TariffRule(
        id="all_ports_light_dues",
        due_type="light_dues",
        port="All Ports",
        description="Light dues on vessels",
        rate_structure=FlatRate(
            rate=rate,
            per_unit=per_unit,
            base_field=raw.get("base_field", "gross_tonnage"),
            rounding="ceil",
        ),
        source=_make_source(section, doc_name),
        effective_date=raw.get("effective_date", ""),
        notes=raw.get("notes", []),
    )]


def _convert_vts(raw: dict | list, section: DocumentSection, doc_name: str) -> list[TariffRule]:
    if isinstance(raw, dict):
        raw = [raw]

    rules = []
    for entry in raw:
        port = entry.get("port", "All Ports")
        rate = Decimal(_clean_number(entry.get("rate", "0")))
        per_unit = Decimal(_clean_number(entry.get("per_unit", "1")))
        min_fee = entry.get("minimum_fee", "0")
        if min_fee:
            min_fee = Decimal(_clean_number(str(min_fee)))
        else:
            min_fee = Decimal("0")

        rules.append(TariffRule(
            id=f"{port.lower().replace(' ', '_')}_vts_dues",
            due_type="vts_dues",
            port=port,
            description="VTS charges on vessels",
            rate_structure=FlatRate(
                rate=rate,
                per_unit=per_unit,
                base_field="gross_tonnage",
                rounding="ceil",
                minimum_charge=min_fee,
            ),
            source=_make_source(section, doc_name),
        ))

    return rules


def _convert_pilotage(raw: dict | list, section: DocumentSection, doc_name: str) -> list[TariffRule]:
    if isinstance(raw, dict):
        raw = [raw]

    rules = []
    for entry in raw:
        port = entry.get("port", "Unknown")
        basic_fee_raw = entry.get("basic_fee") or "0"
        unit_rate_raw = entry.get("unit_rate") or "0"
        basic_fee = Decimal(_clean_number(str(basic_fee_raw)))
        unit_rate = Decimal(_clean_number(str(unit_rate_raw)))
        per_unit = Decimal(_clean_number(entry.get("per_unit", "100")))

        surcharges = []
        for s in entry.get("surcharges", []):
            surcharges.append(Surcharge(
                percentage=Decimal(_clean_number(str(s.get("percentage", "0")))),
                description=s.get("description", ""),
            ))

        rules.append(TariffRule(
            id=f"{port.lower().replace(' ', '_')}_pilotage_dues",
            due_type="pilotage_dues",
            port=port,
            description="Pilotage services",
            rate_structure=PerServiceRate(
                base_fee=basic_fee,
                unit_rate=unit_rate,
                per_unit=per_unit,
                base_field="gross_tonnage",
                rounding="ceil",
                service_count_field=entry.get("service_count_field", "num_operations"),
            ),
            surcharges=surcharges,
            source=_make_source(section, doc_name),
        ))

    return rules


def _convert_towage(raw: dict | list, section: DocumentSection, doc_name: str) -> list[TariffRule]:
    if isinstance(raw, dict):
        raw = [raw]

    rules = []
    for entry in raw:
        try:
            rule = _convert_single_towage(entry, section, doc_name)
            rules.append(rule)
        except Exception as e:
            port = entry.get("port", "?")
            logger.warning("Skipping towage rule for %s: %s", port, e)

    return rules


def _convert_single_towage(entry: dict, section: DocumentSection, doc_name: str) -> TariffRule:
    port = entry.get("port", "Unknown")

    tiers = []
    for t in entry.get("tiers", []):
        max_val = t.get("max_value")
        if max_val is not None and str(max_val).lower() not in ("null", "none", ""):
            try:
                max_val = Decimal(_clean_number(str(max_val)))
            except Exception:
                max_val = None
        else:
            max_val = None

        base_fee_raw = t.get("base_fee") or "0"
        rate_raw = t.get("rate_per_unit") or "0"
        min_raw = t.get("min_value") or "0"
        pu_raw = t.get("per_unit") or "100"

        tiers.append(Tier(
            min_value=Decimal(_clean_number(str(min_raw))),
            max_value=max_val,
            base_fee=Decimal(_clean_number(str(base_fee_raw))),
            rate_per_unit=Decimal(_clean_number(str(rate_raw))),
            per_unit=Decimal(_clean_number(str(pu_raw))),
        ))

    surcharges = []
    for s in entry.get("surcharges", []):
        surcharges.append(Surcharge(
            percentage=Decimal(_clean_number(str(s.get("percentage", "0")))),
            description=s.get("description", ""),
        ))

    return TariffRule(
        id=f"{port.lower().replace(' ', '_')}_towage_dues",
        due_type="towage_dues",
        port=port,
        description="Tugs/vessel assistance",
        rate_structure=TieredRate(
            tiers=tiers,
            base_field="gross_tonnage",
            rounding="ceil",
        ),
        surcharges=surcharges,
        source=_make_source(section, doc_name),
        notes=entry.get("notes", ["Charged per operation"]),
    )


def _convert_running_lines(raw: dict | list, section: DocumentSection, doc_name: str) -> list[TariffRule]:
    if isinstance(raw, dict):
        raw = [raw]

    rules = []
    for entry in raw:
        port = entry.get("port", "Unknown")
        # Support both flat per_service_fee and basic_fee + unit_rate formats
        basic_fee_raw = entry.get("basic_fee") or entry.get("per_service_fee") or "0"
        unit_rate_raw = entry.get("unit_rate") or "0"
        per_unit_raw = entry.get("per_unit") or "100"

        basic_fee = Decimal(_clean_number(str(basic_fee_raw)))
        unit_rate = Decimal(_clean_number(str(unit_rate_raw)))
        per_unit = Decimal(_clean_number(str(per_unit_raw)))

        rules.append(TariffRule(
            id=f"{port.lower().replace(' ', '_')}_running_lines",
            due_type="running_lines",
            port=port,
            description="Berthing services (running of vessel lines)",
            rate_structure=PerServiceRate(
                base_fee=basic_fee,
                unit_rate=unit_rate,
                per_unit=per_unit,
                base_field=entry.get("base_field", "gross_tonnage"),
                rounding="ceil",
                service_count_field=entry.get("service_count_field", "num_operations"),
            ),
            source=_make_source(section, doc_name),
        ))

    return rules


def _convert_port_dues(raw: dict, section: DocumentSection, doc_name: str) -> list[TariffRule]:
    if isinstance(raw, list):
        raw = raw[0]

    basic_rate = Decimal(_clean_number(raw.get("basic_rate", "192.73")))
    daily_rate = Decimal(_clean_number(raw.get("daily_rate", "57.79")))
    per_unit = Decimal(_clean_number(raw.get("per_unit", "100")))

    reductions = []
    for r in raw.get("reductions", []):
        reductions.append(Reduction(
            percentage=Decimal(_clean_number(str(r.get("percentage", "0")))),
            description=r.get("description", ""),
        ))

    surcharges = []
    for s in raw.get("surcharges", []):
        surcharges.append(Surcharge(
            percentage=Decimal(_clean_number(str(s.get("percentage", "0")))),
            description=s.get("description", ""),
        ))

    return [TariffRule(
        id="all_ports_port_dues",
        due_type="port_dues",
        port="All Ports",
        description="Port dues on vessels",
        rate_structure=CompositeRate(
            components=[
                FlatRate(
                    rate=basic_rate,
                    per_unit=per_unit,
                    base_field="gross_tonnage",
                    rounding="ceil",
                ),
                TimeBasedRate(
                    rate=daily_rate,
                    per_unit=per_unit,
                    base_field="gross_tonnage",
                    time_field="days_alongside",
                    rounding="ceil",
                ),
            ],
        ),
        reductions=reductions,
        surcharges=surcharges,
        source=_make_source(section, doc_name),
        effective_date=raw.get("effective_date", ""),
    )]


def extract_all_rules(
    client: GeminiClient,
    sections: list[DocumentSection],
    document_name: str = "",
) -> list[TariffRule]:
    """
    Extract rules from all relevant sections.

    This is the main entry point for rule extraction.
    """
    all_rules = []
    for section in sections:
        rules = extract_rules_from_section(client, section, document_name)
        all_rules.extend(rules)

    logger.info("Total rules extracted: %d", len(all_rules))
    return all_rules
