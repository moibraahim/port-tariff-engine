"""
Rule Extractor — Stage 3 of the Write Path.

Uses Gemini to transform document sections into structured TariffRule objects.
This is the LLM-heavy stage — runs once per tariff document during ingestion.

Key design: temperature=0 for reproducible extraction.
Each section is processed independently with a focused prompt.

Prompts are generic and work for any port authority tariff document.
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
    "lighthouse": "light_dues",
    "navigation": "light_dues",
    "port due": "port_dues",
    "port fee": "port_dues",
    "port charge": "port_dues",
    "harbour due": "port_dues",
    "harbor due": "port_dues",
    "tug": "towage_dues",
    "towage": "towage_dues",
    "vessel assist": "towage_dues",
    "vts": "vts_dues",
    "vessel traffic": "vts_dues",
    "pilotage": "pilotage_dues",
    "pilot": "pilotage_dues",
    "running": "running_lines",
    "berthing": "running_lines",
    "mooring": "running_lines",
    "line handling": "running_lines",
    "wharfage": "wharfage",
    "anchorage": "anchorage_dues",
    "berth hire": "berth_hire",
    "cargo due": "cargo_dues",
}


# === GENERIC EXTRACTION PROMPTS ===
# These work for any port tariff document, not specific to any port authority.

LIGHT_DUES_PROMPT = """Extract the Light Dues / Lighthouse Dues tariff rule from this section.

Light dues are typically a flat rate applied per unit of tonnage (e.g. per 100 GT or per GT).

Return a JSON object (NOT an array):
{
  "due_type": "light_dues",
  "port": "<port name or 'All Ports' if applies to all ports>",
  "description": "<brief description>",
  "rate": "<exact rate amount>",
  "per_unit": "<tonnage unit, e.g. '100' for per 100 GT, or '1' for per GT>",
  "base_field": "<'gross_tonnage' or 'net_tonnage' — whichever the tariff uses>",
  "rounding": "<'ceil' if 'per X tons or part thereof', otherwise 'round'>",
  "effective_date": "<if stated>",
  "source_pages": [<page numbers>],
  "notes": ["<any important notes, exemptions, conditions>"]
}

IMPORTANT: Extract the standard rate that applies to most commercial vessels (not special categories like naval/government vessels). Remove any spaces within numbers."""

VTS_PROMPT = """Extract the VTS (Vessel Traffic Services) charges from this section.

VTS charges are typically a flat rate per GT per port call. Different ports may have different rates.

Return a JSON array of objects, one per rate variant (different ports or vessel categories):
[{
  "due_type": "vts_dues",
  "port": "<port name or 'All Ports'>",
  "description": "<brief description>",
  "rate": "<rate per unit>",
  "per_unit": "<'1' for per GT, '100' for per 100 GT>",
  "base_field": "<'gross_tonnage' or 'net_tonnage'>",
  "minimum_fee": "<minimum fee if specified, or null>",
  "effective_date": "<if stated>",
  "source_pages": [<page numbers>]
}]

IMPORTANT: If different ports have different rates, create a separate entry for each. Remove spaces within numbers."""

PILOTAGE_PROMPT = """Extract the Pilotage tariff rules from this section.

Pilotage typically has a basic/flat fee per service PLUS a per-tonnage rate.
Different ports often have different rates. The tariff may show a table with ports as columns.

Return a JSON array with one object per port:
[{
  "due_type": "pilotage_dues",
  "port": "<port name>",
  "description": "Pilotage services",
  "basic_fee": "<basic/flat fee per service>",
  "unit_rate": "<rate per unit of tonnage>",
  "per_unit": "<tonnage unit, e.g. '100' for per 100 GT>",
  "base_field": "<'gross_tonnage' or 'net_tonnage'>",
  "service_count_field": "num_operations",
  "surcharges": [{"percentage": "<pct>", "description": "<desc>"}],
  "source_pages": [<page numbers>]
}]

IMPORTANT: Extract from the table if one exists. Remove spaces from numbers (e.g. "18 608.61" becomes "18608.61"). Pilotage is typically charged per service/operation."""

TOWAGE_PROMPT = """Extract the Towage / Tug services tariff rules from this section.

Towage typically uses tiered/bracketed rates based on gross tonnage. Each port may have its own rates.
The tariff often has GT brackets as rows and ports as columns in a table.

Return a JSON array with one object per port:
[{
  "due_type": "towage_dues",
  "port": "<port name>",
  "description": "Towage / tug services",
  "tiers": [
    {"min_value": "<lower GT bound>", "max_value": "<upper GT bound or null for last tier>", "base_fee": "<fee for this tier>", "rate_per_unit": "<incremental rate per unit above min>", "per_unit": "<unit size, e.g. '100'>"}
  ],
  "base_field": "gross_tonnage",
  "service_count_field": "num_operations",
  "surcharges": [{"percentage": "<pct>", "description": "<e.g. outside working hours>"}],
  "source_pages": [<page numbers>]
}]

IMPORTANT:
- Remove spaces from numbers.
- Towage is typically charged per operation/service (entering + leaving).
- Each tier should have a base_fee (the cumulative cost at the start of that bracket) and rate_per_unit (incremental cost per unit of tonnage above the tier minimum).
- If the rate is a flat fee for the bracket (not incremental), set base_fee to the flat fee and rate_per_unit to "0"."""

RUNNING_LINES_PROMPT = """Extract the Berthing Services / Running Lines / Mooring tariff rules from this section.

Berthing/mooring services typically have a basic fee per service PLUS a per-tonnage rate, or just a flat fee per service. Different ports may have different rates.

Return a JSON array with one object per port:
[{
  "due_type": "running_lines",
  "port": "<port name>",
  "description": "Berthing services / running lines",
  "basic_fee": "<basic fee per service>",
  "unit_rate": "<rate per unit of tonnage, or '0' if flat fee only>",
  "per_unit": "<tonnage unit, e.g. '100' for per 100 GT>",
  "base_field": "<'gross_tonnage' or 'net_tonnage'>",
  "service_count_field": "num_operations",
  "source_pages": [<page numbers>]
}]

IMPORTANT: Remove spaces from numbers. This is typically charged per service (berthing + unberthing)."""

PORT_DUES_PROMPT = """Extract the Port Dues tariff rules from this section.

Port Dues often have TWO components:
1. A one-time/arrival fee (per unit of tonnage)
2. A daily/time-based fee (per unit of tonnage per day/24-hour period)

Some ports only have one component. Extract whatever the tariff specifies.

Return a JSON object (NOT an array):
{
  "due_type": "port_dues",
  "port": "<port name or 'All Ports'>",
  "description": "Port dues",
  "basic_rate": "<one-time rate per unit of tonnage>",
  "daily_rate": "<daily rate per unit of tonnage per day, or '0' if none>",
  "per_unit": "<tonnage unit, e.g. '100' for per 100 GT>",
  "base_field": "<'gross_tonnage' or 'net_tonnage'>",
  "rounding": "<'ceil' if 'per X tons or part thereof', otherwise 'round'>",
  "reductions": [
    {"percentage": "<pct>", "description": "<condition for reduction>"}
  ],
  "surcharges": [
    {"percentage": "<pct>", "description": "<condition for surcharge>"}
  ],
  "effective_date": "<if stated>",
  "source_pages": [<page numbers>]
}

IMPORTANT: Extract the exact rates. If there are separate arrival and daily components, include both. Remove spaces from numbers."""

# Generic fallback prompt for unrecognized due types
GENERIC_DUES_PROMPT = """Extract the tariff rules from this section.

Analyze the rate structure and extract as a JSON array:
[{{
  "due_type": "{due_type}",
  "port": "<port name or 'All Ports'>",
  "description": "<brief description>",
  "rate_type": "<one of: 'flat', 'tiered', 'per_service', 'composite'>",
  "rate": "<primary rate amount>",
  "per_unit": "<unit, e.g. '1' for per GT, '100' for per 100 GT>",
  "base_field": "<'gross_tonnage' or 'net_tonnage' or 'dwt'>",
  "basic_fee": "<basic/flat fee if applicable>",
  "unit_rate": "<per-unit rate if separate from basic fee>",
  "tiers": [
    {{"min_value": "<lower>", "max_value": "<upper or null>", "base_fee": "<fee>", "rate_per_unit": "<rate>", "per_unit": "<unit>"}}
  ],
  "surcharges": [{{"percentage": "<pct>", "description": "<desc>"}}],
  "reductions": [{{"percentage": "<pct>", "description": "<desc>"}}],
  "source_pages": [<page numbers>]
}}]

Extract all rate variants (different ports, vessel categories, etc.) as separate entries.
Remove spaces from numbers."""


PROMPT_MAP = {
    "light_dues": LIGHT_DUES_PROMPT,
    "vts_dues": VTS_PROMPT,
    "pilotage_dues": PILOTAGE_PROMPT,
    "towage_dues": TOWAGE_PROMPT,
    "running_lines": RUNNING_LINES_PROMPT,
    "port_dues": PORT_DUES_PROMPT,
}


def identify_due_type(section: DocumentSection) -> str | None:
    """Identify what type of due a section describes based on its title or hint."""
    # Use the hint from section splitter if available
    if section.due_type_hint:
        return section.due_type_hint

    title_lower = section.title.lower()
    # Also check the text content for keywords (first 500 chars)
    content_lower = section.text_content[:500].lower() if section.text_content else ""

    for keyword, due_type in DUE_TYPE_KEYWORDS.items():
        if keyword in title_lower or keyword in content_lower:
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
    Falls back to a generic prompt for unrecognized due types.
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
        # Use the generic prompt for unknown due types
        prompt = GENERIC_DUES_PROMPT.format(due_type=due_type)

    content = section.get_full_content()

    # Retry up to 2 times for JSON parsing failures
    raw_result = None
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

    if raw_result is None:
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
    # Remove currency symbols and commas
    s = s.replace(",", "").replace("$", "").replace("R", "").replace("€", "").replace("£", "")
    s = s.strip()
    # Handle numbers with spaces as thousand separators
    parts = s.split(".")
    if len(parts) == 2:
        integer_part = parts[0].replace(" ", "")
        return f"{integer_part}.{parts[1]}"
    return s.replace(" ", "")


def _safe_decimal(value, default="0") -> Decimal:
    """Safely convert a value to Decimal, returning default on failure."""
    if value is None or str(value).strip().lower() in ("null", "none", "n/a", ""):
        return Decimal(default)
    try:
        return Decimal(_clean_number(str(value)))
    except Exception:
        logger.warning("Could not convert '%s' to Decimal, using %s", value, default)
        return Decimal(default)


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
        return _convert_generic(raw, due_type, section, document_name)


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
    rate = _safe_decimal(raw.get("rate"))
    per_unit = _safe_decimal(raw.get("per_unit"), "100")

    if rate == 0:
        logger.warning("Light dues rate is 0 — check extraction")
        return []

    return [TariffRule(
        id=f"{_slug(raw.get('port', 'all_ports'))}_light_dues",
        due_type="light_dues",
        port=raw.get("port") or "All Ports",
        description=raw.get("description") or "Light dues on vessels",
        rate_structure=FlatRate(
            rate=rate,
            per_unit=per_unit,
            base_field=raw.get("base_field") or "gross_tonnage",
            rounding=raw.get("rounding") or "ceil",
        ),
        source=_make_source(section, doc_name),
        effective_date=raw.get("effective_date") or "",
        notes=raw.get("notes") or [],
    )]


def _convert_vts(raw: dict | list, section: DocumentSection, doc_name: str) -> list[TariffRule]:
    if isinstance(raw, dict):
        raw = [raw]

    rules = []
    for entry in raw:
        port = entry.get("port", "All Ports")
        rate = _safe_decimal(entry.get("rate"))
        per_unit = _safe_decimal(entry.get("per_unit"), "1")
        min_fee = _safe_decimal(entry.get("minimum_fee"))

        if rate == 0:
            logger.warning("VTS rate is 0 for %s — skipping", port)
            continue

        rules.append(TariffRule(
            id=f"{_slug(port)}_vts_dues",
            due_type="vts_dues",
            port=port,
            description=entry.get("description", "VTS charges"),
            rate_structure=FlatRate(
                rate=rate,
                per_unit=per_unit,
                base_field=entry.get("base_field", "gross_tonnage"),
                rounding=entry.get("rounding", "ceil"),
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
        basic_fee = _safe_decimal(entry.get("basic_fee"))
        unit_rate = _safe_decimal(entry.get("unit_rate"))
        per_unit = _safe_decimal(entry.get("per_unit"), "100")

        surcharges = []
        for s in entry.get("surcharges", []):
            surcharges.append(Surcharge(
                percentage=_safe_decimal(s.get("percentage")),
                description=s.get("description", ""),
            ))

        rules.append(TariffRule(
            id=f"{_slug(port)}_pilotage_dues",
            due_type="pilotage_dues",
            port=port,
            description=entry.get("description", "Pilotage services"),
            rate_structure=PerServiceRate(
                base_fee=basic_fee,
                unit_rate=unit_rate,
                per_unit=per_unit,
                base_field=entry.get("base_field") or "gross_tonnage",
                rounding=entry.get("rounding") or "ceil",
                service_count_field=entry.get("service_count_field") or "num_operations",
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

        tiers.append(Tier(
            min_value=_safe_decimal(t.get("min_value")),
            max_value=max_val,
            base_fee=_safe_decimal(t.get("base_fee")),
            rate_per_unit=_safe_decimal(t.get("rate_per_unit")),
            per_unit=_safe_decimal(t.get("per_unit"), "100"),
        ))

    if not tiers:
        raise ValueError(f"No tiers found for towage at {port}")

    surcharges = []
    for s in entry.get("surcharges", []):
        surcharges.append(Surcharge(
            percentage=_safe_decimal(s.get("percentage")),
            description=s.get("description", ""),
        ))

    return TariffRule(
        id=f"{_slug(port)}_towage_dues",
        due_type="towage_dues",
        port=port,
        description=entry.get("description", "Towage / tug services"),
        rate_structure=TieredRate(
            tiers=tiers,
            base_field=entry.get("base_field", "gross_tonnage"),
            rounding=entry.get("rounding", "ceil"),
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
        try:
            port = entry.get("port", "Unknown")
            # Support both flat per_service_fee and basic_fee + unit_rate formats
            basic_fee = _safe_decimal(entry.get("basic_fee") or entry.get("per_service_fee"))
            unit_rate = _safe_decimal(entry.get("unit_rate"))
            per_unit = _safe_decimal(entry.get("per_unit"), "100")

            rules.append(TariffRule(
                id=f"{_slug(port)}_running_lines",
                due_type="running_lines",
                port=port,
                description=entry.get("description", "Berthing services / running lines"),
                rate_structure=PerServiceRate(
                    base_fee=basic_fee,
                    unit_rate=unit_rate,
                    per_unit=per_unit,
                    base_field=entry.get("base_field") or "gross_tonnage",
                    rounding=entry.get("rounding") or "ceil",
                    service_count_field=entry.get("service_count_field") or "num_operations",
                ),
                source=_make_source(section, doc_name),
            ))
        except Exception as e:
            port = entry.get("port", "?")
            logger.warning("Skipping running lines rule for %s: %s", port, e)

    return rules


def _convert_port_dues(raw: dict, section: DocumentSection, doc_name: str) -> list[TariffRule]:
    if isinstance(raw, list):
        raw = raw[0]

    basic_rate = _safe_decimal(raw.get("basic_rate"))
    daily_rate = _safe_decimal(raw.get("daily_rate"))
    per_unit = _safe_decimal(raw.get("per_unit"), "100")
    rounding = raw.get("rounding", "ceil")

    if basic_rate == 0 and daily_rate == 0:
        logger.warning("Port dues rates are both 0 — check extraction")
        return []

    # Build components based on what's present
    components = []
    if basic_rate > 0:
        components.append(FlatRate(
            rate=basic_rate,
            per_unit=per_unit,
            base_field=raw.get("base_field", "gross_tonnage"),
            rounding=rounding,
        ))
    if daily_rate > 0:
        components.append(TimeBasedRate(
            rate=daily_rate,
            per_unit=per_unit,
            base_field=raw.get("base_field", "gross_tonnage"),
            time_field="days_alongside",
            rounding=rounding,
        ))

    # If only one component, use it directly; otherwise composite
    if len(components) == 1:
        rate_structure = components[0]
    else:
        rate_structure = CompositeRate(components=components)

    reductions = []
    for r in raw.get("reductions", []):
        reductions.append(Reduction(
            percentage=_safe_decimal(r.get("percentage")),
            description=r.get("description", ""),
        ))

    surcharges = []
    for s in raw.get("surcharges", []):
        surcharges.append(Surcharge(
            percentage=_safe_decimal(s.get("percentage")),
            description=s.get("description", ""),
        ))

    return [TariffRule(
        id=f"{_slug(raw.get('port', 'all_ports'))}_port_dues",
        due_type="port_dues",
        port=raw.get("port") or "All Ports",
        description=raw.get("description") or "Port dues",
        rate_structure=rate_structure,
        reductions=reductions,
        surcharges=surcharges,
        source=_make_source(section, doc_name),
        effective_date=raw.get("effective_date") or "",
    )]


def _convert_generic(raw: dict | list, due_type: str, section: DocumentSection, doc_name: str) -> list[TariffRule]:
    """Convert generic/unrecognized due type rules."""
    if isinstance(raw, dict):
        raw = [raw]

    rules = []
    for entry in raw:
        port = entry.get("port", "All Ports")
        rate_type = entry.get("rate_type", "flat")

        try:
            base_field = entry.get("base_field") or "gross_tonnage"

            if rate_type == "tiered" and entry.get("tiers"):
                rate_structure = TieredRate(
                    tiers=[
                        Tier(
                            min_value=_safe_decimal(t.get("min_value")),
                            max_value=_safe_decimal(t.get("max_value")) if t.get("max_value") else None,
                            base_fee=_safe_decimal(t.get("base_fee")),
                            rate_per_unit=_safe_decimal(t.get("rate_per_unit")),
                            per_unit=_safe_decimal(t.get("per_unit"), "100"),
                        )
                        for t in entry["tiers"]
                    ],
                    base_field=base_field,
                )
            elif rate_type == "per_service":
                rate_structure = PerServiceRate(
                    base_fee=_safe_decimal(entry.get("basic_fee")),
                    unit_rate=_safe_decimal(entry.get("unit_rate") or entry.get("rate")),
                    per_unit=_safe_decimal(entry.get("per_unit"), "100"),
                    base_field=base_field,
                )
            else:
                rate_structure = FlatRate(
                    rate=_safe_decimal(entry.get("rate")),
                    per_unit=_safe_decimal(entry.get("per_unit"), "100"),
                    base_field=base_field,
                )

            surcharges = [
                Surcharge(
                    percentage=_safe_decimal(s.get("percentage")),
                    description=s.get("description", ""),
                )
                for s in entry.get("surcharges", [])
            ]

            reductions = [
                Reduction(
                    percentage=_safe_decimal(r.get("percentage")),
                    description=r.get("description", ""),
                )
                for r in entry.get("reductions", [])
            ]

            rules.append(TariffRule(
                id=f"{_slug(port)}_{due_type}",
                due_type=due_type,
                port=port,
                description=entry.get("description", due_type.replace("_", " ").title()),
                rate_structure=rate_structure,
                surcharges=surcharges,
                reductions=reductions,
                source=_make_source(section, doc_name),
            ))
        except Exception as e:
            logger.warning("Skipping generic rule for %s at %s: %s", due_type, port, e)

    return rules


def _slug(text: str) -> str:
    """Convert text to a URL-safe slug for rule IDs."""
    return text.lower().replace(" ", "_").replace(",", "").replace("'", "")


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
