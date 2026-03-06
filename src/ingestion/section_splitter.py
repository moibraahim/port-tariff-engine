"""
Section Splitter — Stage 2 of the Write Path.

Splits parsed PDF into semantic sections that correspond to specific
tariff due types. Uses a two-pass approach:

Pass 1 (Fast): Regex heading detection for common tariff formats.
Pass 2 (LLM): If regex finds nothing, use Gemini to discover sections
              dynamically — works with ANY tariff document format.

Each section is kept small (1-3 pages) to avoid LLM output truncation.
"""

import re
import json
import logging
from dataclasses import dataclass, field

from .pdf_parser import ParsedDocument, ParsedTable

logger = logging.getLogger(__name__)


@dataclass
class DocumentSection:
    """A semantic section of the tariff document."""
    section_number: str  # e.g. "1.1.1", "2.1.1"
    title: str
    due_type_hint: str = ""  # optional hint for which due type this covers
    start_page: int = 0
    end_page: int = 0
    text_content: str = ""
    tables: list[ParsedTable] = field(default_factory=list)
    subsections: list["DocumentSection"] = field(default_factory=list)

    def get_full_content(self) -> str:
        """Combine text and table content for LLM processing."""
        parts = [self.text_content]
        for table in self.tables:
            parts.append(f"\n[TABLE on page {table.page_number}]\n{table.raw_text}\n")
        return "\n".join(parts)


# Flexible heading patterns — matches common tariff document formats.
# These intentionally do NOT include section numbers (those vary between documents).
# Patterns are case-insensitive and match headings regardless of numbering scheme.
TARIFF_HEADING_PATTERNS = [
    # Light Dues / Lighthouse Dues
    (r"\bLIGHT\s+DUES\b", "light_dues"),
    (r"\bLIGHTHOUSE\s+DUES\b", "light_dues"),
    (r"\bNAVIGATION(?:AL)?\s+AIDS?\s+DUES?\b", "light_dues"),

    # VTS / Vessel Traffic Services
    (r"\bVTS\s+(?:CHARGES?|DUES?|FEES?|SERVICES?)\b", "vts_dues"),
    (r"\bVESSEL\s+TRAFFIC\s+SERVI?CE?S?\b", "vts_dues"),

    # Pilotage
    (r"\bPILOTAGE\s+(?:SERVICES?|DUES?|FEES?|CHARGES?)\b", "pilotage_dues"),

    # Towage / Tugs
    (r"\bTOWAGE\s+(?:SERVICES?|DUES?|FEES?|CHARGES?)\b", "towage_dues"),
    (r"\bTUGS?\s*[/&]\s*VESSEL\s+ASSIST", "towage_dues"),
    (r"\bTUG\s+(?:SERVICES?|ASSIST)", "towage_dues"),
    (r"\bVESSEL\s+ASSIST(?:ANCE)?\b", "towage_dues"),

    # Running Lines / Berthing / Mooring
    (r"\bBERTHING\s+SERVICES?\b", "running_lines"),
    (r"\bRUNNING\s+(?:OF\s+)?(?:VESSEL\s+)?LINES?\b", "running_lines"),
    (r"\bMOORING\s+(?:SERVICES?|CHARGES?|FEES?)\b", "running_lines"),
    (r"\bLINE\s*(?:HANDLING|RUNNING)\b", "running_lines"),

    # Port Dues / Port Charges
    (r"\bPORT\s+DUES\b", "port_dues"),
    (r"\bPORT\s+FEES?\b", "port_dues"),
    (r"\bPORT\s+CHARGES?\b", "port_dues"),
    (r"\bHARBOU?R\s+DUES\b", "port_dues"),

    # Wharfage (common in many ports)
    (r"\bWHARFAGE\b", "wharfage"),

    # Anchorage
    (r"\bANCHORAGE\s+(?:DUES?|CHARGES?|FEES?)\b", "anchorage_dues"),

    # Berth Hire
    (r"\bBERTH\s+HIRE\b", "berth_hire"),

    # Cargo Dues
    (r"\bCARGO\s+DUES\b", "cargo_dues"),
]


def split_into_sections(doc: ParsedDocument, gemini_client=None) -> list[DocumentSection]:
    """
    Split document into focused sections for each tariff due type.

    Strategy:
    1. Try flexible regex heading detection (works for most structured tariffs)
    2. If regex finds fewer than 3 sections and a Gemini client is provided,
       use LLM-based section discovery as fallback
    3. Last resort: page-by-page processing
    """
    sections = _detect_sections_by_headings(doc)

    if len(sections) < 3 and gemini_client:
        logger.info(
            "Regex found only %d sections — using LLM for section discovery",
            len(sections),
        )
        llm_sections = _discover_sections_via_llm(doc, gemini_client)
        if len(llm_sections) > len(sections):
            sections = llm_sections

    if not sections:
        logger.warning("No tariff sections found — falling back to page-by-page")
        sections = _fallback_page_sections(doc)

    logger.info("Split document into %d sections", len(sections))
    return sections


def _detect_sections_by_headings(doc: ParsedDocument) -> list[DocumentSection]:
    """
    Pass 1: Search pages for tariff-related headings using flexible regex.

    Skips the first few pages (cover, TOC, definitions) to avoid
    matching heading patterns in the table of contents.
    """
    found_sections: list[tuple[int, str, str, str]] = []  # (page, heading_text, title, due_type)

    # Skip initial pages (cover, TOC, definitions) — heuristic: skip first 15% of pages, min 2
    skip_pages = max(2, int(doc.total_pages * 0.15))

    for page in doc.pages:
        if page.page_number <= skip_pages:
            continue
        page_text = page.text

        for pattern, due_type in TARIFF_HEADING_PATTERNS:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                # Avoid duplicate detections for the same due type
                if not any(f[3] == due_type for f in found_sections):
                    heading_text = match.group(0).strip()

                    # Try to extract a section number preceding the heading
                    section_num = _extract_section_number(page_text, match.start())

                    found_sections.append((
                        page.page_number,
                        section_num or str(page.page_number),
                        heading_text.title(),
                        due_type,
                    ))
                    logger.info(
                        "Found section '%s' (%s) on page %d",
                        heading_text, due_type, page.page_number,
                    )

    # Sort by page number
    found_sections.sort(key=lambda x: x[0])

    # Build sections with bounded page ranges
    sections = []
    for i, (start_page, sec_num, title, due_type) in enumerate(found_sections):
        # End page: next section start - 1, or start_page + 2 (max 3 pages)
        if i + 1 < len(found_sections):
            end_page = min(found_sections[i + 1][0] - 1, start_page + 2)
        else:
            end_page = min(start_page + 2, doc.total_pages)

        end_page = max(end_page, start_page)

        text_content = doc.get_pages_range(start_page, end_page)
        tables = doc.get_tables_for_pages(start_page, end_page)

        sections.append(DocumentSection(
            section_number=sec_num,
            title=title,
            due_type_hint=due_type,
            start_page=start_page,
            end_page=end_page,
            text_content=text_content,
            tables=tables,
        ))

    return sections


def _extract_section_number(text: str, heading_pos: int) -> str:
    """
    Try to extract a section number (e.g. "3.8", "1.1.1") that appears
    before a heading in the text.
    """
    # Look at the 30 chars before the heading match
    prefix = text[max(0, heading_pos - 30):heading_pos]
    # Match patterns like "3.8", "1.1.1", "4.1.1", "A.2", etc.
    match = re.search(r"(\d+(?:\.\d+)+\.?\s*$)", prefix)
    if match:
        return match.group(1).strip().rstrip(".")
    return ""


# LLM-based section discovery prompt
SECTION_DISCOVERY_PROMPT = """Analyze this port tariff document and identify which pages contain information about different types of port charges/dues.

For each tariff item found, provide:
- The page number(s) where it appears
- The section number (if any)
- The title/heading
- The type of due (one of: light_dues, port_dues, towage_dues, vts_dues, pilotage_dues, running_lines, wharfage, anchorage_dues, berth_hire, cargo_dues, or "other")

Return a JSON array:
[{
  "start_page": <int>,
  "end_page": <int>,
  "section_number": "<string>",
  "title": "<heading text>",
  "due_type": "<type>"
}]

Only include sections that contain actual rate tables or tariff amounts.
Do NOT include table of contents entries, definitions, or general terms & conditions."""


def _discover_sections_via_llm(doc: ParsedDocument, gemini_client) -> list[DocumentSection]:
    """
    Pass 2: Use Gemini to discover tariff sections in an unfamiliar document.

    Sends a condensed version of each page to Gemini and asks it to
    identify which pages contain tariff-related content.
    """
    # Build a condensed page summary for Gemini
    page_summaries = []
    for page in doc.pages:
        # First 500 chars of each page + table indicators
        summary = f"--- PAGE {page.page_number} ---\n"
        summary += page.text[:800]
        if page.tables:
            summary += f"\n[{len(page.tables)} table(s) on this page]"
        page_summaries.append(summary)

    condensed_doc = "\n\n".join(page_summaries)

    # Truncate if too long (Gemini context limit)
    if len(condensed_doc) > 50000:
        condensed_doc = condensed_doc[:50000] + "\n\n[TRUNCATED]"

    try:
        raw_result = gemini_client.extract_structured(
            SECTION_DISCOVERY_PROMPT, condensed_doc
        )
    except Exception as e:
        logger.error("LLM section discovery failed: %s", e)
        return []

    if not isinstance(raw_result, list):
        raw_result = [raw_result]

    sections = []
    for entry in raw_result:
        try:
            start_page = int(entry.get("start_page", 0))
            end_page = int(entry.get("end_page", start_page))
            # Bound to max 3 pages
            end_page = min(end_page, start_page + 2)
            end_page = max(end_page, start_page)

            if start_page < 1 or start_page > doc.total_pages:
                continue

            text_content = doc.get_pages_range(start_page, end_page)
            tables = doc.get_tables_for_pages(start_page, end_page)

            sections.append(DocumentSection(
                section_number=entry.get("section_number", str(start_page)),
                title=entry.get("title", f"Page {start_page}"),
                due_type_hint=entry.get("due_type", ""),
                start_page=start_page,
                end_page=end_page,
                text_content=text_content,
                tables=tables,
            ))
        except Exception as e:
            logger.warning("Skipping LLM-discovered section: %s", e)

    logger.info("LLM discovered %d sections", len(sections))
    return sections


def _fallback_page_sections(doc: ParsedDocument) -> list[DocumentSection]:
    """If heading detection fails, create one section per page."""
    return [
        DocumentSection(
            section_number=str(page.page_number),
            title=f"Page {page.page_number}",
            start_page=page.page_number,
            end_page=page.page_number,
            text_content=page.text,
            tables=page.tables,
        )
        for page in doc.pages
    ]
