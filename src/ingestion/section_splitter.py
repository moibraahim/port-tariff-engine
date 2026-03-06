"""
Section Splitter — Stage 2 of the Write Path.

Splits parsed PDF into semantic sections that correspond to specific
tariff due types. Uses heading detection in page text rather than
TOC parsing (which fails on multi-column PDF layouts).

Each section is kept small (1-3 pages) to avoid LLM output truncation.
"""

import re
import logging
from dataclasses import dataclass, field

from .pdf_parser import ParsedDocument, ParsedTable

logger = logging.getLogger(__name__)


@dataclass
class DocumentSection:
    """A semantic section of the tariff document."""
    section_number: str  # e.g. "1.1.1", "2.1.1"
    title: str
    start_page: int
    end_page: int
    text_content: str = ""
    tables: list[ParsedTable] = field(default_factory=list)
    subsections: list["DocumentSection"] = field(default_factory=list)

    def get_full_content(self) -> str:
        """Combine text and table content for LLM processing."""
        parts = [self.text_content]
        for table in self.tables:
            parts.append(f"\n[TABLE on page {table.page_number}]\n{table.raw_text}\n")
        return "\n".join(parts)


# Known tariff section heading patterns to search for.
# Each entry: (regex_pattern, section_number, title, due_type_hint)
TARIFF_HEADINGS = [
    (r"(?:1\.1\.1|1\.1)\s+LIGHT\s+DUES", "1.1.1", "Light Dues", "light_dues"),
    (r"2\.1\.1?\s+VTS\s+CHARGES", "2.1.1", "VTS Charges", "vts_dues"),
    (r"3\.3\s+PILOTAGE\s+SERVICES", "3.3", "Pilotage Services", "pilotage_dues"),
    (r"3\.6\s+TUGS?\/?VESSEL\s+ASSIST", "3.6", "Tugs/Vessel Assistance", "towage_dues"),
    (r"3\.8\s+BERTHING\s+SERVICES", "3.8", "Berthing Services (Running Lines)", "running_lines"),
    (r"4\.1\.1?\s+PORT\s+(?:DUES|FEES)", "4.1.1", "Port Dues", "port_dues"),
]


def split_into_sections(doc: ParsedDocument) -> list[DocumentSection]:
    """
    Split document into focused sections for each tariff due type.

    Strategy:
    1. Search each page for known tariff heading patterns
    2. When found, create a section spanning from that page to the next heading
    3. Keep sections small (max 3 pages) to avoid LLM truncation
    """
    found_sections: list[tuple[int, str, str, str]] = []  # (page, sec_num, title, hint)

    # Skip first 4 pages (cover, TOC, definitions) to avoid
    # matching heading patterns in the table of contents
    for page in doc.pages:
        if page.page_number <= 4:
            continue
        page_text = page.text
        for pattern, sec_num, title, hint in TARIFF_HEADINGS:
            if re.search(pattern, page_text, re.IGNORECASE):
                # Avoid duplicate detections
                if not any(f[1] == sec_num for f in found_sections):
                    found_sections.append((page.page_number, sec_num, title, hint))
                    logger.info(
                        "Found section '%s' (%s) on page %d",
                        title, sec_num, page.page_number,
                    )

    # Sort by page number
    found_sections.sort(key=lambda x: x[0])

    # Build sections with bounded page ranges
    sections = []
    for i, (start_page, sec_num, title, hint) in enumerate(found_sections):
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
            start_page=start_page,
            end_page=end_page,
            text_content=text_content,
            tables=tables,
        ))

    if not sections:
        logger.warning("No known tariff headings found — falling back to page-by-page")
        sections = _fallback_page_sections(doc)

    logger.info("Split document into %d sections", len(sections))
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
