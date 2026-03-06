"""
PDF Parser — Stage 1 of the Write Path.

Extracts text and tables from port tariff PDFs using pdfplumber.
Tables are preserved as structured data (list of dicts) while text
captures the rules, conditions, and definitions.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)


@dataclass
class ParsedTable:
    """A table extracted from a PDF page."""
    page_number: int
    headers: list[str]
    rows: list[dict[str, str]]
    raw_text: str = ""


@dataclass
class ParsedPage:
    """Extracted content from a single PDF page."""
    page_number: int
    text: str
    tables: list[ParsedTable] = field(default_factory=list)


@dataclass
class ParsedDocument:
    """Complete parsed PDF document."""
    filename: str
    total_pages: int
    pages: list[ParsedPage] = field(default_factory=list)

    def get_full_text(self) -> str:
        return "\n\n".join(
            f"--- PAGE {p.page_number} ---\n{p.text}" for p in self.pages
        )

    def get_pages_range(self, start: int, end: int) -> str:
        return "\n\n".join(
            f"--- PAGE {p.page_number} ---\n{p.text}"
            for p in self.pages
            if start <= p.page_number <= end
        )

    def get_tables_for_pages(self, start: int, end: int) -> list[ParsedTable]:
        tables = []
        for p in self.pages:
            if start <= p.page_number <= end:
                tables.extend(p.tables)
        return tables


def parse_pdf(pdf_path: str | Path) -> ParsedDocument:
    """
    Parse a PDF file, extracting text and tables per page.

    Tables are extracted separately from text to preserve structure.
    This is critical because tariff rates live in tables while
    conditions and rules are in the surrounding text.
    """
    pdf_path = Path(pdf_path)
    logger.info("Parsing PDF: %s", pdf_path.name)

    pages: list[ParsedPage] = []

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        logger.info("Document has %d pages", total_pages)

        for page in pdf.pages:
            page_num = page.page_number  # 1-indexed
            text = page.extract_text() or ""

            parsed_tables = []
            tables = page.extract_tables()

            for table in tables:
                if not table or len(table) < 2:
                    continue

                # First row as headers, rest as data
                raw_headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(table[0])]
                headers = _deduplicate_headers(raw_headers)

                rows = []
                for row in table[1:]:
                    if row and any(cell for cell in row):
                        row_dict = {}
                        for i, cell in enumerate(row):
                            if i < len(headers):
                                row_dict[headers[i]] = str(cell).strip() if cell else ""
                        rows.append(row_dict)

                if rows:
                    table_text = _table_to_text(headers, rows)
                    parsed_tables.append(ParsedTable(
                        page_number=page_num,
                        headers=headers,
                        rows=rows,
                        raw_text=table_text,
                    ))

            pages.append(ParsedPage(
                page_number=page_num,
                text=text,
                tables=parsed_tables,
            ))

    logger.info("Parsed %d pages, found %d tables total",
                len(pages), sum(len(p.tables) for p in pages))

    return ParsedDocument(
        filename=pdf_path.name,
        total_pages=total_pages,
        pages=pages,
    )


def _deduplicate_headers(headers: list[str]) -> list[str]:
    """Ensure all headers are unique by appending index if needed."""
    seen: dict[str, int] = {}
    result = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            result.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            result.append(h)
    return result


def _table_to_text(headers: list[str], rows: list[dict[str, str]]) -> str:
    """Convert table to readable text format for LLM consumption."""
    lines = [" | ".join(headers)]
    lines.append("-" * len(lines[0]))
    for row in rows:
        lines.append(" | ".join(row.get(h, "") for h in headers))
    return "\n".join(lines)
