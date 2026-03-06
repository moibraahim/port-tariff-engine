"""Calculation result models with full audit trail for data lineage."""

from decimal import Decimal
from pydantic import BaseModel, Field


class AuditEntry(BaseModel):
    """Single step in the calculation audit trail."""
    step: str  # e.g. "base_calculation", "surcharge_applied"
    description: str
    formula: str = ""  # e.g. "ceil(51300/100) * 117.08"
    values: dict[str, str] = Field(default_factory=dict)
    result: str = ""  # string representation of intermediate result


class CalculationLine(BaseModel):
    """Result for a single tariff due type."""
    due_type: str
    description: str = ""
    amount: Decimal
    currency: str = "ZAR"
    rule_id: str = ""
    audit_trail: list[AuditEntry] = Field(default_factory=list)
    source_section: str = ""
    source_pages: list[int] = Field(default_factory=list)


class CalculationResult(BaseModel):
    """Complete calculation result for a vessel at a port."""
    vessel_name: str
    port: str
    lines: list[CalculationLine] = Field(default_factory=list)
    total: Decimal = Decimal("0")
    currency: str = "ZAR"
    warnings: list[str] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)
