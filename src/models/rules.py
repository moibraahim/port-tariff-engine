"""
Tariff rule models — the heart of the system.

Uses Pydantic discriminated unions for type-safe, evolvable rule representation.
Each RateStructure variant encodes a different pricing model found in port tariffs.
"""

from decimal import Decimal
from typing import Annotated, Literal
from pydantic import BaseModel, Field


# === Source Tracing ===

class RuleSource(BaseModel):
    """Traces a rule back to its origin in the PDF."""
    document: str = ""
    section_number: str = ""
    section_title: str = ""
    page_numbers: list[int] = Field(default_factory=list)
    text_excerpt: str = ""


# === Conditions, Exemptions, Surcharges ===

class Condition(BaseModel):
    """A condition that must be met for a rule to apply."""
    field: str  # vessel field to check, e.g. "technical_specs.type"
    operator: str  # "eq", "neq", "gt", "lt", "gte", "lte", "in", "contains"
    value: str | float | list[str]  # value to compare against
    description: str = ""


class Exemption(BaseModel):
    """An exemption that zeroes out the charge if conditions met."""
    conditions: list[Condition]
    description: str = ""


class Surcharge(BaseModel):
    """A percentage surcharge applied on top of the base calculation."""
    percentage: Decimal  # e.g. 25 for 25%
    conditions: list[Condition] = Field(default_factory=list)
    description: str = ""


class Reduction(BaseModel):
    """A percentage reduction applied to the base calculation."""
    percentage: Decimal  # e.g. 35 for 35%
    conditions: list[Condition] = Field(default_factory=list)
    description: str = ""


# === Rate Structures (Discriminated Union) ===

class Tier(BaseModel):
    """A single tier/bracket in a tiered rate structure."""
    min_value: Decimal
    max_value: Decimal | None = None  # None = unlimited
    base_fee: Decimal = Decimal("0")
    rate_per_unit: Decimal = Decimal("0")
    per_unit: Decimal = Decimal("1")  # rate applies per this many units


class FlatRate(BaseModel):
    """Simple flat rate: ceil(base_value / per_unit) * rate."""
    type: Literal["flat"] = "flat"
    rate: Decimal
    per_unit: Decimal = Decimal("100")  # e.g. per 100 GT
    base_field: str = "gross_tonnage"  # vessel field to use
    rounding: str = "ceil"  # "ceil", "floor", "round"
    minimum_charge: Decimal = Decimal("0")


class TieredRate(BaseModel):
    """Tiered/bracket rate: look up tier by base_value, apply tier formula."""
    type: Literal["tiered"] = "tiered"
    tiers: list[Tier]
    base_field: str = "gross_tonnage"
    per_unit: Decimal = Decimal("100")
    rounding: str = "ceil"


class CompositeRate(BaseModel):
    """
    Composition of multiple rate components, summed together.

    Key insight: Port Dues = flat per-tonnage + per-day component.
    This handles arbitrarily complex rate structures through composition.
    """
    type: Literal["composite"] = "composite"
    components: list[Annotated[
        "FlatRate | TieredRate | PerServiceRate | TimeBasedRate",
        Field(discriminator="type")
    ]]


class PerServiceRate(BaseModel):
    """Per-service fee: (base_fee + ceil(value/per_unit) * unit_rate) * num_operations."""
    type: Literal["per_service"] = "per_service"
    base_fee: Decimal = Decimal("0")
    unit_rate: Decimal = Decimal("0")
    per_unit: Decimal = Decimal("100")
    base_field: str = "gross_tonnage"
    rounding: str = "ceil"
    service_count_field: str = "num_operations"


class TimeBasedRate(BaseModel):
    """Time-based rate: ceil(base_value/per_unit) * rate * days."""
    type: Literal["time_based"] = "time_based"
    rate: Decimal
    per_unit: Decimal = Decimal("100")
    base_field: str = "gross_tonnage"
    time_field: str = "days_alongside"
    rounding: str = "ceil"
    minimum_charge: Decimal = Decimal("0")


# Union type for all rate structures
RateStructure = Annotated[
    FlatRate | TieredRate | CompositeRate | PerServiceRate | TimeBasedRate,
    Field(discriminator="type")
]


# === The Main Rule Model ===

class TariffRule(BaseModel):
    """
    A single tariff rule extracted from a port tariff document.

    This is the materialized view — derived from the PDF source of truth,
    persisted for fast, deterministic query-time calculation.
    """
    id: str = ""  # e.g. "durban_light_dues"
    due_type: str  # "light_dues", "port_dues", "towage", etc.
    port: str  # "Durban", "Cape Town", etc.
    description: str = ""
    rate_structure: RateStructure
    conditions: list[Condition] = Field(default_factory=list)
    exemptions: list[Exemption] = Field(default_factory=list)
    surcharges: list[Surcharge] = Field(default_factory=list)
    reductions: list[Reduction] = Field(default_factory=list)
    source: RuleSource = Field(default_factory=RuleSource)
    effective_date: str = ""
    currency: str = "ZAR"
    notes: list[str] = Field(default_factory=list)
