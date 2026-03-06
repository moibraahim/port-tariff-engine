"""
Calculator Engine — the deterministic core of the Read Path.

Pure arithmetic, zero LLM dependency. Every calculation is:
- Reproducible (same input = same output)
- Auditable (full formula trace)
- Testable (unit-testable per rate structure type)

Uses decimal.Decimal for financial-grade precision.
"""

import math
import logging
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP

from ..models.rules import (
    TariffRule, FlatRate, TieredRate, CompositeRate,
    PerServiceRate, TimeBasedRate, Tier,
)
from ..models.vessel import VesselProfile
from ..models.results import CalculationLine, AuditEntry

logger = logging.getLogger(__name__)


def calculate_rule(rule: TariffRule, vessel: VesselProfile) -> CalculationLine:
    """
    Calculate a single tariff rule against a vessel profile.

    Dispatches to the appropriate calculation method based on
    the rate structure type (discriminated union pattern).
    """
    audit_trail: list[AuditEntry] = []

    amount = _calculate_rate_structure(
        rule.rate_structure, vessel, audit_trail
    )

    # Apply the number of operations for per-operation dues
    # (towage, pilotage are typically charged per operation)

    return CalculationLine(
        due_type=rule.due_type,
        description=rule.description,
        amount=amount,
        currency=rule.currency,
        rule_id=rule.id,
        audit_trail=audit_trail,
        source_section=rule.source.section_title,
        source_pages=rule.source.page_numbers,
    )


def _calculate_rate_structure(
    rate: "FlatRate | TieredRate | CompositeRate | PerServiceRate | TimeBasedRate",
    vessel: VesselProfile,
    audit: list[AuditEntry],
) -> Decimal:
    """Dispatch calculation based on rate structure type."""
    if isinstance(rate, FlatRate):
        return _calc_flat(rate, vessel, audit)
    elif isinstance(rate, TieredRate):
        return _calc_tiered(rate, vessel, audit)
    elif isinstance(rate, CompositeRate):
        return _calc_composite(rate, vessel, audit)
    elif isinstance(rate, PerServiceRate):
        return _calc_per_service(rate, vessel, audit)
    elif isinstance(rate, TimeBasedRate):
        return _calc_time_based(rate, vessel, audit)
    else:
        raise ValueError(f"Unknown rate structure: {type(rate)}")


def _get_vessel_value(vessel: VesselProfile, field: str) -> Decimal:
    """
    Extract a numeric value from vessel profile by field name.

    Supports dotted paths like "technical_specs.gross_tonnage".
    """
    # Handle simple field names (map to appropriate location)
    field_map = {
        "gross_tonnage": ("technical_specs", "gross_tonnage"),
        "net_tonnage": ("technical_specs", "net_tonnage"),
        "dwt": ("technical_specs", "dwt"),
        "loa_meters": ("technical_specs", "loa_meters"),
        "beam_meters": ("technical_specs", "beam_meters"),
        "lbp_meters": ("technical_specs", "lbp_meters"),
        "days_alongside": ("operational_data", "days_alongside"),
        "num_operations": ("operational_data", "num_operations"),
        "cargo_quantity_mt": ("operational_data", "cargo_quantity_mt"),
        "num_holds": ("operational_data", "num_holds"),
    }

    if field in field_map:
        section, attr = field_map[field]
        obj = getattr(vessel, section)
        return Decimal(str(getattr(obj, attr)))

    # Handle dotted paths
    parts = field.split(".")
    obj = vessel
    for part in parts:
        obj = getattr(obj, part)

    return Decimal(str(obj))


def _apply_rounding(value: Decimal, rounding: str) -> Decimal:
    """Apply rounding strategy to a value."""
    if rounding == "ceil":
        return value.to_integral_value(rounding=ROUND_CEILING)
    elif rounding == "floor":
        return value.to_integral_value(rounding=ROUND_FLOOR)
    else:
        return value.to_integral_value(rounding=ROUND_HALF_UP)


def _calc_flat(rate: FlatRate, vessel: VesselProfile, audit: list[AuditEntry]) -> Decimal:
    """
    Calculate flat rate: ceil(base_value / per_unit) * rate.

    Example: Light Dues = ceil(51300 / 100) * 117.08 = 513 * 117.08 = 60,062.04
    """
    base_value = _get_vessel_value(vessel, rate.base_field)
    units = _apply_rounding(base_value / rate.per_unit, rate.rounding)
    amount = units * rate.rate
    amount = max(amount, rate.minimum_charge)

    audit.append(AuditEntry(
        step="flat_rate_calculation",
        description=f"Flat rate on {rate.base_field}",
        formula=f"{rate.rounding}({base_value} / {rate.per_unit}) * {rate.rate}",
        values={
            "base_value": str(base_value),
            "per_unit": str(rate.per_unit),
            "units": str(units),
            "rate": str(rate.rate),
        },
        result=str(amount),
    ))

    return amount


def _calc_tiered(rate: TieredRate, vessel: VesselProfile, audit: list[AuditEntry]) -> Decimal:
    """
    Calculate tiered rate: find matching tier by base_value, apply tier formula.

    For towage: look up GT bracket, get base_fee + incremental for excess.
    """
    base_value = _get_vessel_value(vessel, rate.base_field)

    # Find the matching tier
    matching_tier = None
    for tier in rate.tiers:
        if tier.max_value is None:
            if base_value >= tier.min_value:
                matching_tier = tier
                break
        elif tier.min_value <= base_value <= tier.max_value:
            matching_tier = tier
            break

    if matching_tier is None:
        # Use the last tier as fallback (highest bracket)
        matching_tier = rate.tiers[-1]
        audit.append(AuditEntry(
            step="tier_fallback",
            description=f"No exact tier match for {base_value}, using highest bracket",
            values={"base_value": str(base_value)},
        ))

    # Calculate: base_fee + ceil((base_value - min_value) / per_unit) * rate_per_unit
    amount = matching_tier.base_fee

    if matching_tier.rate_per_unit > 0:
        excess = base_value - matching_tier.min_value
        if excess > 0:
            excess_units = _apply_rounding(
                excess / matching_tier.per_unit, rate.rounding
            )
            amount += excess_units * matching_tier.rate_per_unit

    audit.append(AuditEntry(
        step="tiered_rate_calculation",
        description=f"Tiered rate on {rate.base_field}",
        formula=(
            f"base_fee({matching_tier.base_fee}) + "
            f"{rate.rounding}(({base_value} - {matching_tier.min_value}) / "
            f"{matching_tier.per_unit}) * {matching_tier.rate_per_unit}"
        ),
        values={
            "base_value": str(base_value),
            "tier_min": str(matching_tier.min_value),
            "tier_max": str(matching_tier.max_value) if matching_tier.max_value else "unlimited",
            "base_fee": str(matching_tier.base_fee),
            "rate_per_unit": str(matching_tier.rate_per_unit),
        },
        result=str(amount),
    ))

    return amount


def _calc_composite(rate: CompositeRate, vessel: VesselProfile, audit: list[AuditEntry]) -> Decimal:
    """
    Calculate composite rate: sum of all component calculations.

    Example: Port Dues = flat_component + time_based_component
    """
    total = Decimal("0")
    component_results = []

    for i, component in enumerate(rate.components):
        component_audit: list[AuditEntry] = []
        component_amount = _calculate_rate_structure(component, vessel, component_audit)
        total += component_amount
        component_results.append(str(component_amount))
        audit.extend(component_audit)

    audit.append(AuditEntry(
        step="composite_sum",
        description="Sum of composite rate components",
        formula=" + ".join(component_results),
        values={f"component_{i}": v for i, v in enumerate(component_results)},
        result=str(total),
    ))

    return total


def _calc_per_service(rate: PerServiceRate, vessel: VesselProfile, audit: list[AuditEntry]) -> Decimal:
    """
    Calculate per-service rate:
    (base_fee + ceil(value/per_unit) * unit_rate) * num_operations.

    Example: Pilotage = (basic_fee + ceil(GT/100) * rate) * 2 operations
    """
    base_value = _get_vessel_value(vessel, rate.base_field)
    num_ops = int(_get_vessel_value(vessel, rate.service_count_field))

    units = _apply_rounding(base_value / rate.per_unit, rate.rounding)
    per_operation = rate.base_fee + (units * rate.unit_rate)
    amount = per_operation * num_ops

    audit.append(AuditEntry(
        step="per_service_calculation",
        description=f"Per-service rate on {rate.base_field}",
        formula=(
            f"({rate.base_fee} + {rate.rounding}({base_value} / {rate.per_unit}) * "
            f"{rate.unit_rate}) * {num_ops} operations"
        ),
        values={
            "base_value": str(base_value),
            "units": str(units),
            "base_fee": str(rate.base_fee),
            "unit_rate": str(rate.unit_rate),
            "per_operation": str(per_operation),
            "num_operations": str(num_ops),
        },
        result=str(amount),
    ))

    return amount


def _calc_time_based(rate: TimeBasedRate, vessel: VesselProfile, audit: list[AuditEntry]) -> Decimal:
    """
    Calculate time-based rate: ceil(base_value/per_unit) * rate * days.

    Example: Port Dues daily component = ceil(GT/100) * 57.79 * 3.39 days
    """
    base_value = _get_vessel_value(vessel, rate.base_field)
    time_value = _get_vessel_value(vessel, rate.time_field)

    units = _apply_rounding(base_value / rate.per_unit, rate.rounding)
    amount = units * rate.rate * time_value
    amount = max(amount, rate.minimum_charge)

    audit.append(AuditEntry(
        step="time_based_calculation",
        description=f"Time-based rate on {rate.base_field}",
        formula=(
            f"{rate.rounding}({base_value} / {rate.per_unit}) * "
            f"{rate.rate} * {time_value} days"
        ),
        values={
            "base_value": str(base_value),
            "units": str(units),
            "rate": str(rate.rate),
            "time_value": str(time_value),
        },
        result=str(amount),
    ))

    return amount
