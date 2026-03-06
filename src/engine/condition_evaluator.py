"""
Condition Evaluator — deterministic evaluation of rule conditions.

Checks vessel profiles against conditions, exemptions, surcharges,
and reductions defined in tariff rules. No LLM involved.
"""

import logging
from decimal import Decimal

from ..models.rules import TariffRule, Condition, Exemption, Surcharge, Reduction
from ..models.vessel import VesselProfile
from ..models.results import AuditEntry

logger = logging.getLogger(__name__)


def evaluate_conditions(
    rule: TariffRule, vessel: VesselProfile
) -> tuple[bool, list[AuditEntry]]:
    """
    Check if a rule's conditions are met by the vessel.

    Returns (applies: bool, audit_entries: list).
    """
    audit = []

    if not rule.conditions:
        return True, audit

    for condition in rule.conditions:
        met = _check_condition(condition, vessel)
        audit.append(AuditEntry(
            step="condition_check",
            description=f"Checking: {condition.description or condition.field}",
            values={
                "field": condition.field,
                "operator": condition.operator,
                "expected": str(condition.value),
                "met": str(met),
            },
        ))
        if not met:
            return False, audit

    return True, audit


def check_exemptions(
    rule: TariffRule, vessel: VesselProfile
) -> tuple[bool, list[AuditEntry]]:
    """
    Check if vessel is exempt from this rule.

    Returns (is_exempt: bool, audit_entries: list).
    """
    audit = []

    for exemption in rule.exemptions:
        all_conditions_met = all(
            _check_condition(c, vessel) for c in exemption.conditions
        )
        if all_conditions_met:
            audit.append(AuditEntry(
                step="exemption_applied",
                description=f"Vessel exempt: {exemption.description}",
                result="0",
            ))
            return True, audit

    return False, audit


def apply_adjustments(
    base_amount: Decimal,
    rule: TariffRule,
    vessel: VesselProfile,
) -> tuple[Decimal, list[AuditEntry]]:
    """
    Apply surcharges and reductions to a base amount.

    Surcharges are additive percentages on top.
    Reductions are subtractive percentages.

    Returns (adjusted_amount, audit_entries).
    """
    audit = []
    amount = base_amount

    # Apply reductions — only if they have explicit, evaluable conditions.
    # Reductions without conditions are informational and must NOT be
    # auto-applied (they describe special circumstances that need manual review).
    for reduction in rule.reductions:
        if not reduction.conditions:
            # Skip reductions without machine-evaluable conditions
            continue

        applies = all(
            _check_condition(c, vessel) for c in reduction.conditions
        )

        if applies:
            reduction_amount = amount * reduction.percentage / Decimal("100")
            amount -= reduction_amount
            audit.append(AuditEntry(
                step="reduction_applied",
                description=f"Reduction: {reduction.description}",
                formula=f"{base_amount} * (1 - {reduction.percentage}%)",
                values={
                    "percentage": str(reduction.percentage),
                    "reduction_amount": str(reduction_amount),
                },
                result=str(amount),
            ))

    # Apply surcharges — only if they have explicit, evaluable conditions.
    for surcharge in rule.surcharges:
        if not surcharge.conditions:
            continue

        applies = all(
            _check_condition(c, vessel) for c in surcharge.conditions
        )

        if applies:
            surcharge_amount = amount * surcharge.percentage / Decimal("100")
            amount += surcharge_amount
            audit.append(AuditEntry(
                step="surcharge_applied",
                description=f"Surcharge: {surcharge.description}",
                formula=f"{amount - surcharge_amount} * (1 + {surcharge.percentage}%)",
                values={
                    "percentage": str(surcharge.percentage),
                    "surcharge_amount": str(surcharge_amount),
                },
                result=str(amount),
            ))

    return amount, audit


def _check_condition(condition: Condition, vessel: VesselProfile) -> bool:
    """Evaluate a single condition against a vessel profile."""
    try:
        vessel_value = _get_nested_value(vessel, condition.field)
    except (AttributeError, KeyError):
        logger.warning("Could not resolve field '%s' on vessel", condition.field)
        return False

    op = condition.operator.lower()
    expected = condition.value

    if op == "eq":
        return str(vessel_value).lower() == str(expected).lower()
    elif op == "neq":
        return str(vessel_value).lower() != str(expected).lower()
    elif op == "gt":
        return float(vessel_value) > float(expected)
    elif op == "lt":
        return float(vessel_value) < float(expected)
    elif op == "gte":
        return float(vessel_value) >= float(expected)
    elif op == "lte":
        return float(vessel_value) <= float(expected)
    elif op == "in":
        if isinstance(expected, list):
            return str(vessel_value).lower() in [str(v).lower() for v in expected]
        return str(vessel_value).lower() in str(expected).lower()
    elif op == "contains":
        return str(expected).lower() in str(vessel_value).lower()
    else:
        logger.warning("Unknown operator: %s", op)
        return False


def _get_nested_value(obj, path: str):
    """Get a value from a nested object using dot notation."""
    parts = path.split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict):
            current = current[part]
        else:
            current = getattr(current, part)
    return current
