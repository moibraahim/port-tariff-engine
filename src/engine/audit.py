"""
Audit Builder — constructs full data lineage for every calculation.

Implements the data lineage concept from DDIA Ch 12:
Every output traces back through: page -> section -> rule -> formula -> result.
"""

from decimal import Decimal

from ..models.rules import TariffRule, TieredRate
from ..models.vessel import VesselProfile
from ..models.results import CalculationResult, CalculationLine, AuditEntry
from ..ingestion.rule_store import RuleStore
from .rule_matcher import find_applicable_rules, STANDARD_DUE_TYPES
from .condition_evaluator import check_exemptions, apply_adjustments
from .calculator import calculate_rule

# Due types that are charged per operation (entering + leaving = 2)
PER_OPERATION_DUE_TYPES = {"towage_dues"}


def calculate_port_dues(
    store: RuleStore,
    vessel: VesselProfile,
    port: str,
    due_types: list[str] | None = None,
) -> CalculationResult:
    """
    Full calculation pipeline for a vessel at a port.

    This is the main entry point for the Read Path:
    1. Match applicable rules
    2. Check exemptions
    3. Calculate base amounts
    4. Apply adjustments (surcharges/reductions)
    5. Build audit trail

    Returns a complete result with full data lineage.
    """
    result = CalculationResult(
        vessel_name=vessel.vessel_metadata.name,
        port=port,
        metadata={
            "vessel_type": vessel.technical_specs.type,
            "gross_tonnage": str(vessel.technical_specs.gross_tonnage),
            "net_tonnage": str(vessel.technical_specs.net_tonnage),
        },
    )

    # Step 1: Find applicable rules
    matched_rules = find_applicable_rules(store, port, vessel, due_types)

    target_types = due_types or STANDARD_DUE_TYPES
    for due_type in target_types:
        if due_type not in matched_rules:
            result.warnings.append(f"No rule found for {due_type} at {port}")
            continue

        rule = matched_rules[due_type]

        # Step 2: Check exemptions
        is_exempt, exempt_audit = check_exemptions(rule, vessel)
        if is_exempt:
            line = CalculationLine(
                due_type=due_type,
                description=f"{rule.description} (EXEMPT)",
                amount=Decimal("0"),
                rule_id=rule.id,
                audit_trail=exempt_audit,
                source_section=rule.source.section_title,
                source_pages=rule.source.page_numbers,
            )
            result.lines.append(line)
            continue

        # Step 3: Calculate base amount
        line = calculate_rule(rule, vessel)

        # Step 3b: For per-operation dues (towage), multiply by num_operations
        # when the rate structure doesn't already handle it (TieredRate)
        if due_type in PER_OPERATION_DUE_TYPES and isinstance(rule.rate_structure, TieredRate):
            num_ops = vessel.operational_data.num_operations
            per_op_amount = line.amount
            line.amount = line.amount * num_ops
            line.audit_trail.append(AuditEntry(
                step="per_operation_multiplier",
                description=f"Multiply by {num_ops} operations (entering + leaving)",
                formula=f"{per_op_amount} * {num_ops}",
                values={
                    "per_operation": str(per_op_amount),
                    "num_operations": str(num_ops),
                },
                result=str(line.amount),
            ))

        # Step 4: Apply adjustments
        adjusted_amount, adj_audit = apply_adjustments(
            line.amount, rule, vessel
        )
        line.amount = adjusted_amount
        line.audit_trail.extend(adj_audit)

        # Step 5: Round to 2 decimal places for final amount
        line.amount = line.amount.quantize(Decimal("0.01"))

        result.lines.append(line)

    # Calculate total
    result.total = sum(line.amount for line in result.lines)

    return result
