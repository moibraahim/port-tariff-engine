"""
Rule Matcher — finds applicable rules for a given port and due type.

Queries the extracted rules to find ones that match the
calculation request for a given port and vessel.

Handles "All Ports" rules (e.g., light dues, port dues) that apply
to every port unless a port-specific rule overrides them.
"""

import logging

from ..models.rules import TariffRule
from ..models.vessel import VesselProfile
from ..ingestion.rule_store import RuleStore
from .condition_evaluator import evaluate_conditions

logger = logging.getLogger(__name__)

# Common due types — used as defaults if no specific types are requested.
# Dynamically extended with any additional types found in the rule store.
STANDARD_DUE_TYPES = [
    "light_dues",
    "port_dues",
    "towage_dues",
    "vts_dues",
    "pilotage_dues",
    "running_lines",
]


def get_available_due_types(store: RuleStore) -> list[str]:
    """
    Get all due types present in the rule store.
    Returns STANDARD_DUE_TYPES + any additional types discovered during ingestion.
    """
    all_rules = store.load_rules()
    discovered = sorted(set(r.due_type for r in all_rules))

    # Merge: standard types first (in order), then any extras
    result = list(STANDARD_DUE_TYPES)
    for dt in discovered:
        if dt not in result:
            result.append(dt)

    return result


def find_applicable_rules(
    store: RuleStore,
    port: str,
    vessel: VesselProfile,
    due_types: list[str] | None = None,
) -> dict[str, TariffRule]:
    """
    Find the applicable rule for each due type at a port.

    Returns a dict mapping due_type -> TariffRule.

    Resolution order:
    1. Port-specific rule (e.g., "Durban" towage)
    2. "All Ports" rule (e.g., light dues apply everywhere)
    3. "Other Ports" / "Other" rule (catch-all for ports not listed)
    """
    target_types = due_types or get_available_due_types(store)
    all_rules = store.load_rules()
    result: dict[str, TariffRule] = {}

    for due_type in target_types:
        # Collect candidate rules for this due type
        candidates: list[TariffRule] = []
        for rule in all_rules:
            if rule.due_type != due_type:
                continue

            port_lower = rule.port.lower()
            target_lower = port.lower()

            # Exact port match
            if port_lower == target_lower:
                candidates.insert(0, rule)  # Priority
            # Port name contains the target (e.g. "Durban and Saldanha Bay" contains "durban")
            elif target_lower in port_lower:
                candidates.insert(0, rule)
            # Comma-separated port lists (e.g. "Durban, Richards Bay, East London")
            elif target_lower in [p.strip().lower() for p in rule.port.split(",")]:
                candidates.insert(0, rule)
            # "All Ports" applies everywhere (unless "excluding" the target)
            elif "all ports" in port_lower and "excluding" not in port_lower:
                candidates.append(rule)
            elif "all ports" in port_lower and "excluding" in port_lower and target_lower not in port_lower:
                candidates.append(rule)
            # "Other Ports" / "Other" is a fallback
            elif port_lower in ("other ports", "other", "others"):
                candidates.append(rule)

        if not candidates:
            logger.warning("No rule found for %s at %s", due_type, port)
            continue

        # Pick the best candidate: port-specific > all ports > other
        best_rule = None
        best_score = -1

        for rule in candidates:
            applies, _ = evaluate_conditions(rule, vessel)
            if not applies:
                continue

            # Scoring: exact port match = 100, port in name = 80, All Ports = 10, Other = 5
            port_lower = rule.port.lower()
            target_lower = port.lower()
            if port_lower == target_lower:
                score = 100
            elif target_lower in port_lower or target_lower in [p.strip().lower() for p in rule.port.split(",")]:
                score = 80
            elif "all ports" in port_lower:
                score = 10
            else:
                score = 5

            # More conditions = more specific
            score += len(rule.conditions)

            if score > best_score:
                best_score = score
                best_rule = rule

        if best_rule:
            result[due_type] = best_rule
            logger.info(
                "Matched rule '%s' (port=%s) for %s at %s",
                best_rule.id, best_rule.port, due_type, port,
            )
        else:
            # Last resort: take first candidate regardless of conditions
            result[due_type] = candidates[0]
            logger.info(
                "Using fallback rule '%s' for %s at %s",
                candidates[0].id, due_type, port,
            )

    return result
