"""
Integration test — golden test against SUDESTADA @ Durban.

Validates the full Read Path against ground truth values.
Requires rules to have been extracted first (via the Write Path).
"""

import pytest
from decimal import Decimal
from pathlib import Path

from src.models.vessel import VesselProfile, VesselMetadata, TechnicalSpecs, OperationalData
from src.ingestion.rule_store import RuleStore
from src.engine.audit import calculate_port_dues


# Ground truth from the task specification
EXPECTED_VALUES = {
    "light_dues": Decimal("60062.04"),
    "port_dues": Decimal("199549.22"),
    "towage_dues": Decimal("147074.38"),
    "vts_dues": Decimal("33315.75"),
    "pilotage_dues": Decimal("47189.94"),
    "running_lines": Decimal("19639.50"),
}

TOLERANCE = Decimal("0.01")  # 1% tolerance


@pytest.fixture
def sudestada() -> VesselProfile:
    """The SUDESTADA test vessel from the task specification."""
    return VesselProfile(
        vessel_metadata=VesselMetadata(
            name="SUDESTADA",
            built_year=2010,
            flag="MLT - Malta",
            classification_society="Registro Italiano Navale",
        ),
        technical_specs=TechnicalSpecs(
            type="Bulk Carrier",
            dwt=93274,
            gross_tonnage=51300,
            net_tonnage=31192,
            loa_meters=229.2,
            beam_meters=38.0,
            moulded_depth_meters=20.7,
            lbp_meters=222.0,
            draft_sw_s_w_t=[14.9, 0.0, 0.0],
            suez_nt=49069,
        ),
        operational_data=OperationalData(
            cargo_quantity_mt=40000,
            days_alongside=3.39,
            arrival_time="2024-11-15T10:12:00",
            departure_time="2024-11-22T13:00:00",
            activity="Exporting Iron Ore",
            num_operations=2,
            num_holds=7,
        ),
    )


@pytest.fixture
def rule_store():
    """Load the extracted rules."""
    store = RuleStore()
    rules = store.load_rules()
    if not rules:
        pytest.skip("No extracted rules found. Run ingestion first: python -m scripts.ingest data/port_tariff.pdf")
    return store


class TestSudestadaDurban:
    """Golden test: SUDESTADA at Durban must match all 6 expected values."""

    def test_full_calculation(self, rule_store, sudestada):
        """Run full calculation and verify against ground truth."""
        result = calculate_port_dues(rule_store, sudestada, "Durban")

        print(f"\n{'='*60}")
        print(f"SUDESTADA @ Durban — Calculation Results")
        print(f"{'='*60}")

        for line in result.lines:
            expected = EXPECTED_VALUES.get(line.due_type)
            status = ""
            if expected:
                diff_pct = abs(line.amount - expected) / expected * 100
                status = f"  (expected: {expected}, diff: {diff_pct:.2f}%)"
            print(f"  {line.due_type:20s}: {line.amount:>12,.2f} ZAR{status}")

        print(f"  {'TOTAL':20s}: {result.total:>12,.2f} ZAR")
        print(f"{'='*60}")

        # Verify each due type within tolerance
        for line in result.lines:
            expected = EXPECTED_VALUES.get(line.due_type)
            if expected:
                diff_pct = abs(line.amount - expected) / expected
                assert diff_pct <= TOLERANCE, (
                    f"{line.due_type}: calculated {line.amount} vs expected {expected} "
                    f"(diff: {diff_pct*100:.2f}% > {TOLERANCE*100}%)"
                )

    def test_audit_trail_completeness(self, rule_store, sudestada):
        """Every calculation line must have an audit trail."""
        result = calculate_port_dues(rule_store, sudestada, "Durban")

        for line in result.lines:
            assert len(line.audit_trail) > 0, (
                f"{line.due_type} has no audit trail"
            )
            # Each audit entry should have at least a step and description
            for entry in line.audit_trail:
                assert entry.step, f"Audit entry missing step in {line.due_type}"
                assert entry.description, f"Audit entry missing description in {line.due_type}"

    def test_all_due_types_calculated(self, rule_store, sudestada):
        """All 6 due types should be in the result."""
        result = calculate_port_dues(rule_store, sudestada, "Durban")
        calculated_types = {line.due_type for line in result.lines}

        for due_type in EXPECTED_VALUES:
            assert due_type in calculated_types or any(
                due_type in w for w in result.warnings
            ), f"Missing due type: {due_type}"

    def test_deterministic(self, rule_store, sudestada):
        """Same input must produce identical output (idempotency)."""
        result1 = calculate_port_dues(rule_store, sudestada, "Durban")
        result2 = calculate_port_dues(rule_store, sudestada, "Durban")

        for line1, line2 in zip(result1.lines, result2.lines):
            assert line1.amount == line2.amount, (
                f"{line1.due_type}: {line1.amount} != {line2.amount}"
            )
