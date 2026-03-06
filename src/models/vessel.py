"""Vessel profile model — mirrors the input format from the task specification."""

from datetime import datetime
from pydantic import BaseModel, Field


class VesselMetadata(BaseModel):
    name: str
    built_year: int | None = None
    flag: str | None = None
    classification_society: str | None = None
    call_sign: str | None = None


class TechnicalSpecs(BaseModel):
    imo_number: str | None = None
    type: str  # e.g. "Bulk Carrier"
    dwt: float
    gross_tonnage: float
    net_tonnage: float
    loa_meters: float  # Length Overall
    beam_meters: float
    moulded_depth_meters: float | None = None
    lbp_meters: float | None = None  # Length Between Perpendiculars
    draft_sw_s_w_t: list[float] = Field(default_factory=list)
    suez_gt: float | None = None
    suez_nt: float | None = None


class OperationalData(BaseModel):
    cargo_quantity_mt: float = 0
    days_alongside: float = 0
    arrival_time: datetime | None = None
    departure_time: datetime | None = None
    activity: str = ""
    num_operations: int = 1  # e.g. number of pilotage/towage operations
    num_holds: int | None = None


class VesselProfile(BaseModel):
    """Complete vessel profile used for tariff calculation queries."""
    vessel_metadata: VesselMetadata
    technical_specs: TechnicalSpecs
    operational_data: OperationalData = Field(default_factory=OperationalData)
