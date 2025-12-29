from __future__ import annotations
from datetime import datetime, date
from sqlalchemy import String, Float, Integer, DateTime, Boolean, Text, Date
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import Mapped, mapped_column
from .db import Base

class EmissionFactor(Base):
    __tablename__ = "emission_factors"

    key: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)

    unit: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)  # kgCO2e per unit

    scope: Mapped[str] = mapped_column(String, default="N/A")
    category: Mapped[str] = mapped_column(String, default="Unclassified")
    tags: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)

    region: Mapped[str | None] = mapped_column(String, nullable=True)
    country: Mapped[str | None] = mapped_column(String, nullable=True)
    sector: Mapped[str | None] = mapped_column(String, nullable=True)
    lifecycle_stage: Mapped[str | None] = mapped_column(String, nullable=True)
    activity_type: Mapped[str | None] = mapped_column(String, nullable=True)

    methodology: Mapped[str | None] = mapped_column(String, nullable=True)
    gwp_version: Mapped[str | None] = mapped_column(String, nullable=True)
    publisher: Mapped[str | None] = mapped_column(String, nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    document_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    page: Mapped[str | None] = mapped_column(String, nullable=True)
    table: Mapped[str | None] = mapped_column(String, nullable=True)

    valid_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    valid_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String, default="active")
    supersedes_key: Mapped[str | None] = mapped_column(String, nullable=True)

    uncertainty_value: Mapped[float | None] = mapped_column(Float, nullable=True)  # 0.1=10%
    uncertainty_type: Mapped[str | None] = mapped_column(String, nullable=True)

    gas_breakdown: Mapped[dict] = mapped_column(JSONB, default=dict)
    activity_id_fields: Mapped[dict] = mapped_column(JSONB, default=dict)  # dynamic form + formula spec
    data_quality: Mapped[dict] = mapped_column(JSONB, default=dict)
    meta: Mapped[dict] = mapped_column(JSONB, default=dict)

    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    extra: Mapped[dict] = mapped_column(JSONB, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class Activity(Base):
    __tablename__ = "activities"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    name: Mapped[str] = mapped_column(String, nullable=False)
    ef_key: Mapped[str] = mapped_column(String, nullable=False)

    inputs: Mapped[dict] = mapped_column(JSONB, default=dict)
    scope: Mapped[str] = mapped_column(String, default="Scope3")
    lifecycle_stage: Mapped[str | None] = mapped_column(String, nullable=True)
    period: Mapped[str | None] = mapped_column(String, nullable=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class CalculationRun(Base):
    __tablename__ = "calculation_runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    run_type: Mapped[str] = mapped_column(String, nullable=False)  # CFO/CFP/CREDIT
    total_kgco2e: Mapped[float] = mapped_column(Float, default=0.0)
    total_tco2e: Mapped[float] = mapped_column(Float, default=0.0)

    details: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)


class CarbonCreditProject(Base):
    __tablename__ = "credit_projects"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_code: Mapped[str] = mapped_column(String, unique=True, index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    methodology: Mapped[str | None] = mapped_column(String, nullable=True)

    baseline_tco2e: Mapped[float] = mapped_column(Float, default=0.0)
    project_tco2e: Mapped[float] = mapped_column(Float, default=0.0)
    leakage_tco2e: Mapped[float] = mapped_column(Float, default=0.0)
    buffer_pct: Mapped[float] = mapped_column(Float, default=0.0)
    vintage: Mapped[str] = mapped_column(String, default="2025")

    extra: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
