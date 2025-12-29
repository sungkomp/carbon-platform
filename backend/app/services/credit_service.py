from __future__ import annotations
from sqlalchemy.orm import Session
from app.models import CarbonCreditProject

def calc_carbon_credit(db: Session, project_code: str) -> dict:
    p = db.query(CarbonCreditProject).filter(CarbonCreditProject.project_code == project_code).one_or_none()
    if not p:
        raise ValueError(f"Credit project not found: {project_code}")

    gross = max(0.0, float(p.baseline_tco2e) - float(p.project_tco2e) - float(p.leakage_tco2e))
    buffer = gross * float(p.buffer_pct)
    net = max(0.0, gross - buffer)

    return {
        "project_code": p.project_code,
        "methodology": p.methodology,
        "baseline_tco2e": p.baseline_tco2e,
        "project_tco2e": p.project_tco2e,
        "leakage_tco2e": p.leakage_tco2e,
        "buffer_pct": p.buffer_pct,
        "gross_tco2e": gross,
        "buffer_tco2e": buffer,
        "net_tco2e": net,
        "vintage": p.vintage,
        "extra": p.extra,
    }
