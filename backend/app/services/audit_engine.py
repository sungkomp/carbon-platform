from __future__ import annotations
from datetime import date
from typing import List
from sqlalchemy.orm import Session
from app.models import CalculationRun, EmissionFactor

def _sev_count(findings: List[dict]) -> dict:
    out = {"critical":0,"major":0,"minor":0,"info":0}
    for f in findings:
        s = f["severity"].upper()
        if s == "CRITICAL": out["critical"] += 1
        elif s == "MAJOR": out["major"] += 1
        elif s == "MINOR": out["minor"] += 1
        else: out["info"] += 1
    return out

def audit_run(db: Session, run_id: int) -> dict:
    r = db.query(CalculationRun).filter(CalculationRun.id == run_id).one_or_none()
    if not r:
        raise ValueError("Run not found")

    rows = (r.details or {}).get("rows") or []
    findings: List[dict] = []

    for row in rows:
        ef_key = row.get("ef_key")
        ef = db.query(EmissionFactor).filter(EmissionFactor.key == ef_key).one_or_none()
        if not ef:
            findings.append({
                "code":"EF_MISSING",
                "severity":"CRITICAL",
                "message":f"EF not found: {ef_key}",
                "evidence":{"activity_id": row.get("activity_id"), "ef_key": ef_key},
                "recommendation":"Fix EF reference or import the missing EF."
            })
            continue

        as_of = None
        as_of_s = (row.get("inputs") or {}).get("_as_of")
        if as_of_s:
            try:
                as_of = date.fromisoformat(as_of_s)
            except Exception:
                findings.append({
                    "code":"ASOF_INVALID",
                    "severity":"MINOR",
                    "message":"Invalid _as_of date format; expected YYYY-MM-DD",
                    "evidence":{"ef_key": ef_key, "_as_of": as_of_s},
                    "recommendation":"Store _as_of as ISO date."
                })

        if as_of:
            if ef.valid_from and ef.valid_from > as_of:
                findings.append({
                    "code":"EF_NOT_YET_VALID",
                    "severity":"MAJOR",
                    "message":"EF valid_from is after as-of date",
                    "evidence":{"ef_key": ef_key, "valid_from": str(ef.valid_from), "as_of": str(as_of)},
                    "recommendation":"Select EF valid for the as-of date."
                })
            if ef.valid_to and ef.valid_to < as_of:
                findings.append({
                    "code":"EF_EXPIRED",
                    "severity":"MAJOR",
                    "message":"EF expired for as-of date",
                    "evidence":{"ef_key": ef_key, "valid_to": str(ef.valid_to), "as_of": str(as_of)},
                    "recommendation":"Use a newer EF or correct the as-of date."
                })

        if ef.status != "active":
            findings.append({
                "code":"EF_NOT_ACTIVE",
                "severity":"MAJOR",
                "message":"EF status is not active",
                "evidence":{"ef_key": ef_key, "status": ef.status},
                "recommendation":"Use an active EF; keep deprecated factors for history only."
            })

        if not ef.meta or not ef.meta.get("reference"):
            findings.append({
                "code":"EF_NO_REFERENCE",
                "severity":"MAJOR",
                "message":"EF missing reference metadata",
                "evidence":{"ef_key": ef_key},
                "recommendation":"Populate EF meta.reference for auditability."
            })

        if ef.uncertainty_value is None:
            findings.append({
                "code":"EF_NO_UNCERTAINTY",
                "severity":"MINOR",
                "message":"EF has no uncertainty_value",
                "evidence":{"ef_key": ef_key},
                "recommendation":"Add uncertainty or document why not available."
            })

    score = 100
    for f in findings:
        sev = f["severity"].upper()
        score -= 25 if sev=="CRITICAL" else 10 if sev=="MAJOR" else 3 if sev=="MINOR" else 1
    score = max(0, score)

    return {"run_id": run_id, "summary": _sev_count(findings), "score": score, "findings": findings}
