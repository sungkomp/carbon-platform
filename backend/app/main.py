from __future__ import annotations

import io, json
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app.config import settings
from app.db import Base, engine, get_db
from app.models import EmissionFactor, Activity, CalculationRun, CarbonCreditProject

from app.services.ef_service import upsert_seed_efs
from app.services.calc_service import compute_run
from app.services.credit_service import calc_carbon_credit
from app.services.audit_engine import audit_run
from app.services.report_export import export_run_pdf, export_run_excel

from app.auth.routes import router as auth_router
from app.auth.security import require_roles, hash_password
from app.auth.models import User

app = FastAPI(title="Carbon Platform", version="3.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.cors_origins.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    # NOTE: In enterprise mode, use Alembic migrations instead of create_all()
    Base.metadata.create_all(bind=engine)
    db = next(get_db())
    try:
        n, warnings = upsert_seed_efs(db)
        if warnings:
            print("[seed warnings]", *warnings, sep="\n- ")
        print(f"[seed] upserted {n} EF rows")

        admin = db.query(User).filter(User.username == "admin").one_or_none()
        if not admin:
            db.add(User(username="admin", password_hash=hash_password("admin1234"), roles=["ADMIN"]))
            db.commit()
            print("[seed] created admin user (admin/admin1234)")
    finally:
        db.close()

app.include_router(auth_router)

# -------- EF --------
@app.get("/api/efs")
def list_efs(q: str | None = None, limit: int = 500, db: Session = Depends(get_db)):
    qry = db.query(EmissionFactor)
    if q:
        like = f"%{q}%"
        qry = qry.filter((EmissionFactor.name.ilike(like)) | (EmissionFactor.key.ilike(like)))
    rows = qry.limit(limit).all()
    return [{
        "key": r.key, "name": r.name, "unit": r.unit, "value": r.value,
        "scope": r.scope, "category": r.category, "tags": r.tags,
        "activity_id_fields": r.activity_id_fields, "gas_breakdown": r.gas_breakdown,
        "gwp_version": r.gwp_version, "meta": r.meta
    } for r in rows]

@app.get("/api/efs/{key}")
def get_ef(key: str, db: Session = Depends(get_db)):
    r = db.query(EmissionFactor).filter(EmissionFactor.key == key).one_or_none()
    if not r:
        raise HTTPException(404, "EF not found")
    return {
        "key": r.key, "name": r.name, "unit": r.unit, "value": r.value,
        "scope": r.scope, "category": r.category, "tags": r.tags,
        "activity_id_fields": r.activity_id_fields, "gas_breakdown": r.gas_breakdown,
        "methodology": r.methodology, "gwp_version": r.gwp_version,
        "publisher": r.publisher, "document_title": r.document_title,
        "valid_from": r.valid_from.isoformat() if r.valid_from else None,
        "valid_to": r.valid_to.isoformat() if r.valid_to else None,
        "uncertainty_value": r.uncertainty_value,
        "uncertainty_type": r.uncertainty_type,
        "meta": r.meta
    }

@app.post("/api/efs")
def upsert_ef(payload: dict, db: Session = Depends(get_db), user=Depends(require_roles("EXPERT","ADMIN"))):
    key = payload.get("key")
    if not key:
        raise HTTPException(400, "key required")
    obj = db.query(EmissionFactor).filter(EmissionFactor.key == key).one_or_none()
    if obj:
        for k, v in payload.items():
            setattr(obj, k, v)
    else:
        db.add(EmissionFactor(**payload))
    db.commit()
    return {"ok": True, "key": key}

@app.post("/api/efs/import")
async def import_efs(file: UploadFile = File(...), db: Session = Depends(get_db), user=Depends(require_roles("EXPERT","ADMIN"))):
    content = await file.read()
    name = (file.filename or "").lower()
    if name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(content))
    elif name.endswith(".xlsx") or name.endswith(".xls"):
        df = pd.read_excel(io.BytesIO(content))
    else:
        raise HTTPException(400, "Only CSV/Excel")
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"key","name","unit","scope","category"}
    if not required.issubset(set(df.columns)):
        raise HTTPException(400, f"Missing columns: need {sorted(required)}")

    def j(v):
        if v is None or (isinstance(v, float) and pd.isna(v)): return {}
        if isinstance(v, dict): return v
        s = str(v).strip()
        if not s or s.lower() == "nan": return {}
        try: return json.loads(s)
        except: return {}

    def tags(v):
        if v is None or (isinstance(v, float) and pd.isna(v)): return []
        if isinstance(v, list): return v
        s = str(v).strip()
        return [x.strip() for x in s.split(",") if x.strip()]

    count = 0
    for _, row in df.iterrows():
        payload = {k: row.get(k) for k in df.columns}
        payload["value"] = None if ("value" not in payload or pd.isna(payload.get("value"))) else float(payload["value"])
        payload["tags"] = tags(payload.get("tags"))
        payload["activity_id_fields"] = j(payload.get("activity_id_fields"))
        payload["gas_breakdown"] = j(payload.get("gas_breakdown"))
        payload["meta"] = j(payload.get("meta"))
        key = str(payload["key"]).strip()
        obj = db.query(EmissionFactor).filter(EmissionFactor.key == key).one_or_none()
        if obj:
            for k, v in payload.items():
                setattr(obj, k, v)
        else:
            db.add(EmissionFactor(**payload))
        count += 1
    db.commit()
    return {"ok": True, "imported": count}

# -------- Activities --------
@app.get("/api/activities")
def list_activities(db: Session = Depends(get_db), user=Depends(require_roles("CALCULATOR","EXPERT","ADMIN"))):
    rows = db.query(Activity).order_by(Activity.id.desc()).all()
    return [{
        "id": a.id, "name": a.name, "ef_key": a.ef_key,
        "inputs": a.inputs, "scope": a.scope, "period": a.period
    } for a in rows]

@app.post("/api/activities")
def create_activity(payload: dict, db: Session = Depends(get_db), user=Depends(require_roles("CALCULATOR","EXPERT","ADMIN"))):
    if not payload.get("ef_key"):
        raise HTTPException(400, "ef_key required")
    a = Activity(
        name=payload.get("name",""),
        ef_key=payload["ef_key"],
        inputs=payload.get("inputs") or {},
        scope=payload.get("scope","Scope3"),
        period=payload.get("period"),
        note=payload.get("note"),
    )
    db.add(a)
    db.commit()
    db.refresh(a)
    return {"ok": True, "id": a.id}

@app.delete("/api/activities/{activity_id}")
def delete_activity(activity_id: int, db: Session = Depends(get_db), user=Depends(require_roles("CALCULATOR","EXPERT","ADMIN"))):
    a = db.query(Activity).filter(Activity.id == activity_id).one_or_none()
    if not a:
        return {"ok": False}
    db.delete(a)
    db.commit()
    return {"ok": True}

@app.post("/api/activities/import")
async def import_activities(file: UploadFile = File(...), db: Session = Depends(get_db), user=Depends(require_roles("CALCULATOR","EXPERT","ADMIN"))):
    content = await file.read()
    name = (file.filename or "").lower()
    if name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(content))
    elif name.endswith(".xlsx") or name.endswith(".xls"):
        df = pd.read_excel(io.BytesIO(content))
    else:
        raise HTTPException(400, "Only CSV/Excel")
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"name","ef_key"}
    if not required.issubset(set(df.columns)):
        raise HTTPException(400, f"Missing columns: need {sorted(required)}")

    def j(v):
        if v is None or (isinstance(v, float) and pd.isna(v)): return {}
        if isinstance(v, dict): return v
        s = str(v).strip()
        if not s or s.lower() == "nan": return {}
        try: return json.loads(s)
        except: return {}

    count = 0
    for _, row in df.iterrows():
        a = Activity(
            name=str(row.get("name","")).strip(),
            ef_key=str(row.get("ef_key","")).strip(),
            inputs=j(row.get("inputs")),
            scope=str(row.get("scope","Scope3")).strip(),
            period=(None if "period" not in df.columns else str(row.get("period")).strip()),
        )
        db.add(a)
        count += 1
    db.commit()
    return {"ok": True, "imported": count}

# -------- Runs (CFO/CFP) --------
@app.post("/api/calc/run")
def run_calc(payload: dict, db: Session = Depends(get_db), user=Depends(require_roles("CALCULATOR","EXPERT","ADMIN"))):
    run_type = payload.get("run_type","CFO")
    activity_ids = payload.get("activity_ids") or []
    if not activity_ids:
        raise HTTPException(400, "activity_ids required")
    result = compute_run(db, activity_ids, run_type)

    r = CalculationRun(
        run_type=result["run_type"],
        total_kgco2e=result["total_kgco2e"],
        total_tco2e=result["total_tco2e"],
        details=result["details"]
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return {"ok": True, "run_id": r.id, **result}

@app.get("/api/calc/runs")
def list_runs(db: Session = Depends(get_db), user=Depends(require_roles("CALCULATOR","EXPERT","AUDITOR","VERIFIER","ADMIN"))):
    rows = db.query(CalculationRun).order_by(CalculationRun.id.desc()).limit(50).all()
    return [{
        "id": r.id, "run_type": r.run_type,
        "total_tco2e": r.total_tco2e,
        "created_at": r.created_at.isoformat()
    } for r in rows]

# -------- Carbon Credit Project Developer --------
@app.get("/api/credit/projects")
def list_credit_projects(db: Session = Depends(get_db), user=Depends(require_roles("PROJECT_DEVELOPER","EXPERT","ADMIN"))):
    rows = db.query(CarbonCreditProject).order_by(CarbonCreditProject.id.desc()).all()
    return [{
        "project_code": p.project_code, "name": p.name, "methodology": p.methodology,
        "baseline_tco2e": p.baseline_tco2e, "project_tco2e": p.project_tco2e,
        "leakage_tco2e": p.leakage_tco2e, "buffer_pct": p.buffer_pct, "vintage": p.vintage
    } for p in rows]

@app.post("/api/credit/projects")
def upsert_credit_project(payload: dict, db: Session = Depends(get_db), user=Depends(require_roles("PROJECT_DEVELOPER","EXPERT","ADMIN"))):
    code = payload.get("project_code")
    if not code:
        raise HTTPException(400, "project_code required")
    p = db.query(CarbonCreditProject).filter(CarbonCreditProject.project_code == code).one_or_none()
    if p:
        for k, v in payload.items():
            setattr(p, k, v)
    else:
        p = CarbonCreditProject(**payload)
        db.add(p)
    db.commit()
    return {"ok": True, "project_code": code}

@app.post("/api/credit/calc")
def calc_credit(payload: dict, db: Session = Depends(get_db), user=Depends(require_roles("PROJECT_DEVELOPER","EXPERT","ADMIN"))):
    code = payload.get("project_code")
    if not code:
        raise HTTPException(400, "project_code required")
    trace = calc_carbon_credit(db, code)

    r = CalculationRun(
        run_type="CREDIT",
        total_kgco2e=trace["net_tco2e"] * 1000.0,
        total_tco2e=trace["net_tco2e"],
        details={"credit_trace": trace}
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return {"ok": True, "run_id": r.id, **trace}

# -------- Audit --------
@app.post("/api/audit/run/{run_id}")
def audit(run_id: int, db: Session = Depends(get_db), user=Depends(require_roles("AUDITOR","VERIFIER","ADMIN"))):
    return audit_run(db, run_id)

# -------- Report export --------
@app.get("/api/reports/run/{run_id}.pdf")
def report_pdf(run_id: int, db: Session = Depends(get_db), user=Depends(require_roles("AUDITOR","VERIFIER","EXPERT","ADMIN"))):
    data = export_run_pdf(db, run_id)
    return Response(content=data, media_type="application/pdf",
                    headers={"Content-Disposition": f"attachment; filename=run_{run_id}.pdf"})

@app.get("/api/reports/run/{run_id}.xlsx")
def report_xlsx(run_id: int, db: Session = Depends(get_db), user=Depends(require_roles("AUDITOR","VERIFIER","EXPERT","CALCULATOR","ADMIN"))):
    data = export_run_excel(db, run_id)
    return Response(content=data, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition": f"attachment; filename=run_{run_id}.xlsx"})

# -------- Dashboard --------
@app.get("/api/dashboard")
def dashboard(db: Session = Depends(get_db), user=Depends(require_roles("CALCULATOR","EXPERT","AUDITOR","VERIFIER","PROJECT_DEVELOPER","ADMIN"))):
    return {
        "counts": {
            "efs": db.query(EmissionFactor).count(),
            "activities": db.query(Activity).count(),
            "runs": db.query(CalculationRun).count(),
            "credit_projects": db.query(CarbonCreditProject).count(),
        }
    }
