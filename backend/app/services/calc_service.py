from __future__ import annotations
from sqlalchemy.orm import Session
from app.models import EmissionFactor, Activity
from app.services.gwp import resolve_gwp
from app.services.formula_engine import eval_expression

def _per_unit_co2e_from_gas_breakdown(ef: EmissionFactor) -> float:
    gb = ef.gas_breakdown or {}
    gases = gb.get("gases") or {}
    gwp = resolve_gwp(ef.gwp_version)
    per_unit = 0.0
    for gas, val in gases.items():
        g = gas.strip().upper()
        if g in gwp:
            per_unit += float(val) * float(gwp[g])
    return per_unit

def compute_activity_quantity(ef: EmissionFactor, inputs: dict) -> tuple[float, dict]:
    spec = ef.activity_id_fields or {}
    required = spec.get("required") or []
    formula = spec.get("formula")
    quantity_field = spec.get("quantity_field")

    for r in required:
        if r not in inputs:
            raise ValueError(f"Missing required input '{r}' for EF={ef.key}")

    if formula:
        expr = formula.get("expression")
        out = formula.get("output") or quantity_field or "quantity"
        q = eval_expression(expr, inputs)
        return q, {"method":"formula","expression":expr,"output":out,"quantity":q,"unit":formula.get("unit")}

    if quantity_field and quantity_field in inputs:
        q = float(inputs[quantity_field])
        return q, {"method":"quantity_field","field":quantity_field,"quantity":q}

    if required:
        q = float(inputs[required[0]])
        return q, {"method":"first_required","field":required[0],"quantity":q}

    if "amount" in inputs:
        q = float(inputs["amount"])
        return q, {"method":"fallback_amount","field":"amount","quantity":q}

    raise ValueError("No quantity derivation possible")

def compute_activity_kgco2e(db: Session, activity: Activity) -> tuple[float, dict]:
    ef = db.query(EmissionFactor).filter(EmissionFactor.key == activity.ef_key).one_or_none()
    if not ef:
        raise ValueError(f"EF not found: {activity.ef_key}")

    inputs = activity.inputs or {}
    qty, qtrace = compute_activity_quantity(ef, inputs)

    if ef.value is not None:
        kg = qty * float(ef.value)
        return kg, {"method":"direct_value","qty":qty,"ef_value":ef.value,"qtrace":qtrace,"ef_key":ef.key,"meta":ef.meta}

    per_unit = _per_unit_co2e_from_gas_breakdown(ef)
    kg = qty * per_unit
    return kg, {"method":"gas_breakdown","qty":qty,"per_unit_co2e":per_unit,"qtrace":qtrace,"ef_key":ef.key,"meta":ef.meta}

def compute_run(db: Session, activity_ids: list[int], run_type: str) -> dict:
    total = 0.0
    rows = []
    for aid in activity_ids:
        a = db.query(Activity).filter(Activity.id == aid).one_or_none()
        if not a:
            raise ValueError(f"Activity not found: {aid}")
        kg, trace = compute_activity_kgco2e(db, a)
        total += kg
        rows.append({"activity_id":a.id,"activity_name":a.name,"ef_key":a.ef_key,"inputs":a.inputs,"kgco2e":kg,"trace":trace})
    return {"run_type":run_type,"total_kgco2e":total,"total_tco2e":total/1000.0,"details":{"rows":rows}}
