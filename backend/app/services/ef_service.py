from sqlalchemy.orm import Session
from app.models import EmissionFactor
from app.seed import all_seed_items

def upsert_seed_efs(db: Session):
    items, warnings = all_seed_items()
    upserted = 0
    for it in items:
        data = it.as_dict()
        key = data["key"]
        obj = db.query(EmissionFactor).filter(EmissionFactor.key == key).one_or_none()
        if obj:
            for k, v in data.items():
                setattr(obj, k, v)
        else:
            db.add(EmissionFactor(**data))
        upserted += 1
    db.commit()
    return upserted, warnings
