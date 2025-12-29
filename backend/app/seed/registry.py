from __future__ import annotations
import importlib, pkgutil
from typing import List, Tuple
from .base import EFSeedItem

def discover(package: str = "app.seed.sources") -> list[str]:
    pkg = importlib.import_module(package)
    prefix = pkg.__name__ + "."
    mods = []
    for m in pkgutil.walk_packages(pkg.__path__, prefix=prefix):
        if not m.ispkg and not m.name.split(".")[-1].startswith("_"):
            mods.append(m.name)
    return mods

def load_all(package: str = "app.seed.sources") -> Tuple[List[EFSeedItem], List[str]]:
    warnings, items = [], []
    for modname in discover(package):
        try:
            m = importlib.import_module(modname)
            if hasattr(m, "items"):
                items.extend(list(m.items()))
            else:
                warnings.append(f"{modname} missing items()")
        except Exception as e:
            warnings.append(f"Failed import {modname}: {e}")
    dedup = {it.key: it for it in items}
    return list(dedup.values()), warnings
