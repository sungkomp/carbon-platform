from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict

@dataclass(frozen=True)
class SeedMeta:
    source: str
    year: int
    version: str
    dataset: str
    reference: str
    license_note: str = ""

@dataclass(frozen=True)
class EFSeedItem:
    key: str
    name: str
    unit: str
    value: float | None
    scope: str
    category: str
    tags: list[str]
    meta: SeedMeta

    activity_id_fields: Dict[str, Any] | None = None
    gas_breakdown: Dict[str, Any] | None = None
    methodology: str | None = None
    gwp_version: str | None = None
    publisher: str | None = None
    document_title: str | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    region: str | None = None

    def as_dict(self) -> dict:
        return {
            "key": self.key,
            "name": self.name,
            "unit": self.unit,
            "value": self.value,
            "scope": self.scope,
            "category": self.category,
            "tags": self.tags,
            "activity_id_fields": self.activity_id_fields or {},
            "gas_breakdown": self.gas_breakdown or {},
            "methodology": self.methodology,
            "gwp_version": self.gwp_version,
            "publisher": self.publisher,
            "document_title": self.document_title,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
            "region": self.region,
            "meta": {
                "source": self.meta.source,
                "year": self.meta.year,
                "version": self.meta.version,
                "dataset": self.meta.dataset,
                "reference": self.meta.reference,
                "license_note": self.meta.license_note,
            },
        }
