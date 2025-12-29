from app.seed.base import EFSeedItem, SeedMeta

META = SeedMeta(
  source="TGO Thailand",
  year=2022,
  version="2022-07",
  dataset="CFP Emission Factor",
  reference="Emission_Factor CFP 1 ก.ค. 65.pdf"
)

def items():
  yield EFSeedItem(
    key="th_tgo_electricity_gridmix_2016_2018_kwh",
    name="Electricity, grid mix (Thailand 2016–2018)",
    unit="kWh",
    value=0.5986,
    scope="Scope2",
    category="Electricity",
    tags=["th","tgo","electricity","gridmix"],
    meta=META,
    region="TH",
    activity_id_fields={
      "required":["amount"],
      "fields":{"amount":{"label":"Electricity use","type":"number","unit":"kWh"}},
      "quantity_field":"amount"
    },
    methodology="CFP (published by TGO)",
    gwp_version="IPCC_2013_GWP100",
    publisher="TGO",
    document_title="Emission Factor CFP (1 ก.ค. 65)",
    valid_from="2023-01-01"
  )

  yield EFSeedItem(
    key="th_tgo_diesel_kg",
    name="Diesel (น้ำมันดีเซล / น้ำมันโซล่าร์)",
    unit="kg",
    value=0.3522,
    scope="Scope1",
    category="Fuel",
    tags=["th","tgo","fuel","diesel"],
    meta=META,
    region="TH",
    activity_id_fields={
      "required":["amount"],
      "fields":{"amount":{"label":"Diesel mass","type":"number","unit":"kg"}},
      "quantity_field":"amount"
    },
    methodology="CFP (published by TGO)",
    gwp_version="IPCC_2013_GWP100",
    publisher="TGO",
    document_title="Emission Factor CFP (1 ก.ค. 65)",
    valid_from="2023-01-01"
  )

  yield EFSeedItem(
    key="th_tgo_truck_van_4w_small_normal_0load_km",
    name="Truck: van 4 wheels small, normal driving, 0% loading (≤1.5 ton)",
    unit="km",
    value=0.2415,
    scope="Scope3",
    category="Transport",
    tags=["th","tgo","transport","truck"],
    meta=META,
    region="TH",
    activity_id_fields={
      "required":["distance_km"],
      "fields":{"distance_km":{"label":"Distance","type":"number","unit":"km"}},
      "quantity_field":"distance_km"
    },
    methodology="CFP (published by TGO)",
    gwp_version="IPCC_2013_GWP100",
    publisher="TGO",
    document_title="Emission Factor CFP (1 ก.ค. 65)",
    valid_from="2023-01-01"
  )

  # Formula demo: tkm
  yield EFSeedItem(
    key="demo_truck_tkm_formula",
    name="Demo Truck EF per ton-km (tkm) (formula example)",
    unit="tkm",
    value=0.12,
    scope="Scope3",
    category="Transport",
    tags=["demo","transport","formula"],
    meta=META,
    activity_id_fields={
      "required":["distance_km","payload_ton"],
      "fields":{
        "distance_km":{"label":"Distance","type":"number","unit":"km"},
        "payload_ton":{"label":"Payload","type":"number","unit":"ton"},
        "load_factor":{"label":"Load factor","type":"number","unit":"","default":1.0}
      },
      "formula":{"output":"tkm","expression":"distance_km * payload_ton * load_factor","unit":"tkm"},
      "quantity_field":"tkm"
    }
  )
