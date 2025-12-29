GWP = {
    "IPCC_AR5": {"CO2": 1.0, "CH4": 28.0, "N2O": 265.0},
    "IPCC_AR6": {"CO2": 1.0, "CH4": 27.2, "N2O": 273.0},
    "IPCC_2013_GWP100": {"CO2": 1.0, "CH4": 28.0, "N2O": 265.0},
}

def resolve_gwp(gwp_version: str | None) -> dict:
    if not gwp_version:
        return GWP["IPCC_AR5"]
    k = gwp_version.strip().upper().replace(" ", "_")
    return GWP.get(k, GWP["IPCC_AR5"])
