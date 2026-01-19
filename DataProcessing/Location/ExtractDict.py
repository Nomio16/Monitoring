import requests
import pandas as pd
import time

# ---------------------------------------
# CONFIG
# ---------------------------------------
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

OUTPUT_FILE = "location_dictionary_merged.xlsx"
SLEEP_BETWEEN_QUERIES = 10  

# ---------------------------------------
# OVERPASS QUERIES
# ---------------------------------------
QUERIES = {
    "admin": """
    [out:json][timeout:180];
    area["ISO3166-1"="MN"][admin_level=2]->.mn;
    relation["boundary"="administrative"]["admin_level"~"4|6|8|10"]["name"](area.mn);
    out center tags;
    """,
    

    "railway": """
    [out:json][timeout:120];
    area["ISO3166-1"="MN"][admin_level=2]->.mn;
    (
    node["railway"="station"]["name"](area.mn);
    node["railway"="halt"]["name"](area.mn);
    );
    out body;
    """,

    "tourism": """
    [out:json][timeout:120];
    area["ISO3166-1"="MN"][admin_level=2]->.mn;
    node["tourism"~"camp_site|resort|guest_house|hotel"]["name"](area.mn);
    out body;
    """,

    "bus_stop": """
    [out:json][timeout:120];
    (
    node["highway"="bus_stop"](47.80,106.70,48.05,107.05);
    way["highway"="bus_stop"](47.80,106.70,48.05,107.05);

    node["public_transport"="platform"](47.80,106.70,48.05,107.05);
    way["public_transport"="platform"](47.80,106.70,48.05,107.05);

    node["public_transport"="stop_position"](47.80,106.70,48.05,107.05);
);
out center tags;
    """
}

# ---------------------------------------
# FETCH FUNCTION (SAFE)
# ---------------------------------------
def fetch_overpass(query):
    for url in OVERPASS_URLS:
        try:
            print(f"🔹 Querying {url}")
            r = requests.post(url, data=query, timeout=300)
            if r.status_code != 200:
                print("HTTP error:", r.status_code)
                continue
            return r.json()
        except Exception as e:
            print("Error:", e)
            continue
    raise RuntimeError(" All Overpass servers failed")

# ---------------------------------------
# PARSE ELEMENTS
# ---------------------------------------
def parse_elements(elements):
    rows = []

    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name")
        if not name:
            continue

        # -------- TYPE LOGIC --------
        loc_type = "other"

        if tags.get("boundary") == "administrative":
            lvl = tags.get("admin_level")
            loc_type = {
                "4": "aimag",
                "6": "sum",
                "8": "district",
                "10": "khoroo"
            }.get(lvl, "admin")

        elif tags.get("railway") in ["station", "halt"]:
            loc_type = "railway_station"

        elif "tourism" in tags:
            loc_type = "tourism"

        elif tags.get("highway") == "bus_stop" or tags.get("public_transport") in ["platform", "stop_position"]:
            loc_type = "bus_stop"

        # -------- LAT / LON --------
        lat = el.get("lat") or el.get("center", {}).get("lat")
        lon = el.get("lon") or el.get("center", {}).get("lon")

        rows.append({
            "raw_text": name.strip().lower(),
            "canonical": name.strip(),
            "type": loc_type,
            "lat": lat,
            "lon": lon,
            "source": "osm"
        })

    return rows

# ---------------------------------------
# MAIN PIPELINE
# ---------------------------------------
all_rows = []

for name, query in QUERIES.items():
    print(f"\n Running query: {name}")
    data = fetch_overpass(query)
    elements = data.get("elements", [])
    rows = parse_elements(elements)
    print(f"   ➕ extracted rows: {len(rows)}")
    all_rows.extend(rows)
    time.sleep(SLEEP_BETWEEN_QUERIES)

df = pd.DataFrame(all_rows)

print("\n Raw counts by type:")
print(df["type"].value_counts())

# ---------------------------------------
# CLEANING
# ---------------------------------------

# remove junk like "1", "1-р байр"
df = df[~df["canonical"].str.match(r"^\d+(-р)?$", na=False)]

# normalize name for dedup
df["norm_name"] = df["canonical"].str.lower().str.strip()

# ---------------------------------------
# DEDUPLICATION (CANONICAL NAME LEVEL)
# ---------------------------------------
dedup_rows = []

for name, g in df.groupby("norm_name"):
    dedup_rows.append({
        "raw_text": name,
        "canonical": g.iloc[0]["canonical"],
        "type": g.iloc[0]["type"],
        "lat": g["lat"].mean(),
        "lon": g["lon"].mean(),
        "source": "osm",
        "count": len(g)
    })

final_df = pd.DataFrame(dedup_rows)

print("\n After deduplication:")
print(final_df["type"].value_counts())
print("Total canonical locations:", len(final_df))

# ---------------------------------------
# SAVE
# ---------------------------------------
final_df.to_excel(OUTPUT_FILE, index=False)
print(f"\n Saved: {OUTPUT_FILE}")