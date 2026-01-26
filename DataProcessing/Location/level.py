import json
from collections import defaultdict

INPUT = "location_dictionary_updated.json"

level_data = {
    1: {},
    2: {},

    
    3: {},
    4: {},
    5: [],
    6: []
}

def norm(text):
    return text.strip()

with open(INPUT, encoding="utf-8") as f:
    raw = json.load(f)

# === Level 1 (Монгол Улс) ===
level_data[1]["Монгол Улс"] = {
    "canonical": "Монгол Улс",
    "level": 1,
    "parent": None,
    "lat": 46.8625,
    "lon": 103.8467,
    "location_type": "country"
}

for _, v in raw.items():
    canonical = norm(v["canonical"])
    ltype = v.get("type")
    lat = v.get("lat")
    lon = v.get("lon")

    # --- Level 2 ---
    if "2" in v:
        aimag_city = norm(v["2"])
        level_data[2][aimag_city] = {
            "canonical": aimag_city,
            "level": 2,
            "parent": {"canonical": "Монгол Улс", "level": 1},
            "lat": None,
            "lon": None,
            "location_type": "city"
        }

    # --- Level 3 ---
    if "3" in v:
        district = norm(v["3"])
        level_data[3][district] = {
            "canonical": district,
            "level": 3,
            "parent": {"canonical": aimag_city, "level": 2},
            "lat": None,
            "lon": None,
            "location_type": "district"
        }

    # --- Level 4 ---
    if "4" in v:
        khoroo = f"{district} {norm(v['4'])}"
        level_data[4][khoroo] = {
            "canonical": khoroo,
            "level": 4,
            "parent": {"canonical": district, "level": 3}
        }

    # --- Level 5 ---
    parent_lvl4 = khoroo if "4" in v else district
    level_data[5].append({
        "canonical": canonical,
        "level": 5,
        "parent": {
            "canonical": parent_lvl4,
            "level": 4 if "4" in v else 3
        },
        "lat": lat,
        "lon": lon,
        "location_type": ltype
    })

    # --- Level 6 ---
    for alias in v.get("aliases", []):
        level_data[6].append({
            "canonical": norm(alias),
            "level": 6,
            "parent": {"canonical": canonical, "level": 5}
        })

# === WRITE FILES ===
for lvl in range(1, 7):
    out = f"level_{lvl}.json"
    data = list(level_data[lvl].values()) if isinstance(level_data[lvl], dict) else level_data[lvl]
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

print("✅ 6 JSON файл үүслээ")
