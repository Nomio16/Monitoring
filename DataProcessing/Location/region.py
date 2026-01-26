from geopy.geocoders import Nominatim
from tqdm import tqdm
import json
import time

# =========================
# ТОХИРУУЛГА
# =========================
INPUT_FILE = "location_dictionary.json"
OUTPUT_FILE = "location_dictionary_updated.json"
FAILED_FILE = "reverse_failed.json"
SLEEP_SECONDS = 1.1

# Nominatim тохиргоо
geolocator = Nominatim(
    user_agent="mongolia_location_enricher_v1",
    timeout=10
)

# Cache (давтагдсан координатад дахин request хийхгүй)
cache = {}
failed = []

# =========================
# ТУСЛАХ ФУНКЦ
# =========================
def is_valid_coord(lat, lon):
    try:
        lat = float(lat)
        lon = float(lon)
        return -90 <= lat <= 90 and -180 <= lon <= 180
    except Exception:
        return False


def get_region_info(lat, lon):
    """
    lat/lon → (aimag, sum_duureg)
    """
    key = f"{lat},{lon}"

    # Cache шалгах
    if key in cache:
        return cache[key]

    try:
        location = geolocator.reverse(
            (lat, lon),
            language="mn",
            addressdetails=True
        )

        if not location:
            result = ("Тодорхойгүй", "Тодорхойгүй")
        else:
            address = location.raw.get("address", {})

            # Аймаг / Хот
            aimag = (
                address.get("state")
                or address.get("province")
                or address.get("region")
                or address.get("city")
            )

            # Сум / Дүүрэг
            sum_duureg = (
                address.get("county")
                or address.get("district")
                or address.get("suburb")
                or address.get("town")
                or address.get("village")
            )

            result = (aimag, sum_duureg)

        cache[key] = result
        return result

    except Exception as e:
        failed.append({
            "lat": lat,
            "lon": lon,
            "error": str(e)
        })
        return None, None


# =========================
# ҮНДСЭН ПРОЦЕСС
# =========================
print("📍 Байршлын мэдээлэл тодорхойлж байна...")

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

for location_key in tqdm(data.keys(), desc="Боловсруулж байна"):
    lat = data[location_key].get("lat")
    lon = data[location_key].get("lon")

    # Координат шалгах
    if not is_valid_coord(lat, lon):
        data[location_key]["aimag"] = None
        data[location_key]["sum"] = None
        continue

    cache_key = f"{lat},{lon}"
    is_new_request = cache_key not in cache

    aimag, sum_val = get_region_info(lat, lon)

    data[location_key]["aimag"] = aimag
    data[location_key]["sum"] = sum_val

    # Rate limit (зөвхөн шинэ request дээр)
    if is_new_request:
        time.sleep(SLEEP_SECONDS)

# =========================
# ФАЙЛУУД ХАДГАЛАХ
# =========================
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

if failed:
    with open(FAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(failed, f, ensure_ascii=False, indent=2)

print("Амжилттай дууслаа!")
print(f"Үр дүн: {OUTPUT_FILE}")
print(f"Алдаатай координат: {len(failed)}")
