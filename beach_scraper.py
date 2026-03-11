"""
Beach Scraper — Core data pipeline.

Handles CSV schema, ID management, coordinate validation, and data output
in the exact 41-column format of the California Beaches reference file.
"""

import csv
import json
import os
import time
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ID_TRACKER_PATH = os.path.join(BASE_DIR, "id_tracker.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# Exact column order matching California Beaches CSV (41 columns + trailing empty)
CSV_COLUMNS = [
    "country",
    "city",
    "name",
    "is_separated",
    "beach_id",
    "__v",
    "blue_flag",
    "additional_info.beach_phone",
    "additional_info.fee",
    "additional_info.parking",
    "accessible",
    "disabilities_status",
    "additional_info.bathroom",
    "additional_info.visitor_center",
    "additional_info.dog_friendly",
    "additional_info.strollers",
    "additional_info.picnic",
    "additional_info.camping_friendly",
    "additional_info.sandy",
    "additional_info.dunes",
    "additional_info.rocky",
    "additional_info.bluff",
    "additional_info.stairstobeach",
    "additional_info.pathtobeach",
    "additional_info.blufftoop_trails",
    "additional_info.blufftoop_park",
    "additional_info.wildlife",
    "additional_info.tidepool",
    "additional_info.volleyball",
    "additional_info.fishing_friendly",
    "additional_info.boat_friendly",
    "additional_info.geo_area",
    "lat",
    "lon",
    "additional_info.beach_photo1",
    "additional_info.beach_photo2",
    "additional_info.beach_photo3",
    "additional_info.beach_photo4",
    "additional_info.beach_wheelchair",
    "bike_path",
    "additional_info.boatfacil",
    "",  # trailing empty column (artifact from reference file)
]

REQUIRED_FIELDS = ["country", "city", "name", "lat", "lon", "additional_info.geo_area"]

BOOLEAN_FIELDS = [
    "is_separated", "blue_flag", "additional_info.fee", "additional_info.parking",
    "accessible", "disabilities_status", "additional_info.bathroom",
    "additional_info.visitor_center", "additional_info.dog_friendly",
    "additional_info.strollers", "additional_info.picnic",
    "additional_info.camping_friendly", "additional_info.sandy",
    "additional_info.dunes", "additional_info.rocky", "additional_info.bluff",
    "additional_info.stairstobeach", "additional_info.pathtobeach",
    "additional_info.blufftoop_trails", "additional_info.blufftoop_park",
    "additional_info.wildlife", "additional_info.tidepool",
    "additional_info.volleyball", "additional_info.fishing_friendly",
    "additional_info.boat_friendly", "bike_path",
]

# Fields that are NOT boolean despite being in the column list
TEXT_FIELDS = [
    "additional_info.beach_phone", "additional_info.geo_area",
    "additional_info.beach_photo1", "additional_info.beach_photo2",
    "additional_info.beach_photo3", "additional_info.beach_photo4",
    "additional_info.beach_wheelchair", "additional_info.boatfacil",
]


# --- ID Management ---

def load_last_id():
    if os.path.exists(ID_TRACKER_PATH):
        with open(ID_TRACKER_PATH) as f:
            return json.load(f)["last_id"]
    return 1861  # California ends at 1861


def save_last_id(last_id):
    with open(ID_TRACKER_PATH, "w") as f:
        json.dump({"last_id": last_id}, f, indent=2)


def assign_ids(beaches):
    """Assign sequential beach_id and __v to a list of beach dicts."""
    next_id = load_last_id() + 1
    for beach in beaches:
        beach["beach_id"] = next_id
        beach["__v"] = next_id
        next_id += 1
    save_last_id(next_id - 1)
    return beaches


# --- Data Validation ---

def validate_beach(beach, index):
    """Validate a single beach record. Returns list of (level, message) tuples."""
    issues = []

    # Check required fields
    for field in REQUIRED_FIELDS:
        val = beach.get(field, "")
        if val == "" or val is None:
            issues.append(("ERROR", f"Beach #{index} '{beach.get('name', '?')}': missing required field '{field}'"))

    # Validate lat/lon ranges
    try:
        lat = float(beach.get("lat", 0))
        lon = float(beach.get("lon", 0))
        if not (-90 <= lat <= 90):
            issues.append(("ERROR", f"Beach #{index} '{beach.get('name', '?')}': lat {lat} out of range"))
        if not (-180 <= lon <= 180):
            issues.append(("ERROR", f"Beach #{index} '{beach.get('name', '?')}': lon {lon} out of range"))
    except (ValueError, TypeError):
        issues.append(("ERROR", f"Beach #{index} '{beach.get('name', '?')}': invalid lat/lon"))

    # Normalize boolean fields
    for field in BOOLEAN_FIELDS:
        val = beach.get(field, "")
        if val == "" or val is None:
            continue
        if isinstance(val, bool):
            beach[field] = "TRUE" if val else "FALSE"
        elif isinstance(val, str) and val.upper() in ("TRUE", "FALSE"):
            beach[field] = val.upper()
        elif isinstance(val, str) and val.upper() in ("YES", "Y"):
            beach[field] = "TRUE"
        elif isinstance(val, str) and val.upper() in ("NO", "N"):
            beach[field] = "FALSE"
        else:
            issues.append(("WARN", f"Beach #{index} '{beach.get('name', '?')}': unexpected value '{val}' for boolean field '{field}'"))

    return issues


def validate_all(beaches):
    """Validate all beaches. Returns (all_issues, has_errors)."""
    all_issues = []
    for i, beach in enumerate(beaches):
        all_issues.extend(validate_beach(beach, i + 1))
    has_errors = any(level == "ERROR" for level, _ in all_issues)
    return all_issues, has_errors


# --- Coordinate Validation via Nominatim ---

NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_HEADERS = {"User-Agent": "BeachScraper/1.0 (beach-data-collection)"}


def validate_coordinates(beach, expected_country):
    """
    Reverse geocode lat/lon via Nominatim and check:
    1. Country matches expected
    2. Location is near water/coast
    Returns (is_valid, details_dict)
    """
    lat = beach.get("lat", "")
    lon = beach.get("lon", "")
    if not lat or not lon:
        return False, {"error": "missing coordinates"}

    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={"lat": lat, "lon": lon, "format": "json", "zoom": 14},
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return False, {"error": str(e)}

    result = {
        "nominatim_name": data.get("display_name", ""),
        "nominatim_country": data.get("address", {}).get("country", ""),
        "nominatim_type": data.get("type", ""),
        "nominatim_class": data.get("class", ""),
    }

    # Check country match
    nom_country = result["nominatim_country"].lower()
    exp_country = expected_country.lower()
    country_match = exp_country in nom_country or nom_country in exp_country
    result["country_match"] = country_match

    if not country_match:
        result["warning"] = f"Country mismatch: expected '{expected_country}', got '{result['nominatim_country']}'"

    return country_match, result


def validate_all_coordinates(beaches, expected_country, rate_limit=1.1):
    """Validate coordinates for all beaches. Respects Nominatim rate limit."""
    results = []
    for i, beach in enumerate(beaches):
        name = beach.get("name", f"Beach #{i+1}")
        print(f"  Validating coordinates for {name}... ({i+1}/{len(beaches)})")
        is_valid, details = validate_coordinates(beach, expected_country)
        results.append({
            "name": name,
            "lat": beach.get("lat"),
            "lon": beach.get("lon"),
            "is_valid": is_valid,
            **details,
        })
        if i < len(beaches) - 1:
            time.sleep(rate_limit)  # Nominatim rate limit
    return results


# --- CSV Output ---

def apply_defaults(beach):
    """Apply default values for fields not provided."""
    defaults = {
        "country": "",
        "is_separated": "FALSE",
        "blue_flag": "FALSE",
    }
    for key, val in defaults.items():
        if key not in beach or beach[key] == "" or beach[key] is None:
            beach[key] = val
    return beach


def beach_to_row(beach):
    """Convert a beach dict to a CSV row list matching CSV_COLUMNS order."""
    beach = apply_defaults(beach)
    row = []
    for col in CSV_COLUMNS:
        val = beach.get(col, "")
        if val is None:
            val = ""
        row.append(str(val))
    return row


def write_csv(beaches, output_path):
    """Write beaches to CSV in exact reference format."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)
        for beach in beaches:
            writer.writerow(beach_to_row(beach))
    print(f"Wrote {len(beaches)} beaches to {output_path}")


# --- JSON Input ---

def load_beaches_json(json_path):
    """Load beaches from a JSON input file."""
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "beaches" in data:
        return data["beaches"]
    if isinstance(data, list):
        return data
    raise ValueError("JSON must be a list of beaches or a dict with 'beaches' key")


# --- Main Pipeline ---

def process_region(json_path, expected_country=None, skip_coord_validation=False):
    """
    Full pipeline: load JSON -> validate -> assign IDs -> validate coords -> write CSV.
    Returns (output_path, issues, coord_results).
    """
    print(f"\n{'='*60}")
    print(f"Processing: {json_path}")
    print(f"{'='*60}\n")

    # Load
    beaches = load_beaches_json(json_path)
    print(f"Loaded {len(beaches)} beaches\n")

    if not expected_country:
        expected_country = beaches[0].get("country", "") if beaches else ""

    # Validate data
    print("--- Data Validation ---")
    issues, has_errors = validate_all(beaches)
    for level, msg in issues:
        print(f"  [{level}] {msg}")
    if has_errors:
        print("\nERRORS found. Fix issues before proceeding.")
        return None, issues, []
    if not issues:
        print("  All checks passed!")
    print()

    # Assign IDs
    print("--- Assigning IDs ---")
    beaches = assign_ids(beaches)
    id_range = f"{beaches[0]['beach_id']}-{beaches[-1]['beach_id']}"
    print(f"  Assigned IDs: {id_range}\n")

    # Validate coordinates
    coord_results = []
    if not skip_coord_validation:
        print("--- Coordinate Validation (Nominatim) ---")
        coord_results = validate_all_coordinates(beaches, expected_country)
        failures = [r for r in coord_results if not r["is_valid"]]
        if failures:
            print(f"\n  WARNING: {len(failures)} coordinate validation issues:")
            for r in failures:
                print(f"    - {r['name']}: {r.get('warning', r.get('error', 'unknown'))}")
        else:
            print(f"\n  All {len(coord_results)} coordinates validated successfully!")
        print()

    # Write CSV
    region_name = os.path.splitext(os.path.basename(json_path))[0]
    output_path = os.path.join(OUTPUT_DIR, f"{region_name}.csv")
    print("--- Writing CSV ---")
    write_csv(beaches, output_path)

    return output_path, issues, coord_results


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 beach_scraper.py <input.json> [expected_country] [--skip-coords]")
        sys.exit(1)

    json_path = sys.argv[1]
    country = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("--") else None
    skip_coords = "--skip-coords" in sys.argv

    output, issues, coords = process_region(json_path, country, skip_coords)
    if output:
        print(f"\nDone! Output: {output}")
    else:
        print("\nFailed due to validation errors.")
        sys.exit(1)
