"""
Beach Scraper Web Interface.

Flask app providing a UI for managing beach data:
- Dashboard with all regions
- Beach list/editor per region
- Data validation and coordinate checking
- Map view with Leaflet.js
- CSV export
"""

import csv
import io
import json
import os
import time
import threading

from flask import (
    Flask, render_template, request, jsonify, send_file, redirect, url_for
)

from beach_scraper import (
    CSV_COLUMNS, REQUIRED_FIELDS, BOOLEAN_FIELDS, TEXT_FIELDS,
    load_beaches_json, validate_all, assign_ids, validate_coordinates,
    write_csv, apply_defaults, beach_to_row, load_last_id, save_last_id,
    OUTPUT_DIR, ID_TRACKER_PATH,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "input")
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")

app = Flask(__name__, template_folder=TEMPLATE_DIR)
app.secret_key = "beach-scraper-dev"

# --- Helpers ---

def get_regions():
    """List all region JSON files in input/."""
    regions = []
    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR, exist_ok=True)
        return regions
    for fname in sorted(os.listdir(INPUT_DIR)):
        if fname.endswith(".json"):
            path = os.path.join(INPUT_DIR, fname)
            try:
                beaches = load_beaches_json(path)
                region_name = os.path.splitext(fname)[0]
                country = beaches[0].get("country", "") if beaches else ""
                csv_exists = os.path.exists(os.path.join(OUTPUT_DIR, f"{region_name}.csv"))
                regions.append({
                    "name": region_name,
                    "filename": fname,
                    "country": country,
                    "beach_count": len(beaches),
                    "csv_exists": csv_exists,
                })
            except Exception:
                pass
    return regions


def load_region(region_name):
    """Load beaches for a region."""
    path = os.path.join(INPUT_DIR, f"{region_name}.json")
    if not os.path.exists(path):
        return None
    return load_beaches_json(path)


def save_region(region_name, beaches):
    """Save beaches to a region JSON file."""
    path = os.path.join(INPUT_DIR, f"{region_name}.json")
    os.makedirs(INPUT_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"beaches": beaches}, f, indent=2, ensure_ascii=False)


# --- Routes ---

@app.route("/")
def dashboard():
    regions = get_regions()
    last_id = load_last_id()
    return render_template("dashboard.html", regions=regions, last_id=last_id)


@app.route("/region/<region_name>")
def region_view(region_name):
    beaches = load_region(region_name)
    if beaches is None:
        return "Region not found", 404
    # Get field metadata
    fields = []
    for col in CSV_COLUMNS:
        if col == "":
            continue
        fields.append({
            "name": col,
            "required": col in REQUIRED_FIELDS,
            "boolean": col in BOOLEAN_FIELDS,
            "auto": col in ("beach_id", "__v"),
        })
    return render_template(
        "region.html",
        region_name=region_name,
        beaches=beaches,
        fields=fields,
        csv_columns=CSV_COLUMNS,
    )


@app.route("/region/<region_name>/map")
def region_map(region_name):
    beaches = load_region(region_name)
    if beaches is None:
        return "Region not found", 404
    # Filter to beaches with coordinates
    map_beaches = [
        {"name": b.get("name", ""), "lat": b.get("lat"), "lon": b.get("lon"),
         "city": b.get("city", ""), "geo_area": b.get("additional_info.geo_area", "")}
        for b in beaches if b.get("lat") and b.get("lon")
    ]
    return render_template("map.html", region_name=region_name, beaches=json.dumps(map_beaches))


@app.route("/api/region", methods=["POST"])
def create_region():
    """Create a new region from JSON data."""
    data = request.get_json()
    region_name = data.get("name", "").strip().lower().replace(" ", "_")
    beaches = data.get("beaches", [])
    if not region_name:
        return jsonify({"error": "Region name required"}), 400
    if not beaches:
        return jsonify({"error": "At least one beach required"}), 400
    save_region(region_name, beaches)
    return jsonify({"success": True, "region": region_name, "count": len(beaches)})


@app.route("/api/region/<region_name>/save", methods=["POST"])
def save_region_data(region_name):
    """Save edited beach data for a region."""
    data = request.get_json()
    beaches = data.get("beaches", [])
    save_region(region_name, beaches)
    return jsonify({"success": True, "count": len(beaches)})


@app.route("/api/region/<region_name>/validate", methods=["POST"])
def validate_region(region_name):
    """Validate beach data for a region."""
    beaches = load_region(region_name)
    if beaches is None:
        return jsonify({"error": "Region not found"}), 404
    issues, has_errors = validate_all(beaches)
    return jsonify({
        "issues": [{"level": level, "message": msg} for level, msg in issues],
        "has_errors": has_errors,
        "beach_count": len(beaches),
    })


@app.route("/api/region/<region_name>/validate-coords", methods=["POST"])
def validate_coords(region_name):
    """Validate coordinates for a single beach."""
    data = request.get_json()
    beach_index = data.get("index", 0)
    beaches = load_region(region_name)
    if beaches is None or beach_index >= len(beaches):
        return jsonify({"error": "Beach not found"}), 404
    beach = beaches[beach_index]
    country = data.get("country", beach.get("country", ""))
    is_valid, details = validate_coordinates(beach, country)
    return jsonify({"is_valid": is_valid, "details": details})


@app.route("/api/region/<region_name>/generate-csv", methods=["POST"])
def generate_csv(region_name):
    """Assign IDs and generate CSV for a region."""
    beaches = load_region(region_name)
    if beaches is None:
        return jsonify({"error": "Region not found"}), 404

    # Validate first
    issues, has_errors = validate_all(beaches)
    if has_errors:
        return jsonify({"error": "Fix validation errors first", "issues": issues}), 400

    # Assign IDs if not already assigned
    if not beaches[0].get("beach_id"):
        beaches = assign_ids(beaches)
        save_region(region_name, beaches)

    # Write CSV
    output_path = os.path.join(OUTPUT_DIR, f"{region_name}.csv")
    write_csv(beaches, output_path)

    return jsonify({
        "success": True,
        "path": output_path,
        "count": len(beaches),
        "id_range": f"{beaches[0]['beach_id']}-{beaches[-1]['beach_id']}",
    })


@app.route("/api/region/<region_name>/download-csv")
def download_csv(region_name):
    """Download the generated CSV file."""
    output_path = os.path.join(OUTPUT_DIR, f"{region_name}.csv")
    if not os.path.exists(output_path):
        return "CSV not generated yet", 404
    return send_file(
        output_path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"{region_name}_beaches.csv",
    )


@app.route("/api/region/<region_name>/delete", methods=["POST"])
def delete_region(region_name):
    """Delete a region's input JSON (not the CSV)."""
    path = os.path.join(INPUT_DIR, f"{region_name}.json")
    if os.path.exists(path):
        os.remove(path)
    return jsonify({"success": True})


@app.route("/api/region/<region_name>/add-beach", methods=["POST"])
def add_beach(region_name):
    """Add a new empty beach to a region."""
    beaches = load_region(region_name)
    if beaches is None:
        beaches = []
    data = request.get_json() or {}
    new_beach = {
        "country": data.get("country", ""),
        "city": data.get("city", ""),
        "name": data.get("name", "New Beach"),
        "additional_info.geo_area": data.get("geo_area", ""),
        "lat": data.get("lat", ""),
        "lon": data.get("lon", ""),
    }
    beaches.append(new_beach)
    save_region(region_name, beaches)
    return jsonify({"success": True, "index": len(beaches) - 1})


@app.route("/api/region/<region_name>/remove-beach", methods=["POST"])
def remove_beach(region_name):
    """Remove a beach by index."""
    data = request.get_json()
    index = data.get("index")
    beaches = load_region(region_name)
    if beaches is None or index is None or index >= len(beaches):
        return jsonify({"error": "Invalid index"}), 400
    removed = beaches.pop(index)
    save_region(region_name, beaches)
    return jsonify({"success": True, "removed": removed.get("name", "")})


# --- Beach Discovery via Overpass API ---

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

def discover_beaches_osm(country, region=""):
    """
    Query OpenStreetMap Overpass API for all beaches in a country/region.
    Returns a list of beach dicts with name, lat, lon, and available tags.
    """
    import requests

    # Build Overpass query - search for beaches tagged as natural=beach
    # within the country boundary
    search_area = region if region and region.lower() != country.lower() else country

    query = f"""
    [out:json][timeout:60];
    area["name:en"="{country}"]["admin_level"="2"]->.country;
    (
      node["natural"="beach"](area.country);
      way["natural"="beach"](area.country);
      relation["natural"="beach"](area.country);
    );
    out center tags;
    """

    try:
        resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=90)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        # Try alternative query with local name
        query_alt = f"""
        [out:json][timeout:60];
        area["name"="{country}"]["admin_level"="2"]->.country;
        (
          node["natural"="beach"](area.country);
          way["natural"="beach"](area.country);
          relation["natural"="beach"](area.country);
        );
        out center tags;
        """
        try:
            resp = requests.post(OVERPASS_URL, data={"data": query_alt}, timeout=90)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e2:
            return [], f"Overpass API error: {str(e2)}"

    elements = data.get("elements", [])
    beaches = []
    seen_names = set()

    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name", tags.get("name:en", ""))
        if not name:
            continue  # Skip unnamed beaches

        # Deduplicate by name
        if name.lower() in seen_names:
            continue
        seen_names.add(name.lower())

        # Get coordinates (center for ways/relations)
        lat = el.get("lat") or el.get("center", {}).get("lat")
        lon = el.get("lon") or el.get("center", {}).get("lon")
        if not lat or not lon:
            continue

        # Map OSM tags to our schema
        beach = {
            "country": country,
            "city": region or country,
            "name": name,
            "lat": round(float(lat), 6),
            "lon": round(float(lon), 6),
            "additional_info.geo_area": tags.get("addr:district", tags.get("addr:city", region or "")),
            "additional_info.sandy": "TRUE" if tags.get("surface") == "sand" else "",
            "additional_info.dog_friendly": "TRUE" if tags.get("dog") == "yes" else ("FALSE" if tags.get("dog") == "no" else ""),
            "accessible": "TRUE" if tags.get("wheelchair") == "yes" else ("FALSE" if tags.get("wheelchair") == "no" else ""),
            "disabilities_status": "TRUE" if tags.get("wheelchair") == "yes" else ("FALSE" if tags.get("wheelchair") == "no" else ""),
            "additional_info.fee": "TRUE" if tags.get("fee") == "yes" else ("FALSE" if tags.get("fee") == "no" else ""),
            "additional_info.parking": "TRUE" if tags.get("parking") else "",
            "additional_info.bathroom": "TRUE" if tags.get("toilets") == "yes" else "",
            "additional_info.beach_phone": tags.get("phone", tags.get("contact:phone", "")),
            "blue_flag": "TRUE" if tags.get("flag:type") == "blue_flag" or "blue flag" in tags.get("description", "").lower() else "FALSE",
        }

        # Add website as photo if available
        website = tags.get("website", tags.get("url", ""))
        if website:
            beach["_website"] = website

        beaches.append(beach)

    # Sort by latitude (north to south)
    beaches.sort(key=lambda b: -float(b["lat"]))

    return beaches, None


# Track discovery jobs
discovery_jobs = {}


@app.route("/api/region/<region_name>/discover", methods=["POST"])
def discover_beaches(region_name):
    """Discover beaches using OpenStreetMap Overpass API."""
    data = request.get_json()
    country = data.get("country", "")
    region = data.get("region", region_name.replace("_", " "))

    if not country:
        return jsonify({"error": "Country name required"}), 400

    # Run discovery
    beaches, error = discover_beaches_osm(country, region)

    if error:
        return jsonify({"error": error}), 500

    if not beaches:
        return jsonify({"error": f"No beaches found for '{country}'. Try the English country name (e.g., 'Israel' not 'ישראל')."}), 404

    # Merge with existing beaches (keep existing, add new)
    existing = load_region(region_name) or []
    existing_names = {b.get("name", "").lower() for b in existing}
    new_beaches = [b for b in beaches if b["name"].lower() not in existing_names]

    merged = existing + new_beaches if existing else beaches
    # Remove the placeholder "New Beach" entry
    merged = [b for b in merged if b.get("name") != "New Beach"]

    save_region(region_name, merged)

    return jsonify({
        "success": True,
        "discovered": len(beaches),
        "new": len(new_beaches),
        "total": len(merged),
    })


@app.route("/api/upload-json", methods=["POST"])
def upload_json():
    """Upload a JSON file for a region."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files["file"]
    if not file.filename.endswith(".json"):
        return jsonify({"error": "Must be a .json file"}), 400
    region_name = os.path.splitext(file.filename)[0].strip().lower().replace(" ", "_")
    content = json.loads(file.read().decode("utf-8"))
    if isinstance(content, list):
        beaches = content
    elif isinstance(content, dict) and "beaches" in content:
        beaches = content["beaches"]
    else:
        return jsonify({"error": "Invalid JSON format"}), 400
    save_region(region_name, beaches)
    return jsonify({"success": True, "region": region_name, "count": len(beaches)})


if __name__ == "__main__":
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    port = int(os.environ.get("PORT", 5555))
    debug = os.environ.get("FLASK_DEBUG", "1") == "1"
    print(f"\n  Beach Scraper Web Interface")
    print(f"  http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=debug)
