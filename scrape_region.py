"""
Region Scraping Orchestrator.

Processes a region's beach JSON through the full pipeline:
  1. Load and validate data
  2. Assign sequential IDs
  3. Validate coordinates via Nominatim
  4. Generate CSV in reference format

Usage:
  python3 scrape_region.py --input input/region_name.json
  python3 scrape_region.py --input input/region_name.json --skip-coords
  python3 scrape_region.py --input input/region_name.json --country "Israel"
"""

import argparse
import json
import os
import sys

from beach_scraper import (
    process_region,
    load_beaches_json,
    CSV_COLUMNS,
    REQUIRED_FIELDS,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def print_summary(beaches):
    """Print a summary table of beaches before processing."""
    print(f"\n{'Beach Name':<40} {'Lat':>10} {'Lon':>12} {'Geo Area':<25}")
    print("-" * 90)
    for b in beaches:
        name = b.get("name", "?")[:38]
        lat = str(b.get("lat", ""))[:10]
        lon = str(b.get("lon", ""))[:12]
        geo = str(b.get("additional_info.geo_area", ""))[:23]
        print(f"{name:<40} {lat:>10} {lon:>12} {geo:<25}")
    print()


def count_filled_fields(beaches):
    """Count how many fields are filled across all beaches."""
    field_counts = {}
    for col in CSV_COLUMNS:
        if col == "":
            continue
        count = sum(1 for b in beaches if b.get(col, "") not in ("", None))
        field_counts[col] = count
    return field_counts


def print_field_coverage(beaches):
    """Print field fill rates."""
    counts = count_filled_fields(beaches)
    total = len(beaches)
    print(f"\n--- Field Coverage ({total} beaches) ---")
    for col, count in sorted(counts.items(), key=lambda x: -x[1]):
        pct = (count / total * 100) if total else 0
        bar = "#" * int(pct / 5)
        if col in REQUIRED_FIELDS:
            marker = " [REQUIRED]"
        else:
            marker = ""
        print(f"  {col:<45} {count:>4}/{total}  ({pct:5.1f}%) {bar}{marker}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Process a region's beach data")
    parser.add_argument("--input", required=True, help="Path to input JSON file")
    parser.add_argument("--country", help="Expected country name for coordinate validation")
    parser.add_argument("--skip-coords", action="store_true", help="Skip coordinate validation")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, don't write CSV or assign IDs")
    args = parser.parse_args()

    input_path = args.input
    if not os.path.isabs(input_path):
        input_path = os.path.join(BASE_DIR, input_path)

    if not os.path.exists(input_path):
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    # Load and preview
    beaches = load_beaches_json(input_path)
    print(f"\nLoaded {len(beaches)} beaches from {input_path}")
    print_summary(beaches)
    print_field_coverage(beaches)

    if args.dry_run:
        print("Dry run complete. No files written.")
        return

    # Process
    output_path, issues, coord_results = process_region(
        input_path,
        expected_country=args.country,
        skip_coord_validation=args.skip_coords,
    )

    if output_path:
        print(f"\nSuccess! CSV written to: {output_path}")

        # Print coordinate validation summary
        if coord_results:
            valid = sum(1 for r in coord_results if r["is_valid"])
            print(f"Coordinates: {valid}/{len(coord_results)} validated successfully")
            failures = [r for r in coord_results if not r["is_valid"]]
            if failures:
                print("\nCoordinate issues to review:")
                for r in failures:
                    print(f"  - {r['name']}: {r.get('warning', r.get('error', ''))}")
    else:
        print("\nProcessing failed. See errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
