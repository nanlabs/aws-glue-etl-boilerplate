#!/usr/bin/env python3
"""
Script para obtener y analizar Custom Fields de TeamTailor API.

Este script obtiene los custom-field-values desde las relaciones
para entender su estructura real.
"""

import json
import os
import sys
from typing import Any, Dict, List

# Add libs to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "libs"))

from libs.common.teamtailor import TeamTailorAPIClient

# Load credentials from environment
API_TOKEN = os.getenv("TEAMTAILOR_API_TOKEN")
API_BASE_URL = os.getenv("TEAMTAILOR_API_BASE_URL", "https://api.teamtailor.com")

if not API_TOKEN:
    print("❌ ERROR: TEAMTAILOR_API_TOKEN not found in environment")
    sys.exit(1)

print(f"🔑 Using API Base URL: {API_BASE_URL}\n")

# Initialize client
client = TeamTailorAPIClient(
    api_token=API_TOKEN,
    base_url=API_BASE_URL,
    max_retries=3,
)


def fetch_custom_field_values(entity_type: str, entity_id: str) -> List[Dict]:
    """Fetch custom-field-values for a specific entity."""
    try:
        # Use the relationship link to get custom-field-values
        # Note: TeamTailor API max page size is 30
        custom_fields_url = f"/v1/{entity_type}/{entity_id}/custom-field-values"
        custom_fields = list(
            client._get_entities_from_url(
                f"{API_BASE_URL}{custom_fields_url}",
                page_size=30,  # Max allowed by TeamTailor API
            )
        )
        return custom_fields
    except Exception:
        # Silently return empty list if no custom fields (404 is OK)
        return []


def analyze_custom_fields_structure(
    entity_type: str, max_entities: int = 10
) -> Dict[str, Any]:
    """Analyze custom fields structure for an entity type."""
    print(f"\n{'='*80}")
    print(f"🔍 Analyzing Custom Fields for: {entity_type}")
    print(f"{'='*80}\n")

    results = {
        "entity_type": entity_type,
        "entities_checked": 0,
        "entities_with_custom_fields": 0,
        "custom_field_values": [],
        "custom_field_definitions": {},
        "unique_custom_fields": set(),
    }

    # Get method based on entity type
    method_map = {
        "candidates": client.get_candidates,
        "jobs": client.get_jobs,
    }

    method = method_map.get(entity_type)
    if not method:
        print(f"❌ Method not found for {entity_type}")
        return results

    # Prepare query parameters
    query_params = {}
    if entity_type == "jobs":
        query_params = {"filter[status]": "all"}

    # Fetch entities and their custom fields
    entity_count = 0
    for entity in method(page_size=30, **query_params):
        if entity_count >= max_entities:
            break

        entity_id = entity.get("id")
        entity_count += 1
        results["entities_checked"] = entity_count

        # Check if entity has custom-field-values relationship
        relationships = entity.get("relationships", {})
        if "custom-field-values" not in relationships:
            continue

        # Fetch custom-field-values
        custom_fields = fetch_custom_field_values(entity_type, entity_id)

        if custom_fields:
            results["entities_with_custom_fields"] += 1

            for cf in custom_fields:
                cf_id = cf.get("id")
                cf_type = cf.get("type")
                cf_attrs = cf.get("attributes", {})
                cf_rels = cf.get("relationships", {})

                # Get custom field definition
                custom_field_rel = cf_rels.get("custom-field", {})
                custom_field_id = None
                if custom_field_rel.get("data"):
                    custom_field_id = custom_field_rel["data"].get("id")

                # Store custom field value
                cf_data = {
                    "id": cf_id,
                    "type": cf_type,
                    "attributes": cf_attrs,
                    "custom_field_id": custom_field_id,
                    "entity_id": entity_id,
                    "entity_type": entity_type,
                }
                results["custom_field_values"].append(cf_data)

                if custom_field_id:
                    results["unique_custom_fields"].add(custom_field_id)

                    # Try to get custom field definition
                    if custom_field_id not in results["custom_field_definitions"]:
                        try:
                            cf_def_url = (
                                f"{API_BASE_URL}/v1/custom-fields/{custom_field_id}"
                            )
                            cf_def_response = client._make_request_url(cf_def_url)
                            cf_def_data = cf_def_response.get("data", {})
                            results["custom_field_definitions"][custom_field_id] = {
                                "id": cf_def_data.get("id"),
                                "type": cf_def_data.get("type"),
                                "attributes": cf_def_data.get("attributes", {}),
                            }
                        except Exception as e:
                            print(
                                f"   ⚠️  Could not fetch custom field definition {custom_field_id}: {e}"
                            )

        if entity_count % 5 == 0:
            print(
                f"   Checked {entity_count} entities, found {results['entities_with_custom_fields']} with custom fields..."
            )

    print(f"\n📊 Summary for {entity_type}:")
    print(f"   - Entities checked: {results['entities_checked']}")
    print(f"   - Entities with custom fields: {results['entities_with_custom_fields']}")
    print(f"   - Total custom field values: {len(results['custom_field_values'])}")
    print(
        f"   - Unique custom field definitions: {len(results['unique_custom_fields'])}"
    )

    # Print sample custom field values
    if results["custom_field_values"]:
        print("\n📋 Sample Custom Field Values (first 3):")
        for i, cf in enumerate(results["custom_field_values"][:3], 1):
            print(f"\n   {i}. Custom Field Value ID: {cf['id']}")
            print(
                f"      Custom Field Definition ID: {cf.get('custom_field_id', 'N/A')}"
            )
            print(f"      Attributes: {list(cf['attributes'].keys())}")
            for key, value in list(cf["attributes"].items())[:5]:
                value_preview = str(value)[:50] if value is not None else None
                print(f"        - {key}: {value_preview}")

    # Print custom field definitions
    if results["custom_field_definitions"]:
        print("\n📋 Custom Field Definitions:")
        for cf_id, cf_def in list(results["custom_field_definitions"].items())[:5]:
            print(f"\n   Custom Field ID: {cf_id}")
            attrs = cf_def.get("attributes", {})
            print(f"      Name: {attrs.get('name', 'N/A')}")
            print(f"      Type: {attrs.get('field-type', 'N/A')}")
            print(f"      Attributes: {list(attrs.keys())}")

    return results


def main():
    """Main function."""
    print("🚀 TeamTailor Custom Fields Deep Analysis")
    print("=" * 80)

    all_results = {}

    # Analyze custom fields for candidates and jobs
    for entity_type in ["candidates", "jobs"]:
        results = analyze_custom_fields_structure(entity_type, max_entities=10)
        all_results[entity_type] = results

    # Save results
    output_file = "teamtailor_custom_fields_analysis.json"
    with open(output_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(f"\n\n💾 Detailed results saved to: {output_file}")

    # Final recommendations
    print(f"\n\n{'='*80}")
    print("💡 RECOMMENDATIONS")
    print(f"{'='*80}\n")

    total_custom_fields = sum(
        len(r.get("custom_field_values", [])) for r in all_results.values()
    )

    if total_custom_fields > 0:
        print("✅ Custom Fields detected!")
        print(f"   - Total custom field values found: {total_custom_fields}")
        print("\n📝 Implementation Recommendations:")
        print("   1. Update Raw Job to fetch custom-field-values as relationships")
        print(
            "   2. Store custom-field-values in separate JSONL files or nested structure"
        )
        print("   3. Update Bronze Job to parse custom-field-values")
        print("   4. Update Silver Job to extract specific custom fields by name/ID")
        print(
            "   5. Consider fetching custom-field definitions separately for metadata"
        )
    else:
        print("ℹ️  No custom fields found in sampled entities.")
        print("   This might mean:")
        print("   - Custom fields are not configured in TeamTailor")
        print("   - Custom fields are only in specific entities")
        print("   - Need to check more entities")


if __name__ == "__main__":
    main()
