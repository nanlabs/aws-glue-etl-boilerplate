#!/usr/bin/env python3
"""
Test script to evaluate TeamTailor API endpoints and Custom Fields.

This script performs real tests with all endpoints we use
and evaluates how Custom Fields are handled.
"""

import json
import os
import sys
from typing import Any, Dict

# Add libs to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "libs"))

from libs.common.teamtailor import TeamTailorAPIClient

# Load credentials from environment (set by direnv/.envrc)
API_TOKEN = os.getenv("TEAMTAILOR_API_TOKEN")
API_BASE_URL = os.getenv("TEAMTAILOR_API_BASE_URL", "https://api.teamtailor.com")

if not API_TOKEN:
    print("❌ ERROR: TEAMTAILOR_API_TOKEN not found in environment")
    print("   Make sure to run: direnv allow (to load environment variables)")
    sys.exit(1)

print(f"🔑 Using API Base URL: {API_BASE_URL}")
print(f"🔑 Token: {API_TOKEN[:10]}...{API_TOKEN[-4:]}\n")

# Initialize client
client = TeamTailorAPIClient(
    api_token=API_TOKEN,
    base_url=API_BASE_URL,
    max_retries=3,
)

# Endpoints to test
ENDPOINTS_TO_TEST = [
    "candidates",
    "jobs",
    "job-applications",
    "interviews",
    "users",
    "departments",
    "stages",
    "nps-responses",
]


def analyze_custom_fields(record: Dict[str, Any], entity_type: str) -> Dict[str, Any]:
    """Analyze custom fields in a record."""
    analysis = {
        "has_custom_fields": False,
        "custom_fields_structure": None,
        "custom_fields_count": 0,
        "custom_field_types": {},
    }

    # Check attributes for custom fields
    attributes = record.get("attributes", {})

    # TeamTailor custom fields are typically in attributes with keys like:
    # - "custom-field-{id}" or similar patterns
    # - Or in a "custom-fields" array/object

    custom_field_keys = [
        k for k in attributes.keys() if "custom" in k.lower() or "field" in k.lower()
    ]

    if custom_field_keys:
        analysis["has_custom_fields"] = True
        analysis["custom_fields_count"] = len(custom_field_keys)
        analysis["custom_fields_structure"] = {
            k: {
                "type": type(attributes[k]).__name__,
                "value_preview": (
                    str(attributes[k])[:100] if attributes[k] is not None else None
                ),
            }
            for k in custom_field_keys[:10]  # Limit to first 10
        }

    # Also check if there's a relationships section with custom fields
    relationships = record.get("relationships", {})
    custom_field_rels = [
        k for k in relationships.keys() if "custom" in k.lower() or "field" in k.lower()
    ]

    if custom_field_rels:
        analysis["has_custom_fields"] = True
        analysis["custom_fields_in_relationships"] = custom_field_rels

    return analysis


def test_endpoint(endpoint: str, max_records: int = 5) -> Dict[str, Any]:
    """Test an endpoint and analyze the response."""
    print(f"\n{'='*80}")
    print(f"📡 Testing endpoint: /v1/{endpoint}")
    print(f"{'='*80}")

    results = {
        "endpoint": endpoint,
        "success": False,
        "records_fetched": 0,
        "sample_records": [],
        "custom_fields_analysis": [],
        "attributes_keys": set(),
        "relationships_keys": set(),
        "errors": [],
    }

    try:
        # Get method based on endpoint
        method_map = {
            "candidates": client.get_candidates,
            "jobs": client.get_jobs,
            "job-applications": client.get_applications,
            "interviews": client.get_interviews,
            "users": client.get_users,
            "departments": client.get_departments,
            "stages": client.get_stages,
            "nps-responses": client.get_nps_responses,
        }

        method = method_map.get(endpoint)
        if not method:
            results["errors"].append(f"Method not found for endpoint: {endpoint}")
            return results

        # Prepare query parameters
        # For jobs endpoint, use filter[status]=all to get all jobs including closed/archived
        query_params = {}
        if endpoint == "jobs":
            query_params = {"filter[status]": "all"}
            print("   Using filter: filter[status]=all")

        # Fetch records
        record_count = 0
        for record in method(page_size=30, **query_params):
            if record_count >= max_records:
                break

            record_count += 1
            results["records_fetched"] = record_count

            # Store sample record (first one)
            if record_count == 1:
                results["sample_records"].append(record)

            # Collect all attribute keys
            attributes = record.get("attributes", {})
            results["attributes_keys"].update(attributes.keys())

            # Collect all relationship keys
            relationships = record.get("relationships", {})
            results["relationships_keys"].update(relationships.keys())

            # Analyze custom fields
            custom_analysis = analyze_custom_fields(record, endpoint)
            if custom_analysis["has_custom_fields"]:
                results["custom_fields_analysis"].append(custom_analysis)

        results["success"] = True
        print(f"✅ Successfully fetched {results['records_fetched']} records")

        # Print summary
        print("\n📊 Summary:")
        print(f"   - Total attributes found: {len(results['attributes_keys'])}")
        print(f"   - Total relationships found: {len(results['relationships_keys'])}")
        print(
            f"   - Records with custom fields: {len(results['custom_fields_analysis'])}"
        )

        # Print attribute keys (first 20)
        print("\n📋 Attribute keys (first 20):")
        for key in sorted(list(results["attributes_keys"]))[:20]:
            print(f"   - {key}")

        # Print relationship keys
        if results["relationships_keys"]:
            print("\n🔗 Relationship keys:")
            for key in sorted(list(results["relationships_keys"])):
                print(f"   - {key}")

        # Print custom fields details
        if results["custom_fields_analysis"]:
            print("\n🎯 Custom Fields Analysis:")
            for i, analysis in enumerate(results["custom_fields_analysis"][:3], 1):
                print(f"\n   Record {i}:")
                print(f"   - Has custom fields: {analysis['has_custom_fields']}")
                print(f"   - Custom fields count: {analysis['custom_fields_count']}")
                if analysis.get("custom_fields_structure"):
                    print("   - Custom fields structure:")
                    for field_key, field_info in analysis[
                        "custom_fields_structure"
                    ].items():
                        print(f"     • {field_key}: {field_info['type']}")

    except Exception as e:
        results["success"] = False
        results["errors"].append(str(e))
        print(f"❌ Error testing endpoint: {e}")
        import traceback

        traceback.print_exc()

    return results


def main():
    """Main function to test all endpoints."""
    print("🚀 TeamTailor API Endpoint Evaluation")
    print("=" * 80)

    all_results = {}

    for endpoint in ENDPOINTS_TO_TEST:
        results = test_endpoint(endpoint, max_records=5)
        all_results[endpoint] = results

    # Print final summary
    print(f"\n\n{'='*80}")
    print("📊 FINAL SUMMARY")
    print(f"{'='*80}\n")

    for endpoint, results in all_results.items():
        status = "✅" if results["success"] else "❌"
        print(
            f"{status} {endpoint:20s} - Records: {results['records_fetched']:3d} - "
            f"Attributes: {len(results['attributes_keys']):3d} - "
            f"Custom Fields: {len(results['custom_fields_analysis'])}"
        )

    # Save detailed results to file
    output_file = "teamtailor_api_evaluation.json"
    with open(output_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(f"\n💾 Detailed results saved to: {output_file}")

    # Recommendations
    print(f"\n\n{'='*80}")
    print("💡 RECOMMENDATIONS")
    print(f"{'='*80}\n")

    endpoints_with_custom_fields = [
        ep for ep, res in all_results.items() if res.get("custom_fields_analysis")
    ]

    if endpoints_with_custom_fields:
        print("✅ Endpoints with Custom Fields detected:")
        for ep in endpoints_with_custom_fields:
            print(f"   - {ep}")
        print("\n⚠️  ACTION REQUIRED:")
        print("   - Review custom fields structure in the saved JSON file")
        print("   - Update Bronze/Silver jobs to handle custom fields")
        print(
            "   - Consider adding custom field extraction similar to ClickUp implementation"
        )
    else:
        print("ℹ️  No custom fields detected in standard attributes.")
        print("   Custom fields might be:")
        print("   - In relationships section")
        print("   - Require specific API parameters to include")
        print("   - Available via separate endpoints")


if __name__ == "__main__":
    main()
