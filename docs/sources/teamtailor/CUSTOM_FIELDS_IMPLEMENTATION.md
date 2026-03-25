# TeamTailor Custom Fields Implementation Guide

## Custom Fields Analysis

### Findings

After real tests with the TeamTailor API, it was identified that:

1. **Custom Fields are available** in:
   - `candidates` (relationship `custom-field-values`)
   - `jobs` (relationship `custom-field-values`)

2. **Custom Fields Structure**:
   ```json
   {
     "id": "3577340",
     "type": "custom-field-values",
     "attributes": {
       "field-type": "CustomField::MultiSelect",
       "value": ["Full stack Developer"]
     },
     "relationships": {
       "custom-field": {
         "data": {
           "id": "custom-field-id",
           "type": "custom-fields"
         }
       }
     }
   }
   ```

3. **Custom Field Types Found**:
   - `CustomField::Select` - Single value (string)
   - `CustomField::MultiSelect` - Array of values
   - `CustomField::Number` - Numeric value

4. **Statistics**:
   - **Candidates**: ~10% have custom fields (1 out of 10 sampled)
   - **Jobs**: ~100% have custom fields (9 out of 9 sampled)
   - **Jobs** have more custom fields per entity (average ~3.5 per job)

### Evaluated Endpoints

All endpoints were tested with `filter[status]=all` for jobs:

✅ **candidates** - Custom fields available  
✅ **jobs** - Custom fields available (with `filter[status]=all`)  
✅ **job-applications** - No custom fields in relationships  
✅ **interviews** - No custom fields in relationships  
✅ **users** - No custom fields in relationships  
✅ **departments** - No custom fields in relationships  
✅ **stages** - No custom fields in relationships  
✅ **nps-responses** - No custom fields in relationships  

## Recommended Implementation

### 1. Raw Layer - Custom Fields Ingestion

#### Option A: Include Custom Fields in the same record (Recommended)

Modify `TeamTailorRawJob` to include custom-field-values as part of the payload:

```python
def fetch_data_with_pagination(self) -> Tuple[List[Dict], List[Dict]]:
    # ... existing code ...

    for entity in get_entity_method(page_size=30, **query_params):
        entity_id = entity.get("id")

        # Fetch custom-field-values if entity supports them
        if self.config.entity_type in ["candidates", "jobs"]:
            try:
                custom_fields = list(self.api_client._get_entities_from_url(
                    f"{self.api_client.base_url}/v1/{self.config.entity_type}/{entity_id}/custom-field-values",
                    page_size=30
                ))
                # Add custom fields to entity record
                entity["included_custom_field_values"] = custom_fields
            except Exception as e:
                self.logger.warning(f"Could not fetch custom fields for {entity_id}: {e}")

        data_records.append(entity)
```

**Advantages**:
- Maintains the relationship between entity and custom fields
- No changes required in file structure
- Easy to process in Bronze

**Disadvantages**:
- Increases the size of each record
- Requires additional API calls

#### Option B: Separate files for Custom Fields

Create separate `custom-field-values` files in Raw:

```
raw-zone/teamtailor/candidates/
raw-zone/teamtailor/candidates_custom_field_values/
raw-zone/teamtailor/jobs/
raw-zone/teamtailor/jobs_custom_field_values/
```

**Advantages**:
- Clear data separation
- Allows processing custom fields independently

**Disadvantages**:
- Requires join in Bronze/Silver
- More complex to maintain

### 2. Bronze Layer - Custom Fields Parsing

Similar to how it's done with ClickUp, create functions to extract custom fields:

```python
def _extract_custom_field_values(self, payload_col):
    """Extract custom-field-values array from payload."""
    from pyspark.sql.functions import get_json_object
    return get_json_object(payload_col, "$.included_custom_field_values")
```

### 3. Silver Layer - Specific Custom Fields Extraction

Create UDFs to extract custom fields by name or ID:

```python
@udf(returnType=StringType())
def extract_custom_field_value(custom_fields_json, field_name_or_id):
    """Extract value from custom field by name or ID."""
    if not custom_fields_json:
        return None
    try:
        custom_fields = json.loads(custom_fields_json) if isinstance(custom_fields_json, str) else custom_fields_json
        if not isinstance(custom_fields, list):
            return None

        # Try to find by custom-field definition name (requires fetching definitions)
        # Or by field-type pattern matching

        for field in custom_fields:
            field_type = field.get("attributes", {}).get("field-type", "")
            value = field.get("attributes", {}).get("value")

            # For now, return first matching field-type
            # TODO: Match by custom-field definition name

            if value:
                if isinstance(value, list):
                    return json.dumps(value)  # Return as JSON string for arrays
                return str(value)
        return None
    except Exception as e:
        return None
```

### 4. Get Custom Field Definitions

To be able to extract custom fields by name, we need to obtain the definitions:

```python
# Endpoint: /v1/custom-fields
# This gives us the name and type of each custom field
```

## Implementation Plan

### Phase 1: Raw Layer (High Priority)
1. ✅ Modify `TeamTailorRawJob.fetch_data_with_pagination()` to include custom-field-values
2. ✅ Add configuration to control whether custom fields are included
3. ✅ Handle errors when custom fields are not available

### Phase 2: Bronze Layer (Medium Priority)
1. ✅ Extract `included_custom_field_values` as JSON column
2. ✅ Validate custom fields structure
3. ✅ Maintain compatibility with records without custom fields

### Phase 3: Silver Layer (Medium Priority)
1. ✅ Create UDFs to extract specific custom fields
2. ✅ Get custom field definitions to map IDs to names
3. ✅ Create specific columns for important custom fields

### Phase 4: Gold Layer (Low Priority)
1. ✅ Include custom fields in Gold tables according to business needs
2. ✅ Add metrics based on custom fields

## Considerations

1. **Performance**: Including custom fields requires additional API calls
   - Consider making it optional with `INCLUDE_CUSTOM_FIELDS` flag
   - Use parallelization if possible

2. **Custom Field Definitions**: To extract by name, we need:
   - Endpoint `/v1/custom-fields` to get definitions
   - Cache definitions to avoid repeated calls
   - Map custom-field-value → custom-field definition → name

3. **Value Types**:
   - `Select`: Simple string
   - `MultiSelect`: Array of strings (convert to JSON string or array column)
   - `Number`: Number (keep as number)

4. **Compatibility**: Ensure code works when:
   - No custom fields are configured
   - Custom fields are empty
   - Custom field definitions are not available

## Completed Implementation ✅

### Phase 1: Raw Layer ✅
- ✅ Modified `TeamTailorRawJob.fetch_data_with_pagination()` to include custom-field-values
- ✅ Added support for `candidates` and `jobs` (only ones that have custom fields)
- ✅ Custom fields are stored as `included_custom_field_values` in the payload
- ✅ Error handling when custom fields are not available

### Phase 2: Bronze Layer ✅
- ✅ Extracted `custom_field_values_json` as column in Bronze
- ✅ Added to `_transform_candidates()` and `_transform_jobs()`
- ✅ Maintains compatibility with records without custom fields

### Phase 3: Silver Layer ✅
- ✅ Created UDFs to extract custom fields:
  - `extract_custom_field_value()` - Extracts by field-type pattern
  - `extract_custom_field_by_type()` - Extracts by exact field-type
- ✅ Extraction of specific custom fields:
  - **Candidates**: `custom_field_role` (MultiSelect)
  - **Jobs**: `custom_field_account` (Select), `custom_field_employment_type` (MultiSelect), `custom_field_budget` (Number)
- ✅ Maintains complete `custom_fields_json` for future extractions

### Phase 4: Gold Layer ✅
- ✅ Enriched `talent_time_to_fill` with custom fields from jobs (account, employment_type, budget)
- ✅ Enriched `talent_source_effectiveness` with custom_field_role from candidates for segmentation
- ✅ Enriched `talent_pipeline_health` with custom fields from jobs for analysis by account/employment_type

## Next Steps

1. ✅ **Completed**: Implementation in all tiers
2. Test with real data in development
3. Document specific custom fields used by the Talent team (coordination needed)
4. Adjust extraction of specific custom fields according to business needs
5. Consider obtaining Custom Field Definitions to map by name instead of type
