# Team Tailor Raw Data Ingestion Job

## Overview

This job extracts raw data from the Team Tailor API and stores it in S3 as JSONL files with metadata. It uses AWS Glue Python Shell and requires AWS Secrets Manager for secure credential management.

**Job Type**: AWS Glue Python Shell (PyShell)  
**Script**: `jobs/raw/teamtailor_raw_job.py`  
**Base Class**: `PyShellJobBase` from `libs/pyshell`

> **🎯 Multi-Entity Support**: This job supports **8 different Team Tailor entities** (candidates, jobs, applications, interviews, users, departments, stages, activities).  
> See **[docs/sources/teamtailor/RAW.md](../../docs/sources/teamtailor/RAW.md#supported-entities)** for complete documentation.

## Key Features

- ✅ **Multi-Entity Support**: Extract from 8 different Team Tailor entities (candidates, jobs, applications, etc.)
- ✅ **Secure Credentials**: API token loaded from AWS Secrets Manager (no hardcoded credentials)
- ✅ **Auto-Configuration**: Endpoint and S3 paths auto-configured based on entity type
- ✅ **Consistent Naming**: All Team Tailor API variables use `teamtailor_api_*` prefix
- ✅ **Rate Limiting**: Respects Team Tailor API rate limits with exponential backoff
- ✅ **Pagination**: Handles API pagination automatically (JSON API format)
- ✅ **Metadata Tracking**: Each record includes ingestion metadata (timestamps, job name, batch ID, etc.)
- ✅ **Partitioning**: Data stored with year/month partitioning for time-based entities
- ✅ **Error Handling**: Comprehensive error handling with retry logic

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Team Tailor API                                 │
│                  https://api.teamtailor.com/v1/candidates               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ API calls (rate limited)
                                    │ Authentication via Bearer token
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│              AWS Glue Python Shell Job                                  │
│                teamtailor_raw_job.py                                    │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────┐        │
│  │  1. Load API token from Secrets Manager                    │        │
│  │  2. Fetch entities with pagination                        │        │
│  │  3. Add metadata to each record                            │        │
│  │  4. Write JSONL files to S3                                │        │
│  └────────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ JSONL files
                                    │ Partitioned by year/month (for time-based entities)
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                         S3 Raw Zone                                     │
│  s3://bucket/raw-zone/teamtailor/candidates/                          │
│    └─ year=2025/                                                        │
│         └─ month=10/                                                    │
│              └─ teamtailor_data_YYYYMMDD_HHMMSS_batch_NNN.jsonl       │
└─────────────────────────────────────────────────────────────────────────┘
```

## Configuration

### ⚠️ Important Notes

> **Parameter Naming Convention**: All Glue job parameters MUST use lowercase with underscores (e.g., `--aws_region`, `--job_name`). Using uppercase parameters (e.g., `--AWS_REGION`) will cause them to be ignored and default values will be used instead.

> **AWS Region**: ALWAYS specify `--aws_region` to match the region where your resources (Secrets Manager, S3) are located. The default is `us-west-2`.

### Required Parameters

- `--job_name`: Name of the job (e.g., `teamtailor_raw_candidates`)
- `--entity_type`: Entity type to extract (candidates, jobs, applications, etc.)
- `--aws_region`: AWS region where resources are located (e.g., `us-west-2`)
- `--teamtailor_api_secret_name`: Secrets Manager secret name containing API token

### Optional Parameters (with defaults)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--source_name` | `teamtailor` | Source system name |
| `--raw_zone_path` | Auto-configured | S3 raw zone path |
| `--api_base_url` | `https://api.teamtailor.com` | Team Tailor API base URL |
| `--api_endpoint` | Auto-configured | API endpoint path based on entity_type |
| `--start_date` | `None` | Start date for extraction (YYYY-MM-DD) |
| `--end_date` | Current date | End date for extraction (YYYY-MM-DD) |
| `--rate_limit_per_second` | `2.0` | API rate limit |
| `--max_pages_per_batch` | `20` | Max pages to fetch |
| `--max_retries` | `3` | Retry attempts |
| `--batch_size` | `1000` | Records per batch |

## Deployment

### Prerequisites

1. **AWS Secrets Manager Secret**: The Team Tailor API token must be stored in AWS Secrets Manager

   ```json
   {
     "api_token": "your_teamtailor_api_token_here"
   }
   ```

2. **IAM Permissions**: The Glue execution role must have permissions to access Secrets Manager

### Step 1: Setup IAM Permissions

IAM permissions for Secrets Manager access are managed through infrastructure automation (Terraform). See [OPERATIONS.md](../../docs/OPERATIONS.md) for details.

### Step 2: Upload Job Script

Upload the job script to S3:

```bash
AWS_ENDPOINT_URL="" aws s3 cp \
  jobs/raw/teamtailor_raw_job.py \
  s3://nan-data-lake-develop-storage/scripts/jobs/teamtailor_raw_job.py \
  --profile nan-data-lake-develop-terraform-execution \
  --region us-west-2
```

### Step 3: Create Glue Job

Create the Glue job using AWS CLI:

```bash
aws glue create-job \
  --name teamtailor-raw-candidates \
  --role arn:aws:iam::816582314932:role/nan-data-lake-develop-glue-glue-execution \
  --command Name=pythonshell,ScriptLocation=s3://nan-data-lake-develop-storage/scripts/jobs/teamtailor_raw_job.py,PythonVersion=3.9 \
  --default-arguments '{
    "--additional-python-modules":"pydantic==2.5.3,requests==2.32.3,python-dateutil==2.9.0,urllib3==2.0.7",
    "--extra-py-files":"s3://nan-data-lake-develop-storage/scripts/libs.zip",
    "--job_name":"teamtailor_raw_candidates",
    "--entity_type":"candidates",
    "--source_name":"teamtailor",
    "--aws_region":"us-west-2",
    "--teamtailor_api_secret_name":"nan-data-lake-develop/teamtailor-api"
  }' \
  --max-capacity 0.0625 \
  --timeout 2880 \
  --max-retries 0 \
  --glue-version 1.0 \
  --profile nan-data-lake-develop-terraform-execution \
  --region us-west-2
```

## Execution

### Running Jobs

Jobs are executed through AWS Glue console or infrastructure automation. See [OPERATIONS.md](../../docs/OPERATIONS.md) for deployment and execution procedures.

### Using AWS CLI

```bash
AWS_ENDPOINT_URL="" aws glue start-job-run \
  --job-name teamtailor-raw-candidates \
  --profile nan-data-lake-develop-terraform-execution \
  --region us-west-2
```

### Monitor Job

Check job status:

```bash
JOB_RUN_ID="jr_xxx..."

AWS_ENDPOINT_URL="" aws glue get-job-run \
  --job-name teamtailor-raw-candidates \
  --run-id $JOB_RUN_ID \
  --profile nan-data-lake-develop-terraform-execution \
  --region us-west-2 \
  --query 'JobRun.[JobRunState,ExecutionTime,ErrorMessage]'
```

View logs:

```bash
AWS_ENDPOINT_URL="" aws logs filter-log-events \
  --log-group-name /aws-glue/python-jobs/output \
  --log-stream-name-prefix $JOB_RUN_ID \
  --profile nan-data-lake-develop-terraform-execution \
  --region us-west-2
```

## Output Data Structure

### S3 Location

```
s3://nan-data-lake-develop-storage/
  raw-zone/teamtailor/candidates/
    year=2025/
      month=10/
        teamtailor_data_20251014_201133_batch_001.jsonl
        teamtailor_data_20251014_201133_batch_002.jsonl
        ...
```

### JSONL Record Format

Each line in the JSONL file contains:

```json
{
  "payload": {
    "id": "candidate_001",
    "type": "candidates",
    "attributes": {
      "first_name": "John",
      "last_name": "Doe",
      "email": "john.doe@example.com",
      "created_at": "2025-10-14T20:11:00Z",
      "updated_at": "2025-10-14T20:11:00Z"
    },
    "relationships": { ... }
  },
  "year": 2025,
  "month": 10,
  "metadata": {
    "request_timestamp": "2025-10-14T20:11:23Z",
    "request_url": "https://api.teamtailor.com/v1/candidates",
    "response_status_code": 200,
    "response_time_ms": 596,
    "ingestion_timestamp": "2025-10-14T20:11:33Z",
    "ingestion_job": "teamtailor_raw_candidates",
    "ingestion_source": "teamtailor",
    "ingestion_method": "api_polling",
    "record_id": "20251014_201133_001_000000",
    "batch_id": "20251014_201133_batch_001",
    "file_name": "teamtailor_data_20251014_201133_batch_001.jsonl"
  }
}
```

## Troubleshooting

### Problem: ResourceNotFoundException - Secret not found

**Error:**
```
An error occurred (ResourceNotFoundException) when calling the GetSecretValue operation:
Secrets Manager can't find the specified secret.
```

**Root Cause:**
The Secrets Manager client is using a different AWS region than where the secret is stored. This commonly happens when `--aws_region` parameter is not specified.

**Solution:**
1. Verify the secret exists and note its region:
   ```bash
   aws secretsmanager describe-secret \
     --secret-id nan-data-lake-develop/teamtailor-api \
     --profile nan-data-lake-develop-terraform-execution \
     --region us-west-2
   ```

2. Update the Glue job to include the `--aws_region` parameter:
   ```bash
   aws glue update-job --job-name teamtailor-raw-candidates \
     --job-update '{
       "Role": "arn:aws:iam::ACCOUNT:role/ROLE_NAME",
       "DefaultArguments": {
         "--aws_region": "us-west-2",
         ...other parameters...
       }
     }' \
     --profile PROFILE --region REGION
   ```

3. Verify the region in job logs by looking for: `AWS Region: us-west-2`

**Prevention:**
Always specify `--aws_region` when creating Glue jobs. Add it to your job creation templates.

### Problem: Parameters not being applied (using uppercase)

**Symptom:**
Job parameters appear to be ignored, job uses default values instead.

**Root Cause:**
Glue job parameters are specified in UPPERCASE (e.g., `--AWS_REGION`, `--JOB_NAME`) but the configuration system expects lowercase with underscores (e.g., `--aws_region`, `--job_name`).

**Solution:**
Use lowercase parameter names with underscores:
```bash
# ❌ WRONG
"--AWS_REGION": "us-west-2"
"--JOB_NAME": "teamtailor_raw_candidates"

# ✅ CORRECT
"--aws_region": "us-west-2"
"--job_name": "teamtailor_raw_candidates"
```

**Verification:**
Check the job logs for the configuration section to confirm parameters are being read correctly.

### Problem: AccessDeniedException when accessing Secrets Manager

**Error:**
```
An error occurred (AccessDeniedException) when calling the GetSecretValue operation:
User is not authorized to perform: secretsmanager:GetSecretValue
```

**Solution:**
1. Ensure IAM permissions are configured through infrastructure automation (Terraform)
2. Wait 1-2 minutes for IAM changes to propagate
3. Run the job again

**Verify permissions manually:**
```bash
aws iam get-role-policy \
  --role-name nan-data-lake-develop-glue-glue-execution \
  --policy-name TeamTailorSecretsManagerAccess \
  --profile nan-data-lake-develop-terraform-execution \
  --region us-west-2
```

### Problem: Secret not found

**Error:**
```
ResourceNotFoundException: Secrets Manager can't find the specified secret.
```

**Solution:**
Create the secret:
```bash
aws secretsmanager create-secret \
  --name nan-data-lake-develop/teamtailor-api \
  --secret-string '{"api_token":"YOUR_TEAMTAILOR_API_TOKEN"}' \
  --profile nan-data-lake-develop-terraform-execution \
  --region us-west-2
```

### Problem: Job fails with "Command failed with exit code 1"

**Solution:**
Check CloudWatch logs for detailed error:
```bash
JOB_RUN_ID="jr_xxx..."

AWS_ENDPOINT_URL="" aws logs filter-log-events \
  --log-group-name /aws-glue/python-jobs/output \
  --log-stream-name-prefix $JOB_RUN_ID \
  --profile nan-data-lake-develop-terraform-execution \
  --region us-west-2 \
  --query 'events[*].message' \
  --output text | grep -A 10 "ERROR"
```

### Problem: Rate limit errors from Team Tailor API

**Error:**
```
429 Too Many Requests
```

**Solution:**
The client automatically handles rate limiting with exponential backoff. If issues persist, reduce the rate limit in job arguments:
```bash
--rate_limit_per_second 1.0
```

## Security Best Practices

✅ **API credentials stored in Secrets Manager**
- Never hardcode API tokens in code
- Use AWS Secrets Manager for all credentials
- Rotate credentials regularly

✅ **IAM permissions follow least privilege**
- Role has access only to specific secret
- No wildcard permissions

✅ **Audit trail maintained**
- All secret access logged in CloudTrail
- Job execution tracked in Glue

✅ **Code follows AGENTS.md guidelines**
- No hardcoded credentials
- Secrets Manager mandatory
- No bypass options

## Related Documentation
- [Job Development Guide](../../docs/JOB_DEVELOPMENT.md) - Building Spark and PyShell jobs
- [Team Tailor RAW Zone](../../docs/sources/teamtailor/RAW.md) - Data structure and API details
- [Secrets Management](../../docs/SECRETS_MANAGEMENT.md) - AWS Secrets Manager usage
- [Development Guide](../../docs/DEVELOPMENT.md) - Running jobs locally

## Performance

- **Execution Time**: ~50-60 seconds for typical entity extraction
- **Data Volume**: Varies by entity type
- **Cost**: $0.44 per DPU-Hour (Python Shell: 0.0625 DPU)

## Next Steps

After raw data ingestion, data flows to:
1. **Bronze Layer**: `jobs/bronze/teamtailor_bronze_job.py` - Creates Iceberg table
2. **Silver Layer**: `jobs/silver/teamtailor_silver_job.py` - Cleaned and typed tables
3. **Gold Layer**: `jobs/gold/teamtailor_gold_job.py` - Talent analytics tables
