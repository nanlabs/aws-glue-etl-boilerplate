# LocalStack Configuration

This directory contains the LocalStack initialization scripts that automatically set up AWS resources when LocalStack starts.

## Structure

```
docker/localstack/
├── init.d/                    # Initialization scripts (executed automatically)
│   ├── 01-create-s3-buckets.sh  # Creates S3 buckets for Medallion Architecture
│   └── 02-load-sample-data.sh   # Loads sample data if available
├── s3/                        # Sample data for testing
│   └── sampledata.tar.xz     # Sample JSON files
└── README.md                 # This file
```

## How it Works

LocalStack automatically executes all scripts in the `init.d/` directory when it starts up. This follows the standard LocalStack initialization pattern.

### Initialization Scripts

1. **01-create-s3-buckets.sh**: Creates the main data lake bucket with Medallion Architecture structure:
   - `s3://local-bucket/bronze/` - Raw data ingestion
   - `s3://local-bucket/silver/` - Cleaned and enriched data  
   - `s3://local-bucket/gold/` - Business-ready aggregated data
   - `s3://local-bucket/warehouse/` - Iceberg warehouse location

2. **02-load-sample-data.sh**: Optionally loads sample data from the `s3/sampledata.tar.xz` file into a separate `sampledata` bucket for testing.

## Secrets and SSM Parameters

**Important**: Secrets and SSM parameters are **NOT** created automatically during LocalStack initialization. Instead, use the manual preparation script:

```bash
# Login to AWS SSO (if needed)
./scripts/aws-login.sh

# Prepare LocalStack with secrets and SSM parameters from AWS real
make prepare-localstack
```

This approach ensures:
- Secrets are synced from AWS real (not hardcoded)
- You have control over when secrets are synced
- Secrets can be updated without restarting LocalStack

## Usage

The initialization scripts run automatically when you start the Docker Compose stack:

```bash
docker compose up -d
```

After LocalStack starts, prepare secrets and SSM parameters:

```bash
make prepare-localstack
```

## Testing

Once LocalStack is running, you can verify the setup:

```bash
# List all buckets
awslocal s3 ls

# List bucket contents
awslocal s3 ls s3://local-bucket/ --recursive

# List secrets
awslocal secretsmanager list-secrets

# List SSM parameters
awslocal ssm get-parameters-by-path --path "/nan-wl-" --recursive

# Test with sample data (if loaded)
awslocal s3 ls s3://sampledata/
```

## Adding Custom Resources

To add more AWS resources during initialization:

1. Create a new script in `init.d/` with a numeric prefix (e.g., `03-create-dynamodb.sh`)
2. Make it executable: `chmod +x init.d/03-create-dynamodb.sh`
3. LocalStack will execute it automatically on startup

## Cleaning Up

To clean all LocalStack resources:

```bash
make clean-localstack
```

This removes all secrets, SSM parameters, S3 buckets, and Glue databases from LocalStack.
