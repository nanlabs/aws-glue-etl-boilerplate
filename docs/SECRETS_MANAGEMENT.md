# Secrets Management

Simplified AWS Secrets Manager integration with automatic resource naming following the pattern: `nan-wl-workloads-data-lake-{environment}/{secret_key}`

## Quick Start

```python
from libs.utils.secrets import get_secret_by_key, get_secret_json_by_key

# Get full secret (JSON string)
api_credentials = get_secret_by_key("teamtailor-api")

# Get specific key from JSON secret
api_token = get_secret_json_by_key("teamtailor-api", "api_token")
```

## Environment Detection

Automatically detects environment from `ENVIRONMENT` or `ENV` variables:

| Input | Mapped | Secret Name |
|-------|--------|-------------|
| `local`, `dev` | `develop` | `nan-wl-workloads-data-lake-develop/teamtailor-api` |
| `staging` | `staging` | `nan-wl-workloads-data-lake-staging/teamtailor-api` |
| `prod` | `prod` | `nan-wl-workloads-data-lake-prod/teamtailor-api` |

## Override with Environment Variables

```bash
# Override specific secret name
export TEAMTAILOR_API_SECRET_NAME="my-custom-secret"

# Pattern: {SECRET_KEY_UPPER}_SECRET_NAME (dashes become underscores)
export DATABASE_CREDENTIALS_SECRET_NAME="prod-db-secret"
```

## Secret Formats

### JSON Secrets (Recommended)

```json
{
    "api_key": "pk_live_123",
    "private_key": "sk_live_456",
    "webhook_secret": "whsec_789"
}
```

**Usage**:

```python
api_token = get_secret_json_by_key("teamtailor-api", "api_token")
webhook_secret = get_secret_json_by_key("teamtailor-api", "webhook_secret")
```

### String Secrets

```python
# For simple string values
api_key = get_secret_by_key("simple-api-key")
```

## Common Usage in Jobs

```python
from libs.pyspark import MedallionJobBase
from libs.utils.secrets import get_secret_json_by_key

class MyJob(MedallionJobBase):
    def __init__(self, glue_context):
        super().__init__(glue_context, layer="bronze")

    def extract(self):
        # Get API credentials
        api_key = get_secret_json_by_key("external-service-api", "api_key")

        # Use in external service client
        client = ExternalServiceClient(api_key=api_key)
        data = client.fetch_data()
        return data
```

## Local Development

When `STAGE=local`, secrets are read from LocalStack instead of AWS real.

**Setup**:
1. Set `STAGE=local` in your `.env` file
2. Run `direnv allow` to enable automatic environment loading
3. (Optional) Login to AWS SSO: `make aws-login` (wraps `./scripts/aws-login.sh`)
4. Run `make prepare-localstack` to sync secrets and SSM parameters from AWS real to LocalStack

**Manual Operations**:
```bash
# Login to AWS SSO (if credentials are not available)
make aws-login

# Prepare LocalStack with secrets and SSM parameters from AWS real
make prepare-localstack

# Clean all LocalStack resources (secrets, SSM, S3, Glue)
make clean-localstack
```

**Note**: Make sure to run `direnv allow` first to load environment variables from `.envrc`. If AWS credentials are not available, run `make aws-login` before `make prepare-localstack`.

### Secrets Created in LocalStack

When synced, the following secrets are available in LocalStack:
- `nan-wl-{account}/teamtailor-api`
- `nan-wl-{account}/clickup-api`

And SSM parameters:
- `/nan-wl-{account}/data-lake/*` (all data-lake configuration parameters)
- `/nan-wl-{account}/teamtailor-api-base-url`
- `/nan-wl-{account}/clickup-api-base-url`

## Production Setup

### Create Secret in AWS

```bash
aws secretsmanager create-secret \
    --name "nan-wl-workloads-data-lake-prod/teamtailor-api" \
    --description "Team Tailor API credentials" \
    --secret-string '{"api_token": "xxx", "webhook_secret": "xxx"}'
```

### IAM Permissions

```json
{
    "Effect": "Allow",
    "Action": ["secretsmanager:GetSecretValue"],
    "Resource": ["arn:aws:secretsmanager:*:*:secret:nan-wl-workloads-data-lake-*"]
}
```

## Error Handling

```python
# Returns None if secret doesn't exist
api_key = get_secret_by_key("nonexistent-secret")
if api_key is None:
    raise ValueError("API credentials not configured")

# Use default values
api_key = get_secret_json_by_key("service-api", "api_key", default="test_key")
```

## Best Practices

- Use **kebab-case** for secret keys: `teamtailor-api`, `database-credentials`
- Use **JSON format** for structured credentials
- **Never log** secret values
- Use **LocalStack** for local development
- **Override** with environment variables when needed

---

**Examples**: [secrets_usage_example.py](../examples/secrets_usage_example.py) | **🏠 Back**: [README.md](./README.md)
