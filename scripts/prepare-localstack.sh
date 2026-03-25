#!/bin/bash
# Sync secrets and SSM parameters from AWS real to LocalStack
# This script assumes environment variables are already loaded (via direnv)
# It uses env -u AWS_ENDPOINT_URL to read from AWS real, and awslocal to write to LocalStack

set -o pipefail

sync_secrets_to_localstack() {
  local account_name="${AWS_ACCOUNT_NAME:-workloads-data-lake-develop}"
  local secrets_prefix="nan-wl-${account_name}"
  local ssm_prefix="/nan-wl-${account_name}"
  local aws_profile="nan-wl-${account_name}-terraform-execution"
  local aws_region="us-west-2"

  echo "🔄 Syncing secrets and SSM parameters from AWS real to LocalStack..."
  echo "   Account: ${account_name}"
  echo "   Secrets prefix: ${secrets_prefix}"
  echo "   SSM prefix: ${ssm_prefix}"
  echo ""

  # Check if LocalStack is available
  if ! curl -s "${AWS_ENDPOINT_URL}/_localstack/health" > /dev/null 2>&1; then
    echo "⚠️  Warning: LocalStack is not available at ${AWS_ENDPOINT_URL}"
    echo "   Skipping secret synchronization. Make sure LocalStack is running."
    return 1
  fi

  # Check if AWS credentials are available (read from AWS real without LocalStack variables)
  # Remove LocalStack-specific variables that interfere with AWS real access
  if ! env -u AWS_ENDPOINT_URL -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
    AWS_PROFILE="${aws_profile}" AWS_REGION="${aws_region}" \
    aws sts get-caller-identity > /dev/null 2>&1; then
    echo "⚠️  AWS credentials not available (profile: ${aws_profile})"
    echo "   Attempting to login with SSO..."
    # Try to login with SSO (remove LocalStack variables)
    if env -u AWS_ENDPOINT_URL -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
      AWS_PROFILE="${aws_profile}" aws sso login > /dev/null 2>&1; then
      echo "   ✅ SSO login successful"
    else
      echo "   ❌ SSO login failed"
      echo "   Skipping secret synchronization. Secrets will not be available in LocalStack."
      echo "   Try running: AWS_PROFILE=${aws_profile} aws sso login"
      return 1
    fi
    # Verify credentials again after login
    if ! env -u AWS_ENDPOINT_URL -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
      AWS_PROFILE="${aws_profile}" AWS_REGION="${aws_region}" \
      aws sts get-caller-identity > /dev/null 2>&1; then
      echo "   ❌ Still unable to access AWS after login"
      return 1
    fi
  fi

  local synced_secrets=0
  local synced_params=0
  local errors=0

  # Sync secrets from AWS Secrets Manager to LocalStack
  echo "📦 Syncing secrets..."
  local secrets_to_sync=(
    "${secrets_prefix}/public-api"
  )

  for secret_name in "${secrets_to_sync[@]}"; do
    echo -n "   ${secret_name}... "

    # Read from AWS real (remove LocalStack variables for this command only)
    local secret_value
    secret_value=$(env -u AWS_ENDPOINT_URL -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
      AWS_PROFILE="${aws_profile}" AWS_REGION="${aws_region}" \
      aws secretsmanager get-secret-value \
      --secret-id "${secret_name}" \
      --query 'SecretString' \
      --output text 2>/dev/null)

    if [[ $? -eq 0 ]] && [[ -n "${secret_value}" ]]; then
      # Write to LocalStack using awslocal (uses AWS_ENDPOINT_URL from environment)
      if timeout 10 awslocal secretsmanager create-secret \
        --name "${secret_name}" \
        --secret-string "${secret_value}" \
        --output json > /dev/null 2>&1 || \
         timeout 10 awslocal secretsmanager update-secret \
        --secret-id "${secret_name}" \
        --secret-string "${secret_value}" \
        --output json > /dev/null 2>&1; then
        echo "✅"
        ((synced_secrets++))
      else
        echo "❌ (failed to write to LocalStack)"
        ((errors++))
      fi
    else
      echo "⚠️  (not found in AWS, skipping)"
    fi
  done

  echo ""

  # Sync SSM parameters from AWS real to LocalStack
  echo "📋 Syncing SSM parameters..."

  # Sync data-lake parameters
  local data_lake_path="${ssm_prefix}/data-lake"
  echo "   ${data_lake_path}/*"

  # Read from AWS real (remove LocalStack variables for this command only)
  local params
  params=$(env -u AWS_ENDPOINT_URL -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
    AWS_PROFILE="${aws_profile}" AWS_REGION="${aws_region}" \
    aws ssm get-parameters-by-path \
    --path "${data_lake_path}" \
    --recursive \
    --query 'Parameters[*].[Name,Value,Type]' \
    --output text 2>/dev/null)

  if [[ -n "${params}" ]]; then
    while IFS=$'\t' read -r param_name param_value param_type; do
      if [[ -n "${param_name}" ]] && [[ -n "${param_value}" ]]; then
        echo -n "     ${param_name}... "
        # Write to LocalStack using awslocal
        if timeout 10 awslocal ssm put-parameter \
          --name "${param_name}" \
          --value "${param_value}" \
          --type "${param_type:-String}" \
          --overwrite \
          --output json > /dev/null 2>&1; then
          echo "✅"
          ((synced_params++))
        else
          echo "❌"
          ((errors++))
        fi
      fi
    done <<< "${params}"
  fi

  # Sync direct parameters (public-api-base-url)
  local direct_params=(
    "${ssm_prefix}/public-api-base-url"
  )

  for param_name in "${direct_params[@]}"; do
    echo -n "   ${param_name}... "

    # Read from AWS real (remove LocalStack variables for this command only)
    local param_value
    param_value=$(env -u AWS_ENDPOINT_URL -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
      AWS_PROFILE="${aws_profile}" AWS_REGION="${aws_region}" \
      aws ssm get-parameter \
      --name "${param_name}" \
      --query 'Parameter.Value' \
      --output text 2>/dev/null)

    if [[ $? -eq 0 ]] && [[ -n "${param_value}" ]]; then
      # Write to LocalStack using awslocal
      if timeout 10 awslocal ssm put-parameter \
        --name "${param_name}" \
        --value "${param_value}" \
        --type "String" \
        --overwrite \
        --output json > /dev/null 2>&1; then
        echo "✅"
        ((synced_params++))
      else
        echo "❌"
        ((errors++))
      fi
    else
      echo "⚠️  (not found in AWS, skipping)"
    fi
  done

  echo ""
  echo "✅ Synchronization complete:"
  echo "   Secrets synced: ${synced_secrets}"
  echo "   Parameters synced: ${synced_params}"
  if [[ ${errors} -gt 0 ]]; then
    echo "   Errors: ${errors}"
  fi
  echo ""

  # Create S3 buckets and Glue databases from synced parameters
  create_localstack_resources "${ssm_prefix}"

  return 0
}

create_localstack_resources() {
  local ssm_prefix="${1}"

  echo "🏗️  Creating LocalStack resources (S3 buckets and Glue databases)..."
  echo ""

  local created_buckets=0
  local resource_errors=0

  # Read bucket names from SSM
  echo "📦 Creating S3 buckets..."

  local storage_bucket
  storage_bucket=$(awslocal ssm get-parameter \
    --name "${ssm_prefix}/data-lake/storage_bucket_name" \
    --query 'Parameter.Value' \
    --output text 2>/dev/null)

  if [[ -n "${storage_bucket}" ]]; then
    echo -n "   ${storage_bucket}... "
    if awslocal s3 mb "s3://${storage_bucket}" > /dev/null 2>&1; then
      echo "✅"
      ((created_buckets++))
    elif awslocal s3 ls "s3://${storage_bucket}" > /dev/null 2>&1; then
      echo "ℹ️  (already exists)"
    else
      echo "❌"
      ((resource_errors++))
    fi
  fi

  local temp_bucket
  temp_bucket=$(awslocal ssm get-parameter \
    --name "${ssm_prefix}/data-lake/temp_bucket_name" \
    --query 'Parameter.Value' \
    --output text 2>/dev/null)

  if [[ -n "${temp_bucket}" ]]; then
    echo -n "   ${temp_bucket}... "
    if awslocal s3 mb "s3://${temp_bucket}" > /dev/null 2>&1; then
      echo "✅"
      ((created_buckets++))
    elif awslocal s3 ls "s3://${temp_bucket}" > /dev/null 2>&1; then
      echo "ℹ️  (already exists)"
    else
      echo "❌"
      ((resource_errors++))
    fi
  fi

  echo ""
  echo "✅ Resource creation complete:"
  echo "   Buckets created: ${created_buckets}"
  if [[ ${resource_errors} -gt 0 ]]; then
    echo "   Errors: ${resource_errors}"
  fi
  echo ""
}

# Execute the function if script is run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  sync_secrets_to_localstack
fi
