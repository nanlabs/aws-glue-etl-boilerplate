#!/bin/bash
# Clean all LocalStack resources (secrets, SSM parameters, S3 buckets, Glue databases)
# This script assumes environment variables are already loaded (via direnv)
# It uses awslocal which works with AWS_ENDPOINT_URL from the environment

set -o pipefail

echo "🧹 Cleaning LocalStack resources..."
echo "⚠️  This will delete all secrets, SSM parameters, S3 buckets, and Glue databases in LocalStack"
read -p "Are you sure? [y/N] " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Cancelled."
  exit 0
fi

# Check if LocalStack is available
if ! curl -s "${AWS_ENDPOINT_URL}/_localstack/health" > /dev/null 2>&1; then
  echo "⚠️  Warning: LocalStack is not available at ${AWS_ENDPOINT_URL}"
  echo "   Make sure LocalStack is running."
  exit 1
fi

errors=0

# Clean secrets
echo "Cleaning secrets..."
secrets=$(awslocal secretsmanager list-secrets \
  --query "SecretList[?starts_with(Name, 'nan-wl-')].Name" \
  --output text 2>/dev/null || echo "")

if [[ -n "${secrets}" ]]; then
  for secret in ${secrets}; do
    if [[ -n "${secret}" ]]; then
      echo "  Deleting secret: ${secret}"
      if ! timeout 10 awslocal secretsmanager delete-secret \
        --secret-id "${secret}" \
        --force-delete-without-recovery 2>/dev/null; then
        echo "    ⚠️  Failed to delete secret: ${secret}"
        ((errors++))
      fi
    fi
  done
else
  echo "  No secrets found to delete"
fi

# Clean SSM parameters
echo "Cleaning SSM parameters..."
params=$(awslocal ssm get-parameters-by-path \
  --path "/nan-wl-" \
  --recursive \
  --query "Parameters[*].Name" \
  --output text 2>/dev/null || echo "")

if [[ -n "${params}" ]]; then
  for param in ${params}; do
    if [[ -n "${param}" ]]; then
      echo "  Deleting parameter: ${param}"
      if ! timeout 10 awslocal ssm delete-parameter \
        --name "${param}" 2>/dev/null; then
        echo "    ⚠️  Failed to delete parameter: ${param}"
        ((errors++))
      fi
    fi
  done
else
  echo "  No SSM parameters found to delete"
fi

# Clean S3 buckets
echo "Cleaning S3 buckets..."
buckets=$(awslocal s3 ls --output text 2>/dev/null | awk '{print $3}' || echo "")

if [[ -n "${buckets}" ]]; then
  for bucket in ${buckets}; do
    if [[ -n "${bucket}" ]]; then
      echo "  Deleting bucket: ${bucket}"
      if ! timeout 10 awslocal s3 rb "s3://${bucket}" --force 2>/dev/null; then
        echo "    ⚠️  Failed to delete bucket: ${bucket}"
        ((errors++))
      fi
    fi
  done
else
  echo "  No S3 buckets found to delete"
fi

# Clean Glue databases
echo "Cleaning Glue databases..."
databases=$(awslocal glue get-databases \
  --query "DatabaseList[?starts_with(Name, 'nan_')].Name" \
  --output text 2>/dev/null || echo "")

if [[ -n "${databases}" ]]; then
  for db in ${databases}; do
    if [[ -n "${db}" ]]; then
      echo "  Deleting database: ${db}"
      if ! timeout 10 awslocal glue delete-database \
        --name "${db}" 2>/dev/null; then
        echo "    ⚠️  Failed to delete database: ${db}"
        ((errors++))
      fi
    fi
  done
else
  echo "  No Glue databases found to delete"
fi

if [[ ${errors} -gt 0 ]]; then
  echo ""
  echo "⚠️  Cleanup completed with ${errors} error(s)."
  exit 1
else
  echo ""
  echo "✅ LocalStack cleanup complete!"
  exit 0
fi
