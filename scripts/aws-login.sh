#!/bin/bash
# AWS SSO login helper script
# This script handles AWS SSO login for data workload accounts
# It temporarily removes LocalStack variables to ensure SSO login uses real AWS

set -o pipefail

# Function to check and login to AWS SSO for data workload accounts
# IMPORTANT: Temporarily unset AWS_ENDPOINT_URL for SSO operations
# SSO login must use real AWS, not LocalStack
check_and_login() {
  local profile=$1
  echo "Checking AWS session for profile: $profile"

  # Save current endpoint setting
  local saved_aws_endpoint_url="${AWS_ENDPOINT_URL:-}"
  local saved_aws_access_key_id="${AWS_ACCESS_KEY_ID:-}"
  local saved_aws_secret_access_key="${AWS_SECRET_ACCESS_KEY:-}"

  # Temporarily unset LocalStack variables for SSO operations (must use real AWS)
  unset AWS_ENDPOINT_URL
  unset AWS_ACCESS_KEY_ID
  unset AWS_SECRET_ACCESS_KEY

  if ! AWS_PROFILE=$profile aws sts get-caller-identity > /dev/null 2>&1; then
    echo "AWS session expired or not found for profile $profile. Logging in with SSO..."
    AWS_PROFILE=$profile aws sso login
  else
    echo "AWS session valid for profile: $profile"
  fi

  # Restore endpoint setting
  [[ -n "${saved_aws_endpoint_url}" ]] && export AWS_ENDPOINT_URL="${saved_aws_endpoint_url}"
  [[ -n "${saved_aws_access_key_id}" ]] && export AWS_ACCESS_KEY_ID="${saved_aws_access_key_id}"
  [[ -n "${saved_aws_secret_access_key}" ]] && export AWS_SECRET_ACCESS_KEY="${saved_aws_secret_access_key}"
}

# Function to login to data workload profiles
login_data_profile() {
  local account_name="${AWS_ACCOUNT_NAME:-workloads-data-lake-develop}"
  local profile="nan-wl-${account_name}-terraform-execution"

  echo "=== Logging in to Data Workload AWS SSO profiles ==="
  check_and_login "${profile}"
  echo "=== Data workload profiles logged in ==="
}

# Helper function to run AWS SSO login with proper endpoint configuration
# Usage: aws_sso_login [profile_name]
# Example: aws_sso_login nan-wl-workloads-data-lake-develop-terraform-execution
aws_sso_login() {
  local profile="${1:-${AWS_PROFILE}}"
  if [[ -z "${profile}" ]]; then
    echo "Error: No profile specified. Usage: aws_sso_login [profile_name]"
    return 1
  fi

  echo "Logging in to AWS SSO for profile: $profile"

  # Save current endpoint settings
  local saved_aws_endpoint_url="${AWS_ENDPOINT_URL:-}"
  local saved_aws_access_key_id="${AWS_ACCESS_KEY_ID:-}"
  local saved_aws_secret_access_key="${AWS_SECRET_ACCESS_KEY:-}"

  # Temporarily unset LocalStack variables for SSO operations (must use real AWS)
  unset AWS_ENDPOINT_URL
  unset AWS_ACCESS_KEY_ID
  unset AWS_SECRET_ACCESS_KEY

  # Execute SSO login
  AWS_PROFILE="$profile" aws sso login

  local exit_code=$?

  # Restore endpoint settings
  [[ -n "${saved_aws_endpoint_url}" ]] && export AWS_ENDPOINT_URL="${saved_aws_endpoint_url}"
  [[ -n "${saved_aws_access_key_id}" ]] && export AWS_ACCESS_KEY_ID="${saved_aws_access_key_id}"
  [[ -n "${saved_aws_secret_access_key}" ]] && export AWS_SECRET_ACCESS_KEY="${saved_aws_secret_access_key}"

  return $exit_code
}

# Execute login_data_profile if script is run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  login_data_profile
fi
