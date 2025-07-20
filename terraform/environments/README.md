# Environment-Specific Configurations

This directory contains environment-specific variable files for the Terraform Glue deployment.

## Available Environments

- `dev.tfvars`: Development environment configuration
- `staging.tfvars`: Staging environment configuration

## Usage

To apply the Terraform configuration with a specific environment:

```bash
# Initialize Terraform (only needed once)
terraform init

# Plan with environment-specific variables
terraform plan -var-file=environments/dev.tfvars

# Apply with environment-specific variables
terraform apply -var-file=environments/dev.tfvars
```

## Creating New Environments

To create a new environment:

1. Copy an existing environment file (e.g., `dev.tfvars`)
2. Rename it to match your environment (e.g., `prod.tfvars`)
3. Modify the variables as needed for your environment

Example for production:

```hcl
aws_region  = "us-east-1"
environment = "prod"
namespace   = "glue-poc"
prefix      = "terraform-glue"
bucket_name = "glue-artifacts-prod"

# Production-specific tags
tags = {
  Environment = "Production"
  Project     = "Glue POC"
  ManagedBy   = "Terraform"
  Team        = "Data Engineering"
}

# Add other production-specific configurations...
```

## Variable Precedence

Variables are loaded in the following order of precedence (highest to lowest):

1. Command line flags (`-var` and `-var-file`)
2. Environment variables (`TF_VAR_*`)
3. Variable files specified on the command line (`-var-file`)
4. `terraform.tfvars` or `terraform.tfvars.json`
5. `*.auto.tfvars` or `*.auto.tfvars.json`
6. Default values in variable declarations