# Terraform Glue Deployment

This document provides instructions for deploying AWS Glue jobs and infrastructure using Terraform.

## Prerequisites

- [Terraform](https://www.terraform.io/downloads.html) >= 1.0
- [AWS CLI](https://aws.amazon.com/cli/) configured with appropriate credentials
- Python 3.9+ for local development and testing

## AWS Credentials Setup

Configure your AWS credentials using one of the following methods:

### Option 1: AWS CLI Configuration
```bash
aws configure
```

### Option 2: Environment Variables
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

### Option 3: IAM Roles (for EC2/ECS deployments)
Ensure your instance has an IAM role with the necessary permissions.

## Required AWS Permissions

Your AWS credentials need the following permissions:
- `glue:*` - Full Glue service access
- `iam:*` - IAM role and policy management
- `s3:*` - S3 bucket operations
- `logs:*` - CloudWatch logs access
- `ec2:DescribeVpcs`, `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups` - VPC operations

## Deployment Steps

### 1. Initialize Terraform

Navigate to the terraform directory and initialize:

```bash
cd terraform
terraform init
```

### 2. Configure Variables

Copy and customize the terraform variables:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your specific values:

```hcl
# Required variables
project_name = "my-glue-project"
environment = "dev"
aws_region = "us-east-1"

# Optional variables
glue_version = "4.0"
python_version = "3"
```

### 3. Plan Deployment

Review the planned changes:

```bash
terraform plan
```

### 4. Deploy Infrastructure

Apply the Terraform configuration:

```bash
terraform apply
```

Type `yes` when prompted to confirm the deployment.

### 5. Upload Job Scripts

After infrastructure deployment, upload your Glue job scripts:

```bash
# Make the upload script executable
chmod +x test_script_upload.sh

# Upload scripts to S3
./test_script_upload.sh
```

## Deployment Verification

After deployment, verify the resources were created:

```bash
# List Glue jobs
aws glue get-jobs

# Check S3 buckets
aws s3 ls

# Verify IAM roles
aws iam list-roles --query 'Roles[?contains(RoleName, `glue`)]'
```

## Environment Management

### Development Environment
```bash
terraform workspace new dev
terraform workspace select dev
terraform apply -var-file="environments/dev.tfvars"
```

### Production Environment
```bash
terraform workspace new prod
terraform workspace select prod
terraform apply -var-file="environments/prod.tfvars"
```

## Cleanup

To destroy all resources:

```bash
terraform destroy
```

**Warning**: This will permanently delete all resources. Ensure you have backups of any important data.

## Troubleshooting

### Common Issues

1. **Permission Denied Errors**
   - Verify AWS credentials are configured correctly
   - Check IAM permissions match requirements above

2. **Resource Already Exists**
   - Check if resources exist from previous deployments
   - Use `terraform import` to import existing resources

3. **S3 Bucket Name Conflicts**
   - S3 bucket names must be globally unique
   - Modify the `project_name` variable to use a unique prefix

### Getting Help

- Check Terraform logs: `TF_LOG=DEBUG terraform apply`
- Review AWS CloudTrail for API call details
- Consult the [Terraform AWS Provider documentation](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
##
 Migration from Serverless Framework

This section provides guidance for migrating from the existing Serverless Framework deployment to Terraform.

### Key Differences

| Aspect | Serverless Framework | Terraform |
|--------|---------------------|-----------|
| **Configuration** | YAML-based (`serverless.yml`, `glue.yml`) | HCL-based (`.tf` files) |
| **State Management** | CloudFormation stacks | Terraform state files |
| **Resource Naming** | Auto-generated with service/stage | Explicit naming with variables |
| **Modularity** | Plugin-based | Module-based |
| **Variable Management** | Environment variables + YAML | `.tfvars` files |

### Side-by-Side Comparison

#### Glue Job Definition

**Serverless Framework (`glue.yml`):**
```yaml
jobs:
  - name: pyspark_hello_world
    scriptPath: jobs/pyspark_hello_world.py
    type: spark
    role: !Sub arn:aws:iam::${AWS::AccountId}:role/GlueJobRole/GlueJobRole
    glueVersion: python3-4.0
    MaxConcurrentRuns: 3
    WorkerType: Standard
    NumberOfWorkers: 1
    Timeout: 3000
    MaxRetries: 1
    DefaultArguments:
      usePostgresDriver: true
      enableS3ParquetOptimizedCommitter: true
      extraPyFiles: s3://bucket/libs.zip,s3://bucket/jobs.zip
```

**Terraform (`terraform/glue_jobs.tf`):**
```hcl
resource "aws_glue_job" "pyspark_jobs" {
  for_each = {
    for k, v in var.glue_jobs : k => v
    if v.type == "spark"
  }
  
  name              = "${local.name_prefix}-${each.key}"
  role_arn          = module.glue_job_role.arn
  glue_version      = each.value.glue_version
  worker_type       = each.value.worker_type
  number_of_workers = each.value.number_of_workers
  timeout           = each.value.timeout
  max_retries       = each.value.max_retries
  
  execution_property {
    max_concurrent_runs = each.value.max_concurrent_runs
  }
  
  command {
    name            = "glueetl"
    script_location = "s3://${module.s3_bucket.bucket_id}/${each.value.script_path}"
    python_version  = "3"
  }
  
  default_arguments = merge(
    {
      "--enable-s3-parquet-optimized-committer" = "true"
      "--extra-py-files" = "s3://${module.s3_bucket.bucket_id}/libs.zip,s3://${module.s3_bucket.bucket_id}/jobs.zip"
    },
    each.value.default_arguments
  )
}
```

#### IAM Role Configuration

**Serverless Framework (`resource/GlueResources.yml`):**
```yaml
GlueJobRole:
  Type: AWS::IAM::Role
  Properties:
    Path: /${self:service}-${sls:stage}-JobRole/
    RoleName: ${self:service}-${sls:stage}-JobRole
    AssumeRolePolicyDocument:
      Version: "2012-10-17"
      Statement:
        - Effect: Allow
          Principal:
            Service:
              - glue.amazonaws.com
          Action: sts:AssumeRole
    Policies:
      - PolicyName: GlueJobPolicy
        PolicyDocument:
          Version: "2012-10-17"
          Statement:
            - Effect: Allow
              Action:
                - s3:GetObject
                - s3:PutObject
              Resource:
                - "arn:aws:s3:::bucket/*"
```

**Terraform (`terraform/iam.tf`):**
```hcl
module "glue_job_role" {
  source  = "cloudposse/iam-role/aws"
  version = "~> 0.19"
  
  name             = "glue-job-role"
  stage            = var.environment
  namespace        = var.namespace
  role_description = "IAM role for AWS Glue job execution"
  
  principals = {
    Service = ["glue.amazonaws.com"]
  }
  
  policy_documents = [
    data.aws_iam_policy_document.glue_s3_policy.json,
    data.aws_iam_policy_document.glue_cloudwatch_policy.json,
    data.aws_iam_policy_document.glue_vpc_policy.json
  ]
}

data "aws_iam_policy_document" "glue_s3_policy" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = [
      "${module.s3_bucket.bucket_arn}/*"
    ]
  }
}
```

#### Security Group Configuration

**Serverless Framework (`resource/GlueResources.yml`):**
```yaml
GlueSecurityGroup:
  Type: AWS::EC2::SecurityGroup
  Properties:
    GroupName: ${self:service}-${sls:stage}-security-group
    GroupDescription: ${self:service}-${sls:stage} Security Group
    SecurityGroupIngress:
      - IpProtocol: -1
        Description: Allow all traffic by default
        CidrIp: 0.0.0.0/0
    VpcId: ${env:VPC_ID}
```

**Terraform (`terraform/networking.tf`):**
```hcl
resource "aws_security_group" "glue_connection" {
  name_prefix = "${local.name_prefix}-glue-connection-"
  description = "Security group for AWS Glue connections"
  vpc_id      = var.vpc_id

  egress {
    description = "PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.database_cidr_blocks
  }

  egress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```

### Migration Steps

#### 1. Backup Current Deployment

Before migrating, document your current resources:

```bash
# Export current CloudFormation stack
aws cloudformation describe-stacks --stack-name your-service-dev > current-stack.json

# List current Glue jobs
aws glue get-jobs > current-glue-jobs.json

# List current IAM roles
aws iam list-roles --query 'Roles[?contains(RoleName, `glue`)]' > current-iam-roles.json
```

#### 2. Configure Terraform Variables

Create `terraform.tfvars` based on your current serverless configuration:

```hcl
# From serverless.yml service name and stage
project_name = "your-service-name"
environment = "dev"

# From environment variables in serverless
aws_region = "us-east-1"
vpc_id = "vpc-xxxxxxxxx"
subnet_id = "subnet-xxxxxxxxx"

# Glue job configurations
glue_jobs = {
  pyspark_hello_world = {
    script_path = "jobs/pyspark_hello_world.py"
    type = "spark"
    glue_version = "5.0"
    worker_type = "Standard"
    number_of_workers = 1
    timeout = 3000
    max_retries = 1
    max_concurrent_runs = 3
    connections = ["database-connection"]
    default_arguments = {
      "--additional-python-modules" = "tldextract==3.3.0"
      "--CUSTOM_ARGUMENT" = "CUSTOM_VALUE"
    }
  }
  pythonshell_hello_world = {
    script_path = "jobs/pythonshell_hello_world.py"
    type = "pythonshell"
    timeout = 3000
    max_retries = 1
    max_concurrent_runs = 3
    connections = ["database-connection"]
    default_arguments = {
      "--additional-python-modules" = "tldextract==3.3.0"
      "--CUSTOM_ARGUMENT" = "CUSTOM_VALUE"
    }
  }
}
```

#### 3. Import Existing Resources (Optional)

If you want to manage existing resources with Terraform:

```bash
# Import S3 bucket
terraform import module.s3_bucket.aws_s3_bucket.this your-service-dev-glue-bucket-deploy

# Import IAM role
terraform import module.glue_job_role.aws_iam_role.default your-service-dev-JobRole

# Import Glue jobs
terraform import aws_glue_job.pyspark_jobs[\"pyspark_hello_world\"] pyspark_hello_world
terraform import aws_glue_job.pythonshell_jobs[\"pythonshell_hello_world\"] pythonshell_hello_world
```

#### 4. Deploy with Terraform

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

#### 5. Cleanup Serverless Resources

After verifying Terraform deployment:

```bash
# Remove serverless deployment
npx serverless remove --stage dev
```

### Configuration Mapping

| Serverless Config | Terraform Equivalent | Notes |
|------------------|---------------------|-------|
| `${self:service}` | `var.project_name` | Project identifier |
| `${sls:stage}` | `var.environment` | Environment name |
| `${env:VPC_ID}` | `var.vpc_id` | VPC configuration |
| `${env:SUBNET_ID}` | `var.subnet_id` | Subnet configuration |
| `!Ref GlueConnection` | `aws_glue_connection.database_connection[0].name` | Connection reference |
| `bucketDeploy` | `module.s3_bucket.bucket_id` | S3 bucket reference |

### Benefits of Migration

1. **Better State Management**: Terraform state provides better tracking of resource changes
2. **Improved Modularity**: Reusable modules for common patterns
3. **Enhanced Variable Management**: Type-safe variables with validation
4. **Better Resource Dependencies**: Explicit dependency management
5. **Multi-Environment Support**: Easier environment management with workspaces
6. **Community Modules**: Access to CloudPosse and other community modules

### Troubleshooting Migration

**Resource Name Conflicts:**
- Terraform uses different naming conventions
- Update references in job scripts if they use hardcoded resource names

**State Drift:**
- Use `terraform plan` to identify differences
- Consider using `terraform import` for critical resources

**Permission Issues:**
- Ensure Terraform has the same permissions as Serverless Framework
- May need additional permissions for state management