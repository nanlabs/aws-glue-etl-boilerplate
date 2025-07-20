# IAM roles and policies for Glue resources
# This file contains IAM-related resources for Glue job execution

# Data source for AWS account ID
data "aws_caller_identity" "current" {}

# Data source for AWS partition
data "aws_partition" "current" {}

# IAM policy document for S3 access
data "aws_iam_policy_document" "glue_s3_policy" {
  statement {
    sid    = "S3BucketAccess"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:GetBucketVersioning"
    ]
    resources = [
      module.s3_bucket.bucket_arn
    ]
  }
  
  statement {
    sid    = "S3ObjectAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "${module.s3_bucket.bucket_arn}/*"
    ]
  }
}

# IAM policy document for CloudWatch Logs access
data "aws_iam_policy_document" "glue_cloudwatch_policy" {
  statement {
    sid    = "CloudWatchLogsAccess"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
    ]
  }
}

# IAM policy document for VPC resources access
data "aws_iam_policy_document" "glue_vpc_policy" {
  statement {
    sid    = "VPCNetworkAccess"
    effect = "Allow"
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeVpcs",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeRouteTables",
      "ec2:CreateTags",
      "ec2:DeleteTags",
      "ec2:DescribeTags"
    ]
    resources = ["*"]
  }
  
  statement {
    sid    = "ENIAttachDetach"
    effect = "Allow"
    actions = [
      "ec2:AttachNetworkInterface",
      "ec2:DetachNetworkInterface"
    ]
    resources = ["*"]
  }
}

# Create IAM policies
resource "aws_iam_policy" "glue_s3_policy" {
  name        = "${var.prefix}-glue-s3-policy"
  description = "IAM policy for Glue job S3 access"
  policy      = data.aws_iam_policy_document.glue_s3_policy.json
  
  tags = local.common_tags
}

resource "aws_iam_policy" "glue_cloudwatch_policy" {
  name        = "${var.prefix}-glue-cloudwatch-policy"
  description = "IAM policy for Glue job CloudWatch Logs access"
  policy      = data.aws_iam_policy_document.glue_cloudwatch_policy.json
  
  tags = local.common_tags
}

resource "aws_iam_policy" "glue_vpc_policy" {
  name        = "${var.prefix}-glue-vpc-policy"
  description = "IAM policy for Glue job VPC access"
  policy      = data.aws_iam_policy_document.glue_vpc_policy.json
  
  tags = local.common_tags
}

# Glue job role using CloudPosse module
module "glue_job_role" {
  source  = "cloudposse/iam-role/aws"
  version = "~> 0.19"
  
  name             = "glue-job-role"
  stage            = var.environment
  namespace        = var.namespace
  attributes       = ["glue", "job"]
  role_description = "IAM role for AWS Glue job execution"
  
  # Configure principals that can assume this role
  principals = {
    Service = ["glue.amazonaws.com"]
  }
  
  # Attach custom policies
  policy_documents = [
    data.aws_iam_policy_document.glue_s3_policy.json,
    data.aws_iam_policy_document.glue_cloudwatch_policy.json,
    data.aws_iam_policy_document.glue_vpc_policy.json
  ]
  
  policy_document_count = 3
  
  tags = local.common_tags
}

# Attach AWS managed policy for Glue service
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = module.glue_job_role.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}