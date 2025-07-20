# S3 bucket configuration for Glue artifacts using CloudPosse module
# This file contains all S3-related resources

module "s3_bucket" {
  source  = "cloudposse/s3-bucket/aws"
  version = "~> 4.0"
  
  name        = var.bucket_name
  stage       = var.environment
  namespace   = var.namespace
  attributes  = ["glue", "artifacts"]
  
  # Enable versioning for artifact management
  versioning_enabled = true
  
  # Security settings
  acl                          = "private"
  block_public_acls            = true
  block_public_policy          = true
  ignore_public_acls           = true
  restrict_public_buckets      = true
  
  # Server-side encryption
  sse_algorithm = "AES256"
  
  # Lifecycle configuration for cost optimization
  lifecycle_configuration_rules = [
    {
      enabled = true
      id      = "delete_old_versions"
      
      noncurrent_version_expiration = {
        noncurrent_days = 30
      }
      
      abort_incomplete_multipart_upload = {
        days_after_initiation = 7
      }
    }
  ]
  
  # Additional tags specific to Glue artifacts
  tags = {
    Purpose = "glue-artifacts"
    Usage   = "job-scripts-and-support-files"
  }
}