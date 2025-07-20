locals {
  # Common tags applied to all resources
  common_tags = {
    Environment = var.environment
    Project     = var.namespace
    ManagedBy   = "terraform"
  }
  
  # Resource naming convention
  name_prefix = "${var.prefix}-${var.environment}"
  
  # S3 bucket name with prefix
  bucket_name = "${local.name_prefix}-${var.bucket_name}"
}