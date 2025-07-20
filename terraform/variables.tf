# Essential variables for the POC
# These variables define the basic configuration for the Terraform Glue deployment

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "namespace" {
  description = "Namespace for resource naming"
  type        = string
  default     = "glue-poc"
}

variable "prefix" {
  description = "Prefix for resource names"
  type        = string
  default     = "terraform-glue"
}

variable "bucket_name" {
  description = "Name for the S3 bucket storing Glue artifacts"
  type        = string
  default     = "glue-artifacts"
}

variable "tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "glue_jobs" {
  description = "Map of Glue jobs to create"
  type = map(object({
    script_path         = string
    type               = string # "spark" or "pythonshell"
    glue_version       = optional(string)
    python_version     = optional(string)
    max_concurrent_runs = number
    worker_type        = string
    number_of_workers  = optional(number)
    timeout            = number
    max_retries        = number
    connections        = list(string)
    default_arguments  = map(string)
    support_files = list(object({
      local_path = string
      s3_prefix  = string
    }))
  }))
  default = {}
}

# Networking variables
variable "vpc_id" {
  description = "VPC ID for Glue connections"
  type        = string
  default     = null
}

variable "database_cidr_blocks" {
  description = "CIDR blocks for database access"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}

variable "subnet_id" {
  description = "Subnet ID for Glue connections"
  type        = string
  default     = null
}

# CloudWatch logging variables
variable "log_retention_days" {
  description = "Number of days to retain Glue job logs"
  type        = number
  default     = 14
  validation {
    condition = contains([
      1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653
    ], var.log_retention_days)
    error_message = "Log retention days must be a valid CloudWatch log retention value."
  }
}

variable "service_log_retention_days" {
  description = "Number of days to retain Glue service logs"
  type        = number
  default     = 7
  validation {
    condition = contains([
      1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653
    ], var.service_log_retention_days)
    error_message = "Service log retention days must be a valid CloudWatch log retention value."
  }
}

variable "enable_crawler_logs" {
  description = "Enable CloudWatch log group for Glue crawlers"
  type        = bool
  default     = false
}