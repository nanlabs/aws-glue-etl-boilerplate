# Output values for important resource identifiers
# This file defines outputs for the Terraform Glue deployment

# S3 bucket outputs
output "s3_bucket_name" {
  description = "Name of the S3 bucket for Glue artifacts"
  value       = module.s3_bucket.bucket_id
  precondition {
    condition     = module.s3_bucket.bucket_id != null
    error_message = "S3 bucket creation failed. Check AWS permissions and bucket name availability."
  }
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket for Glue artifacts"
  value       = module.s3_bucket.bucket_arn
}

output "s3_bucket_domain_name" {
  description = "Domain name of the S3 bucket for Glue artifacts"
  value       = module.s3_bucket.bucket_domain_name
}

output "s3_bucket_region" {
  description = "Region of the S3 bucket for Glue artifacts"
  value       = var.aws_region
}

# Environment information outputs
output "environment" {
  description = "Environment name"
  value       = var.environment
}

output "namespace" {
  description = "Namespace for resource naming"
  value       = var.namespace
}

output "deployment_prefix" {
  description = "Prefix used for resource naming"
  value       = var.prefix
}

# Script upload outputs
output "pyspark_script_s3_path" {
  description = "S3 path to the PySpark hello world script"
  value       = "s3://${module.s3_bucket.bucket_id}/${aws_s3_object.pyspark_hello_world_script.key}"
  precondition {
    condition     = aws_s3_object.pyspark_hello_world_script.etag != null
    error_message = "PySpark script upload failed. Check if the source file exists at ../jobs/pyspark_hello_world.py"
  }
}

output "pythonshell_script_s3_path" {
  description = "S3 path to the Python shell hello world script"
  value       = "s3://${module.s3_bucket.bucket_id}/${aws_s3_object.pythonshell_hello_world_script.key}"
  precondition {
    condition     = aws_s3_object.pythonshell_hello_world_script.etag != null
    error_message = "Python shell script upload failed. Check if the source file exists at ../jobs/pythonshell_hello_world.py"
  }
}

output "libs_s3_path" {
  description = "S3 path to the support libraries zip file"
  value       = "s3://${module.s3_bucket.bucket_id}/${aws_s3_object.libs_zip.key}"
}

# Glue job outputs
output "glue_jobs" {
  description = "Map of created Glue jobs with their ARNs"
  value = merge(
    {
      for k, v in aws_glue_job.pyspark_jobs : k => {
        name = v.name
        arn  = v.arn
        type = "spark"
      }
    },
    {
      for k, v in aws_glue_job.pythonshell_jobs : k => {
        name = v.name
        arn  = v.arn
        type = "pythonshell"
      }
    }
  )
}

output "glue_job_names" {
  description = "Names of all created Glue jobs"
  value = concat(
    [for job in aws_glue_job.pyspark_jobs : job.name],
    [for job in aws_glue_job.pythonshell_jobs : job.name]
  )
}

# IAM role outputs
output "glue_job_role_arn" {
  description = "ARN of the IAM role used by Glue jobs"
  value       = module.glue_job_role.arn
  precondition {
    condition     = module.glue_job_role.arn != null
    error_message = "Glue job IAM role creation failed. Check AWS permissions and role name availability."
  }
}

output "glue_job_role_name" {
  description = "Name of the IAM role used by Glue jobs"
  value       = module.glue_job_role.name
}

# CloudWatch log group outputs
output "glue_job_log_groups" {
  description = "CloudWatch log group names for all Glue jobs"
  value = {
    for k, v in aws_cloudwatch_log_group.glue_job_logs : k => v.name
  }
}

output "glue_job_log_group_arns" {
  description = "ARNs of all Glue job CloudWatch log groups"
  value = {
    for k, v in aws_cloudwatch_log_group.glue_job_logs : k => v.arn
  }
}

output "glue_service_log_group" {
  description = "CloudWatch log group name for Glue service logs"
  value       = aws_cloudwatch_log_group.glue_service_logs.name
}

output "glue_service_log_group_arn" {
  description = "ARN of the Glue service CloudWatch log group"
  value       = aws_cloudwatch_log_group.glue_service_logs.arn
}

output "glue_crawler_log_group" {
  description = "CloudWatch log group name for Glue crawlers (if enabled)"
  value       = var.enable_crawler_logs ? aws_cloudwatch_log_group.glue_crawler_logs[0].name : null
}

# Networking outputs
output "glue_security_group_id" {
  description = "ID of the security group for Glue connections"
  value       = var.vpc_id != null ? aws_security_group.glue_connection.id : null
}

output "glue_connection_name" {
  description = "Name of the Glue connection for database access (if created)"
  value       = var.vpc_id != null && var.subnet_id != null ? aws_glue_connection.database_connection[0].name : null
}

# Deployment summary
output "deployment_summary" {
  description = "Summary of the Terraform Glue deployment"
  value = {
    environment      = var.environment
    region           = var.aws_region
    s3_bucket        = module.s3_bucket.bucket_id
    job_count        = length(var.glue_jobs)
    pyspark_jobs     = length([for k, v in var.glue_jobs : k if v.type == "spark"])
    pythonshell_jobs = length([for k, v in var.glue_jobs : k if v.type == "pythonshell"])
    vpc_deployed     = var.vpc_id != null
  }
}