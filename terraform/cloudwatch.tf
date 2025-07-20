# CloudWatch log groups for Glue jobs
# This file contains CloudWatch logging configuration for AWS Glue jobs
# Implements requirements 3.1 (essential Glue resources) and 3.3 (standard security configurations)

# CloudWatch log groups for all Glue jobs (unified approach)
resource "aws_cloudwatch_log_group" "glue_job_logs" {
  for_each = var.glue_jobs
  
  name              = "/aws-glue/jobs/${local.name_prefix}-${each.key}"
  retention_in_days = var.log_retention_days
  
  # Prevent accidental deletion of log groups
  skip_destroy = false
  
  tags = merge(local.common_tags, {
    JobName = each.key
    JobType = each.value.type
    LogType = "glue-job-logs"
    Purpose = "Glue job execution logs"
  })
}

# CloudWatch log group for Glue service logs
resource "aws_cloudwatch_log_group" "glue_service_logs" {
  name              = "/aws-glue/service/${local.name_prefix}"
  retention_in_days = var.service_log_retention_days
  
  # Prevent accidental deletion of service log groups
  skip_destroy = false
  
  tags = merge(local.common_tags, {
    LogType = "glue-service-logs"
    Purpose = "Glue service operational logs"
  })
}

# CloudWatch log group for Glue crawler logs (if needed in future)
resource "aws_cloudwatch_log_group" "glue_crawler_logs" {
  count = var.enable_crawler_logs ? 1 : 0
  
  name              = "/aws-glue/crawlers/${local.name_prefix}"
  retention_in_days = var.service_log_retention_days
  
  tags = merge(local.common_tags, {
    LogType = "glue-crawler-logs"
    Purpose = "Glue crawler execution logs"
  })
}