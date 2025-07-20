# Staging environment configuration for Terraform Glue deployment

aws_region  = "us-east-1"
environment = "staging"
namespace   = "glue-poc"
prefix      = "terraform-glue"
bucket_name = "glue-artifacts-staging"

# Staging environment specific tags
tags = {
  Environment = "Staging"
  Project     = "Glue POC"
  ManagedBy   = "Terraform"
  Team        = "Data Engineering"
}

# Staging environment specific Glue job configurations
glue_jobs = {
  pyspark_hello_world = {
    script_path         = "jobs/pyspark_hello_world.py"
    type               = "spark"
    glue_version       = "5.0"
    max_concurrent_runs = 2
    worker_type        = "Standard"
    number_of_workers  = 2
    timeout            = 3000
    max_retries        = 2  # More retries in staging
    connections        = []
    default_arguments = {
      "--additional-python-modules" = "tldextract==3.3.0"
      "--CUSTOM_ARGUMENT"          = "STAGING_VALUE"
      "--enable-metrics"           = "true"
      "--enable-spark-ui"          = "true"
      "--job-bookmark-option"      = "job-bookmark-enable"  # Enable bookmarks in staging
    }
    support_files = [
      {
        local_path = "libs.zip"
        s3_prefix  = ""
      },
      {
        local_path = "jobs.zip"
        s3_prefix  = ""
      }
    ]
  }
  
  pythonshell_hello_world = {
    script_path         = "jobs/pythonshell_hello_world.py"
    type               = "pythonshell"
    max_concurrent_runs = 2
    worker_type        = "Standard"
    number_of_workers  = null
    timeout            = 3000
    max_retries        = 2  # More retries in staging
    connections        = []
    default_arguments = {
      "--additional-python-modules" = "tldextract==3.3.0"
      "--CUSTOM_ARGUMENT"          = "STAGING_VALUE"
      "--enable-metrics"           = "true"
      "--job-bookmark-option"      = "job-bookmark-enable"  # Enable bookmarks in staging
    }
    support_files = [
      {
        local_path = "libs.zip"
        s3_prefix  = ""
      },
      {
        local_path = "jobs.zip"
        s3_prefix  = ""
      }
    ]
  }
}

# Staging environment specific networking configuration
vpc_id              = null  # Will be populated with actual VPC ID for staging
subnet_id           = null  # Will be populated with actual subnet ID for staging
database_cidr_blocks = ["10.0.0.0/8"]

# Staging environment specific logging configuration
log_retention_days        = 14  # Medium retention for staging
service_log_retention_days = 7   # Medium retention for staging
enable_crawler_logs       = true  # Enable crawler logs for staging testing