# Development environment configuration for Terraform Glue deployment

aws_region  = "us-east-1"
environment = "dev"
namespace   = "glue-poc"
prefix      = "terraform-glue"
bucket_name = "glue-artifacts-dev"

# Development environment specific tags
tags = {
  Environment = "Development"
  Project     = "Glue POC"
  ManagedBy   = "Terraform"
  Team        = "Data Engineering"
}

# Development environment specific Glue job configurations
glue_jobs = {
  pyspark_hello_world = {
    script_path         = "jobs/pyspark_hello_world.py"
    type               = "spark"
    glue_version       = "5.0"
    max_concurrent_runs = 3
    worker_type        = "Standard"
    number_of_workers  = 2  # More workers for development testing
    timeout            = 3000
    max_retries        = 1
    connections        = []
    default_arguments = {
      "--additional-python-modules" = "tldextract==3.3.0"
      "--CUSTOM_ARGUMENT"          = "DEV_VALUE"
      "--enable-metrics"           = "true"
      "--enable-spark-ui"          = "true"
      "--job-bookmark-option"      = "job-bookmark-disable"
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
    python_version     = "3.11"
    max_concurrent_runs = 3
    worker_type        = "Standard"
    number_of_workers  = null
    timeout            = 3000
    max_retries        = 1
    connections        = []
    default_arguments = {
      "--additional-python-modules" = "tldextract==3.3.0"
      "--CUSTOM_ARGUMENT"          = "DEV_VALUE"
      "--enable-metrics"           = "true"
      "--job-bookmark-option"      = "job-bookmark-disable"
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

# Development environment specific networking configuration
vpc_id              = null  # Use default VPC for development
subnet_id           = null  # Use default subnet for development
database_cidr_blocks = ["10.0.0.0/8"]

# Development environment specific logging configuration
log_retention_days        = 7  # Shorter retention for development
service_log_retention_days = 3  # Shorter retention for development
enable_crawler_logs       = true  # Enable crawler logs for development testing