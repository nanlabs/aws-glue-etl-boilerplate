# AWS Glue job configurations
# This file contains all Glue job resource definitions

# Upload support files to S3
resource "aws_s3_object" "libs_zip" {
  bucket = module.s3_bucket.bucket_id
  key    = "libs.zip"
  source = "${path.module}/../build/libs.zip"
  etag   = filemd5("${path.module}/../build/libs.zip")

  tags = merge(local.common_tags, {
    Name = "libs-zip"
    Type = "glue-support-files"
  })
}

# Upload support files to S3
resource "aws_s3_object" "jobs_zip" {
  bucket = module.s3_bucket.bucket_id
  key    = "jobs.zip"
  source = "${path.module}/../build/jobs.zip"
  etag   = filemd5("${path.module}/../build/jobs.zip")

  tags = merge(local.common_tags, {
    Name = "jobs-zip"
    Type = "glue-support-files"
  })
}
# Upload requirements.txt to S3
resource "aws_s3_object" "requirements_txt" {
  bucket = module.s3_bucket.bucket_id
  key    = "requirements.txt"
  source = "${path.module}/../build/requirements.txt" 
  etag   = filemd5("${path.module}/../build/requirements.txt")
  tags = merge(local.common_tags, {
    Name = "requirements-txt"
    Type = "glue-support-files"
  })
}



# Upload job scripts to S3
resource "aws_s3_object" "job_scripts" {
  for_each = var.glue_jobs
  
  bucket = module.s3_bucket.bucket_id
  key    = each.value.script_path
  source = "../${each.value.script_path}"
  etag   = filemd5("../${each.value.script_path}")
  
  tags = merge(local.common_tags, {
    JobName = each.key
    JobType = each.value.type
  })
}

# PySpark Glue Jobs
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
  
  # Default arguments for PySpark jobs
  default_arguments = merge(
    {
      "--enable-metrics"                     = "true"
      "--enable-continuous-cloudwatch-log"  = "true"
      "--enable-continuous-log-filter"      = "true"
      "--continuous-log-logGroup"           = "/aws-glue/jobs/${local.name_prefix}-${each.key}"
      "--enable-spark-ui"                   = "true"
      "--spark-event-logs-path"             = "s3://${module.s3_bucket.bucket_id}/${each.key}/spark-events-logs/"
      "--TempDir"                           = "s3://${module.s3_bucket.bucket_id}/tmp/"
      "--enable-glue-datacatalog"           = "true"
      "--enable-s3-parquet-optimized-committer" = "true"
      "--job-bookmark-option"                = "job-bookmark-disable"
      "--additional-python-modules"         = "s3://${module.s3_bucket.bucket_id}/requirements.txt"
      "--python-modules-installer-option"   = "-r"
      # Support files - reference the uploaded zip files
      "--extra-py-files" = "s3://${module.s3_bucket.bucket_id}/libs.zip,s3://${module.s3_bucket.bucket_id}/jobs.zip"
      "--aws_region"          = var.aws_region
    },
    each.value.default_arguments
  )
  
  tags = merge(local.common_tags, {
    JobName = each.key
    JobType = each.value.type
  })
  
  depends_on = [
    aws_s3_object.job_scripts,
    aws_s3_object.libs_zip,
    aws_s3_object.jobs_zip,
    aws_s3_object.requirements_txt
  ]
}

# Python Shell Glue Jobs
resource "aws_glue_job" "pythonshell_jobs" {
  for_each = {
    for k, v in var.glue_jobs : k => v
    if v.type == "pythonshell"
  }
  
  name              = "${local.name_prefix}-${each.key}"
  role_arn          = module.glue_job_role.arn
  timeout           = each.value.timeout
  max_retries       = each.value.max_retries
  
  command {
    name            = "pythonshell"
    script_location = "s3://${module.s3_bucket.bucket_id}/${each.value.script_path}"
    python_version  = "3"
  }
  
  # Default arguments for Python shell jobs
  default_arguments = merge(
    {
      "--enable-metrics"                     = "true"
      "--enable-continuous-cloudwatch-log"  = "true"
      "--enable-continuous-log-filter"      = "true"
      "--continuous-log-logGroup"           = "/aws-glue/jobs/${local.name_prefix}-${each.key}"
      "--TempDir"                           = "s3://${module.s3_bucket.bucket_id}/tmp/"
      "--enable-glue-datacatalog"           = "true"
      "--job-bookmark-option"               = "job-bookmark-disable"
      # Python requirements.txt support for Glue 5.0+
      "--python-modules-installer-option"   = "-r"
      "--additional-python-modules"         = "s3://${module.s3_bucket.bucket_id}/requirements.txt"
      # Support files - reference the uploaded zip files
      "--extra-py-files" = "s3://${module.s3_bucket.bucket_id}/libs.zip,s3://${module.s3_bucket.bucket_id}/jobs.zip"
    },
    each.value.default_arguments
  )
  
  tags = merge(local.common_tags, {
    JobName = each.key
    JobType = each.value.type
  })
  
  depends_on = [
    aws_s3_object.job_scripts,
    aws_s3_object.libs_zip,
    aws_s3_object.jobs_zip,
    aws_s3_object.requirements_txt
  ]
}