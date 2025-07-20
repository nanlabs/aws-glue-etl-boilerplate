# Script upload configuration for Glue job artifacts
# This file handles uploading job scripts and support files to S3

# Upload PySpark hello world job script
resource "aws_s3_object" "pyspark_hello_world_script" {
  bucket = module.s3_bucket.bucket_id
  key    = "scripts/pyspark_hello_world.py"
  source = "../jobs/pyspark_hello_world.py"
  etag   = filemd5("../jobs/pyspark_hello_world.py")

  tags = merge(local.common_tags, {
    Name = "pyspark-hello-world-script"
    Type = "glue-job-script"
  })
}

# Upload Python shell hello world job script
resource "aws_s3_object" "pythonshell_hello_world_script" {
  bucket = module.s3_bucket.bucket_id
  key    = "scripts/pythonshell_hello_world.py"
  source = "../jobs/pythonshell_hello_world.py"
  etag   = filemd5("../jobs/pythonshell_hello_world.py")

  tags = merge(local.common_tags, {
    Name = "pythonshell-hello-world-script"
    Type = "glue-job-script"
  })
}