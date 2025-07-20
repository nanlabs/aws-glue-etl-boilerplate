import json
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Import modules from the uploaded zip files
# When running in AWS Glue, we need to use direct imports rather than package-style imports
from libs.etl import extract, load_to_s3
from libs.config import get_config
from libs.logger import get_logger, log_configuration

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Set up logging
logger = get_logger("pyspark_hello_world")
logger.info(f"Starting job: {args['JOB_NAME']}")

config = get_config()
log_configuration(logger, config)

logger.info("Starting data processing...")

ddf = extract(glueContext, config)
ddf.printSchema()
ddf.show(5)
logger.info("Data extraction complete, proceeding to load operations")

load_to_s3(ddf, config, path="profiles")
logger.info("S3 load operation complete")

logger.info("Job execution complete")
job.commit()
