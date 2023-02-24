import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from jobs.etl.extract import extract
from jobs.etl.load import load
from libs.config import get_config

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

config = get_config()

paths = [f"s3://{config.s3_bucket_name}/*"]

ddf = extract(glueContext, paths, config)
ddf.printSchema()

ddf = ddf.relationalize()
ddf.printSchema()

load(ddf, config)

job.commit()
