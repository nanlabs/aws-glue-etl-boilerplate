import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from jobs.etl import extract, load_to_s3, load_to_postgresql_db, load_to_document_db
from libs.config import get_config

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

config = get_config()

ddf = extract(glueContext, config)
ddf.printSchema()

load_to_s3(ddf, config, path="profiles")

load_to_postgresql_db(ddf, config, "profiles")
# load_to_document_db(ddf, config, "profiles")

job.commit()
