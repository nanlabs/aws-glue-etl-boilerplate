import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from jobs.etl.extract import extract
from jobs.etl.load import load_to_postgresql_db
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

load_to_postgresql_db(ddf, config, "profiles")

job.commit()
