import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


persons = glueContext.create_dynamic_frame.from_catalog(
    database="legislators", table_name="persons_json"
)

persons.printSchema()

persons = persons.relationalize()

persons.printSchema()

glueContext.write_dynamic_frame.from_options(
    frame=persons,
    connection_type="postgresql",
    connection_options={
        "url": "foo__postgresql__conn__string",
        "user": "foou",
        "password": "bar",
    },
    format="parquet",
)

job.commit()
