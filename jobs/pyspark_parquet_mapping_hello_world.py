import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from jobs.etl import extract_from_s3_csv
from awsglue.utils import getResolvedOptions

args_dict = getResolvedOptions(sys.argv, ["JOB_NAME", "file_bucket", "file_path", 
                                          "target_bucket", "farm_id", "report_folder", 
                                          "report_name"])

SOURCE_BUCKET = args_dict["file_bucket"]
FILE_PATH = args_dict["file_path"]
TARGET_BUCKET = args_dict["target_bucket"]
FARM_ID = args_dict["farm_id"]
REPORT_FOLDER = args_dict["report_folder"]
REPORT_NAME = args_dict["report_folder"]

sc = SparkContext()
glue_context = GlueContext(sc)
job = Job(glue_context)
job.init(args_dict["JOB_NAME"], args_dict)
ddf = extract_from_s3_csv(
    glue_context,
    SOURCE_BUCKET,
    FILE_PATH,
    TARGET_BUCKET,
    REPORT_FOLDER,
    REPORT_NAME
)
ddf.printSchema()
ddf.show()
job.commit()
