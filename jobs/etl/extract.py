from typing import List
from awsglue.context import GlueContext

from jobs.io import read_from_options
from libs.config import Config
from libs.aws import AwsS3Client
from jobs.io import get_df_mapping


def extract(glueContext: GlueContext, config: Config):
    client = AwsS3Client(**config.s3_vars)

    connection_params = config.aws_client_vars
    connection_params["engine"] = "s3"
    connection_params["paths"] = [
        f"s3://{o.bucket_name}/{o.key}" for o in client.get_objects()
    ]
    ddf = read_from_options(glueContext, **connection_params)
    return ddf

# this function take a csv file from an s3bucket and create a column type mapped parquet file using dynamic frame
def extract_from_s3_csv(
    glue_context: GlueContext,
    file_bucket,
    file_path,
    target_bucket,
    target_folder,
    report_name
):
    s3_root_folder = "s3://{}/{}"
    
    #open the file directly wth the dinamic frame
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_root_folder.format(file_bucket, file_path)]},
        format="csv",
        format_options={
            "withHeader": True,
        },
    )
    
    # apply column type mapping
    dynamic_frame = dynamic_frame.apply_mapping(get_df_mapping(report_name))
    
    # store the mapped dynamic frame as parquet
    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": s3_root_folder.format(target_bucket, target_folder),
        }
    )
    
    # return the result DF of the mapped dynamic frame
    return dynamic_frame.toDF()