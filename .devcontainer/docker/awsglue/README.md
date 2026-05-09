# AWS Glue local image notes

Docker Compose mounts `docker/awsglue/spark-defaults.conf` into `/opt/spark/conf/spark-defaults.conf`.
When `env=local` and `use_spark_defaults_conf=true` (default), jobs won’t set Spark configs in code; Spark uses the mounted defaults.
The defaults set `spark.sql.defaultCatalog=glue_catalog`, point Iceberg to `s3://local-bucket/warehouse/`, and wire LocalStack endpoints for S3.
To tweak local Spark settings, edit `docker/awsglue/spark-defaults.conf` and restart the container.
To force programmatic configs locally for a job, run with `--use_spark_defaults_conf=false` or set `<JOB_NAME>_USE_SPARK_DEFAULTS_CONF=false`.
# AWS Glue local image notes

- The compose file mounts `docker/awsglue/spark-defaults.conf` into `/opt/spark/conf/spark-defaults.conf` inside the container.
- When `env=local` and `use_spark_defaults_conf=true` (default in ConfigBase), the jobs won't set Spark configs programmatically. Spark will read from the mounted defaults file instead.
- The defaults define the Iceberg catalog `glue_catalog` with LocalStack endpoints and s3:// warehouse path.
- To tweak local behavior, edit `spark-defaults.conf` and restart the container.
