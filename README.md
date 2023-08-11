# nan-aws-glue-etl-boilerplate

A complete example of an AWS Glue application that uses the Serverless Framework to deploy the infrastructure and DevContainers and/or Docker Compose to run the application locally with AWS Glue Libs, Spark, Jupyter Notebook, AWS CLI, among other tools. It provides jobs using Python Shell and PySpark.

# Local Development

## CI/CD

We're using GitHub Actions to build our pipelines. We know iterating on pipelines is always somewhat painful,
this is the recomendation about how to do it with less friction.

Use [act](https://github.com/nektos/act) to run the workflows locally and iterate faster without cluttering
the repo history.

@edwinabot tested this successfully in Ubuntu 22.04 running on WSL2 with Docker Desktop 4.16.3 (96739).
[Installed act](https://github.com/nektos/act#bash-script) via Bash script.

## PostgreSQL & pgAdmin

The dev environment comes with [pgAdmin](https://www.pgadmin.org/) configured so you can query the local PostgreSQL instance. After `docker compose up` you can access pgAdmin at http://localhost:5050

Look at the [.env.example](local/.env.example) for default users and passwords for Postgres and pgAdmin. The env vars are:

- POSTGRES_DB_USERNAME
- POSTGRES_DB_PASSWORD
- PGADMIN_DEFAULT_EMAIL
- PGADMIN_DEFAULT_PASSWORD

Exploring the configured servers for the first time in pgAdmin will prompt for the password for the server, that is POSTGRES_DB_PASSWORD.

## Notes

### Why the Dockerfile is at the root of the project and not in local?

Since we need to add some Python deps for our project, we need to `ADD` (or `COPY`) the `Pipfile`. The reason to have the Dockerfile at the root of the project is that:

> The <src> path must be inside the context of the build; you cannot ADD ../something /something, because the first step of a docker build is to send the context directory (and subdirectories) to the docker daemon. [Dockerfile ADD documentation](https://docs.docker.com/engine/reference/builder/#add)

To be more precise, we need the Dokerfile to be at least at the same level of the Pipfile.

### Why MongoDB 4.0 and not the Latest version?

At the time of this writing, March 2nd 2023, DocumentDB is [compatible with MongoDB 4.0](https://docs.aws.amazon.com/documentdb/latest/developerguide/compatibility.html). Using MongoDB 4.0 in the development environment makes sense for three reasons:

- Since these are compatible, we can use MongoDB to mimmic DocumentDB for local development.
- There's no official Docker image for DocumentDB.
- AWS Glue Lib Docker image comes with a Spark version that does not support the latest mongodb-driver-sync versions.

### Why the mongo driver downgrade?

The following traceback will give you a hint of what is going on:

```
Traceback (most recent call last):
  File "/home/glue_user/workspace/jobs/hello_world.py", line 25, in <module>
    load_to_document_db(ddf, config, "profiles")
  File "/home/glue_user/workspace/jobs/etl/load_documentdb.py", line 10, in load_to_document_db
    write_from_options(ddf, mode="overwrite", **connection_params)
  File "/home/glue_user/workspace/jobs/io/writer.py", line 21, in write_from_options
    ddf.toDF().write.format(format).mode(mode).options(**connection_options).save()
  File "/home/glue_user/spark/python/pyspark/sql/readwriter.py", line 966, in save
    self._jwrite.save()
  File "/home/glue_user/spark/python/lib/py4j-0.10.9.5-src.zip/py4j/java_gateway.py", line 1321, in __call__
  File "/home/glue_user/spark/python/pyspark/sql/utils.py", line 190, in deco
    return f(*a, **kw)
  File "/home/glue_user/spark/python/lib/py4j-0.10.9.5-src.zip/py4j/protocol.py", line 326, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling o90.save.
: java.lang.NoClassDefFoundError: com/mongodb/internal/connection/InternalConnectionPoolSettings
        at com.mongodb.client.internal.MongoClientImpl.createCluster(MongoClientImpl.java:223)
        at com.mongodb.client.internal.MongoClientImpl.<init>(MongoClientImpl.java:70)
        at com.mongodb.client.MongoClients.create(MongoClients.java:108)
        at com.mongodb.client.MongoClients.create(MongoClients.java:93)
        at com.mongodb.client.MongoClients.create(MongoClients.java:78)
        at com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory.create(DefaultMongoClientFactory.java:46)
        at com.mongodb.spark.sql.connector.connection.MongoClientCache.lambda$acquire$0(MongoClientCache.java:99)
        at java.util.HashMap.computeIfAbsent(HashMap.java:1128)
        at com.mongodb.spark.sql.connector.connection.MongoClientCache.acquire(MongoClientCache.java:97)
...
```

Basically, the Spark version that comes with AWS Glue Lib Docker image depends on mongodb-driver-sync-3.10.2.jar.
The provided version mongodb-driver-sync-4.7.2.jar has several breaking changes, an one of those we see in this stacktrace.

# Use Case

Let's consider the following case:

> "We need to build a datalake to support a threat intelligence operation"

We'll leverage OSINT sources, our first integration will be with [Abuse.ch](https://abuse.ch/),
[URLHaus](https://urlhaus.abuse.ch/api/) [Daily MISP Events](https://urlhaus.abuse.ch/downloads/misp/).
We'll implement an ETL for this source.
