## Notes

Here are some notes on the project.

### Why the Dockerfile is at the root of the project and not in local?

To incorporate necessary Python dependencies for our project, we need to `ADD` (or `COPY`) the `Pipfile`. The Dockerfile is placed at the root of the project because:

> The `<src>` path must be inside the context of the build; you cannot ADD ../something /something, because the first step of a docker build is to send the context directory (and subdirectories) to the docker daemon. [Dockerfile ADD documentation](https://docs.docker.com/engine/reference/builder/#add)

In essence, the Dockerfile needs to be at least at the same level as the Pipfile.

### Why MongoDB 4.0 and not the Latest version?

As of March 2nd, 2023, DocumentDB is [compatible with MongoDB 4.0](https://docs.aws.amazon.com/documentdb/latest/developerguide/compatibility.html). Using MongoDB 4.0 in the development environment is advantageous for several reasons:

- Since these versions are compatible, we can use MongoDB to mimic DocumentDB for local development.
- There's no official Docker image for DocumentDB.
- The AWS Glue Lib Docker image comes with a Spark version that does not support the latest mongodb-driver-sync versions.

### Why the mongo driver downgrade?

The provided traceback indicates the issue:

```txt
Traceback (most recent call last):
  ...
py4j.protocol.Py4JJavaError: An error occurred while calling o90.save.
: java.lang.NoClassDefFoundError: com/mongodb/internal/connection/InternalConnectionPoolSettings
        ...
```

The Spark version in the AWS Glue Lib Docker image relies on `mongodb-driver-sync-3.10.2.jar`. The version `mongodb-driver-sync-4.7.2.jar`, provided, introduces breaking changes, one of which is visible in this stack trace.
