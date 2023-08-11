# AWS Glue ETL Boilerplate

Welcome to the AWS Glue ETL Boilerplate repository! This is an example AWS Glue application that uses the Serverless Framework to deploy infrastructure and allows local development with AWS Glue Libs, Spark, Jupyter Notebook, and more. It includes jobs using Python Shell and PySpark.

## Motivation 🚀

Are you ready to supercharge your ETL development with AWS Glue? This repository is here to help you quickly set up, develop, and deploy AWS Glue jobs. Streamline your ETL pipelines, harness the power of AWS Glue Libs and Spark, and unlock efficient local development.

## Features ✨

- **Full AWS Glue Setup:** Deploy Glue jobs using Python Shell Script and PySpark.
- **Flexible Local Development:** Choose between using VSCode + Remote Containers or Docker Compose.
- **Comprehensive Documentation:** Easy-to-follow guides for development and deployment.
- **Reusable Examples:** Building upon multiple examples to provide a well-rounded solution.
- **Serverless Framework:** Utilize Serverless Framework to deploy AWS Glue jobs and other resources.

## Usage

To quickly start a project using this example, follow these steps:

```sh
npx serverless install -u https://github.com/nanlabs/devops-reference/tree/main/examples/serverless-glue-full-boilerplate -n my-project
```

## Overview

This example was created by combining the best practices from our following examples:

- [Serverless Glue example](https://github.com/nanlabs/devops-reference/tree/main/examples/serverless-glue/)
- [AWS Glue docker example](https://github.com/nanlabs/devops-reference/tree/main/examples/docker/glue/)
- [VSCode DevContainer example](https://github.com/nanlabs/devops-reference/tree/main/examples/devcontainer/glue/)

## Requirements

- [Docker](https://www.docker.com/)
- [VSCode](https://code.visualstudio.com/) (optional)

## Quickstart

```sh
cp .env.example .env
```

## Local Development

Choose your preferred local development setup!

### Using VSCode + Remote Containers (recommended)

1. Install Docker
2. Install [VSCode](https://code.visualstudio.com/)
3. Install the [Remote Development](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack) extension
4. Clone this repository
5. Create your application within a container (see gif below)

![Create application within a container](./docs/vscode-open-in-container.gif)

Once the container is running inside VSCode, you can run the Glue jobs locally.

### Using Docker Compose manually

Refer to the [development documentation](./docs/DEVELOPMENT.md) for detailed steps to set up a local development environment using Docker Compose.

## Deployment

We utilize the Serverless Framework to deploy AWS Glue jobs and other resources. For deployment instructions, check out the [deployment documentation](./docs/DEPLOYMENT.md).

## PostgreSQL & pgAdmin

The dev environment comes with [pgAdmin](https://www.pgadmin.org/) configured so you can query the local PostgreSQL instance. After `docker compose up`, you can access pgAdmin at <http://localhost:5050>

Refer to the [.env.example](local/.env.example) for default users and passwords for Postgres and pgAdmin. The environment variables are:

- POSTGRES_DB_USERNAME
- POSTGRES_DB_PASSWORD
- PGADMIN_DEFAULT_EMAIL
- PGADMIN_DEFAULT_PASSWORD

When exploring the configured servers for the first time in pgAdmin, it will prompt for the server password, which is the value of POSTGRES_DB_PASSWORD.

## Notes

### Why the Dockerfile is at the root of the project and not in local?

To incorporate necessary Python dependencies for our project, we need to `ADD` (or `COPY`) the `Pipfile`. The Dockerfile is placed at the root of the project because:

> The <src> path must be inside the context of the build; you cannot ADD ../something /something, because the first step of a docker build is to send the context directory (and subdirectories) to the docker daemon. [Dockerfile ADD documentation](https://docs.docker.com/engine/reference/builder/#add)

In essence, the Dockerfile needs to be at least at the same level as the Pipfile.

### Why MongoDB 4.0 and not the Latest version?

As of March 2nd, 2023, DocumentDB is [compatible with MongoDB 4.0](https://docs.aws.amazon.com/documentdb/latest/developerguide/compatibility.html). Using MongoDB 4.0 in the development environment is advantageous for several reasons:

- Since these versions are compatible, we can use MongoDB to mimic DocumentDB for local development.
- There's no official Docker image for DocumentDB.
- The AWS Glue Lib Docker image comes with a Spark version that does not support the latest mongodb-driver-sync versions.

### Why the mongo driver downgrade?

The provided traceback indicates the issue:

```
Traceback (most recent call last):
  ...
py4j.protocol.Py4JJavaError: An error occurred while calling o90.save.
: java.lang.NoClassDefFoundError: com/mongodb/internal/connection/InternalConnectionPoolSettings
        ...
```

The Spark version in the AWS Glue Lib Docker image relies on mongodb-driver-sync-3.10.2.jar. The version mongodb-driver-sync-4.7.2.jar, provided, introduces breaking changes, one of which is visible in this stack trace.

## Use Case

Imagine the scenario:

> "We need to build a datalake to support a threat intelligence operation."

We'll leverage OSINT sources, starting with an integration with [Abuse.ch](https://abuse.ch/),
[URLHaus](https://urlhaus.abuse.ch/api/), and [Daily MISP Events](https://urlhaus.abuse.ch/downloads/misp/). Our ETL implementation will focus on this source.
