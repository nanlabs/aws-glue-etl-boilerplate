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

Look at the [.env.example](local-dev/.env.example) for default users and passwords for Postgres and pgAdmin. The env vars are:

* POSTGRES_DB_USERNAME
* POSTGRES_DB_PASSWORD
* PGADMIN_DEFAULT_EMAIL
* PGADMIN_DEFAULT_PASSWORD

Exploring the configured servers for the first time in pgAdmin will prompt for the password for the server, that is POSTGRES_DB_PASSWORD.
## Notes

### Why the Dockerfile is at the root of the project and not in local-dev?

Since we need to add some Python deps for our project, we need to `ADD` (or `COPY`) the `Pipfile`. The reason  to have the Dockerfile at the root of the project is that:

> The <src> path must be inside the context of the build; you cannot ADD ../something /something, because the first step of a docker build is to send the context directory (and subdirectories) to the docker daemon. [Dockerfile ADD documentation](https://docs.docker.com/engine/reference/builder/#add)

To be more precise, we need the Dokerfile to be at least at the same level of the Pipfile.

# Use Case

Let's consider the following case:

> "We need to build a datalake to support a threat intelligence operation"

We'll leverage OSINT sources, our first integration will be with [Abuse.ch](https://abuse.ch/),
[URLHaus](https://urlhaus.abuse.ch/api/) [Daily MISP Events](https://urlhaus.abuse.ch/downloads/misp/).
We'll implement an ETL for this source.
