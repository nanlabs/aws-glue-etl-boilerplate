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
