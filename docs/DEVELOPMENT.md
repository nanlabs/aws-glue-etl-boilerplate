# AWS Glue libs

## Requirements

- [docker](https://www.docker.com/)

## Quickstart

```sh
docker compose -f local-dev/compose.yml up
```

## Execute jobs

You can test the example `jobs/hello_world.py` in the following way:

```sh
# attach to the container
docker compose -f local-dev/compose.yml exec -it awsglue /bin/bash
```

and then run the following command inside the container:

```sh
# Prepare the environment
pip3 install -U requirements.txt
pip3 install -U --editable .

# Run the job
glue-spark-submit jobs/hello_world.py --JOB_NAME job_example --CUSTOM_ARGUMENT custom_value
```
