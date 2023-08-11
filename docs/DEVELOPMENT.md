# Development using Docker Compose

## Requirements

- [docker](https://www.docker.com/)
- [docker-compose](https://docs.docker.com/compose/)

## Quickstart

```sh
cp .envrc.example .envrc
direnv allow
docker-compose -f local/compose.yml up
```

## Execute jobs

You can test the example `jobs/pyspark_hello_world.py` in the following way:

```sh
# attach to the container
docker-compose -f local/compose.yml exec -it awsglue /bin/bash
```

and then run the following command inside the container:

```sh
glue-spark-submit jobs/pyspark_hello_world.py --JOB_NAME job_example --CUSTOM_ARGUMENT custom_value
```
