#!/bin/sh

# Lets check if localstack is available. If we can't reach to localstack
# in 60 seconds we error out
counter=0
until $(curl --output /dev/null --silent --head --fail http://localstack:4566); do
    if [ ${counter} -eq 60 ];then
      echo "Timeout: Failed to reach localstack."
      exit 1
    fi
    counter=$(($counter+1))
    printf '.'
    sleep 1
done

# Create a bucket for the sample data
aws --endpoint-url=https://localstack:4566 --no-verify-ssl \
    s3api create-bucket --bucket sampledata

# Make a temp dir for the sample data
mkdir /tmp/s3

# Un-tar the sample data to the temp directory
tar --overwrite -xf  /project/s3/sampledata.tar.xz --directory /tmp/s3/

# Iterate over the sample json files and upload them to the bucket
for file in /tmp/s3/*.json; do
    filename=$(basename ${file})
    aws --endpoint-url=https://localstack:4566 --no-verify-ssl \
        s3api put-object --bucket sampledata --key ${filename} --body ${file}
done
