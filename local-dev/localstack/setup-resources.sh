#!/bin/sh

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


tar -xf /home/glue_user/workspace/s3/sampledata.tar.xz --directory /home/glue_user/workspace/s3/
awslocal s3api create-bucket --bucket sampledata
for file in /localstack/s3/*.json; do
    awslocal s3api put-object --bucket sampledata --key file --body /localstack/s3/$file
done
