#!/bin/sh

# Needed so all localstack components will startup correctly (i'm sure there's a better way to do this)
sleep 10


tar -xf /localstack/s3/sampledata.tar.xz --directory /localstack/s3/
awslocal s3api create-bucket --bucket sampledata
for file in /localstack/s3/*.json; do
    awslocal s3api put-object --bucket sampledata --key file --body /localstack/s3/$file
done
