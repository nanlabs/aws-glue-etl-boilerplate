#!/bin/bash
set -e

echo "🚀 Starting S3 bucket creation and migration file upload..."

# check if awslocal command is available
if ! command -v awslocal &> /dev/null; then
    echo "ℹ️  awslocal command could not be found, skipping..."
    exit 1
fi

# Set default values for environment variables
S3_BUCKET_NAME=${S3_BUCKET_NAME:-local-bucket}
MIGRATIONS_SOURCE_PATH=${MIGRATIONS_SOURCE_PATH:-./migrations}

echo "🔍 S3_BUCKET_NAME: ${S3_BUCKET_NAME}"
echo "🔍 MIGRATIONS_SOURCE_PATH: ${MIGRATIONS_SOURCE_PATH}"

# Create S3 buckets for the data lake following Medallion Architecture
echo "📦 Creating S3 bucket: ${S3_BUCKET_NAME} in ${LOCALSTACK_ENDPOINT_URL}"
if awslocal s3 mb s3://${S3_BUCKET_NAME} 2>/dev/null; then
    echo "✅ Bucket ${S3_BUCKET_NAME} created successfully"
else
    echo "ℹ️  Bucket ${S3_BUCKET_NAME} already exists, continuing..."
fi


# Upload migrations if they exist
if [ -d "${MIGRATIONS_SOURCE_PATH}" ]; then
    echo "📤 Uploading migration files..."
    awslocal s3 cp ${MIGRATIONS_SOURCE_PATH} s3://${S3_BUCKET_NAME}/migrations --recursive
    echo "✅ Migration files uploaded"
else
    echo "ℹ️  No migration files found at ${MIGRATIONS_SOURCE_PATH}, skipping..."
fi

awslocal s3 ls s3://${S3_BUCKET_NAME} --recursive
echo "✅ S3 buckets and folder structure created for Medallion Architecture"
echo "📍 Endpoint: ${LOCALSTACK_ENDPOINT_URL}"
echo "🪣 Bucket: ${S3_BUCKET_NAME}"
