#!/bin/sh

# Load sample data if it exists
if [ -f /etc/localstack/init/ready.d/s3/sampledata.tar.xz ]; then
    echo "📦 Loading sample data..."

    # Create a bucket for the sample data
    awslocal s3 mb s3://sampledata

    # Make a temp dir for the sample data
    mkdir -p /tmp/s3

    # Un-tar the sample data to the temp directory
    tar --overwrite -xf /etc/localstack/init/ready.d/s3/sampledata.tar.xz --directory /tmp/s3/

    # Iterate over the sample json files and upload them to the bucket
    for file in /tmp/s3/*.json; do
        if [ -f "$file" ]; then
            filename=$(basename "${file}")
            awslocal s3api put-object --bucket sampledata --key "${filename}" --body "${file}"
            echo "  📄 Uploaded: ${filename}"
        fi
    done

    echo "✅ Sample data loaded successfully"
else
    echo "ℹ️  No sample data found, skipping..."
fi
