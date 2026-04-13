#!/bin/bash
set -e

echo "Starting AWS Glue Development Environment..."

# Set up environment variables
export PYTHONPATH="/home/hadoop/workspace:${PYTHONPATH}"
export SPARK_CONF_DIR="/opt/spark/conf"

# Create necessary directories
mkdir -p /home/hadoop/workspace/jupyter_workspace
mkdir -p /tmp/spark-events

# Fix permissions
chmod -R 755 /tmp/spark-events

# In development mode, just keep the container running
if [ "${DEVELOPMENT_MODE}" = "true" ]; then
    echo "Development mode enabled. Container will stay running for VS Code."
    echo "You can start Jupyter manually with: jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' --notebook-dir=/home/hadoop/workspace/jupyter_workspace"

    # Keep container running
    tail -f /dev/null
else
    echo "Starting Jupyter Lab..."
    cd /home/hadoop/workspace
    exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' --notebook-dir=/home/hadoop/workspace/jupyter_workspace
fi
