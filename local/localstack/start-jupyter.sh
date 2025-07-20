#!/bin/bash
set +X
# Set up any necessary environment variables
export PYTHONPATH="/home/hadoop/workspace:${PYTHONPATH}"

# Create Jupyter workspace directory if it doesn't exist
mkdir -p /home/hadoop/workspace/jupyter_workspace

# Start Jupyter Lab
cd /home/hadoop/workspace
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' --notebook-dir=/home/hadoop/workspace/jupyter_workspace

# If JupyterLab exits, keep container running unless specifically terminated
if [ $? -ne 0 ]; then
    echo "JupyterLab exited with error code $?"
    echo "Keeping container running. Use Ctrl+C to stop."
    # Keep container running
    tail -f /dev/null
fi