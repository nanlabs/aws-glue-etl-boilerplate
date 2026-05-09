#!/usr/bin/env bash

set +X
# Set up any necessary environment variables
export PYTHONPATH="/home/hadoop/workspace:${PYTHONPATH}"

# Create Jupyter workspace directory if it doesn't exist
mkdir -p /home/hadoop/workspace/jupyter_workspace

# Build Jupyter auth flags based on env
JUPYTER_FLAGS=("--ip=0.0.0.0" "--port=8888" "--no-browser" "--notebook-dir=/home/hadoop/workspace/jupyter_workspace")
if [ -n "${JUPYTER_TOKEN:-}" ]; then
  # Use provided token
  JUPYTER_FLAGS+=("--NotebookApp.token=${JUPYTER_TOKEN}")
else
  # Disable auth only if no token provided
  JUPYTER_FLAGS+=("--NotebookApp.token=''" "--NotebookApp.password=''")
fi

# Start Jupyter Lab
cd /home/hadoop/workspace || exit 1
jupyter lab "${JUPYTER_FLAGS[@]}"
exit_code=$?

# If JupyterLab exits, keep container running unless specifically terminated
if [ $exit_code -ne 0 ]; then
    echo "JupyterLab exited with error code ${exit_code}"
    echo "Keeping container running. Use Ctrl+C to stop."
    # Keep container running
    tail -f /dev/null
fi
