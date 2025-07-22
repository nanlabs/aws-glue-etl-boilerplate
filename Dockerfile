# Use AWS Glue 5.0 base image (x86_64 or arm64)
FROM --platform=${BUILDPLATFORM:-linux/amd64} public.ecr.aws/glue/aws-glue-libs:5

# Switch to root to install dependencies
USER root

# Prepare working directory
WORKDIR /home/hadoop/workspace

# Install system dependencies for compiling native packages
RUN yum update -y && \
    yum install -y gcc python3-devel wget && \
    yum clean all && \
    rm -rf /var/cache/yum

# Install Python tools and extract requirements
COPY Pipfile Pipfile.lock ./
RUN pip3 install --upgrade pip && \
    pip3 install pipenv && \
    pipenv requirements > requirements.txt

# Install JupyterLab and tools for Glue 5.0
RUN pip3 install jupyterlab==3.6.8 ipykernel==5.5.6 ipywidgets==7.7.2
# Copy startup script and make it executable
COPY local/localstack/start-jupyter.sh /home/hadoop/start-jupyter.sh
RUN chmod +x /home/hadoop/start-jupyter.sh

# Set PYTHONPATH to include workspace
ENV PYTHONPATH="/home/hadoop/workspace:${PYTHONPATH}"

# Install extra Spark drivers (PostgreSQL + MongoDB)
RUN mkdir -p /opt/spark/jars && \
    wget https://jdbc.postgresql.org/download/postgresql-42.2.12.jar -O /opt/spark/jars/postgresql-42.2.12.jar && \
    wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.5.1/mongodb-driver-sync-5.5.1.jar -O /opt/spark/jars/mongodb-driver-sync-5.5.1.jar

# Create spark-events directory (used by Spark UI)
RUN mkdir -p /tmp/spark-events && \
    chown -R hadoop:hadoop /tmp/spark-events && \
    chmod -R 755 /tmp/spark-events

# Update UID of hadoop user if needed
RUN id -u hadoop >/dev/null 2>&1 && \
    [ "$(id -u hadoop)" != "1000" ] && \
    usermod -u 1000 hadoop || \
    echo "User hadoop already has UID 1000 or doesn't exist"



# Switch back to hadoop user
USER hadoop

# Ensure pip-installed binaries are on PATH
ENV PATH="/home/hadoop/.local/bin:$PATH"

# Set default entrypoint to use the absolute path to the script
ENTRYPOINT ["/home/hadoop/start-jupyter.sh"]