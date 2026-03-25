# AWS Glue 5.0 base image (Amazon Linux 2-based)
FROM --platform=${BUILDPLATFORM:-linux/amd64} public.ecr.aws/glue/aws-glue-libs:5

USER root
WORKDIR /home/hadoop/workspace

# --- Spark extra drivers (Iceberg, AWS SDK, Hadoop AWS, Hive Glue Client) ---
RUN mkdir -p /usr/lib/spark/jars && \
    wget -q https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar -P /usr/lib/spark/jars/ && \
    wget -q https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.2/iceberg-aws-bundle-1.9.2.jar -P /usr/lib/spark/jars/ && \
    wget -q https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.788/aws-java-sdk-bundle-1.12.788.jar -P /usr/lib/spark/jars/ && \
    wget -q https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -P /usr/lib/spark/jars/ && \
    wget -q https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar -P /usr/lib/spark/jars/ && \
    wget -q https://repo.maven.apache.org/maven2/software/amazon/awssdk/s3-transfer-manager/2.32.19/s3-transfer-manager-2.32.19.jar -P /usr/lib/spark/jars/ && \
    wget -q https://github.com/jirislav/aws-glue-data-catalog-client-for-apache-hive-metastore/releases/download/spark-3.3.0/spark-3.3.0-jars.tgz && \
    wget -q https://github.com/jirislav/aws-glue-data-catalog-client-for-apache-hive-metastore/releases/download/spark-3.3.0/spark-3.3.0-jars.tgz.sha512 && \
    sha512sum -c spark-3.3.0-jars.tgz.sha512 && \
    tar -xzf spark-3.3.0-jars.tgz -C /usr/lib/spark/jars/ && \
    rm -f spark-3.3.0-jars.tgz spark-3.3.0-jars.tgz.sha512

# --- System dependencies ---
RUN yum -y update --exclude=curl* && \
    yum -y install \
      git wget make unzip zip jq tar gzip xz which \
      python3-pip python3-devel gcc && \
    yum clean all && rm -rf /var/cache/yum

# --- Install AWS CLI v2 ---
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
      AWS_CLI_URL="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"; \
    elif [ "$ARCH" = "aarch64" ]; then \
      AWS_CLI_URL="https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip"; \
    else \
      echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    cd /tmp && \
    curl -fsSL "$AWS_CLI_URL" -o awscliv2.zip && \
    unzip -q awscliv2.zip && \
    ./aws/install --install-dir /usr/local/aws-cli --bin-dir /usr/local/bin && \
    rm -rf aws awscliv2.zip && \
    # Remove AWS CLI v1 if installed via pip (it may come from dependencies)
    python3 -m pip uninstall -y awscli || true && \
    # Verify AWS CLI v2 is active
    aws --version

# --- Install direnv ---
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then DIR_ARCH="linux-amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then DIR_ARCH="linux-arm64"; \
    else echo "Unsupported architecture: $ARCH" && exit 1; fi && \
    wget -q -O /usr/local/bin/direnv "https://github.com/direnv/direnv/releases/download/v2.34.0/direnv.${DIR_ARCH}" && \
    chmod +x /usr/local/bin/direnv

RUN echo '# direnv config' >> /home/hadoop/.bashrc && \
    echo 'eval "$(direnv hook bash)"' >> /home/hadoop/.bashrc && \
    chown hadoop:hadoop /home/hadoop/.bashrc

# Configuration is loaded directly from AWS SSM Parameter Store using AWS CLI in .envrc
# No external tools like Teller are needed - simplifies setup and reduces dependencies

# --- Install fnm (Fast Node Manager) + Node.js LTS ---
# Install fnm at /usr/local/bin and configure the environment for login and non-login shells
RUN curl -fsSL https://fnm.vercel.app/install | bash -s -- --install-dir /usr/local/bin --skip-shell && \
    ln -s /usr/local/bin/fnm /usr/bin/fnm && \
    printf '%s\n' \
      '# fnm global env (login shells)' \
      'eval "$(fnm env --use-on-cd)"' \
      > /etc/profile.d/fnm.sh && \
    chmod +x /etc/profile.d/fnm.sh && \
    # also for interactive non-login shells of the hadoop user
    echo 'eval "$(fnm env --use-on-cd)"' >> /home/hadoop/.bashrc && \
    chown hadoop:hadoop /home/hadoop/.bashrc && \
    # Install Node LTS and specific version v22.11.0, set default
    su - hadoop -c 'eval "$(fnm env)"; fnm install --lts && fnm install 22.11.0 && fnm default 22.11.0' && \
    # Minimal verification (does not fail the build if PATH is missing at this step)
    su - hadoop -c 'eval "$(fnm env)"; node -v && npm -v || true'

# --- Python env ---
COPY Pipfile Pipfile.lock ./
ENV PIP_NO_CACHE_DIR=1
RUN python3 -m pip install --upgrade pip setuptools wheel pipenv && \
    pipenv install --dev --deploy --system -v

# Ensure psutil exists (usado por Jupyter kernel heartbeat)
RUN python3 - <<'PY'
import importlib.util, sys, subprocess
sys.exit(0) if importlib.util.find_spec('psutil') else subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil==6.0.0"])
PY

# --- AWS Glue libs from GitHub ---
RUN python3 -m pip install --no-cache-dir \
    git+https://github.com/awslabs/aws-glue-libs.git@f973095b9f2aa784cbcc87681a00da3127125337

# --- Extra tools for notebooks ---
RUN python3 -m pip install --no-cache-dir \
    jupyterlab==3.6.8 ipykernel==5.5.6 ipywidgets==7.7.2

# --- Install PySpark (moved from Pipfile; keep version aligned with Glue 5 / Spark 3.5) ---
RUN python3 -m pip install --no-cache-dir pyspark==3.5.1

# --- Ensure AWS CLI v2 is active (remove v1 if it was installed as a dependency) ---
RUN python3 -m pip uninstall -y awscli || true && \
    aws --version | grep -q "aws-cli/2" || (echo "ERROR: AWS CLI v1 still active" && exit 1)

# --- Jupyter start script ---
COPY .devcontainer/docker/awsglue/start-jupyter.sh /home/hadoop/start-jupyter.sh
RUN chmod +x /home/hadoop/start-jupyter.sh && \
    chown hadoop:hadoop /home/hadoop/start-jupyter.sh

# --- Spark event logs & Glue home ---
RUN mkdir -p /tmp/spark-events /home/hadoop/awsglue && \
    chown -R hadoop:hadoop /tmp/spark-events /home/hadoop

# --- Align hadoop UID with host ---
RUN if id -u hadoop >/dev/null 2>&1 && [ "$(id -u hadoop)" != "1000" ]; then usermod -u 1000 hadoop; fi

# --- Suppress AWS SDK v1 Deprecation Warning ---
RUN echo 'spark.driver.extraJavaOptions -Daws.java.v1.disableDeprecationAnnouncement=true' >> /usr/lib/spark/conf/spark-defaults.conf && \
    echo 'spark.executor.extraJavaOptions -Daws.java.v1.disableDeprecationAnnouncement=true' >> /usr/lib/spark/conf/spark-defaults.conf

USER hadoop
ENV PATH="/home/hadoop/.local/bin:$PATH"
ENV PYTHONPATH="/home/hadoop/workspace:${PYTHONPATH}"
ENV FNM_RESOLVE_ENGINES=true

ENTRYPOINT ["/home/hadoop/start-jupyter.sh"]
