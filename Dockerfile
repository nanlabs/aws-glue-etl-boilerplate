ARG GLUE_TAG=glue_libs_4.0.0_image_01

FROM amazon/aws-glue-libs:${GLUE_TAG}
# GLUE_TAG is specified again because the FROM directive resets ARGs
# (but their default value is retained if set previously)
ARG GLUE_TAG

WORKDIR /home/glue_user/workspace/aws-glue-etl-local

COPY Pipfile Pipfile.lock .

RUN pip3 install -U pipenv && \
    python3 -m pipenv requirements --dev >requirements.txt && \
    pip3 install -r requirements.txt

ENV PYTHONPATH=${PYTHONPATH}:/home/glue_user/workspace/aws-glue-etl-local

RUN rm /home/glue_user/spark/jars/mongodb-driver-sync-4.7.2.jar && \
    wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/3.10.2/mongodb-driver-sync-3.10.2.jar -O /home/glue_user/spark/jars/mongodb-driver-sync-3.10.2.jar

USER root

# TODO: This is a temporary hack to solve the issue with the VSCode permissions.
#       This should be removed once the issue is resolved.
RUN chmod -R 777 /tmp/spark-events && usermod -u 1000 glue_user

# Return to original base image's user
USER glue_user
