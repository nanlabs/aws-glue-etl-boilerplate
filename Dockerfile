FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

EXPOSE 4040 18080

WORKDIR /home/glue_user/workspace/

COPY Pipfile /home/glue_user/workspace/Pipfile
COPY Pipfile.lock /home/glue_user/workspace/Pipfile.lock

RUN pip3 install -U pipenv && python3 -m pipenv requirements --dev > requirements.txt && pip3 install -r requirements.txt

ENV PYTHONPATH=${PYTHONPATH}:/home/glue_user/workspace/

RUN rm /home/glue_user/spark/jars/mongodb-driver-sync-4.7.2.jar
RUN wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/3.8.2/mongodb-driver-sync-3.8.2.jar -O /home/glue_user/spark/jars/mongodb-driver-sync-3.8.2.jar
