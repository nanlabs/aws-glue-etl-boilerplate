FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

EXPOSE 4040 18080

WORKDIR /home/glue_user/workspace/

COPY Pipfile /home/glue_user/workspace/Pipfile
COPY Pipfile.lock /home/glue_user/workspace/Pipfile.lock

RUN pip3 install -U pipenv && python3 -m pipenv requirements > requirements.txt && pip3 install -r requirements.txt

ENV PYTHONPATH=${PYTHONPATH}:/home/glue_user/workspace/
