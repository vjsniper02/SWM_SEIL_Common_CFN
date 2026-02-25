FROM python:3.9 AS builder
ARG LAMBDA_TASK_ROOT="/var/task"

RUN pip3 install poetry==1.2.1 invoke==1.7.1 toml==0.10.2 pytest==7.1.3 pytest-cov==3.0.0 pytest-html==3.1.1

WORKDIR $LAMBDA_TASK_ROOT
COPY dist $LAMBDA_TASK_ROOT/

RUN poetry config virtualenvs.create false &&\
    poetry update &&\
    poetry export -f requirements.txt --without-hashes > requirements.txt
RUN if [ -d "lib" ] ; then pytest lib/ --html=/reports/lib.html --self-contained-html --cov lib/ --cov-branch --cov-report term-missing; fi
RUN pytest tests/ --html=/reports/lambda.html --self-contained-html --cov --cov-branch --cov-report term-missing

FROM scratch AS export-stage
COPY --from=builder /reports /

FROM public.ecr.aws/lambda/python:3.9 AS release
COPY --from=builder /$LAMBDA_TASK_ROOT ${LAMBDA_TASK_ROOT}
RUN pip3 install -r requirements.txt -t "${LAMBDA_TASK_ROOT}"

CMD [ "app.handler" ]
