FROM docker-repo.frontiir.net/erp-dev/python:3.9-slim-bullseye

ARG WORK_DIR=/frnt_kafka_consumer

WORKDIR ${WORK_DIR}

COPY requirements.txt .

RUN python -m venv /.venv

ENV PATH="/.venv/bin:${PATH}"

RUN pip install -r requirements.txt

COPY . .

ENV LOG_DIR=/var/log/consumer

RUN mkdir -p ${LOG_DIR}

VOLUME [ "${LOG_DIR}" ]

EXPOSE 8080

CMD ["python", "server.py"]
