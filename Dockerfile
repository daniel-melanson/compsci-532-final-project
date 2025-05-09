# Python virtual environment
FROM docker.io/python:3.11 AS builder

ENV PROJECT_NAME=analytics-engine
ENV PROJECT_DIR=/usr/src/${PROJECT_NAME}

WORKDIR ${PROJECT_DIR}
COPY Pipfile Pipfile.lock ${PROJECT_DIR}/

# Tell pipenv to install the virtual environment in the project directory
ENV PIPENV_VENV_IN_PROJECT=1
RUN pip install pipenv
RUN pipenv install

# Spark
FROM spark:3.5.5-scala2.12-java17-ubuntu AS spark

ENV PROJECT_NAME=analytics-engine
ENV PROJECT_DIR=/usr/src/${PROJECT_NAME}

# Download PostgreSQL JDBC driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.5.jar -O /opt/spark/jars/postgresql-42.7.5.jar

# Copy python and virtual environment from builder
USER root

COPY --from=builder /usr/local /usr/local
COPY --from=builder /usr/lib /usr/lib
RUN mkdir -p ${PROJECT_DIR}/.venv
COPY --from=builder ${PROJECT_DIR}/.venv/ ${PROJECT_DIR}/.venv/

USER spark

# Verify python environment
RUN ${PROJECT_DIR}/.venv/bin/python -c "import requests; print(requests.__version__)"

WORKDIR ${PROJECT_DIR}

COPY ./src ${PROJECT_DIR}/src

ENV KAGGLEHUB_CACHE=/usr/src/data/.kaggle
CMD ["./.venv/bin/python", "./src/main.py"]