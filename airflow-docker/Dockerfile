FROM astrocrpublic.azurecr.io/runtime:3.1-5
USER root
COPY ./project /usr/local/airflow/project
RUN pip install --upgrade pip setuptools wheel && \
    pip install -e /usr/local/airflow/project
USER astro
