FROM python:3.6.8-slim

WORKDIR /usr/src/app

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=off
ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PIP_DEFAULT_TIMEOUT=100
ENV POETRY_VERSION=0.12.14
ENV PATH=$PATH:/usr/src/app

ARG ENVIRONMENT=prod

COPY . .

RUN set -ex ; \
    if [ $ENVIRONMENT = "local" ] || [ $ENVIRONMENT = "test" ] ; then \
      POETRY_INSTALL_EXTRA="" ; \
    else \
      POETRY_INSTALL_EXTRA="--no-dev" ; \
    fi ; \
    apt-get update ; \
    apt-get install -y --no-install-recommends git gcc ; \
    pip install "poetry==$POETRY_VERSION" ; \
    poetry config settings.virtualenvs.create false ; \
    poetry install --no-interaction --no-ansi $POETRY_INSTALL_EXTRA ; \
    apt-get remove -y --purge git gcc ; \
    apt-get autoremove -y ; \
    apt-get clean -y ; \
    rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["./entrypoint.sh"]
