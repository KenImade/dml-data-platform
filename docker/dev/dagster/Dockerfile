ARG PYTHON_VERSION="3.13"

FROM python:${PYTHON_VERSION}-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

ADD https://astral.sh/uv/install.sh /uv-installer.sh

RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

ENV DAGSTER_HOME=/opt/app

RUN mkdir -p ${DAGSTER_HOME}

WORKDIR /opt/app

COPY dml-orchestrator/uv.lock \
    dml-orchestrator/pyproject.toml \
    dml-orchestrator/dagster.yaml ./

COPY dml-orchestrator/src  ./src

RUN uv sync --frozen --no-cache --group docker

ENV PATH="/opt/app/.venv/bin:$PATH"

CMD [ "dagster-webserver", "-h", "0.0.0.0", "-p", "3000" ]
