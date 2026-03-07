# ---------------------------------------------------------------------------------------
# BUILDER
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

VOLUME ["/root/.config"]

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /install

COPY requirements.txt .

RUN uv pip install --system --upgrade pip && \
    uv pip install --system build && \
    uv pip install --system --prefix=/install -r requirements.txt

COPY pyproject.toml README.md MANIFEST.in ./
COPY src ./src

RUN uv pip install --system --prefix=/install .
# ---------------------------------------------------------------------------------------
# PRODUCTION IMAGE
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS prod

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# COPY PYTHON PACKAGES
COPY --from=builder /install /usr/local

# APACHE BEAM INTEGRATION
COPY --from=apache/beam_python3.12_sdk:2.71.0 /opt/apache/beam /opt/apache/beam
ENTRYPOINT ["/opt/apache/beam/boot"]

WORKDIR /opt/project

# ---------------------------------------------------------------------------------------
# DEVELOPMENT IMAGE
# ---------------------------------------------------------------------------------------
FROM builder AS dev

WORKDIR /opt/project

COPY . .
RUN uv pip install --system -e .[lint,dev,build] && \
    uv pip install --system -r requirements-test.txt

# ---------------------------------------------------------------------------------------
# TEST IMAGE
# ---------------------------------------------------------------------------------------
FROM prod AS test

COPY ./requirements-test.txt .
RUN pip install -r requirements-test.txt

COPY ./tests ./tests

# Suppress all warnings during tests
# To see/address warnings, run tests in your development environment.
ENV PYTHONWARNINGS=ignore