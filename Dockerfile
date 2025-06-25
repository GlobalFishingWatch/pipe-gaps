# ---------------------------------------------------------------------------------------
# BASE IMAGE
# ---------------------------------------------------------------------------------------
FROM python:3.12.10-slim-bookworm AS base

# Setup a volume for configuration and authtentication.
VOLUME ["/root/.config"]

# Update system and install build tools. Remove unneeded stuff afterwards.
# Upgrade PIP.
# Create working directory.
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ build-essential && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade pip && \
    mkdir -p /opt/project

# Set working directory.
WORKDIR /opt/project

# ---------------------------------------------------------------------------------------
# DEPENDENCIES IMAGE (installed project dependencies)
# ---------------------------------------------------------------------------------------
# We do this first so when we modify code while development, this layer is reused
# from cache and only the layer installing the package executes again.
FROM base AS deps
COPY requirements.txt .
RUN pip install -r requirements.txt

# ---------------------------------------------------------------------------------------
# Apache Beam integration IMAGE
# ---------------------------------------------------------------------------------------
FROM deps AS beam
# Copy files from official SDK image, including script/dependencies.
# IMPORTANT: This version must match the one in requirements.txt
COPY --from=apache/beam_python3.12_sdk:2.65.0 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]

# ---------------------------------------------------------------------------------------
# PRODUCTION IMAGE
# ---------------------------------------------------------------------------------------
FROM beam AS prod

COPY . /opt/project
RUN pip install . && \
    rm -rf /root/.cache/pip && \
    rm -rf /opt/project/*

# ---------------------------------------------------------------------------------------
# DEVELOPMENT IMAGE (editable install and development tools)
# ---------------------------------------------------------------------------------------
FROM beam AS dev

COPY . /opt/project
RUN pip install -e .[lint,test,dev,build]

# ---------------------------------------------------------------------------------------
# TEST IMAGE (This checks that package is properly installed in prod image)
# ---------------------------------------------------------------------------------------
FROM prod AS test

COPY ./tests /opt/project/tests
COPY ./requirements-test.txt /opt/project/

RUN pip install -r requirements-test.txt