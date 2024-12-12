# ---------------------------------------------------------------------------------------
# BASE
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim AS base

# Configure the working directory
RUN mkdir -p /opt/project
WORKDIR /opt/project

# Setup a volume for configuration and auth data
VOLUME ["/root/.config"]

# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.12_sdk:2.60.0 /opt/apache/beam /opt/apache/beam

# Install application dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]

# ---------------------------------------------------------------------------------------
# PROD
# ---------------------------------------------------------------------------------------
FROM base AS prod

# Install app package
COPY . /opt/project
RUN pip install .

# ---------------------------------------------------------------------------------------
# DEV
# ---------------------------------------------------------------------------------------
FROM base AS dev

COPY ./requirements/dev.txt .
COPY ./requirements/test.txt .

RUN pip install --no-cache-dir -r dev.txt
RUN pip install --no-cache-dir -r test.txt

# Install app package
COPY . /opt/project
RUN pip install -e .