VENV_NAME:=.venv
REQS_PROD_IN:=requirements/prod.in
REQS_PROD_TXT:=requirements.txt
REQS_DEV:=requirements/dev.txt
DOCKER_IMAGE_DEV:=dev

GCP_PROJECT:=world-fishing-827
GCP_DOCKER_VOLUME:=gcp

## help: Prints this list of commands.
## gcp: Authenticates to google cloud and configure the project.
## build: Builds docker image.
## dockershell: Enters to docker container shell.
## reqs: Compiles requirements file with pip-tools.
## upgrade-reqs: Upgrades requirements file based on .in constraints.
## venv: Creates a virtual environment.
## install: Installs all dependencies needed for development.
## test: Runs unit tests.
## testintegration: Runs unit and integration tests.
## testdocker: Runs unit and integration tests inside docker container.


help:
	@echo "\nUsage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/-/'

gcp:
	docker volume create --name ${GCP_DOCKER_VOLUME}
	docker compose run gcloud auth application-default login
	docker compose run gcloud config set project ${GCP_PROJECT}
	docker compose run gcloud auth application-default set-quota-project ${GCP_PROJECT}

build:
	docker compose build

dockershell:
	docker compose run --entrypoint /bin/bash -it ${DOCKER_IMAGE_DEV}

reqs:
	docker compose run --entrypoint /bin/bash -it ${DOCKER_IMAGE_DEV} -c \
		'pip-compile -o ${REQS_PROD_TXT} ${REQS_PROD_IN} -v'

upgrade-reqs:
	docker compose run --entrypoint /bin/bash -it ${DOCKER_IMAGE_DEV} -c \
	'pip-compile -o ${REQS_PROD_TXT} -U ${REQS_PROD_IN} -v'

venv:
	python3 -m venv ${VENV_NAME}

install:
	pip install -r ${REQS_DEV}
	pip install -e .

test:
	pytest

testintegration:
	docker volume create --name ${GCP_DOCKER_VOLUME}
	docker compose up bigquery --detach
	INTEGRATION_TESTS=true pytest
	docker compose down bigquery

testdocker:
	docker compose run test


.PHONY: help gcp build dockershell reqs upgrade-reqs venv install test testintegration testdocker
