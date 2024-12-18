VENV_NAME:=.venv
REQS_PROD_IN:=requirements/prod.in
REQS_PROD_TXT:=requirements.txt
REQS_DEV:=requirements/dev.txt
DOCKER_COMPOSER_SERVICE_DEV:=dev

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
## profile: Runs profiling.
## ci-test: Runs tests for the CI exporting coverage.xml report.

help:
	@echo "\nUsage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/-/'

docker-volume:
	docker volume create --name ${GCP_DOCKER_VOLUME}

gcp:
	make docker-volume
	docker compose run gcloud auth application-default login
	docker compose run gcloud config set project ${GCP_PROJECT}
	docker compose run gcloud auth application-default set-quota-project ${GCP_PROJECT}

build:
	docker compose build

dockershell:
	docker compose run --rm --entrypoint /bin/bash -it ${DOCKER_COMPOSER_SERVICE_DEV}

reqs:
	docker compose run --rm --entrypoint /bin/bash -it ${DOCKER_COMPOSER_SERVICE_DEV} -c \
		'pip-compile -o ${REQS_PROD_TXT} setup.py --extra beam -v'

upgrade-reqs:
	docker compose run --rm --entrypoint /bin/bash -it ${DOCKER_COMPOSER_SERVICE_DEV} -c \
	'pip-compile -o ${REQS_PROD_TXT} -U setup.py --extra beam -v'

venv:
	python3 -m venv ${VENV_NAME}

install:
	pip install -r ${REQS_DEV}
	pip install -e .[beam]

test:
	pytest

testintegration:
	make docker-volume
	docker compose up bigquery --detach
	INTEGRATION_TESTS=true python -m pytest
	docker compose down bigquery

testdocker: docker-volume
	docker compose run --rm test

ci-test: docker-volume
	docker compose run --rm --entrypoint pytest test --cov-report=xml

profile:
	python -m cProfile -o gaps.prof tests/benchmark.py 10e6


.PHONY: help gcp build dockershell reqs upgrade-reqs venv install test testintegration testdocker profile
