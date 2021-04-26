.PHONY: create-env update-env

REPO=$(shell basename $(CURDIR))

setup-os:
	sudo apt-get install git gcc libpq-dev  python-dev  python-pip python3-dev python3-pip python3-venv python3-wheel -y

create-env:
	python3 -m venv .$(REPO);

# Needed when installing on a new machine on Google Cloud
install-grpcio:
	. .$(REPO)/bin/activate; \
			pip3 install --upgrade wheel; \
			pip3 install --upgrade pip; \
			python3 -m pip install --upgrade setuptools; \
			pip3 install --no-cache-dir  --force-reinstall -Iv grpcio;

install-env:
	. .$(REPO)/bin/activate; \
			pip install -U pip \
			pip3 install --upgrade  -r requirements-dev.txt; \
			python setup.py develop;

update-env:
	. .$(REPO)/bin/activate; \
	pip install -U pip \
	pip install --upgrade -r requirements-dev.txt;

attach-kernel:
	. .$(REPO)/bin/activate; python -m ipykernel install --user --name=$(REPO);

setup-workspace:
	mkdir data

run-daemon: 
	. .$(REPO)/bin/activate; DAGSTER_HOME=$$(pwd)/.dagster_workspace dagster-daemon run

run-dagit:
	. .$(REPO)/bin/activate; DAGSTER_HOME=$$(pwd)/.dagster_workspace dagit
