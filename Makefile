.PHONY: create-env update-env

REPO=$(shell basename $(CURDIR))

setup-os:
	sudo apt-get install git gcc libpq-dev  python-dev  python-pip python3-dev python3-pip python3-venv python3-wheel -y

create-env:
	python3 -m venv .$(REPO);
	. .$(REPO)/bin/activate; \
			pip3 install --upgrade  -r requirements-dev.txt; \
			python setup.py develop;

update-env:
	. .$(REPO)/bin/activate; \
	pip3 install --upgrade -r requirements-dev.txt;

attach-kernel:
	. .$(REPO)/bin/activate; python -m ipykernel install --user --name=$(REPO);

setup-workspace:
	mkdir data

run-daemon: 
	. .$(REPO)/bin/activate; DAGSTER_HOME=$$(pwd)/.dagster_workspace dagster-daemon run

run-dagit:
	. .$(REPO)/bin/activate; DAGSTER_HOME=$$(pwd)/.dagster_workspace dagit