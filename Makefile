.PHONY: create-env update-env

REPO=$(shell basename $(CURDIR))

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

run-dagit: setup-workspace
	. .$(REPO)/bin/activate; DAGSTER_HOME=$$(pwd)/.dagster_workspace dagit