SHELL=/bin/bash
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

.PHONY: install-env run-daemon run-dagit run-grpc

REPO=$(shell basename $(CURDIR))

install-env:
	($(CONDA_ACTIVATE) smtr; pip3 install -U -r requirements.txt)

run-daemon: 
	($(CONDA_ACTIVATE) smtr; set -a; . $$(pwd)/.env_local; set +a; DAGSTER_HOME=$$(pwd)/.dagster_workspace dagster-daemon run)

run-dagit:
	($(CONDA_ACTIVATE) smtr; set -a; . $$(pwd)/.env_local; set +a; DAGSTER_HOME=$$(pwd)/.dagster_workspace dagit)

run-grpc:
	($(CONDA_ACTIVATE) smtr; set -a; . $$(pwd)/.env_local; set +a; DAGSTER_HOME=$$(pwd)/.dagster_workspace dagster api grpc -h 0.0.0.0 -p 4000 -f repositories/repository.py)