VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: help setup run clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: $(VENV) ## Create venv and install dependencies
$(VENV):
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt

run: setup ## Start the service (reload on changes)
	$(PYTHON) app.py

clean: ## Remove venv and data
	rm -rf $(VENV) data/
