
UV = uv run
UV_EXISTS := $(shell command -v uv 2>/dev/null)

.PHONY: help
help:
	@echo "usage:"
	@echo ""
	@echo "serve-w-reload"
	@echo "   start web API with automatic reload"
	@echo ""
	@echo "dev | install-dev"
	@echo "   setup development environment"
	@echo ""
	@echo "fmt"
	@echo "   run formatter and lint with autofix on all code"
	@echo ""
	@echo "check-types"
	@echo "   run typechecker on code"

.PHONY: ensure-uv
ensure-uv:
ifeq ($(UV_EXISTS),)
	ifeq (${VIRTUAL_ENV},)
		@echo "Set up either uv or a virtual environment to install with this Makefile. See README.md"
		@false
	else
		pip install uv
		export UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV
	endif
else
	@:
endif

.PHONY: dev install-dev
dev: install-dev
install-dev: ensure-uv
	pip install --upgrade pip
	pip install uv
	uv sync --group prod

run:
	mkdir run

.PHONY: serve serve-w-reload

PORT ?= 9000
NUM_WORKERS ?= 1

GUNICORN_BASE = $(UV) gunicorn karps.api:app --control-socket run/gunicorn.ctl --worker-class asgi --workers $(NUM_WORKERS) --bind 127.0.0.1:$(PORT) --pid run/gunicorn.pid

serve: install-dev run
	$(GUNICORN_BASE)

serve-w-reload: install-dev run
	$(GUNICORN_BASE) --reload

.PHONY: reload
reload: run/gunicorn.ctl
	$(UV) gunicornc -s run/gunicorn.ctl -c "reload"

.PHONY: test
test:
	PYTHONPATH=. $(UV) pytest

.PHONY: update-snapshots
update-snapshots:
	PYTHONPATH=. $(UV) pytest --snapshot-update

.PHONY: fmt
fmt:
	$(UV) ruff format .
	$(UV) ruff check . --fix

.PHONY: check-types
check-types:
	$(UV) basedpyright
